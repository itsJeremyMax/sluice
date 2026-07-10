use tokio::net::TcpListener;
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, ResponseTemplate};

// Bring the library modules into the integration test by path.
// (These are compiled as part of the `sluice` bin crate; exposed via the lib target.)
use sluice::admin::Ready;
use sluice::config::load::load_str;
use sluice::observability::init_metrics;
use sluice::server::serve_on;

async fn spawn_gateway(config_toml: String) -> (String, tokio::sync::oneshot::Sender<()>) {
    let cfg = load_str(&config_toml).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let ready = Ready::new();
    let metrics = init_metrics();
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        let _ = serve_on(listener, cfg, ready, metrics, async {
            let _ = rx.await;
        })
        .await;
    });
    (format!("http://{addr}"), tx)
}

/// A `short_circuit` from an early step returns the given response immediately:
/// a later step in the chain must never be called, and the upstream must
/// never be called either.
#[tokio::test]
async fn short_circuit_skips_later_step_and_upstream() {
    let upstream = MockServer::start().await;
    // Mounted (so a hit would succeed rather than 404) but must receive zero
    // requests — checked explicitly below via `received_requests`.
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string("upstream-reached"))
        .mount(&upstream)
        .await;

    let gate = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"action":"short_circuit","response":{"status":200,"headers":{},"body_b64":"Y2FjaGVk"}}"#,
        ))
        .mount(&gate)
        .await;

    let later = MockServer::start().await;
    // Mounted so a hit would succeed rather than 404, but must never be called.
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string("later-ran"))
        .mount(&later)
        .await;

    let cfg = format!(
        r#"
        [[route]]
        id = "claude"
        upstream = "{up}"
          [[route.step]]
          name = "gate"
          type = "url"
          url = "{gate}/run"
          [[route.step]]
          name = "later"
          type = "url"
          url = "{later}/run"
    "#,
        up = upstream.uri(),
        gate = gate.uri(),
        later = later.uri(),
    );
    let (base, _guard) = spawn_gateway(cfg).await;

    let resp = reqwest::Client::new()
        .post(format!("{base}/claude/v1/messages"))
        .body("ping")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "cached");

    assert!(
        later.received_requests().await.unwrap().is_empty(),
        "later step must not be called after a short_circuit"
    );
    assert!(
        upstream.received_requests().await.unwrap().is_empty(),
        "upstream must not be called after a short_circuit"
    );
}

/// An `abort` from an early step behaves the same as `short_circuit` for skip
/// purposes: it returns the error response immediately, skipping both later
/// steps and the upstream call.
#[tokio::test]
async fn abort_skips_later_step_and_upstream() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string("upstream-reached"))
        .mount(&upstream)
        .await;

    let gate = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"action":"abort","response":{"status":403,"headers":{},"body_b64":"YmxvY2tlZA=="}}"#,
        ))
        .mount(&gate)
        .await;

    let later = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string("later-ran"))
        .mount(&later)
        .await;

    let cfg = format!(
        r#"
        [[route]]
        id = "claude"
        upstream = "{up}"
          [[route.step]]
          name = "gate"
          type = "url"
          url = "{gate}/run"
          [[route.step]]
          name = "later"
          type = "url"
          url = "{later}/run"
    "#,
        up = upstream.uri(),
        gate = gate.uri(),
        later = later.uri(),
    );
    let (base, _guard) = spawn_gateway(cfg).await;

    let resp = reqwest::Client::new()
        .post(format!("{base}/claude/v1/messages"))
        .body("ping")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403);
    assert_eq!(resp.text().await.unwrap(), "blocked");

    assert!(
        later.received_requests().await.unwrap().is_empty(),
        "later step must not be called after an abort"
    );
    assert!(
        upstream.received_requests().await.unwrap().is_empty(),
        "upstream must not be called after an abort"
    );
}

/// The `short_circuit` response is not returned to the client verbatim: it
/// flows through the single reconstruction function like any other outbound
/// message, so gateway-internal `x-sluice-*` headers set by the step are
/// stripped before the client sees them.
#[tokio::test]
async fn short_circuit_response_strips_internal_headers() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string("upstream-reached"))
        .mount(&upstream)
        .await;

    let gate = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"action":"short_circuit","response":{"status":200,"headers":{"x-sluice-internal":"secret"},"body_b64":"Y2FjaGVk"}}"#,
        ))
        .mount(&gate)
        .await;

    let cfg = format!(
        r#"
        [[route]]
        id = "claude"
        upstream = "{up}"
          [[route.step]]
          name = "gate"
          type = "url"
          url = "{gate}/run"
    "#,
        up = upstream.uri(),
        gate = gate.uri(),
    );
    let (base, _guard) = spawn_gateway(cfg).await;

    let resp = reqwest::Client::new()
        .post(format!("{base}/claude/v1/messages"))
        .body("ping")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert!(
        resp.headers().get("x-sluice-internal").is_none(),
        "x-sluice-* headers on a short_circuit response must be stripped by reconstruction"
    );
    assert_eq!(resp.text().await.unwrap(), "cached");

    assert!(upstream.received_requests().await.unwrap().is_empty());
}
