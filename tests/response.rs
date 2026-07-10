//! Integration tests for M10 Task 1 (`on_response` steps: buffer, run,
//! mutate/abort).

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

/// An `on_response` step returning `continue` with a `set_header` op mutates
/// the buffered upstream response before it reaches the client: the client
/// must see the header the step added, not just the upstream's own headers.
#[tokio::test]
async fn on_response_continue_set_header_reaches_client() {
    let upstream = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_string("upstream body"))
        .mount(&upstream)
        .await;

    let tagger = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"action":"continue","ops":[{"op":"set_header","name":"x-response-tag","value":"seen"}]}"#,
        ))
        .mount(&tagger)
        .await;

    let cfg = format!(
        r#"
        [[route]]
        id = "claude"
        upstream = "{up}"
          [[route.step]]
          name = "tagger"
          hook = "on_response"
          type = "url"
          url = "{tagger}/run"
    "#,
        up = upstream.uri(),
        tagger = tagger.uri(),
    );
    let (base, _guard) = spawn_gateway(cfg).await;

    let resp = reqwest::Client::new()
        .get(format!("{base}/claude/v1/messages"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers()
            .get("x-response-tag")
            .map(|v| v.to_str().unwrap()),
        Some("seen"),
        "the on_response step's set_header op must reach the client"
    );
    assert_eq!(resp.text().await.unwrap(), "upstream body");
}

/// An `on_response` step returning `abort` short-circuits the (already
/// buffered) upstream response: the client sees the abort's response, not
/// the upstream's.
#[tokio::test]
async fn on_response_abort_returns_451_to_client() {
    let upstream = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_string("upstream body"))
        .mount(&upstream)
        .await;

    let gate = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"action":"abort","response":{"status":451,"headers":{},"body_b64":"dW5hdmFpbGFibGU="}}"#,
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
          hook = "on_response"
          type = "url"
          url = "{gate}/run"
    "#,
        up = upstream.uri(),
        gate = gate.uri(),
    );
    let (base, _guard) = spawn_gateway(cfg).await;

    let resp = reqwest::Client::new()
        .get(format!("{base}/claude/v1/messages"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 451);
    assert_eq!(resp.text().await.unwrap(), "unavailable");
}

/// An `on_response` step that writes `set_context` (e.g. a cost tag) mutates
/// only the internal `context` map, not anything client-visible — the
/// request still succeeds normally.
#[tokio::test]
async fn on_response_set_context_still_returns_200() {
    let upstream = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_string("upstream body"))
        .mount(&upstream)
        .await;

    let cost_tagger = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"action":"continue","ops":[{"op":"set_context","value":{"cost_usd":0.002}}]}"#,
        ))
        .mount(&cost_tagger)
        .await;

    let cfg = format!(
        r#"
        [[route]]
        id = "claude"
        upstream = "{up}"
          [[route.step]]
          name = "cost"
          hook = "on_response"
          type = "url"
          url = "{cost_tagger}/run"
    "#,
        up = upstream.uri(),
        cost_tagger = cost_tagger.uri(),
    );
    let (base, _guard) = spawn_gateway(cfg).await;

    let resp = reqwest::Client::new()
        .get(format!("{base}/claude/v1/messages"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "upstream body");
}

/// Routes with no `on_response` steps must keep the existing streaming
/// pass-through unchanged (no buffering, no behavior change from pre-M10).
#[tokio::test]
async fn route_with_no_on_response_steps_streams_through_unchanged() {
    let upstream = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_string("plain passthrough"))
        .mount(&upstream)
        .await;

    let cfg = format!(
        r#"
        [[route]]
        id = "claude"
        upstream = "{up}"
    "#,
        up = upstream.uri(),
    );
    let (base, _guard) = spawn_gateway(cfg).await;

    let resp = reqwest::Client::new()
        .get(format!("{base}/claude/v1/messages"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "plain passthrough");
}
