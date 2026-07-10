//! Integration tests for M2 Task 4: every outbound message (client-bound and
//! upstream-bound) now passes through `reconstruct`. These exercise the
//! rewired `proxy::handle`/`proxy::forward` end-to-end against a real
//! wiremock upstream (and, where relevant, a wiremock step service).

use tokio::net::TcpListener;
use wiremock::matchers::{body_string, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

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

/// A step's `short_circuit`/`abort` response carries `x-sluice-*` headers
/// (the gateway's internal namespace) alongside an ordinary header. With no
/// `expose_headers` allowlist configured, only the ordinary header should
/// reach the client.
#[tokio::test]
async fn short_circuit_strips_internal_header_by_default() {
    let upstream = MockServer::start().await;
    // No mock is mounted on `upstream`: the request should never reach it.

    let step = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"action":"short_circuit","response":{"status":200,"headers":{"x-sluice-secret":"1","content-type":"text/plain"},"body_b64":""}}"#,
        ))
        .mount(&step)
        .await;

    let cfg = format!(
        r#"
        [[route]]
        id = "claude"
        upstream = "{up}"
          [[route.step]]
          name = "sc"
          type = "url"
          url = "{step}/run"
    "#,
        up = upstream.uri(),
        step = step.uri(),
    );
    let (base, _guard) = spawn_gateway(cfg).await;

    let resp = reqwest::Client::new()
        .post(format!("{base}/claude/v1/messages"))
        .body("ping")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    assert_eq!(resp.headers().get("content-type").unwrap(), "text/plain");
    assert!(resp.headers().get("x-sluice-secret").is_none());
}

/// A step returns an out-of-range status (1000). `reconstruct::client_response`
/// clamps this to 502 rather than panicking on `StatusCode::from_u16`. Proves
/// panic-safety end-to-end: the client gets a real response, and the gateway
/// process is still alive to serve a follow-up request afterward.
#[tokio::test]
async fn short_circuit_with_invalid_status_does_not_crash_gateway() {
    let upstream = MockServer::start().await;

    let step = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"action":"short_circuit","response":{"status":1000,"headers":{},"body_b64":""}}"#,
        ))
        .mount(&step)
        .await;

    let cfg = format!(
        r#"
        [[route]]
        id = "claude"
        upstream = "{up}"
          [[route.step]]
          name = "sc"
          type = "url"
          url = "{step}/run"
    "#,
        up = upstream.uri(),
        step = step.uri(),
    );
    let (base, _guard) = spawn_gateway(cfg).await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{base}/claude/v1/messages"))
        .body("ping")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 502);

    // Follow-up request on the same gateway process still gets served.
    let resp2 = client
        .post(format!("{base}/claude/v1/messages"))
        .body("ping again")
        .send()
        .await
        .unwrap();
    assert_eq!(resp2.status(), 502);
}

/// A `continue` step shrinks the body via `set_body`. The upstream request
/// must carry a `content-length` matching the *new* (shrunk) body, proving
/// reqwest recomputes framing from the final body rather than forwarding the
/// client's original (now-stale) `content-length`.
#[tokio::test]
async fn set_body_shrink_recomputes_content_length_to_upstream() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .and(header("content-length", "2"))
        .and(body_string("hi"))
        .respond_with(ResponseTemplate::new(200).set_body_string("ok"))
        .mount(&upstream)
        .await;

    let step = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"action":"continue","ops":[{"op":"set_body","body_b64":"aGk="}]}"#,
        ))
        .mount(&step)
        .await;

    let cfg = format!(
        r#"
        [[route]]
        id = "claude"
        upstream = "{up}"
          [[route.step]]
          name = "shrinker"
          type = "url"
          url = "{step}/run"
    "#,
        up = upstream.uri(),
        step = step.uri(),
    );
    let (base, _guard) = spawn_gateway(cfg).await;

    // Original client body is much longer than the shrunk "hi" (2 bytes);
    // if the stale client content-length were forwarded, the upstream mock
    // (which requires content-length: 2) would not match and wiremock would
    // 404 instead of returning "ok".
    let resp = reqwest::Client::new()
        .post(format!("{base}/claude/v1/messages"))
        .body("this is a much longer original request body")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "ok");
}

/// With `expose_headers` configured, a `short_circuit` response's allowlisted
/// `x-sluice-*` header reaches the client, while a non-allowlisted one in the
/// same namespace does not.
#[tokio::test]
async fn expose_headers_allowlist_relaxes_only_named_header() {
    let upstream = MockServer::start().await;

    let step = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"action":"short_circuit","response":{"status":200,"headers":{"x-sluice-chain-ms":"5","x-sluice-other":"7","content-type":"text/plain"},"body_b64":""}}"#,
        ))
        .mount(&step)
        .await;

    let cfg = format!(
        r#"
        [gateway]
        expose_headers = ["x-sluice-chain-ms"]

        [[route]]
        id = "claude"
        upstream = "{up}"
          [[route.step]]
          name = "sc"
          type = "url"
          url = "{step}/run"
    "#,
        up = upstream.uri(),
        step = step.uri(),
    );
    let (base, _guard) = spawn_gateway(cfg).await;

    let resp = reqwest::Client::new()
        .post(format!("{base}/claude/v1/messages"))
        .body("ping")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    assert_eq!(resp.headers().get("x-sluice-chain-ms").unwrap(), "5");
    assert!(resp.headers().get("x-sluice-other").is_none());
}

/// No steps configured: the request should still pass straight through to
/// the upstream and the upstream's body/status should reach the client
/// unchanged, now that `forward` builds both ends via `reconstruct`.
#[tokio::test]
async fn passthrough_with_no_steps_still_works() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(ResponseTemplate::new(200).set_body_string("pong"))
        .mount(&upstream)
        .await;

    let cfg = format!(
        r#"
        [[route]]
        id = "claude"
        upstream = "{}"
    "#,
        upstream.uri()
    );
    let (base, _guard) = spawn_gateway(cfg).await;

    let resp = reqwest::Client::new()
        .post(format!("{base}/claude/v1/messages"))
        .body("ping")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "pong");
}
