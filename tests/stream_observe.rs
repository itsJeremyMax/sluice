//! Integration tests for M10 Task 3 (`on_stream` observe: bounded tee,
//! normalized deltas).

use std::time::{Duration, Instant};

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

/// A small multi-event Anthropic-shaped SSE body: two `content_block_delta`
/// text events followed by a `message_stop`. Real enough for
/// `AnthropicAdapter::parse_delta` to normalize a non-null `delta` out of
/// it, proving the observe tee's delta parsing end to end (not just that
/// the `delta` JSON key is present-but-null).
fn sse_body() -> String {
    concat!(
        "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hello\"}}\n\n",
        "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\" world\"}}\n\n",
        "data: {\"type\":\"message_stop\"}\n\n",
    )
    .to_string()
}

/// The client must receive the exact, unmodified upstream SSE stream even
/// though an `on_stream` observe step is configured on the route — observe
/// is purely a tee, never a rewrite.
#[tokio::test]
async fn client_receives_full_sse_stream_unchanged() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(sse_body())
                .insert_header("content-type", "text/event-stream"),
        )
        .mount(&upstream)
        .await;

    let observe = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&observe)
        .await;

    let cfg = format!(
        r#"
        [[route]]
        id = "claude"
        upstream = "{up}"
          [route.adapter]
          ingress = "anthropic"
          [[route.step]]
          name = "watcher"
          hook = "on_stream"
          type = "url"
          url = "{observe}/observe"
    "#,
        up = upstream.uri(),
        observe = observe.uri(),
    );
    let (base, _guard) = spawn_gateway(cfg).await;

    let resp = reqwest::Client::new()
        .post(format!("{base}/claude/v1/messages"))
        .body("ping")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), sse_body());
}

/// The observe step must receive at least one chunk envelope whose body
/// contains both `data_b64` and a normalized (non-null) `delta` — proving
/// the tee actually frames SSE events and parses them via the route's
/// ingress adapter, not just forwards raw bytes to the step.
#[tokio::test]
async fn observe_step_receives_chunk_envelope_with_delta() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(sse_body())
                .insert_header("content-type", "text/event-stream"),
        )
        .mount(&upstream)
        .await;

    let observe = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&observe)
        .await;

    let cfg = format!(
        r#"
        [[route]]
        id = "claude"
        upstream = "{up}"
          [route.adapter]
          ingress = "anthropic"
          [[route.step]]
          name = "watcher"
          hook = "on_stream"
          type = "url"
          url = "{observe}/observe"
    "#,
        up = upstream.uri(),
        observe = observe.uri(),
    );
    let (base, _guard) = spawn_gateway(cfg).await;

    let resp = reqwest::Client::new()
        .post(format!("{base}/claude/v1/messages"))
        .body("ping")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    // Drain the client stream fully; the observe tee only enqueues events as
    // they're framed off this same byte stream.
    let _ = resp.bytes().await.unwrap();

    // The background poster is fire-and-forget and off the critical path,
    // so give it a bounded amount of time to actually deliver before
    // asserting — timing-robust via polling rather than a fixed sleep.
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut bodies: Vec<String> = Vec::new();
    while Instant::now() < deadline {
        let reqs = observe.received_requests().await.unwrap();
        if !reqs.is_empty() {
            bodies = reqs
                .iter()
                .map(|r| String::from_utf8_lossy(&r.body).to_string())
                .collect();
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    assert!(
        !bodies.is_empty(),
        "observe step must receive at least one chunk envelope"
    );
    assert!(
        bodies.iter().any(|b| b.contains("\"data_b64\"")),
        "chunk envelope body must contain data_b64: {bodies:?}"
    );
    assert!(
        bodies.iter().any(|b| b.contains("\"delta\"")
            && b.contains("\"text\"")
            && !b.contains("\"delta\":null")),
        "at least one chunk envelope must carry a normalized non-null delta: {bodies:?}"
    );
}

/// A SLOW observe step must NEVER delay the client: the client must finish
/// reading the whole stream promptly even though the observe step takes
/// much longer to respond than the whole test is willing to wait.
#[tokio::test]
async fn slow_observe_step_does_not_delay_client_stream() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(sse_body())
                .insert_header("content-type", "text/event-stream"),
        )
        .mount(&upstream)
        .await;

    let observe = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(10)))
        .mount(&observe)
        .await;

    let cfg = format!(
        r#"
        [[route]]
        id = "claude"
        upstream = "{up}"
          [route.adapter]
          ingress = "anthropic"
          [[route.step]]
          name = "watcher"
          hook = "on_stream"
          type = "url"
          url = "{observe}/observe"
    "#,
        up = upstream.uri(),
        observe = observe.uri(),
    );
    let (base, _guard) = spawn_gateway(cfg).await;

    let start = Instant::now();
    let resp = reqwest::Client::new()
        .post(format!("{base}/claude/v1/messages"))
        .body("ping")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    let elapsed = start.elapsed();

    assert_eq!(body, sse_body());
    assert!(
        elapsed < Duration::from_secs(2),
        "client stream must not be gated on a slow observe step; took {elapsed:?}"
    );
}
