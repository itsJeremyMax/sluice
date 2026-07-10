//! Integration tests for M5 Task 2 (request body ceiling / upstream timeout),
//! M5 Task 3 (global `max_inflight` concurrency ceiling -> 503), and M16
//! Task 1 (configurable `gateway.max_event_bytes` SSE per-event ceiling).

use base64::Engine;
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

/// A body larger than `max_body_bytes` must be rejected with 413 before it
/// ever reaches the upstream — the upstream mock is mounted (so a hit would
/// succeed rather than 404) but must receive zero requests.
#[tokio::test]
async fn body_too_large_returns_413_and_never_reaches_upstream() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string("upstream-reached"))
        .mount(&upstream)
        .await;

    let cfg = format!(
        r#"
        [gateway]
        max_body_bytes = 16

        [[route]]
        id = "claude"
        upstream = "{}"
    "#,
        upstream.uri()
    );
    let (base, _guard) = spawn_gateway(cfg).await;

    let big_body = "x".repeat(100);
    let resp = reqwest::Client::new()
        .post(format!("{base}/claude/v1/messages"))
        .body(big_body)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 413);
    assert!(
        upstream.received_requests().await.unwrap().is_empty(),
        "oversize body must be rejected before forwarding to upstream"
    );
}

/// An upstream that takes longer than `upstream_timeout_ms` to respond must
/// surface as a 504 to the client, not hang indefinitely or 502.
#[tokio::test]
async fn upstream_timeout_returns_504() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_delay(std::time::Duration::from_millis(500)))
        .mount(&upstream)
        .await;

    let cfg = format!(
        r#"
        [gateway]
        upstream_timeout_ms = 50

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

    assert_eq!(resp.status(), 504);
}

/// With `max_inflight = 1`, a second request sent while the first is still
/// held by a slow upstream must be shed with 503 — not queued behind the
/// first. The upstream's 300ms delay guarantees the two requests overlap: we
/// spawn request A, sleep briefly to let it claim the single permit, then
/// send B inline and assert it gets 503 before A completes.
#[tokio::test]
async fn over_max_inflight_returns_503() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_delay(std::time::Duration::from_millis(300)))
        .mount(&upstream)
        .await;

    let cfg = format!(
        r#"
        [gateway]
        max_inflight = 1

        [[route]]
        id = "claude"
        upstream = "{}"
    "#,
        upstream.uri()
    );
    let (base, _guard) = spawn_gateway(cfg).await;

    let client = reqwest::Client::new();
    let base_a = base.clone();
    let client_a = client.clone();
    let request_a = tokio::spawn(async move {
        client_a
            .post(format!("{base_a}/claude/v1/messages"))
            .body("a")
            .send()
            .await
            .unwrap()
    });

    // Give request A time to be accepted and claim the single permit before
    // we fire request B.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let resp_b = client
        .post(format!("{base}/claude/v1/messages"))
        .body("b")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp_b.status(),
        503,
        "request sent while the single permit is held must be shed"
    );

    let resp_a = request_a.await.unwrap();
    assert_eq!(
        resp_a.status(),
        200,
        "the request that held the permit must complete normally"
    );
}

/// The concurrency permit must be held for the *entire* request lifetime,
/// including while the response body is still streaming to the client — not
/// just until upstream response headers arrive. We prove this with a raw
/// upstream that writes its response headers immediately (so request A's
/// `forward()` call returns almost instantly) and then trickles the body as
/// three separate slow chunked-encoding writes. A second request sent while
/// A is only streaming its body (well after A's headers were received) must
/// still be shed with 503 — if the permit were released at header time (the
/// bug this fix addresses), request B would incorrectly succeed here.
#[tokio::test]
async fn over_max_inflight_during_body_streaming_returns_503() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (mut socket, _) = match listener.accept().await {
                Ok(pair) => pair,
                Err(_) => break,
            };
            tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                // We don't care about the request itself, just draining it
                // enough that the peer isn't blocked writing it.
                let _ = socket.read(&mut buf).await;

                let head = b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n";
                if socket.write_all(head).await.is_err() {
                    return;
                }
                for _ in 0..3 {
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    if socket.write_all(b"5\r\nhello\r\n").await.is_err() {
                        return;
                    }
                }
                let _ = socket.write_all(b"0\r\n\r\n").await;
            });
        }
    });

    let cfg = format!(
        r#"
        [gateway]
        max_inflight = 1

        [[route]]
        id = "claude"
        upstream = "http://{upstream_addr}"
    "#
    );
    let (base, _guard) = spawn_gateway(cfg).await;

    let client = reqwest::Client::new();
    let base_a = base.clone();
    let client_a = client.clone();
    let request_a = tokio::spawn(async move {
        client_a
            .post(format!("{base_a}/claude/v1/messages"))
            .body("a")
            .send()
            .await
            .unwrap()
    });

    // Upstream headers arrive almost immediately; this sleep lands us
    // squarely in the middle of the 300ms of slow body chunks that follow,
    // proving the permit outlives header arrival.
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    let resp_b = client
        .post(format!("{base}/claude/v1/messages"))
        .body("b")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp_b.status(),
        503,
        "request sent while the first request's response body is still streaming must be shed"
    );

    let resp_a = request_a.await.unwrap();
    assert_eq!(resp_a.status(), 200);
    let body_a = resp_a.bytes().await.unwrap();
    assert_eq!(body_a, bytes::Bytes::from_static(b"hellohellohello"));
}

fn b64(s: &str) -> String {
    base64::engine::general_purpose::STANDARD.encode(s.as_bytes())
}

/// M16 Task 1: `gateway.max_event_bytes`, when configured smaller than an
/// oversized SSE event's buffered (never-`"\n\n"`-terminated) tail, must
/// make `SseFramer::push` return `SseError::EventTooLarge` for that tail —
/// which the `on_stream` mutate pipeline (`proxy::mutate_stream`) treats as
/// "nothing salvageable to forward" (see its doc comment) and silently
/// drops, while an earlier, properly terminated event in the SAME upstream
/// chunk still reaches the client unaffected. This proves the configured
/// ceiling is actually threaded into the framer construction, not just
/// parsed and ignored.
#[tokio::test]
async fn oversized_event_dropped_from_mutate_stream_with_small_max_event_bytes() {
    let upstream = MockServer::start().await;
    // "keep" is a complete, terminated event (survives regardless of the
    // cap - the size check only ever applies to the un-terminated leftover
    // tail, see `sse::SseFramer::push`). The oversized tail deliberately has
    // no closing "\n\n": it's still "arriving" when the cap is exceeded, the
    // same shape a real streaming provider would produce.
    let big_tail = "z".repeat(200);
    let body = format!("data: keep\n\ndata: {big_tail}");
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(body)
                .insert_header("content-type", "text/event-stream"),
        )
        .mount(&upstream)
        .await;

    let mutate = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string(format!(
            r#"{{"action":"emit","chunk":{{"data_b64":"{}"}}}}"#,
            b64("keep")
        )))
        .mount(&mutate)
        .await;

    let cfg = format!(
        r#"
        [gateway]
        max_event_bytes = 32

        [[route]]
        id = "claude"
        upstream = "{up}"
          [[route.step]]
          name = "mutator"
          hook = "on_stream"
          type = "url"
          chunk_mode = "mutate"
          url = "{mutate}/mutate"
    "#,
        up = upstream.uri(),
        mutate = mutate.uri(),
    );
    let (base, _guard) = spawn_gateway(cfg).await;

    let resp = reqwest::Client::new()
        .post(format!("{base}/claude/v1/messages"))
        .body("ping")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert_eq!(
        body, "data: keep\n\n",
        "the oversized unterminated tail must be silently dropped, never forwarded: {body:?}"
    );
}

/// `max_event_bytes = 0` must be rejected at config load, before the gateway
/// ever binds — a zero ceiling would make `SseFramer::push` return
/// `EventTooLarge` for every single byte of every event.
#[tokio::test]
async fn zero_max_event_bytes_rejected_at_config_load() {
    let cfg_toml = r#"
        [gateway]
        max_event_bytes = 0

        [[route]]
        id = "claude"
        upstream = "http://127.0.0.1:1"
    "#;
    let err = sluice::config::load::load_str(cfg_toml).unwrap_err();
    assert!(
        err.to_string().contains("max_event_bytes must be >= 1"),
        "{err}"
    );
}

/// M16 Task 2: on the buffered response path (a route with an `on_response`
/// step), a response body larger than `max_response_bytes` under the default
/// `oversize = "reject"` policy must be refused with 413 — the client sees the
/// oversize error, NOT the upstream body.
#[tokio::test]
async fn oversize_response_with_reject_returns_413() {
    let upstream = MockServer::start().await;
    let big = "x".repeat(100);
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string(big.clone()))
        .mount(&upstream)
        .await;

    // An on_response step forces the buffered response path (where the
    // response cap applies). It returns a no-op `continue`, but the overflow
    // happens during buffering, before the step is ever consulted.
    let tagger = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"action":"continue","ops":[{"op":"set_header","name":"x-tagged","value":"yes"}]}"#,
        ))
        .mount(&tagger)
        .await;

    let cfg = format!(
        r#"
        [gateway]
        max_response_bytes = 16
        oversize = "reject"

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
        .post(format!("{base}/claude/v1/messages"))
        .body("ping")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 413);
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("max_response_bytes"),
        "reject policy must surface the oversize error, got: {body:?}"
    );
    assert_ne!(
        body, big,
        "the upstream body must NOT be forwarded on reject"
    );
}

/// M16 Task 2: the SAME oversize response under `oversize = "stream_through"`
/// must be streamed to the client in FULL (unbuffered), preserving the
/// upstream status. The `on_response` step cannot run on an unbuffered body,
/// so it is skipped — proven by the absence of the header it would have added.
#[tokio::test]
async fn oversize_response_with_stream_through_forwards_full_body() {
    let upstream = MockServer::start().await;
    let big = "x".repeat(100);
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string(big.clone()))
        .mount(&upstream)
        .await;

    let tagger = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"action":"continue","ops":[{"op":"set_header","name":"x-tagged","value":"yes"}]}"#,
        ))
        .mount(&tagger)
        .await;

    let cfg = format!(
        r#"
        [gateway]
        max_response_bytes = 16
        oversize = "stream_through"

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
        .post(format!("{base}/claude/v1/messages"))
        .body("ping")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200, "upstream status must be preserved");
    assert!(
        resp.headers().get("x-tagged").is_none(),
        "on_response steps must be skipped for a streamed-through oversize body"
    );
    let body = resp.text().await.unwrap();
    assert_eq!(
        body,
        big,
        "stream_through must forward the full oversize body ({} bytes), got {} bytes",
        big.len(),
        body.len()
    );
}

/// H1 (audit): a TRANSLATE route must NEVER honor `oversize = "stream_through"`
/// on the response. Streaming the raw `to`-dialect (upstream) bytes to a
/// `from`-dialect client would leak untranslated bytes as HTTP 200 (dialect
/// corruption / single-reconstruction violation). Even with the gateway set to
/// `stream_through`, a translate route whose upstream response overflows
/// `max_response_bytes` must return 413, not a 200 with raw upstream bytes.
#[tokio::test]
async fn translate_route_oversize_response_rejects_even_under_stream_through() {
    let upstream = MockServer::start().await;
    // A large, NON-event-stream body: forces the buffered translate egress path
    // (the streaming translate branch only triggers on `text/event-stream`),
    // which is where the overflow decision lives. The bytes are raw `openai`
    // dialect the client must never see verbatim.
    let big = format!(
        r#"{{"id":"chatcmpl","object":"chat.completion","raw":"{}"}}"#,
        "x".repeat(200)
    );
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string(big.clone()))
        .mount(&upstream)
        .await;

    let cfg = format!(
        r#"
        [gateway]
        max_response_bytes = 16
        oversize = "stream_through"

        [[route]]
        id = "x"
        upstream = "{up}"
          [route.translate]
          from = "anthropic"
          to = "openai"
    "#,
        up = upstream.uri(),
    );
    let (base, _guard) = spawn_gateway(cfg).await;

    let resp = reqwest::Client::new()
        .post(format!("{base}/x/v1/messages"))
        .header("content-type", "application/json")
        .body(
            r#"{"model":"claude-3-5-sonnet-20240620","max_tokens":16,"messages":[{"role":"user","content":"hi"}]}"#,
        )
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        413,
        "a translate route must reject an oversize response, never stream raw upstream bytes"
    );
    let body = resp.text().await.unwrap();
    assert!(
        !body.contains(&"x".repeat(200)),
        "the raw untranslated upstream body must not leak to the client: {body:?}"
    );
    assert!(
        body.contains("max_response_bytes"),
        "reject must surface the oversize error, got: {body:?}"
    );
}

/// M16 Task 2 regression: the `oversize` policy governs RESPONSE bodies only.
/// A request body larger than `max_body_bytes` must still be rejected with 413
/// even when `oversize = "stream_through"`, and must never reach upstream.
#[tokio::test]
async fn request_body_oversize_still_413_under_stream_through() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string("upstream-reached"))
        .mount(&upstream)
        .await;

    let cfg = format!(
        r#"
        [gateway]
        max_body_bytes = 16
        oversize = "stream_through"

        [[route]]
        id = "claude"
        upstream = "{}"
    "#,
        upstream.uri()
    );
    let (base, _guard) = spawn_gateway(cfg).await;

    let resp = reqwest::Client::new()
        .post(format!("{base}/claude/v1/messages"))
        .body("x".repeat(100))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 413);
    assert!(
        upstream.received_requests().await.unwrap().is_empty(),
        "oversize request body must be rejected before forwarding, regardless of oversize policy"
    );
}
