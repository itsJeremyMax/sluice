//! Integration tests for M11 Task 3 (`on_stream` `mutate` steps: per-event
//! `emit`/`drop`/`abort` gating the client stream, cross-event context).

use base64::Engine;
use tokio::net::TcpListener;
use wiremock::matchers::{body_string_contains, method};
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

fn b64(s: &str) -> String {
    base64::engine::general_purpose::STANDARD.encode(s.as_bytes())
}

/// A mutate step that returns `emit` with a REWRITTEN chunk must have its
/// rewrite reach the client — not the original upstream event.
#[tokio::test]
async fn emit_rewritten_chunk_reaches_client() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string("data: original\n\n")
                .insert_header("content-type", "text/event-stream"),
        )
        .mount(&upstream)
        .await;

    let mutate = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string(format!(
            r#"{{"action":"emit","chunk":{{"data_b64":"{}"}}}}"#,
            b64("REWRITTEN")
        )))
        .mount(&mutate)
        .await;

    let cfg = format!(
        r#"
        [[route]]
        id = "claude"
        upstream = "{up}"
          [[route.step]]
          name = "rewriter"
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
    assert_eq!(resp.text().await.unwrap(), "data: REWRITTEN\n\n");
}

/// A mutate step that returns `drop` for one specific event must omit that
/// event from the client stream, while a `keep` event elsewhere in the same
/// stream still reaches the client.
#[tokio::test]
async fn drop_omits_event_from_client_stream() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string("data: keep\n\ndata: gone\n\n")
                .insert_header("content-type", "text/event-stream"),
        )
        .mount(&upstream)
        .await;

    let mutate = MockServer::start().await;
    // Only the "gone" event is dropped; "keep" is passed through unchanged
    // via an explicit `emit` (a mutate route never gets raw-forwarded bytes
    // for free — every framed event goes through the step).
    Mock::given(method("POST"))
        .and(body_string_contains(b64("gone")))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"action":"drop"}"#))
        .mount(&mutate)
        .await;
    Mock::given(method("POST"))
        .and(body_string_contains(b64("keep")))
        .respond_with(ResponseTemplate::new(200).set_body_string(format!(
            r#"{{"action":"emit","chunk":{{"data_b64":"{}"}}}}"#,
            b64("keep")
        )))
        .mount(&mutate)
        .await;

    let cfg = format!(
        r#"
        [[route]]
        id = "claude"
        upstream = "{up}"
          [[route.step]]
          name = "filter"
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
        "dropped event must not appear: {body:?}"
    );
}

/// A mutate step that returns `abort` must terminate the client stream
/// outright: an event delivered before the abort still reaches the client,
/// but nothing after the aborting event does.
#[tokio::test]
async fn abort_terminates_client_stream() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string("data: keep\n\ndata: stop-here\n\n")
                .insert_header("content-type", "text/event-stream"),
        )
        .mount(&upstream)
        .await;

    let mutate = MockServer::start().await;
    Mock::given(method("POST"))
        .and(body_string_contains(b64("stop-here")))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(r#"{"action":"abort","response":{"status":200}}"#),
        )
        .mount(&mutate)
        .await;
    Mock::given(method("POST"))
        .and(body_string_contains(b64("keep")))
        .respond_with(ResponseTemplate::new(200).set_body_string(format!(
            r#"{{"action":"emit","chunk":{{"data_b64":"{}"}}}}"#,
            b64("keep")
        )))
        .mount(&mutate)
        .await;

    let cfg = format!(
        r#"
        [[route]]
        id = "claude"
        upstream = "{up}"
          [[route.step]]
          name = "guard"
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
        "stream must terminate at the abort, forwarding nothing past it: {body:?}"
    );
}

/// A mutate step that errors (transport failure) with `on_error =
/// fail_open` must forward the ORIGINAL event unchanged — not drop it, and
/// not surface the failure to the client.
#[tokio::test]
async fn step_error_with_fail_open_forwards_original_event() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string("data: original-text\n\n")
                .insert_header("content-type", "text/event-stream"),
        )
        .mount(&upstream)
        .await;

    let mutate = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&mutate)
        .await;

    let cfg = format!(
        r#"
        [[route]]
        id = "claude"
        upstream = "{up}"
          [[route.step]]
          name = "flaky"
          hook = "on_stream"
          type = "url"
          chunk_mode = "mutate"
          on_error = "fail_open"
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
    assert_eq!(resp.text().await.unwrap(), "data: original-text\n\n");

    // Prove this was actually fail_open recovery from a failed mutate call —
    // not, say, a routing bug that skipped the mutate chain entirely and
    // happened to produce the same bytes via plain passthrough.
    let mutate_requests = mutate.received_requests().await.unwrap();
    assert!(
        !mutate_requests.is_empty(),
        "the mutate step must actually have been called (and failed) for fail_open to be exercised"
    );
}

/// A mutate step that errors (transport failure) with `on_error =
/// fail_closed` (the default, and the only legal setting for an
/// `is_guardrail` step — see `config::load::validate`) must ABORT the client
/// stream outright: the client must never see the event that triggered the
/// failure, and the connection must end without it — never fail back to the
/// unguarded original event the way `fail_open` does.
#[tokio::test]
async fn step_error_with_fail_closed_aborts_client_stream() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string("data: original-text\n\n")
                .insert_header("content-type", "text/event-stream"),
        )
        .mount(&upstream)
        .await;

    let mutate = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&mutate)
        .await;

    let cfg = format!(
        r#"
        [[route]]
        id = "claude"
        upstream = "{up}"
          [[route.step]]
          name = "flaky"
          hook = "on_stream"
          type = "url"
          chunk_mode = "mutate"
          on_error = "fail_closed"
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
        body, "",
        "fail_closed must abort the stream, forwarding none of the failing event: {body:?}"
    );

    // Prove the mutate step was actually invoked (and failed) rather than,
    // say, a routing bug that produced an empty body for an unrelated
    // reason.
    let mutate_requests = mutate.received_requests().await.unwrap();
    assert!(
        !mutate_requests.is_empty(),
        "the mutate step must actually have been called (and failed) for fail_closed to be exercised"
    );
}

/// Cross-event context: a mutate step that sets context via `set_context` ops
/// on one event must have that value visible in the envelope for the NEXT
/// event on the same stream — proving `context` is threaded across events
/// (not just across steps within one event). Verified by inspecting the raw
/// envelopes the mutate mock actually received for each event.
#[tokio::test]
async fn mutate_context_set_on_one_event_is_visible_on_the_next() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string("data: first\n\ndata: second\n\n")
                .insert_header("content-type", "text/event-stream"),
        )
        .mount(&upstream)
        .await;

    let mutate = MockServer::start().await;
    // Event 1 ("first"): emit it unchanged, and set context under this
    // step's own namespace.
    Mock::given(method("POST"))
        .and(body_string_contains(b64("first")))
        .respond_with(ResponseTemplate::new(200).set_body_string(format!(
            r#"{{"action":"emit","chunk":{{"data_b64":"{}"}},"ops":[{{"op":"set_context","value":{{"seen":"event-1"}}}}]}}"#,
            b64("first")
        )))
        .mount(&mutate)
        .await;
    // Event 2 ("second"): just emit it unchanged; we only care about what
    // envelope this call itself received.
    Mock::given(method("POST"))
        .and(body_string_contains(b64("second")))
        .respond_with(ResponseTemplate::new(200).set_body_string(format!(
            r#"{{"action":"emit","chunk":{{"data_b64":"{}"}}}}"#,
            b64("second")
        )))
        .mount(&mutate)
        .await;

    let cfg = format!(
        r#"
        [[route]]
        id = "claude"
        upstream = "{up}"
          [[route.step]]
          name = "guard"
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
    assert_eq!(
        resp.text().await.unwrap(),
        "data: first\n\ndata: second\n\n"
    );

    // Find the request the mutate step received for event 2 ("second") and
    // confirm its envelope's `context` map carries the value event 1 set,
    // under the step's own namespace (`"guard"` — see `apply_stream_ops`).
    let mutate_requests = mutate.received_requests().await.unwrap();
    let second_event_req = mutate_requests
        .iter()
        .find(|r| {
            let body: serde_json::Value = serde_json::from_slice(&r.body).unwrap();
            body["chunk"]["data_b64"].as_str() == Some(b64("second").as_str())
        })
        .expect("mutate step must have been called for event 2");
    let body: serde_json::Value = serde_json::from_slice(&second_event_req.body).unwrap();
    assert_eq!(
        body["context"]["guard"]["seen"], "event-1",
        "event 2's envelope must carry the context event 1 set: {body:?}"
    );
}
