use tokio::net::TcpListener;
use wiremock::matchers::{body_string_contains, method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

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

/// A later step's envelope must carry an earlier step's `set_context` write,
/// proving `context` is threaded (not reset) across the chain.
#[tokio::test]
async fn later_step_envelope_carries_earlier_steps_context() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(ResponseTemplate::new(200).set_body_string("done"))
        .mount(&upstream)
        .await;

    let writer = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"action":"continue","ops":[{"op":"set_context","value":{"seen":true}}]}"#,
        ))
        .mount(&writer)
        .await;

    let reader = MockServer::start().await;
    // Only matches if the posted envelope's `context` already contains the
    // writer's namespaced write — proving propagation, not just presence.
    Mock::given(method("POST"))
        .and(body_string_contains(r#""writer":{"seen":true}"#))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"action":"continue"}"#))
        .mount(&reader)
        .await;

    let cfg = format!(
        r#"
        [[route]]
        id = "claude"
        upstream = "{up}"
          [[route.step]]
          name = "writer"
          type = "url"
          url = "{writer}/run"
          [[route.step]]
          name = "reader"
          type = "url"
          url = "{reader}/run"
    "#,
        up = upstream.uri(),
        writer = writer.uri(),
        reader = reader.uri(),
    );
    let (base, _guard) = spawn_gateway(cfg).await;

    let resp = reqwest::Client::new()
        .post(format!("{base}/claude/v1/messages"))
        .body("ping")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "done");
}

/// An oversized `set_context` write on a `fail_closed` step aborts the chain
/// with a 502, never reaching the upstream.
#[tokio::test]
async fn oversize_context_write_fail_closed_returns_502() {
    let upstream = MockServer::start().await;
    // No mock mounted: if the gateway ever forwarded the request, this test
    // would fail with a connection/match error rather than a false pass.

    let step = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"action":"continue","ops":[{"op":"set_context","value":{"data":"way too big for the cap"}}]}"#,
        ))
        .mount(&step)
        .await;

    let cfg = format!(
        r#"
        [gateway]
        max_context_bytes = 8

        [[route]]
        id = "claude"
        upstream = "{up}"
          [[route.step]]
          name = "writer"
          type = "url"
          url = "{step}/run"
          on_error = "fail_closed"
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
    assert_eq!(resp.status(), 502);
}

/// The same oversized write on a `fail_open` step is skipped, but the chain
/// still continues and the request still reaches the upstream.
#[tokio::test]
async fn oversize_context_write_fail_open_still_reaches_upstream() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(ResponseTemplate::new(200).set_body_string("reached"))
        .mount(&upstream)
        .await;

    let step = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"action":"continue","ops":[{"op":"set_context","value":{"data":"way too big for the cap"}}]}"#,
        ))
        .mount(&step)
        .await;

    let cfg = format!(
        r#"
        [gateway]
        max_context_bytes = 8

        [[route]]
        id = "claude"
        upstream = "{up}"
          [[route.step]]
          name = "writer"
          type = "url"
          url = "{step}/run"
          on_error = "fail_open"
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
    assert_eq!(resp.text().await.unwrap(), "reached");
}

/// A step trying to set a protected (hop-by-hop) header via `set_header` on
/// a `fail_closed` step aborts the chain with a 502, never reaching the
/// upstream.
#[tokio::test]
async fn protected_header_op_fail_closed_returns_502() {
    let upstream = MockServer::start().await;
    // No mock mounted: if the gateway ever forwarded the request, this test
    // would fail with a connection/match error rather than a false pass.

    let step = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"action":"continue","ops":[{"op":"set_header","name":"content-length","value":"5"}]}"#,
        ))
        .mount(&step)
        .await;

    let cfg = format!(
        r#"
        [[route]]
        id = "claude"
        upstream = "{up}"
          [[route.step]]
          name = "writer"
          type = "url"
          url = "{step}/run"
          on_error = "fail_closed"
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
    assert_eq!(resp.status(), 502);
}

/// The same protected-header op on a `fail_open` step is skipped, and the
/// chain still continues to the upstream. Critically, this also proves
/// `apply_ops` is atomic: the step's ops set a benign header BEFORE the
/// protected one, and that benign header must NOT reach the upstream,
/// because the whole op batch is rolled back on error.
#[tokio::test]
async fn protected_header_op_fail_open_reaches_upstream_without_benign_header() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .and(|req: &Request| !req.headers.contains_key("x-canary"))
        .respond_with(ResponseTemplate::new(200).set_body_string("reached"))
        .mount(&upstream)
        .await;

    let step = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"action":"continue","ops":[{"op":"set_header","name":"x-canary","value":"benign"},{"op":"set_header","name":"content-length","value":"5"}]}"#,
        ))
        .mount(&step)
        .await;

    let cfg = format!(
        r#"
        [[route]]
        id = "claude"
        upstream = "{up}"
          [[route.step]]
          name = "writer"
          type = "url"
          url = "{step}/run"
          on_error = "fail_open"
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
    assert_eq!(resp.text().await.unwrap(), "reached");
}

/// Context accumulates across three chained steps: two writer steps each set
/// their own namespaced `context` entry, and a third reader step's envelope
/// carries both writes, proving accumulation (not overwrite) across the
/// chain.
#[tokio::test]
async fn three_step_chain_accumulates_context_from_each_writer() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(ResponseTemplate::new(200).set_body_string("done"))
        .mount(&upstream)
        .await;

    let step1 = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"action":"continue","ops":[{"op":"set_context","value":{"v":"a"}}]}"#,
        ))
        .mount(&step1)
        .await;

    let step2 = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"action":"continue","ops":[{"op":"set_context","value":{"v":"b"}}]}"#,
        ))
        .mount(&step2)
        .await;

    let reader = MockServer::start().await;
    // Only matches if the posted envelope's `context` contains both
    // namespaced writes — proving accumulation across the whole chain.
    Mock::given(method("POST"))
        .and(body_string_contains(r#""step1":{"v":"a"}"#))
        .and(body_string_contains(r#""step2":{"v":"b"}"#))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"action":"continue"}"#))
        .mount(&reader)
        .await;

    let cfg = format!(
        r#"
        [[route]]
        id = "claude"
        upstream = "{up}"
          [[route.step]]
          name = "step1"
          type = "url"
          url = "{step1}/run"
          [[route.step]]
          name = "step2"
          type = "url"
          url = "{step2}/run"
          [[route.step]]
          name = "reader"
          type = "url"
          url = "{reader}/run"
    "#,
        up = upstream.uri(),
        step1 = step1.uri(),
        step2 = step2.uri(),
        reader = reader.uri(),
    );
    let (base, _guard) = spawn_gateway(cfg).await;

    let resp = reqwest::Client::new()
        .post(format!("{base}/claude/v1/messages"))
        .body("ping")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "done");
}
