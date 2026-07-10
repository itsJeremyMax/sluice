use tokio::net::TcpListener;
use wiremock::matchers::{method, path};
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

#[tokio::test]
async fn passes_request_through_to_upstream() {
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

#[tokio::test]
async fn unknown_route_returns_404() {
    let cfg = r#"
        [[route]]
        id = "claude"
        upstream = "http://127.0.0.1:1"
    "#
    .to_string();
    let (base, _guard) = spawn_gateway(cfg).await;
    let resp = reqwest::Client::new()
        .get(format!("{base}/nope/x"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn on_request_step_injects_header_seen_by_upstream() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .and(wiremock::matchers::header("x-tag", "seen"))
        .respond_with(ResponseTemplate::new(200).set_body_string("tagged"))
        .mount(&upstream)
        .await;

    let step = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"action":"continue","ops":[{"op":"set_header","name":"x-tag","value":"seen"}]}"#,
        ))
        .mount(&step)
        .await;

    let cfg = format!(
        r#"
        [[route]]
        id = "claude"
        upstream = "{up}"
          [[route.step]]
          name = "tagger"
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
    assert_eq!(resp.text().await.unwrap(), "tagged");
}

#[tokio::test]
async fn on_request_step_set_path_rewrites_outbound_url() {
    let upstream = MockServer::start().await;
    // Only the rewritten path should ever be hit; if the gateway ignores
    // `set_path` and forwards the original /v1/messages path, there is no
    // mock to match it and the request will fail/404.
    Mock::given(method("POST"))
        .and(path("/v2/messages"))
        .respond_with(ResponseTemplate::new(200).set_body_string("rewritten"))
        .mount(&upstream)
        .await;

    let step = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"action":"continue","ops":[{"op":"set_path","path":"/claude/v2/messages"}]}"#,
        ))
        .mount(&step)
        .await;

    let cfg = format!(
        r#"
        [[route]]
        id = "claude"
        upstream = "{up}"
          [[route.step]]
          name = "rewriter"
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
    assert_eq!(resp.text().await.unwrap(), "rewritten");
}
