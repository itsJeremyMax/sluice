//! Integration tests for M6 Task 4: the admin listener (`/healthz`,
//! `/readyz`, `/metrics`) wired up alongside the data-plane listener, and
//! proof that request/step metrics recorded by `proxy::handle` actually show
//! up in a `/metrics` scrape after real traffic — plus proof that the
//! internal correlation id never leaks to the client as a response header.

use std::time::Duration;

use tokio::net::TcpListener;
use wiremock::matchers::method;
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

/// Reserve an ephemeral port by binding then immediately dropping the
/// listener. The admin listener binds from a `listen: &str` address (unlike
/// the data-plane listener, which is bound by the caller and handed to
/// `serve_on` as an already-live `TcpListener`), so tests need a free port
/// number up front to put in the `admin_listen` config string.
async fn reserve_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap().port()
}

/// The admin listener is bound inside a task spawned by `serve_on`, so it
/// isn't guaranteed live the instant `spawn_gateway` returns. Poll
/// `/healthz` (always unauthenticated) until it answers.
async fn wait_for_admin(client: &reqwest::Client, admin_base: &str) {
    for _ in 0..100 {
        if client
            .get(format!("{admin_base}/healthz"))
            .send()
            .await
            .is_ok()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("admin listener at {admin_base} never came up");
}

#[tokio::test]
async fn admin_listener_serves_healthz_readyz_and_metrics_with_bearer_auth() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string("pong"))
        .mount(&upstream)
        .await;

    let admin_port = reserve_port().await;
    let cfg = format!(
        r#"
        [gateway]
        admin_listen = "127.0.0.1:{admin_port}"
        admin_token = "s3cret"

        [[route]]
        id = "claude"
        upstream = "{up}"
    "#,
        admin_port = admin_port,
        up = upstream.uri(),
    );
    let (base, _guard) = spawn_gateway(cfg).await;
    let admin_base = format!("http://127.0.0.1:{admin_port}");
    let client = reqwest::Client::new();
    wait_for_admin(&client, &admin_base).await;

    // /healthz never requires auth.
    let resp = client
        .get(format!("{admin_base}/healthz"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // /readyz requires the bearer once a token is configured.
    let resp = client
        .get(format!("{admin_base}/readyz"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);

    let resp = client
        .get(format!("{admin_base}/readyz"))
        .bearer_auth("s3cret")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Drive one data request through the gateway so `sluice_requests_total`
    // has something to count.
    let data_resp = client
        .post(format!("{base}/claude/v1/messages"))
        .body("ping")
        .send()
        .await
        .unwrap();
    assert_eq!(data_resp.status(), 200);

    let metrics_resp = client
        .get(format!("{admin_base}/metrics"))
        .bearer_auth("s3cret")
        .send()
        .await
        .unwrap();
    assert_eq!(metrics_resp.status(), 200);
    let body = metrics_resp.text().await.unwrap();
    assert!(
        body.contains("sluice_requests_total"),
        "expected /metrics body to contain sluice_requests_total, got: {body}"
    );
    // The 200 from the mocked upstream above must be recorded with
    // `outcome="ok"` (not just a bare `route` label) — labels can render in
    // either order, so check for both independently rather than a single
    // exact-order substring.
    assert!(
        body.contains("route=\"claude\""),
        "expected sluice_requests_total to carry a route label, got: {body}"
    );
    assert!(
        body.contains("outcome=\"ok\""),
        "expected sluice_requests_total to carry an outcome=\"ok\" label for the 200 response, got: {body}"
    );
    // `sluice_inflight` must also show up once real traffic has driven a
    // request through `proxy::handle` (wired up in this milestone via
    // `InflightGuard`); the precise moved/returned-to-baseline behavior is
    // covered without cross-test interference by
    // `observability::tests::inflight_guard_moves_gauge_and_returns_to_baseline_on_drop`,
    // since this admin listener's Prometheus recorder is shared process-wide
    // across every test in this binary.
    assert!(
        body.contains("sluice_inflight"),
        "expected /metrics body to contain sluice_inflight, got: {body}"
    );
}

/// The gateway derives a correlation id from an inbound `x-request-id`
/// header (or generates one), but that id is internal-only: it must never
/// be echoed back to the client as a response header.
#[tokio::test]
async fn inbound_x_request_id_is_not_echoed_back_as_a_response_header() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string("pong"))
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
        .post(format!("{base}/claude/v1/messages"))
        .header("x-request-id", "client-supplied-id-12345")
        .body("ping")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    assert!(
        resp.headers().get("x-request-id").is_none(),
        "correlation id must stay internal: got headers {:?}",
        resp.headers()
    );
    for (name, value) in resp.headers() {
        assert_ne!(
            value.to_str().unwrap_or_default(),
            "client-supplied-id-12345",
            "inbound x-request-id leaked back as header {name}"
        );
    }
}
