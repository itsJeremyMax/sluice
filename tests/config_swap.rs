//! Integration test for M7 Task 1: the live config is held behind an
//! `ArcSwap` and `proxy::handle` snapshots it once per request, so a config
//! swap between two requests changes routing for the *next* request without
//! needing to restart the gateway.
//!
//! Unlike the other integration tests in this suite (which only exercise the
//! public `serve_on` entrypoint), this test needs a handle to the running
//! server's `Arc<ProxyState>` so it can call `state.config.store(...)`
//! mid-test. `server::build_state` + `server::serve_with_state` exist for
//! exactly this: they split "build the state" and "run the accept loop" so a
//! caller can hang onto the state after the server starts.

use std::sync::Arc;

use tokio::net::TcpListener;
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, ResponseTemplate};

use sluice::admin::Ready;
use sluice::config::load::load_str;
use sluice::observability::init_metrics;
use sluice::server::{build_state, serve_with_state};

/// Like the `spawn_gateway` helper in the other integration test files, but
/// returns the `Arc<ProxyState>` too, so the test can swap its config.
async fn spawn_gateway_with_state(
    config_toml: String,
) -> (
    String,
    Arc<sluice::proxy::ProxyState>,
    tokio::sync::oneshot::Sender<()>,
) {
    let cfg = load_str(&config_toml).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let ready = Ready::new();
    let metrics = init_metrics();
    let state = build_state(cfg);
    let state_for_server = state.clone();
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        let _ = serve_with_state(
            listener,
            state_for_server,
            String::new(),
            String::new(),
            ready,
            metrics,
            async {
                let _ = rx.await;
            },
        )
        .await;
    });
    (format!("http://{addr}"), state, tx)
}

/// After `state.config.store(...)` swaps in a config whose only route is a
/// new id, a fresh request using the new route id must now match — proving
/// the swap takes effect for new requests without a restart. (The old route
/// id, present only in the pre-swap config, is expected to now 404, which we
/// also assert to make sure we're really observing the new config and not
/// some stale/merged state.)
#[tokio::test]
async fn config_swap_changes_routing_for_next_request() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string("pong"))
        .mount(&upstream)
        .await;

    let initial_cfg = format!(
        r#"
        [[route]]
        id = "claude"
        upstream = "{up}"
    "#,
        up = upstream.uri(),
    );
    let (base, state, _guard) = spawn_gateway_with_state(initial_cfg).await;

    // Before the swap: the initial route id works, and the not-yet-existing
    // new route id 404s.
    let resp = reqwest::Client::new()
        .post(format!("{base}/claude/v1/messages"))
        .body("ping")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let resp = reqwest::Client::new()
        .post(format!("{base}/openai/v1/messages"))
        .body("ping")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        404,
        "route id from the not-yet-swapped-in config must not match yet"
    );

    // Swap in a config whose only route is "openai" (the old "claude" route
    // is gone).
    let new_cfg_toml = format!(
        r#"
        [[route]]
        id = "openai"
        upstream = "{up}"
    "#,
        up = upstream.uri(),
    );
    let new_cfg = load_str(&new_cfg_toml).unwrap();
    state.config.store(Arc::new(new_cfg));

    // After the swap: a fresh request to the new route id now matches...
    let resp = reqwest::Client::new()
        .post(format!("{base}/openai/v1/messages"))
        .body("ping")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "a fresh request must observe the swapped-in config's new route"
    );

    // ...and the old route id, now absent from the live config, 404s.
    let resp = reqwest::Client::new()
        .post(format!("{base}/claude/v1/messages"))
        .body("ping")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        404,
        "a fresh request must not see a route id removed by the swap"
    );
}
