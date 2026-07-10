//! Integration tests for M13 Task 2: multi-listener `serve` with
//! per-listener route filtering.
//!
//! Rather than driving `server::serve` itself (which hard-binds its
//! shutdown to `ctrl_c` and isn't designed to be torn down mid-test), these
//! tests replicate exactly what `serve`'s multi-listener path does —
//! `resolve_gateways` to pick which `GatewayDef`s to bind, `build_state` +
//! `derive_state` to give each its own `allowed_routes`, and `serve_multi`
//! to run them — using the same public `server` API `serve` itself calls.
//! This gets full coverage of the real multi-listener machinery while
//! keeping an injectable shutdown for a clean test teardown.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::oneshot;
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, ResponseTemplate};

use sluice::admin::Ready;
use sluice::config::{Config, Gateway, GatewayDef, Route};
use sluice::observability::init_metrics;
use sluice::server::{build_state, derive_state, resolve_gateways, serve_multi};

/// Bind an ephemeral port, read back its concrete address, then drop the
/// listener to free it again. Used only where a test needs a *known*
/// address to write into a `GatewayDef` up front (to later prove our own
/// server never bound it) rather than an actual open listener.
async fn free_addr() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    format!("127.0.0.1:{}", addr.port())
}

fn route(id: &str, upstream: &str) -> Route {
    Route {
        id: id.to_string(),
        upstream: upstream.to_string(),
        steps: Vec::new(),
        adapter: None,
        translate: None,
    }
}

/// Bind every `GatewayDef` in `gateway_defs`, give each its own `ProxyState`
/// (sharing `base_cfg`'s config/client/registry/etc. via `derive_state`),
/// and start `serve_multi` in the background. Returns each gateway's bound
/// `http://host:port` base URL (keyed by name) plus a shutdown handle.
async fn spawn_multi(
    base_cfg: Config,
    gateway_defs: &[GatewayDef],
) -> (HashMap<String, String>, oneshot::Sender<()>) {
    let base_state = build_state(base_cfg);

    let mut listeners = Vec::with_capacity(gateway_defs.len());
    let mut addrs = HashMap::new();
    for gw in gateway_defs {
        let listener = TcpListener::bind(&gw.listen).await.unwrap();
        let addr = listener.local_addr().unwrap();
        addrs.insert(gw.name.clone(), format!("http://{addr}"));
        let allowed: HashSet<String> = gw.routes.iter().cloned().collect();
        let state = derive_state(&base_state, Some(Arc::new(allowed)));
        listeners.push((listener, state));
    }

    let (tx, rx) = oneshot::channel::<()>();
    let ready = Ready::new();
    let metrics = init_metrics();
    tokio::spawn(async move {
        let _ = serve_multi(
            listeners,
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

    (addrs, tx)
}

/// Two gateways, each exposing a disjoint route, both bound in one process:
/// each listener serves its own route and 404s the other gateway's route,
/// even though both routes are defined in the same shared config.
#[tokio::test]
async fn two_listeners_filter_disjoint_routes() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string("pong"))
        .mount(&upstream)
        .await;

    let cfg = Config {
        gateway: Gateway::default(),
        routes: vec![route("a", &upstream.uri()), route("b", &upstream.uri())],
        gateways: vec![
            GatewayDef {
                name: "public".to_string(),
                listen: "127.0.0.1:0".to_string(),
                routes: vec!["a".to_string()],
            },
            GatewayDef {
                name: "internal".to_string(),
                listen: "127.0.0.1:0".to_string(),
                routes: vec!["b".to_string()],
            },
        ],
    };

    let gateway_defs = resolve_gateways(&cfg, None).unwrap();
    assert_eq!(
        gateway_defs.len(),
        2,
        "no --gateway selection binds all of them"
    );

    let (addrs, _shutdown) = spawn_multi(cfg, &gateway_defs).await;
    let public = addrs.get("public").unwrap();
    let internal = addrs.get("internal").unwrap();

    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{public}/a/v1/messages"))
        .body("ping")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "public's own route must work");

    let resp = client
        .post(format!("{public}/b/v1/messages"))
        .body("ping")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        404,
        "internal's route must 404 on public's listener even though it's a defined route"
    );

    let resp = client
        .post(format!("{internal}/b/v1/messages"))
        .body("ping")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "internal's own route must work");

    let resp = client
        .post(format!("{internal}/a/v1/messages"))
        .body("ping")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        404,
        "public's route must 404 on internal's listener"
    );
}

/// `serve_multi` must actually return once told to shut down, rather than
/// hang forever — a regression guard for the M13 review finding that its
/// old sequential `for handle in handles { handle.await??; }` awaited every
/// listener's `JoinHandle` in vec order, so it could never notice a later
/// listener finishing (whether cleanly or with an error) while an earlier
/// one was still running. `select_all`-based racing means every listener
/// finishing on shutdown resolves the whole thing promptly regardless of
/// which one goes first.
#[tokio::test]
async fn serve_multi_returns_promptly_on_shutdown_not_hang() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string("pong"))
        .mount(&upstream)
        .await;

    let cfg = Config {
        gateway: Gateway::default(),
        routes: vec![route("a", &upstream.uri()), route("b", &upstream.uri())],
        gateways: vec![
            GatewayDef {
                name: "public".to_string(),
                listen: "127.0.0.1:0".to_string(),
                routes: vec!["a".to_string()],
            },
            GatewayDef {
                name: "internal".to_string(),
                listen: "127.0.0.1:0".to_string(),
                routes: vec!["b".to_string()],
            },
        ],
    };

    let gateway_defs = resolve_gateways(&cfg, None).unwrap();
    let (addrs, shutdown) = spawn_multi(cfg, &gateway_defs).await;
    // Strip the `http://` prefix `spawn_multi` adds to get back a bindable
    // `host:port`.
    let public_listen = addrs.get("public").unwrap().replace("http://", "");

    // spawn_multi already put `serve_multi` in the background; there's no
    // handle to await directly, so drive an equivalent race here: tell it to
    // shut down, then poll until the process-wide effect of that (the
    // listener port becoming free again) is observable, bounded by a
    // timeout so a reintroduced hang fails the test instead of wedging the
    // suite.
    shutdown.send(()).expect("shutdown receiver still alive");

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if TcpListener::bind(&public_listen).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect(
        "serve_multi must release its listeners promptly on shutdown, not hang \
         on an earlier listener's still-open accept loop",
    );
}

/// `resolve_gateways` (the pure lookup `serve` uses for `--gateway <name>`)
/// rejects a name no `gateways.d` entry defines.
#[test]
fn resolve_gateways_rejects_unknown_name() {
    let cfg = Config {
        gateway: Gateway::default(),
        routes: vec![route("a", "http://upstream")],
        gateways: vec![GatewayDef {
            name: "public".to_string(),
            listen: "127.0.0.1:0".to_string(),
            routes: vec!["a".to_string()],
        }],
    };

    let err = resolve_gateways(&cfg, Some("nope")).unwrap_err();
    assert!(
        err.to_string().contains("nope"),
        "error must name the unknown gateway: {err}"
    );
}

/// `--gateway internal` binds only the named gateway: its route works, and
/// the other gateway's port is never bound at all (proven by successfully
/// rebinding it ourselves after the selective server is up).
#[tokio::test]
async fn gateway_selection_binds_only_the_named_gateway() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string("pong"))
        .mount(&upstream)
        .await;

    // Probe two free addresses up front, then release them immediately, so
    // the config can name concrete addresses and we can later prove one of
    // them was genuinely never bound by our server (rather than just
    // "we never learned its port").
    let public_addr = free_addr().await;
    let internal_addr = free_addr().await;

    let cfg = Config {
        gateway: Gateway::default(),
        routes: vec![route("a", &upstream.uri()), route("b", &upstream.uri())],
        gateways: vec![
            GatewayDef {
                name: "public".to_string(),
                listen: public_addr.clone(),
                routes: vec!["a".to_string()],
            },
            GatewayDef {
                name: "internal".to_string(),
                listen: internal_addr.clone(),
                routes: vec!["b".to_string()],
            },
        ],
    };

    let gateway_defs = resolve_gateways(&cfg, Some("internal")).unwrap();
    assert_eq!(gateway_defs.len(), 1);
    assert_eq!(gateway_defs[0].name, "internal");

    let (addrs, _shutdown) = spawn_multi(cfg, &gateway_defs).await;
    assert_eq!(
        addrs.len(),
        1,
        "only the selected gateway's listener should have been bound"
    );

    let resp = reqwest::Client::new()
        .post(format!("http://{internal_addr}/b/v1/messages"))
        .body("ping")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "the selected gateway's route must work");

    // public's address was never bound by our server: binding it ourselves
    // must succeed.
    let _reclaimed = TcpListener::bind(&public_addr)
        .await
        .expect("public's port must be free: `--gateway internal` must not have bound it");
}
