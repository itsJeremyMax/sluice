//! End-to-end tests for the loopback park/callback/resume round trip (design
//! doc §4.5, M12 Task 3): a client request parks on a `mode = "loopback"`
//! step, a "tool" server receives the fire-and-forget loopback POST and
//! calls back into the gateway's own callback endpoint, and the gateway
//! resumes the chain from where it parked, ultimately unparking the
//! original client connection with the real upstream's response.

use std::convert::Infallible;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder;
use tokio::net::TcpListener;
use wiremock::matchers::{body_string_contains, method, path};
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

/// A minimal HTTP server standing in for a `mode = "loopback"` step's tool:
/// on every request it receives, it reads the `x-chain-token` header and
/// FORWARDS the request (same method + body, plus that same token header) to
/// `callback_url` — the gateway's own loopback callback endpoint — then
/// answers ITS OWN caller with whatever the callback returned.
///
/// That reply is irrelevant to every test here: `proxy::dispatch_loopback`
/// fires the loopback POST fire-and-forget and never looks at the tool's own
/// response (the real client response instead comes back later via the
/// callback's resume, through the parked `oneshot`) — what actually matters,
/// and what this stands in for, is that the tool's forward-with-token call
/// happens at all.
///
/// Takes an already-bound listener (rather than binding and returning its own
/// address) so tests can learn the tool's address BEFORE they know
/// `callback_url` (which itself depends on the gateway's address), bind the
/// tool's listener first, build the gateway's config around that address,
/// spawn the gateway, and only then start this server's accept loop with the
/// now-known callback URL.
fn spawn_mock_tool(listener: TcpListener, callback_url: String) {
    tokio::spawn(async move {
        loop {
            let (stream, _peer) = match listener.accept().await {
                Ok(pair) => pair,
                Err(_) => break,
            };
            let io = TokioIo::new(stream);
            let callback_url = callback_url.clone();
            tokio::spawn(async move {
                let service = service_fn(move |req: Request<Incoming>| {
                    let callback_url = callback_url.clone();
                    async move {
                        let method = req.method().clone();
                        let token = req
                            .headers()
                            .get("x-chain-token")
                            .and_then(|v| v.to_str().ok())
                            .unwrap_or_default()
                            .to_string();
                        let body = req
                            .into_body()
                            .collect()
                            .await
                            .map(|c| c.to_bytes())
                            .unwrap_or_default();

                        let client = reqwest::Client::new();
                        let (status, resp_body) = match client
                            .request(method, &callback_url)
                            .header("x-chain-token", token)
                            .body(body)
                            .send()
                            .await
                        {
                            Ok(r) => {
                                let status = r.status().as_u16();
                                let body = r.bytes().await.unwrap_or_default();
                                (status, body)
                            }
                            Err(_) => (502u16, Bytes::new()),
                        };

                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(status)
                                .body(Full::new(resp_body).boxed())
                                .expect("bare status+body response always builds"),
                        )
                    }
                });
                let _ = Builder::new(TokioExecutor::new())
                    .serve_connection(io, service)
                    .await;
            });
        }
    });
}

/// The full park -> tool -> callback -> resume -> upstream -> parked-client
/// round trip: a client request hits a route with ONE `mode = "loopback"`
/// step; the mock tool forwards the token it's handed to the gateway's own
/// callback endpoint; the gateway resumes the (now step-exhausted) chain
/// straight to the real (wiremock) upstream; and the ORIGINAL client
/// connection — which has been parked this whole time — receives that
/// upstream's response, never the tool's.
#[tokio::test]
async fn client_receives_upstream_response_via_park_tool_callback_resume() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .and(body_string_contains("ping"))
        .respond_with(ResponseTemplate::new(200).set_body_string("upstream-done"))
        .mount(&upstream)
        .await;

    // Bind the tool's listener now (to learn its address for the route
    // config below) but don't start serving it until the gateway (and hence
    // the callback URL) exists — see `spawn_mock_tool`'s doc comment.
    let tool_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let tool_addr = tool_listener.local_addr().unwrap();
    let tool_url = format!("http://{tool_addr}");

    let cfg = format!(
        r#"
        [gateway]
        loopback_secret = "test-secret"

        [[route]]
        id = "claude"
        upstream = "{up}"
          [[route.step]]
          type = "url"
          mode = "loopback"
          url = "{tool}"
    "#,
        up = upstream.uri(),
        tool = tool_url,
    );
    let (base, _guard) = spawn_gateway(cfg).await;
    let callback_url = format!("{base}/__sluice/loopback");
    spawn_mock_tool(tool_listener, callback_url);

    let resp = reqwest::Client::new()
        .post(format!("{base}/claude/v1/messages"))
        .body("ping")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    assert!(
        resp.headers().get("x-chain-token").is_none(),
        "the client must never see the internal chain token"
    );
    let body = resp.text().await.unwrap();
    assert_eq!(
        body, "upstream-done",
        "the client must receive the UPSTREAM's response, not the tool's"
    );

    let upstream_requests = upstream.received_requests().await.unwrap();
    assert_eq!(
        upstream_requests.len(),
        1,
        "the upstream must have been hit exactly once (by the resumed chain, not by the tool)"
    );
    assert!(
        !upstream_requests[0].headers.contains_key("x-chain-token"),
        "x-chain-token must never reach the real upstream"
    );
}

/// A chain with more `mode = "loopback"` steps than `max_hops` allows must
/// abort with 508 (Loop Detected) rather than looping forever or hanging —
/// exercised end-to-end (not just as a unit test) using the SAME
/// always-forward mock tool for every hop: with `max_hops = 2` and three
/// chained loopback steps, the third hop's dispatch itself refuses to park
/// (see `proxy::dispatch_loopback`), and that abort response bubbles back
/// through the two nested park/resume layers to the original client.
#[tokio::test]
async fn chain_exceeding_max_hops_aborts_508_all_the_way_back_to_the_client() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string("must-not-be-reached"))
        .mount(&upstream)
        .await;

    let tool_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let tool_addr = tool_listener.local_addr().unwrap();
    let tool_url = format!("http://{tool_addr}");

    let cfg = format!(
        r#"
        [gateway]
        loopback_secret = "test-secret"
        max_hops = 2

        [[route]]
        id = "claude"
        upstream = "{up}"
          [[route.step]]
          name = "hop1"
          type = "url"
          mode = "loopback"
          url = "{tool}"
          [[route.step]]
          name = "hop2"
          type = "url"
          mode = "loopback"
          url = "{tool}"
          [[route.step]]
          name = "hop3"
          type = "url"
          mode = "loopback"
          url = "{tool}"
    "#,
        up = upstream.uri(),
        tool = tool_url,
    );
    let (base, _guard) = spawn_gateway(cfg).await;
    let callback_url = format!("{base}/__sluice/loopback");
    spawn_mock_tool(tool_listener, callback_url);

    let resp = reqwest::Client::new()
        .post(format!("{base}/claude/v1/messages"))
        .body("ping")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 508);
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("hop3") && body.contains("max_hops"),
        "the abort must name the offending step: {body}"
    );

    assert!(
        upstream.received_requests().await.unwrap().is_empty(),
        "an aborted chain must never reach the upstream"
    );
}

/// A callback request with no `x-chain-token` header at all is rejected with
/// 401 — the gateway must fail closed, never treat a tokenless callback as
/// implicitly trusted.
#[tokio::test]
async fn callback_without_token_is_401() {
    let cfg = r#"
        [gateway]
        loopback_secret = "test-secret"

        [[route]]
        id = "claude"
        upstream = "http://127.0.0.1:1"
    "#
    .to_string();
    let (base, _guard) = spawn_gateway(cfg).await;

    let resp = reqwest::Client::new()
        .post(format!("{base}/__sluice/loopback"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 401);
}

/// A callback request with a garbage/invalid `x-chain-token` (bad signature,
/// not merely absent) is likewise rejected with 401.
#[tokio::test]
async fn callback_with_invalid_token_is_401() {
    let cfg = r#"
        [gateway]
        loopback_secret = "test-secret"

        [[route]]
        id = "claude"
        upstream = "http://127.0.0.1:1"
    "#
    .to_string();
    let (base, _guard) = spawn_gateway(cfg).await;

    let resp = reqwest::Client::new()
        .post(format!("{base}/__sluice/loopback"))
        .header("x-chain-token", "not-a-real-token")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 401);
}

/// A callback bearing a validly-signed token whose `cid` names a
/// continuation the registry no longer has (already taken, swept, or never
/// existed) is rejected with 410, never a hang or a 5xx that suggests a
/// transient failure.
#[tokio::test]
async fn callback_with_unknown_continuation_is_410() {
    let cfg = r#"
        [gateway]
        loopback_secret = "test-secret"

        [[route]]
        id = "claude"
        upstream = "http://127.0.0.1:1"
    "#
    .to_string();
    let (base, _guard) = spawn_gateway(cfg).await;

    let token = sluice::loopback::token::ChainToken {
        cid: "no-such-continuation".to_string(),
        route_id: "claude".to_string(),
        resume_index: 1,
        hop: 1,
        expires_at_unix: u64::MAX,
    }
    .sign(b"test-secret");

    let resp = reqwest::Client::new()
        .post(format!("{base}/__sluice/loopback"))
        .header("x-chain-token", token)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 410);
}

/// The `context` map accumulated by an `on_request` step BEFORE a `mode =
/// "loopback"` step must survive the park: a step AFTER the loopback step
/// must see it in its own envelope, proving `handle_callback` resumes
/// `run_from` with the continuation's carried-over `context`, not a fresh
/// empty map. The `reader` step's mock only matches a request whose body
/// already contains the `writer` step's namespaced `set_context` write, so a
/// regression back to a fresh empty map on resume would make `reader`'s mock
/// go unmatched, the step fail (`fail_closed` by default), and the whole
/// chain 502 instead of reaching the upstream.
#[tokio::test]
async fn context_set_before_a_loopback_park_survives_to_a_step_after_it() {
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
    // writer's namespaced write — proving propagation across the park, not
    // just presence of a `context` field at all.
    Mock::given(method("POST"))
        .and(body_string_contains(r#""writer":{"seen":true}"#))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"action":"continue"}"#))
        .mount(&reader)
        .await;

    let tool_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let tool_addr = tool_listener.local_addr().unwrap();
    let tool_url = format!("http://{tool_addr}");

    let cfg = format!(
        r#"
        [gateway]
        loopback_secret = "test-secret"

        [[route]]
        id = "claude"
        upstream = "{up}"
          [[route.step]]
          name = "writer"
          type = "url"
          url = "{writer}/run"
          [[route.step]]
          type = "url"
          mode = "loopback"
          url = "{tool}"
          [[route.step]]
          name = "reader"
          type = "url"
          url = "{reader}/run"
    "#,
        up = upstream.uri(),
        writer = writer.uri(),
        tool = tool_url,
        reader = reader.uri(),
    );
    let (base, _guard) = spawn_gateway(cfg).await;
    let callback_url = format!("{base}/__sluice/loopback");
    spawn_mock_tool(tool_listener, callback_url);

    let resp = reqwest::Client::new()
        .post(format!("{base}/claude/v1/messages"))
        .body("ping")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert_eq!(
        body, "done",
        "the writer's context must have survived the park for reader's mock to match at all"
    );
}

/// With `max_inflight = 1`, a single client request must still complete
/// successfully all the way through park -> tool -> callback -> resume ->
/// upstream. `proxy::handle_callback` deliberately acquires no permit of its
/// own — the parked original handler already holds the single permit for the
/// chain's whole lifetime — so this proves that design doesn't regress into
/// a reentrancy deadlock (the callback trying to acquire a permit that's
/// only available once the very request it's resuming completes).
#[tokio::test]
async fn max_inflight_one_loopback_chain_completes_without_deadlock() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string("upstream-done")
                .set_delay(std::time::Duration::from_millis(200)),
        )
        .mount(&upstream)
        .await;

    let plain_upstream = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_string("plain-done"))
        .mount(&plain_upstream)
        .await;

    let tool_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let tool_addr = tool_listener.local_addr().unwrap();
    let tool_url = format!("http://{tool_addr}");

    let cfg = format!(
        r#"
        [gateway]
        loopback_secret = "test-secret"
        max_inflight = 1

        [[route]]
        id = "claude"
        upstream = "{up}"
          [[route.step]]
          type = "url"
          mode = "loopback"
          url = "{tool}"

        [[route]]
        id = "plain"
        upstream = "{plain_up}"
    "#,
        up = upstream.uri(),
        tool = tool_url,
        plain_up = plain_upstream.uri(),
    );
    let (base, _guard) = spawn_gateway(cfg).await;
    let callback_url = format!("{base}/__sluice/loopback");
    spawn_mock_tool(tool_listener, callback_url);

    let client = reqwest::Client::new();
    let base_a = base.clone();
    let client_a = client.clone();
    let request_a = tokio::spawn(async move {
        client_a
            .post(format!("{base_a}/claude/v1/messages"))
            .body("ping")
            .send()
            .await
            .unwrap()
    });

    // Give request A time to be admitted, claim the single permit, and park
    // on the loopback callback before we fire request B. The upstream's
    // 200ms delay (after resume) keeps the permit held long enough for B to
    // land squarely inside that window.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let resp_b = client
        .get(format!("{base}/plain/v1/messages"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp_b.status(),
        503,
        "a plain request sent while the single permit is held by the parked chain must be shed"
    );

    let resp_a = request_a.await.unwrap();
    assert_eq!(
        resp_a.status(),
        200,
        "the parked loopback chain must still complete via callback/resume with no permit of its own"
    );
    assert_eq!(resp_a.text().await.unwrap(), "upstream-done");
}
