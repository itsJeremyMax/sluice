//! Integration tests for M11 Task 2 (script runtime: oneshot subprocess
//! steps, gated by `[gateway] allow_scripts`).

use std::process::Command as StdCommand;

use tokio::net::TcpListener;
use wiremock::matchers::method;
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

/// Write a small shell script (under the OS temp dir) that drains stdin (the
/// envelope JSON the gateway writes to it) and prints a fixed `set_header`
/// directive to stdout — the script-step analogue of the URL-step mock
/// servers other integration tests POST to.
fn write_header_script() -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("sluice-script-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("set_header.sh");
    std::fs::write(
        &path,
        r#"#!/bin/sh
cat >/dev/null
echo '{"action":"continue","ops":[{"op":"set_header","name":"x-script-tag","value":"from-script"}]}'
"#,
    )
    .unwrap();
    path
}

/// An `on_request` script step that reads the envelope off stdin and prints
/// a `set_header` directive: the upstream must see the header the script
/// added, proving the oneshot subprocess runtime is wired into the request
/// pipeline the same way a `type = "url"` step is.
#[tokio::test]
async fn on_request_script_step_set_header_reaches_upstream() {
    let script = write_header_script();

    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(|req: &Request| {
            req.headers
                .get("x-script-tag")
                .and_then(|v| v.to_str().ok())
                == Some("from-script")
        })
        .respond_with(ResponseTemplate::new(200).set_body_string("upstream saw it"))
        .mount(&upstream)
        .await;

    let cfg = format!(
        r#"
        [gateway]
        allow_scripts = true

        [[route]]
        id = "claude"
        upstream = "{up}"
          [[route.step]]
          name = "tagger"
          type = "script"
          cmd = ["sh", "{script}"]
    "#,
        up = upstream.uri(),
        script = script.display(),
    );
    let (base, _guard) = spawn_gateway(cfg).await;

    let resp = reqwest::Client::new()
        .post(format!("{base}/claude/v1/messages"))
        .body("ping")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "upstream saw it");

    let _ = std::fs::remove_dir_all(script.parent().unwrap());
}

/// The worker framing protocol (`u32-le len + JSON`, bidirectional) is
/// awkward to drive reliably from `sh`; these worker e2e tests use `python3`.
/// On a host without `python3` they self-skip rather than fail — they exercise
/// the gateway's worker dispatch, not the interpreter (mirrors the unit tests
/// in `src/step/script.rs`).
fn have_python3() -> bool {
    StdCommand::new("python3")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Write `program` as a python3 worker script under the OS temp dir and return
/// its path. Caller cleans up the parent dir.
fn write_python_worker(name: &str, program: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "sluice-worker-test-{}-{}",
        std::process::id(),
        name
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(format!("{name}.py"));
    std::fs::write(&path, program).unwrap();
    path
}

/// e2e: a streaming route with a `type = "script"`, `script_mode = "worker"`,
/// `hook = "on_stream"`, `chunk_mode = "mutate"` step whose worker upcases each
/// SSE event's body. The client must receive the TRANSFORMED stream, proving
/// the per-stream worker is spawned at stream start and framed each chunk
/// envelope in-path (the headline M17 feature).
#[tokio::test]
async fn on_stream_worker_mutate_upcases_client_stream() {
    if !have_python3() {
        eprintln!("skipping on_stream_worker_mutate_upcases_client_stream: python3 not available");
        return;
    }
    // Framed worker loop: read one envelope frame, upcase `chunk.data_b64`'s
    // decoded text, emit it back as a framed `emit` directive.
    let worker = write_python_worker(
        "upcase",
        r#"import sys, struct, json, base64
while True:
    hdr = sys.stdin.buffer.read(4)
    if len(hdr) < 4:
        break
    n = struct.unpack('<I', hdr)[0]
    data = sys.stdin.buffer.read(n)
    env = json.loads(data)
    text = base64.b64decode(env["chunk"]["data_b64"]).decode()
    out = base64.b64encode(text.upper().encode()).decode()
    resp = json.dumps({"action": "emit", "chunk": {"data_b64": out}}).encode()
    sys.stdout.buffer.write(struct.pack('<I', len(resp)))
    sys.stdout.buffer.write(resp)
    sys.stdout.buffer.flush()
"#,
    );

    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string("data: hello\n\ndata: world\n\n")
                .insert_header("content-type", "text/event-stream"),
        )
        .mount(&upstream)
        .await;

    let cfg = format!(
        r#"
        [gateway]
        allow_scripts = true

        [[route]]
        id = "claude"
        upstream = "{up}"
          [[route.step]]
          name = "upcaser"
          hook = "on_stream"
          type = "script"
          script_mode = "worker"
          chunk_mode = "mutate"
          cmd = ["python3", "{worker}"]
    "#,
        up = upstream.uri(),
        worker = worker.display(),
    );
    let (base, _guard) = spawn_gateway(cfg).await;

    let resp = reqwest::Client::new()
        .post(format!("{base}/claude/v1/messages"))
        .body("ping")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "data: HELLO\n\ndata: WORLD\n\n");

    let _ = std::fs::remove_dir_all(worker.parent().unwrap());
}

/// An `on_request` `script_mode = "worker"` step transforms a header via the
/// CACHED worker, and a second request reuses the SAME cached worker process.
/// The worker keeps an in-process counter and stamps it onto `x-count`; the
/// upstream mocks match on that value. If each request spawned a fresh process
/// (or a oneshot) both requests would carry `x-count: 1`; because the worker
/// persists in `ProxyState::worker_cache`, the second request carries
/// `x-count: 2` — the mock that only matches `x-count: 2` proves reuse.
#[tokio::test]
async fn on_request_worker_step_is_cached_and_persists_across_requests() {
    if !have_python3() {
        eprintln!(
            "skipping on_request_worker_step_is_cached_and_persists_across_requests: python3 not \
             available"
        );
        return;
    }
    let worker = write_python_worker(
        "counter",
        r#"import sys, struct, json
count = 0
while True:
    hdr = sys.stdin.buffer.read(4)
    if len(hdr) < 4:
        break
    n = struct.unpack('<I', hdr)[0]
    _ = sys.stdin.buffer.read(n)
    count += 1
    resp = json.dumps({"action": "continue", "ops": [
        {"op": "set_header", "name": "x-count", "value": str(count)}
    ]}).encode()
    sys.stdout.buffer.write(struct.pack('<I', len(resp)))
    sys.stdout.buffer.write(resp)
    sys.stdout.buffer.flush()
"#,
    );

    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(|req: &Request| req.headers.get("x-count").and_then(|v| v.to_str().ok()) == Some("1"))
        .respond_with(ResponseTemplate::new(200).set_body_string("count-one"))
        .mount(&upstream)
        .await;
    Mock::given(method("POST"))
        .and(|req: &Request| req.headers.get("x-count").and_then(|v| v.to_str().ok()) == Some("2"))
        .respond_with(ResponseTemplate::new(200).set_body_string("count-two"))
        .mount(&upstream)
        .await;

    let cfg = format!(
        r#"
        [gateway]
        allow_scripts = true

        [[route]]
        id = "claude"
        upstream = "{up}"
          [[route.step]]
          name = "counter"
          type = "script"
          script_mode = "worker"
          cmd = ["python3", "{worker}"]
    "#,
        up = upstream.uri(),
        worker = worker.display(),
    );
    let (base, _guard) = spawn_gateway(cfg).await;
    let client = reqwest::Client::new();

    let r1 = client
        .post(format!("{base}/claude/v1/messages"))
        .body("ping")
        .send()
        .await
        .unwrap();
    assert_eq!(r1.status(), 200);
    assert_eq!(r1.text().await.unwrap(), "count-one");

    let r2 = client
        .post(format!("{base}/claude/v1/messages"))
        .body("ping")
        .send()
        .await
        .unwrap();
    assert_eq!(
        r2.status(),
        200,
        "second request should reuse the cached worker"
    );
    assert_eq!(
        r2.text().await.unwrap(),
        "count-two",
        "the persistent worker's in-process counter must advance to 2, proving process reuse"
    );

    let _ = std::fs::remove_dir_all(worker.parent().unwrap());
}

/// A worker that dies between requests (it exits after emitting exactly one
/// response) must be respawned so the NEXT request still succeeds. The step is
/// `on_error = fail_closed`, so without respawn the second request's dead-worker
/// call would surface as a 502; asserting 200 on the second request proves the
/// cache evicted the dead entry and respawned once (retrying the call).
#[tokio::test]
async fn on_request_dead_worker_is_respawned_for_next_request() {
    if !have_python3() {
        eprintln!(
            "skipping on_request_dead_worker_is_respawned_for_next_request: python3 not available"
        );
        return;
    }
    // Handles exactly one request, then exits — so every spawned process is
    // dead by the time the next request arrives.
    let worker = write_python_worker(
        "one_shot_worker",
        r#"import sys, struct, json
hdr = sys.stdin.buffer.read(4)
if len(hdr) >= 4:
    n = struct.unpack('<I', hdr)[0]
    _ = sys.stdin.buffer.read(n)
    resp = json.dumps({"action": "continue", "ops": [
        {"op": "set_header", "name": "x-alive", "value": "yes"}
    ]}).encode()
    sys.stdout.buffer.write(struct.pack('<I', len(resp)))
    sys.stdout.buffer.write(resp)
    sys.stdout.buffer.flush()
"#,
    );

    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(|req: &Request| {
            req.headers.get("x-alive").and_then(|v| v.to_str().ok()) == Some("yes")
        })
        .respond_with(ResponseTemplate::new(200).set_body_string("worker-ran"))
        .mount(&upstream)
        .await;

    let cfg = format!(
        r#"
        [gateway]
        allow_scripts = true

        [[route]]
        id = "claude"
        upstream = "{up}"
          [[route.step]]
          name = "fragile"
          type = "script"
          script_mode = "worker"
          on_error = "fail_closed"
          cmd = ["python3", "{worker}"]
    "#,
        up = upstream.uri(),
        worker = worker.display(),
    );
    let (base, _guard) = spawn_gateway(cfg).await;
    let client = reqwest::Client::new();

    let r1 = client
        .post(format!("{base}/claude/v1/messages"))
        .body("ping")
        .send()
        .await
        .unwrap();
    assert_eq!(r1.status(), 200);
    assert_eq!(r1.text().await.unwrap(), "worker-ran");

    // Worker 1 has exited; this request must respawn and still succeed.
    let r2 = client
        .post(format!("{base}/claude/v1/messages"))
        .body("ping")
        .send()
        .await
        .unwrap();
    assert_eq!(
        r2.status(),
        200,
        "a dead worker must be respawned so the next request still succeeds"
    );
    assert_eq!(r2.text().await.unwrap(), "worker-ran");

    let _ = std::fs::remove_dir_all(worker.parent().unwrap());
}

/// `sluice check` rejects a config with a `type = "script"` step when
/// `allow_scripts` is left at its default (`false`) — scripts are opt-in per
/// design doc §4.5, not a silent no-op.
#[test]
fn check_rejects_script_step_without_allow_scripts() {
    let dir = std::env::temp_dir().join(format!("sluice-script-check-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("script-not-allowed.toml");
    std::fs::write(
        &path,
        r#"
        [[route]]
        id = "claude"
        upstream = "https://api.anthropic.com"
          [[route.step]]
          type = "script"
          cmd = ["/bin/true"]
    "#,
    )
    .unwrap();

    let status = StdCommand::new(env!("CARGO_BIN_EXE_sluice"))
        .args(["check", "--config", path.to_str().unwrap()])
        .status()
        .unwrap();
    assert!(!status.success());

    let _ = std::fs::remove_dir_all(&dir);
}
