//! Benchmark scenario definitions: the fixed set of route shapes the
//! harness measures (a plain pass-through, a cross-provider translation, a
//! script step, a wasm step, and a streaming pass-through), the sluice TOML
//! config each one generates, and the in-process sluice launcher that turns
//! that TOML into a live listener.
//!
//! Every scenario proxies onto the same simulated upstream (`upstream::Sim`)
//! started by the caller; this module only knows how to point a fresh
//! sluice instance at that upstream's address for each scenario shape.

use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

/// The fixed set of scenarios the benchmark harness measures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scenario {
    Passthrough,
    Translation,
    ScriptStep,
    WasmStep,
    Streaming,
}

impl Scenario {
    /// Every scenario, in the order they're reported.
    pub fn all() -> Vec<Scenario> {
        vec![
            Scenario::Passthrough,
            Scenario::Translation,
            Scenario::ScriptStep,
            Scenario::WasmStep,
            Scenario::Streaming,
        ]
    }

    /// Kebab-case scenario name, used for chart labels and temp-dir naming.
    pub fn name(&self) -> &'static str {
        match self {
            Scenario::Passthrough => "passthrough",
            Scenario::Translation => "translation",
            Scenario::ScriptStep => "script-step",
            Scenario::WasmStep => "wasm-step",
            Scenario::Streaming => "streaming",
        }
    }

    /// The path this scenario's client request hits on the sluice listener
    /// (route id `bench`).
    ///
    /// Translation's route speaks `from = "openai"`, so per
    /// docs/translation.md ("The client always calls the route using
    /// `from`'s own endpoint shape") the client-facing path is OpenAI's own
    /// endpoint shape, `/v1/chat/completions`, under the route id: this
    /// benchmark's translate route (from = openai, to = anthropic) is client
    /// facing on `/bench/v1/chat/completions`.
    pub fn request_path(&self) -> &'static str {
        match self {
            Scenario::Passthrough | Scenario::ScriptStep | Scenario::WasmStep => {
                "/bench/v1/messages"
            }
            Scenario::Translation => "/bench/v1/chat/completions",
            Scenario::Streaming => "/bench/v1/stream",
        }
    }

    /// The request body posted to `request_path()`.
    pub fn request_body(&self) -> &'static str {
        match self {
            Scenario::Translation => {
                r#"{"model":"gpt-5","messages":[{"role":"user","content":"benchmark"}]}"#
            }
            _ => "{}",
        }
    }

    /// The equivalent path on the bare simulated upstream (no sluice in
    /// front), used to measure baseline (no-gateway) latency.
    pub fn baseline_path(&self) -> &'static str {
        match self {
            Scenario::Streaming => "/v1/stream",
            _ => "/v1/messages",
        }
    }

    /// Whether this scenario's request is a streaming (SSE) response.
    pub fn is_streaming(&self) -> bool {
        matches!(self, Scenario::Streaming)
    }

    /// Whether this scenario can run on the current platform. `ScriptStep`
    /// spawns `sh`, so it's unix-only.
    pub fn available(&self) -> bool {
        match self {
            Scenario::ScriptStep => cfg!(unix),
            _ => true,
        }
    }
}

/// The noop script fixture content, mirroring `tests/scripts.rs`'s
/// `write_header_script` fixture: drain stdin (the envelope JSON the
/// gateway writes), then print a `continue` directive to stdout.
const NOOP_SCRIPT: &str = "#!/bin/sh\ncat >/dev/null\necho '{\"action\":\"continue\"}'\n";

/// Write the noop script fixture into `dir` (as `noop_step.sh`), marking it
/// executable on unix. Only called for `Scenario::ScriptStep`, which is
/// unavailable on non-unix platforms.
fn write_noop_script(dir: &Path) {
    let path = dir.join("noop_step.sh");
    std::fs::write(&path, NOOP_SCRIPT).expect("write noop script");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
            .expect("chmod noop script");
    }
}

/// Build the sluice TOML config for `scenario`, pointing at `upstream` and
/// listening on `listen`. `workspace_root` locates the wasm fixture for
/// `WasmStep`; `tmp` is where `ScriptStep`'s script fixture was written.
fn route_toml(
    scenario: &Scenario,
    upstream: SocketAddr,
    listen: &str,
    workspace_root: &Path,
    tmp: &Path,
) -> String {
    match scenario {
        Scenario::Passthrough | Scenario::Streaming => format!(
            r#"[gateway]
listen = "{listen}"

[[route]]
id       = "bench"
upstream = "http://{upstream}"
"#
        ),
        Scenario::Translation => format!(
            r#"[gateway]
listen = "{listen}"

[[route]]
id       = "bench"
upstream = "http://{upstream}"

  [route.translate]
  from  = "openai"
  to    = "anthropic"
  model = "claude-opus-4-1-20250805"
"#
        ),
        Scenario::ScriptStep => {
            let script = tmp.join("noop_step.sh");
            format!(
                r#"[gateway]
listen        = "{listen}"
allow_scripts = true

[[route]]
id       = "bench"
upstream = "http://{upstream}"

  [[route.step]]
  name = "noop-script"
  type = "script"
  hook = "on_request"
  cmd  = ["sh", "{script}"]
  timeout_ms = 2000
  on_error   = "fail_closed"
"#,
                script = script.display(),
            )
        }
        Scenario::WasmStep => {
            let wasm = workspace_root.join("examples").join("noop-guardrail.wasm");
            format!(
                r#"[gateway]
listen = "{listen}"

[[route]]
id       = "bench"
upstream = "http://{upstream}"

  [[route.step]]
  name = "noop-wasm"
  type = "wasm"
  hook = "on_request"
  wasm = "{wasm}"
  timeout_ms = 2000
  on_error   = "fail_closed"
"#,
                wasm = wasm.display(),
            )
        }
    }
}

/// Pick a free port by binding :0 and immediately dropping the listener.
/// Racy in principle; fine for a local benchmark harness.
fn free_listen_addr() -> String {
    let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    format!("127.0.0.1:{}", l.local_addr().unwrap().port())
}

/// Asynchronously wait (via short polling sleeps) until `addr` accepts a TCP
/// connection, or panic after 100 * 20ms = 2s. Uses tokio primitives to avoid
/// blocking worker threads in async runtimes.
async fn wait_until_accepting(addr: SocketAddr) {
    for _ in 0..100 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("sluice listener at {addr} never started accepting connections");
}

/// Launch a fresh in-process sluice instance configured for `scenario`,
/// proxying onto `upstream`. Writes the scenario's TOML (and, for
/// `ScriptStep`, its script fixture) to a temp dir, spawns
/// `sluice::server::serve` on the current tokio runtime, and waits until
/// the listener is accepting before returning its address.
pub async fn start_sluice(
    scenario: &Scenario,
    upstream: SocketAddr,
    workspace_root: &Path,
) -> SocketAddr {
    let listen = free_listen_addr();
    let tmp = std::env::temp_dir().join(format!(
        "sluice-bench-{}-{}",
        std::process::id(),
        scenario.name()
    ));
    std::fs::create_dir_all(&tmp).expect("mk temp dir");
    if matches!(scenario, Scenario::ScriptStep) {
        write_noop_script(&tmp);
    }
    let config_path = tmp.join("sluice.toml");
    std::fs::write(
        &config_path,
        route_toml(scenario, upstream, &listen, workspace_root, &tmp),
    )
    .expect("write config");

    let cfg = sluice::config::load::load_file(&config_path).expect("bench config valid");
    let source = sluice::config::watch::ConfigSource::File(config_path);
    tokio::spawn(async move {
        if let Err(e) = sluice::server::serve(cfg, source, None).await {
            eprintln!("sluice serve failed: {e}");
        }
    });

    let addr: SocketAddr = listen.parse().expect("listen addr");
    wait_until_accepting(addr).await;
    addr
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::upstream::{self, Sim};

    #[tokio::test(flavor = "multi_thread")]
    async fn passthrough_scenario_round_trips_through_sluice() {
        let up = upstream::start(Sim::default_bench());
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
        let sluice_addr = start_sluice(&Scenario::Passthrough, up, &root).await;
        let resp = reqwest::Client::new()
            .post(format!(
                "http://{sluice_addr}{}",
                Scenario::Passthrough.request_path()
            ))
            .body(Scenario::Passthrough.request_body())
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let v: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(v["type"], "message");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn translation_scenario_round_trips_through_sluice() {
        let up = upstream::start(Sim::default_bench());
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
        let sluice_addr = start_sluice(&Scenario::Translation, up, &root).await;
        let resp = reqwest::Client::new()
            .post(format!(
                "http://{sluice_addr}{}",
                Scenario::Translation.request_path()
            ))
            .header("content-type", "application/json")
            .body(Scenario::Translation.request_body())
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let v: serde_json::Value = resp.json().await.unwrap();
        // Translated back into openai's response shape (from=openai).
        assert_eq!(v["object"], "chat.completion");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn wasm_step_scenario_round_trips_through_sluice() {
        let up = upstream::start(Sim::default_bench());
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
        let sluice_addr = start_sluice(&Scenario::WasmStep, up, &root).await;
        let resp = reqwest::Client::new()
            .post(format!(
                "http://{sluice_addr}{}",
                Scenario::WasmStep.request_path()
            ))
            .body(Scenario::WasmStep.request_body())
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn streaming_scenario_round_trips_through_sluice() {
        let up = upstream::start(Sim::default_bench());
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
        let sluice_addr = start_sluice(&Scenario::Streaming, up, &root).await;
        let body = reqwest::Client::new()
            .post(format!(
                "http://{sluice_addr}{}",
                Scenario::Streaming.request_path()
            ))
            .body(Scenario::Streaming.request_body())
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert!(body.ends_with("data: [DONE]\n\n"));
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread")]
    async fn script_step_scenario_round_trips_through_sluice() {
        assert!(Scenario::ScriptStep.available());
        let up = upstream::start(Sim::default_bench());
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
        let sluice_addr = start_sluice(&Scenario::ScriptStep, up, &root).await;
        let resp = reqwest::Client::new()
            .post(format!(
                "http://{sluice_addr}{}",
                Scenario::ScriptStep.request_path()
            ))
            .body(Scenario::ScriptStep.request_body())
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
    }

    #[test]
    fn scenario_names_are_kebab_case() {
        assert_eq!(Scenario::Passthrough.name(), "passthrough");
        assert_eq!(Scenario::Translation.name(), "translation");
        assert_eq!(Scenario::ScriptStep.name(), "script-step");
        assert_eq!(Scenario::WasmStep.name(), "wasm-step");
        assert_eq!(Scenario::Streaming.name(), "streaming");
    }

    #[test]
    fn all_returns_five_scenarios() {
        assert_eq!(Scenario::all().len(), 5);
    }

    #[test]
    fn only_streaming_is_a_streaming_scenario() {
        for scenario in Scenario::all() {
            assert_eq!(
                scenario.is_streaming(),
                scenario == Scenario::Streaming,
                "{scenario:?}"
            );
        }
    }

    #[test]
    fn baseline_path_matches_the_upstream_shape_the_scenario_exercises() {
        for scenario in Scenario::all() {
            let expected = if scenario == Scenario::Streaming {
                "/v1/stream"
            } else {
                "/v1/messages"
            };
            assert_eq!(scenario.baseline_path(), expected, "{scenario:?}");
        }
    }

    #[test]
    fn script_step_is_unavailable_on_non_unix() {
        assert_eq!(Scenario::ScriptStep.available(), cfg!(unix));
        for scenario in Scenario::all() {
            if scenario != Scenario::ScriptStep {
                assert!(scenario.available(), "{scenario:?}");
            }
        }
    }
}
