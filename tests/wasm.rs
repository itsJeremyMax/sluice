//! Integration tests for M14 Task 2: wiring `type = "wasm"` steps into the
//! proxy's on_request/on_response dispatch, backed by the compile-once
//! module cache on `ProxyState` (`proxy::get_or_compile_wasm`).

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

/// Escape raw bytes as a WAT string-literal body using `\XX` hex escapes for
/// every byte (mirrors `step::wasm::WasmStep`'s own unit-test helper), so the
/// embedded JSON payload's quotes never have to be hand-escaped for WAT's
/// text-format string syntax.
fn wat_escape(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("\\{b:02x}")).collect()
}

/// A guest module implementing the alloc/run ABI `WasmStep` expects (see
/// `step::wasm::WasmStep`'s doc comment): `alloc` is a trivial bump allocator
/// over a global starting past the data segment holding a canned directive
/// JSON, and `run` ignores its input entirely and always returns the packed
/// ptr/len of that canned JSON — a `continue` directive with a `set_header`
/// op, the wasm analogue of the `set_header` scripts/url steps other
/// integration tests exercise.
fn set_header_module_wat() -> String {
    let payload = br#"{"action":"continue","ops":[{"op":"set_header","name":"x-wasm-tag","value":"from-wasm"}]}"#;
    let data_offset: i64 = 8;
    let bump_start = data_offset + payload.len() as i64;
    let packed: i64 = (data_offset << 32) | (payload.len() as i64);
    format!(
        r#"(module
  (memory (export "memory") 1)
  (data (i32.const {data_offset}) "{escaped}")
  (global $bump (mut i32) (i32.const {bump_start}))
  (func (export "alloc") (param $size i32) (result i32)
    (local $ret i32)
    global.get $bump
    local.set $ret
    global.get $bump
    local.get $size
    i32.add
    global.set $bump
    local.get $ret)
  (func (export "run") (param $ptr i32) (param $len i32) (result i64)
    i64.const {packed}))
"#,
        escaped = wat_escape(payload),
    )
}

/// Compile the embedded WAT to real wasm bytes via `wat::parse_str` and
/// write them to a temp file, so the config's `wasm = "<path>"` points at an
/// actual compiled `.wasm` module exactly like a real deployment would.
fn write_wasm_module() -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("sluice-wasm-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("set_header.wasm");
    let bytes = wat::parse_str(set_header_module_wat()).expect("WAT compiles to wasm bytes");
    std::fs::write(&path, bytes).unwrap();
    path
}

/// An `on_request` wasm step that reads the envelope over the alloc/run ABI
/// and returns a `set_header` directive: the upstream must see the header
/// the guest module added, proving the wasm runtime is wired into the
/// request pipeline the same way `type = "url"`/`type = "script"` steps are.
/// Two requests are sent through the same route so this also exercises the
/// module cache being reused rather than recompiled per call — a repeat hit
/// would panic/fail here just the same as a first hit if the cache path were
/// broken (e.g. an `Arc<WasmStep>` dropped/rebuilt with a stale `Store`).
#[tokio::test]
async fn on_request_wasm_step_set_header_reaches_upstream() {
    let wasm_path = write_wasm_module();

    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(|req: &Request| {
            req.headers.get("x-wasm-tag").and_then(|v| v.to_str().ok()) == Some("from-wasm")
        })
        .respond_with(ResponseTemplate::new(200).set_body_string("upstream saw it"))
        .mount(&upstream)
        .await;

    let cfg = format!(
        r#"
        [[route]]
        id = "claude"
        upstream = "{up}"
          [[route.step]]
          name = "tagger"
          type = "wasm"
          wasm = "{wasm}"
    "#,
        up = upstream.uri(),
        wasm = wasm_path.display(),
    );
    let (base, _guard) = spawn_gateway(cfg).await;
    let client = reqwest::Client::new();

    for _ in 0..2 {
        let resp = client
            .post(format!("{base}/claude/v1/messages"))
            .body("ping")
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 200);
        assert_eq!(resp.text().await.unwrap(), "upstream saw it");
    }

    let _ = std::fs::remove_dir_all(wasm_path.parent().unwrap());
}

/// A guest module implementing the alloc/run ABI that always returns a
/// genuine no-op `{"action":"continue"}` directive (no `ops`) — the actual
/// no-op fixture, as opposed to `set_header_module_wat()` above which tags
/// the response with a `set_header` op to prove the wasm runtime is wired
/// up. This is the source of truth for `examples/noop-guardrail.wasm`,
/// regenerated by `regenerate_noop_guardrail_fixture` below.
fn noop_continue_module_wat() -> String {
    let payload = br#"{"action":"continue"}"#;
    let data_offset: i64 = 8;
    let bump_start = data_offset + payload.len() as i64;
    let packed: i64 = (data_offset << 32) | (payload.len() as i64);
    format!(
        r#"(module
  (memory (export "memory") 1)
  (data (i32.const {data_offset}) "{escaped}")
  (global $bump (mut i32) (i32.const {bump_start}))
  (func (export "alloc") (param $size i32) (result i32)
    (local $ret i32)
    global.get $bump
    local.set $ret
    global.get $bump
    local.get $size
    i32.add
    global.set $bump
    local.get $ret)
  (func (export "run") (param $ptr i32) (param $len i32) (result i64)
    i64.const {packed}))
"#,
        escaped = wat_escape(payload),
    )
}

/// Regenerates `examples/noop-guardrail.wasm` from `noop_continue_module_wat()`
/// above. This is the fixture's documented provenance/regeneration path —
/// the checked-in binary was previously a genuinely broken placeholder whose
/// `run` returned a zero-length output (`i64.const 0`), which
/// `step::wasm::WasmStep::invoke` then failed to parse as JSON
/// (`serde_json::from_slice` on an empty buffer), making every wasm step
/// pointed at it error unconditionally (see `examples/wasm-step.toml`'s
/// `on_error = "fail_closed"` 502'ing every request, and the workaround this
/// fixes in `benchmarks/src/scenarios.rs`).
///
/// Marked `#[ignore]` since it writes a file into the source tree rather
/// than a temp dir — not something every `cargo test` run should do. Run it
/// explicitly with:
/// `cargo test --test wasm regenerate_noop_guardrail_fixture -- --ignored`
/// whenever the fixture needs to be rebuilt.
#[test]
#[ignore]
fn regenerate_noop_guardrail_fixture() {
    let bytes = wat::parse_str(noop_continue_module_wat()).expect("WAT compiles to wasm bytes");
    let out = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("examples")
        .join("noop-guardrail.wasm");
    std::fs::write(&out, &bytes).expect("write examples/noop-guardrail.wasm");
    eprintln!("wrote {} bytes to {}", bytes.len(), out.display());
}

/// Pins the shipped `examples/noop-guardrail.wasm` fixture to the ABI end to
/// end: loads it through the exact same `WasmStep::from_path` entrypoint the
/// gateway uses, runs it, and asserts it actually returns
/// `Directive::Continue` with no ops — so the fixture can't silently rot
/// back into the "always errors" bug this test guards against (see
/// `regenerate_noop_guardrail_fixture`'s doc comment for the history).
#[tokio::test]
async fn shipped_noop_guardrail_fixture_returns_continue() {
    use sluice::directive::Directive;
    use sluice::envelope::build_request_envelope;
    use sluice::http_msg::HttpMsg;
    use sluice::step::wasm::WasmStep;
    use std::time::Duration;

    let wasm_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("examples")
        .join("noop-guardrail.wasm");
    let step = WasmStep::from_path(&wasm_path).expect("shipped fixture compiles");

    let msg = HttpMsg {
        method: "POST".into(),
        path: "/v1/messages".into(),
        headers: std::collections::BTreeMap::new(),
        body_b64: String::new(),
    };
    let env = build_request_envelope(
        "claude",
        "wasm-guardrail",
        &msg,
        &serde_json::Map::new(),
        "corr-noop-guardrail",
        None,
    );

    let directive = step
        .run(&env, Duration::from_millis(1000))
        .await
        .expect("shipped noop-guardrail.wasm must run without error");
    match directive {
        Directive::Continue { ops } => {
            assert!(ops.is_empty(), "expected a genuine no-op, got ops: {ops:?}");
        }
        other => panic!("expected Directive::Continue, got {other:?}"),
    }
}

/// `sluice check` accepts a `type = "wasm"` step that has a `wasm` module
/// path configured AND actually compiles (M14 Task 1 validation, hardened by
/// the M14 final review's Fix B to also compile the module at load time —
/// confirmed end-to-end here since this is the CLI entrypoint operators
/// actually run).
#[test]
fn check_accepts_valid_wasm_config() {
    let dir = std::env::temp_dir().join(format!("sluice-wasm-check-ok-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let wasm_path = dir.join("redact.wasm");
    std::fs::write(
        &wasm_path,
        wat::parse_str(set_header_module_wat()).expect("WAT compiles to wasm bytes"),
    )
    .unwrap();
    let path = dir.join("wasm-ok.toml");
    std::fs::write(
        &path,
        format!(
            r#"
        [[route]]
        id = "claude"
        upstream = "https://api.anthropic.com"
          [[route.step]]
          type = "wasm"
          wasm = "{}"
    "#,
            wasm_path.display()
        ),
    )
    .unwrap();

    let status = StdCommand::new(env!("CARGO_BIN_EXE_sluice"))
        .args(["check", "--config", path.to_str().unwrap()])
        .status()
        .unwrap();
    assert!(status.success());

    let _ = std::fs::remove_dir_all(&dir);
}

/// Companion to `check_accepts_valid_wasm_config`: `sluice check` must
/// reject a `type = "wasm"` step whose `wasm` path names a file that isn't a
/// valid compiled module at all (M14 final review Fix B) — not just a
/// missing `wasm` field (see `check_rejects_wasm_step_without_wasm_path`
/// below).
#[test]
fn check_rejects_uncompilable_wasm_module() {
    let dir = std::env::temp_dir().join(format!(
        "sluice-wasm-check-uncompilable-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let wasm_path = dir.join("garbage.wasm");
    std::fs::write(&wasm_path, b"this is not a wasm module").unwrap();
    let path = dir.join("wasm-bad.toml");
    std::fs::write(
        &path,
        format!(
            r#"
        [[route]]
        id = "claude"
        upstream = "https://api.anthropic.com"
          [[route.step]]
          type = "wasm"
          wasm = "{}"
    "#,
            wasm_path.display()
        ),
    )
    .unwrap();

    let status = StdCommand::new(env!("CARGO_BIN_EXE_sluice"))
        .args(["check", "--config", path.to_str().unwrap()])
        .status()
        .unwrap();
    assert!(!status.success());

    let _ = std::fs::remove_dir_all(&dir);
}

/// `sluice check` rejects a `type = "wasm"` step with no `wasm` path — the
/// config field is required for wasm steps the same way `url`/`cmd` are
/// required for url/script steps.
#[test]
fn check_rejects_wasm_step_without_wasm_path() {
    let dir = std::env::temp_dir().join(format!("sluice-wasm-check-bad-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("wasm-no-path.toml");
    std::fs::write(
        &path,
        r#"
        [[route]]
        id = "claude"
        upstream = "https://api.anthropic.com"
          [[route.step]]
          type = "wasm"
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
