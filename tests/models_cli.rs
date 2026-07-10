//! Integration tests for M8 Task 3: `sluice models list/update/diff`, driven
//! as real subprocesses (`CARGO_BIN_EXE_sluice`) mirroring the
//! `tests/check_cli.rs`/`tests/directory.rs` pattern.
//!
//! M18 Task 1 adds the `--from-network` tests below, mirroring
//! `tests/test_cli.rs`'s pattern: an in-process `wiremock::MockServer`
//! (started from a `#[tokio::test]`, since the test binary itself owns a
//! tokio runtime) stands in for the live models.dev dataset, and the
//! `sluice` subprocess is pointed at it via the hidden `--network-url`
//! override.

use std::process::Command;

use serde_json::Value;
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, ResponseTemplate};

/// A fresh scratch directory under the OS temp dir, unique per test run.
fn scratch_dir(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "sluice-models-cli-test-{}-{}-{}",
        std::process::id(),
        tag,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

/// Write a tiny models.dev-shaped fixture (`models/` + `providers/<id>/`)
/// with a single resolved model, id `"fixture-model"`, provider
/// `"fixture-provider"`, `cost.input` set to `cost_input`.
fn write_fixture(dir: &std::path::Path, cost_input: f64) {
    let models_dir = dir.join("models");
    let provider_dir = dir.join("providers").join("fixture-provider");
    std::fs::create_dir_all(&models_dir).unwrap();
    std::fs::create_dir_all(&provider_dir).unwrap();

    std::fs::write(
        models_dir.join("fixture-model-base.toml"),
        "modalities = [\"text\"]\n[limit]\ncontext = 50000\noutput = 4096\n",
    )
    .unwrap();
    std::fs::write(
        provider_dir.join("fixture-model.toml"),
        format!(
            "base_model = \"fixture-model-base\"\nstatus = \"stable\"\ntool_call = true\n[cost]\ninput = {cost_input}\noutput = 2.0\n"
        ),
    )
    .unwrap();
}

/// Write a fixture that overrides one of the embedded seed's own models
/// (`claude-opus-4-1-20250805`, cost.input = 15.0) with a different price,
/// so `models diff` against the embedded baseline reports it as changed.
fn write_price_change_fixture(dir: &std::path::Path) {
    let models_dir = dir.join("models");
    let provider_dir = dir.join("providers").join("anthropic");
    std::fs::create_dir_all(&models_dir).unwrap();
    std::fs::create_dir_all(&provider_dir).unwrap();

    std::fs::write(
        models_dir.join("claude-opus-4-1-base.toml"),
        "modalities = [\"text\", \"image\"]\n[limit]\ncontext = 200000\noutput = 32000\n",
    )
    .unwrap();
    std::fs::write(
        provider_dir.join("claude-opus-4-1-20250805.toml"),
        "base_model = \"claude-opus-4-1-base\"\nstatus = \"stable\"\ntool_call = true\n[cost]\ninput = 999.0\noutput = 75.0\n",
    )
    .unwrap();
}

#[test]
fn models_list_json_contains_seeded_model_id() {
    let output = Command::new(env!("CARGO_BIN_EXE_sluice"))
        .args(["models", "list", "--json"])
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(
        stdout.contains("claude-opus-4-1-20250805"),
        "expected seeded model id in output, got:\n{stdout}"
    );
}

#[test]
fn models_list_text_table_contains_seeded_model_id() {
    let output = Command::new(env!("CARGO_BIN_EXE_sluice"))
        .args(["models", "list"])
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("claude-opus-4-1-20250805"), "{stdout}");
    assert!(stdout.contains("anthropic"), "{stdout}");
}

#[test]
fn models_list_filters_by_provider() {
    let output = Command::new(env!("CARGO_BIN_EXE_sluice"))
        .args(["models", "list", "--json", "--provider", "openai"])
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("gpt-5"), "{stdout}");
    assert!(
        !stdout.contains("claude-opus-4-1-20250805"),
        "provider filter should exclude anthropic models:\n{stdout}"
    );
}

#[test]
fn models_update_writes_resolved_registry_json() {
    let dir = scratch_dir("update");
    std::fs::create_dir_all(&dir).unwrap();
    write_fixture(&dir, 1.5);
    let out_path = dir.join("out.json");

    let output = Command::new(env!("CARGO_BIN_EXE_sluice"))
        .args([
            "models",
            "update",
            "--source",
            dir.to_str().unwrap(),
            "--out",
            out_path.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");

    let written = std::fs::read_to_string(&out_path).expect("update must write the out file");
    assert!(written.contains("fixture-model"), "{written}");
    assert!(written.contains("fixture-provider"), "{written}");
    assert!(written.contains("1.5"), "{written}");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn models_update_does_not_touch_default_registry_path() {
    // Regression guard: `models update` must write ONLY the file named by
    // `--out`, never `registry::DEFAULT_REGISTRY_PATH` relative to the
    // process's cwd (which would silently start shadowing the embedded seed
    // for every other `sluice` invocation run from this checkout). Actually
    // runs `models update` (with a temp `--out`, so this test never touches
    // the real default path) and asserts both halves: the named `--out` file
    // was written, AND the default path was not created as a side effect.
    let dir = scratch_dir("update-no-default-touch");
    std::fs::create_dir_all(&dir).unwrap();
    write_fixture(&dir, 2.5);
    let out_path = dir.join("custom-out.json");

    let repo_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let default_path = repo_root.join("sluice-models.json");
    assert!(
        !default_path.exists(),
        "precondition: sluice-models.json must not already exist in the repo root"
    );

    let output = Command::new(env!("CARGO_BIN_EXE_sluice"))
        .current_dir(repo_root)
        .args([
            "models",
            "update",
            "--source",
            dir.to_str().unwrap(),
            "--out",
            out_path.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");

    assert!(
        out_path.is_file(),
        "models update must write the file named by --out"
    );
    let written = std::fs::read_to_string(&out_path).unwrap();
    assert!(written.contains("fixture-model"), "{written}");

    assert!(
        !default_path.exists(),
        "models update must not create the default registry path as a side effect"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn models_diff_prints_changed_model_id() {
    let dir = scratch_dir("diff");
    std::fs::create_dir_all(&dir).unwrap();
    write_price_change_fixture(&dir);

    let output = Command::new(env!("CARGO_BIN_EXE_sluice"))
        .args(["models", "diff", "--source", dir.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("claude-opus-4-1-20250805"), "{stdout}");
    assert!(stdout.contains("cost_input"), "{stdout}");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn models_diff_reports_added_model_not_in_current_registry() {
    let dir = scratch_dir("diff-added");
    std::fs::create_dir_all(&dir).unwrap();
    write_fixture(&dir, 1.0);

    let output = Command::new(env!("CARGO_BIN_EXE_sluice"))
        .args(["models", "diff", "--source", dir.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("added"), "{stdout}");
    assert!(stdout.contains("fixture-model"), "{stdout}");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn models_diff_does_not_write_any_file() {
    let dir = scratch_dir("diff-no-write");
    std::fs::create_dir_all(&dir).unwrap();
    write_price_change_fixture(&dir);

    let status = Command::new(env!("CARGO_BIN_EXE_sluice"))
        .args(["models", "diff", "--source", dir.to_str().unwrap()])
        .status()
        .unwrap();
    assert!(status.success());

    // `diff` must not write into its own source directory or anywhere else
    // implied by it.
    assert!(!dir.join("sluice-models.json").exists());

    let _ = std::fs::remove_dir_all(&dir);
}

/// A models.dev `api.json`-shaped payload: 2 providers, 3 models. Exercises
/// the object->flat-vec modalities flatten rule (see
/// `registry::models_dev::flatten_modalities`): `fixture-model`'s
/// `input:["text","image"], output:["text"]` must flatten to
/// `["text","image"]` (dedup, input-first stable order); `fixture-model-2`'s
/// `input:["text"], output:[]` must flatten to `["text"]`; `other-model`'s
/// `input:[], output:["text"]` must flatten to `["text"]`. Also exercises a
/// model missing `cost`/`limit` entirely (`fixture-model-2` has no `limit`,
/// `other-model` has no `cost`/`limit`/`status`), which must yield `None`
/// fields, not a parse error.
const NETWORK_FIXTURE_JSON: &str = r#"
{
    "fixture-provider": {
        "id": "fixture-provider",
        "name": "Fixture Provider",
        "models": {
            "fixture-model": {
                "cost": {"input": 1.5, "output": 3.0},
                "limit": {"context": 50000, "output": 4096},
                "modalities": {"input": ["text", "image"], "output": ["text"]},
                "tool_call": true,
                "status": "stable"
            },
            "fixture-model-2": {
                "cost": {"input": 0.5},
                "modalities": {"input": ["text"], "output": []},
                "tool_call": false
            }
        }
    },
    "second-provider": {
        "id": "second-provider",
        "name": "Second Provider",
        "models": {
            "other-model": {
                "modalities": {"input": [], "output": ["text"]}
            }
        }
    }
}
"#;

/// A models.dev `api.json`-shaped payload overriding the embedded seed's own
/// `claude-opus-4-1-20250805` (anthropic, cost.input = 15.0 in the seed)
/// with a different price, so `models diff --from-network` against the
/// embedded baseline reports it as changed — the network-fetch mirror of
/// `write_price_change_fixture`.
const NETWORK_PRICE_CHANGE_JSON: &str = r#"
{
    "anthropic": {
        "id": "anthropic",
        "name": "Anthropic",
        "models": {
            "claude-opus-4-1-20250805": {
                "cost": {"input": 999.0, "output": 75.0},
                "limit": {"context": 200000, "output": 32000},
                "modalities": {"input": ["text", "image"], "output": ["text"]},
                "tool_call": true,
                "status": "stable"
            }
        }
    }
}
"#;

/// Find the `(id, provider)`-matching object in a `models update`-written
/// JSON array (parsed generically as `serde_json::Value` rather than
/// `ModelFacts`, so the test asserts the actual on-disk shape rather than
/// round-tripping through the same struct the production code uses).
fn find_model<'a>(models: &'a [Value], id: &str, provider: &str) -> &'a Value {
    models
        .iter()
        .find(|m| m["id"] == id && m["provider"] == provider)
        .unwrap_or_else(|| panic!("expected model id={id} provider={provider} in {models:?}"))
}

#[tokio::test]
async fn models_update_from_network_writes_expected_resolved_overlay() {
    let mock_server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_string(NETWORK_FIXTURE_JSON))
        .mount(&mock_server)
        .await;

    let dir = scratch_dir("update-from-network");
    std::fs::create_dir_all(&dir).unwrap();
    let out_path = dir.join("out.json");

    let output = Command::new(env!("CARGO_BIN_EXE_sluice"))
        .args([
            "models",
            "update",
            "--from-network",
            "--network-url",
            &mock_server.uri(),
            "--out",
            out_path.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");

    let written = std::fs::read_to_string(&out_path).expect("update must write the out file");
    let models: Vec<Value> = serde_json::from_str(&written).expect("written JSON must parse");
    assert_eq!(models.len(), 3, "{written}");

    let fixture_model = find_model(&models, "fixture-model", "fixture-provider");
    assert_eq!(fixture_model["cost_input"], 1.5);
    assert_eq!(fixture_model["cost_output"], 3.0);
    assert_eq!(fixture_model["context"], 50000);
    assert_eq!(fixture_model["max_output"], 4096);
    assert_eq!(fixture_model["tool_call"], true);
    assert_eq!(fixture_model["status"], "stable");
    assert_eq!(
        fixture_model["modalities"],
        serde_json::json!(["text", "image"]),
        "modalities must flatten input-then-output, deduped, stable order: {written}"
    );

    let fixture_model_2 = find_model(&models, "fixture-model-2", "fixture-provider");
    assert_eq!(fixture_model_2["cost_input"], 0.5);
    assert_eq!(fixture_model_2["cost_output"], Value::Null);
    assert_eq!(fixture_model_2["context"], Value::Null);
    assert_eq!(fixture_model_2["tool_call"], false);
    assert_eq!(fixture_model_2["status"], Value::Null);
    assert_eq!(
        fixture_model_2["modalities"],
        serde_json::json!(["text"]),
        "{written}"
    );

    let other_model = find_model(&models, "other-model", "second-provider");
    assert_eq!(other_model["cost_input"], Value::Null);
    assert_eq!(other_model["cost_output"], Value::Null);
    assert_eq!(other_model["modalities"], serde_json::json!(["text"]));

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn models_diff_from_network_renders_expected_delta_vs_embedded_seed() {
    let mock_server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_string(NETWORK_PRICE_CHANGE_JSON))
        .mount(&mock_server)
        .await;

    let output = Command::new(env!("CARGO_BIN_EXE_sluice"))
        .args([
            "models",
            "diff",
            "--from-network",
            "--network-url",
            &mock_server.uri(),
        ])
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("claude-opus-4-1-20250805"), "{stdout}");
    assert!(stdout.contains("cost_input"), "{stdout}");
    assert!(stdout.contains("changed"), "{stdout}");
}

#[tokio::test]
async fn models_update_from_network_500_exits_nonzero_and_leaves_overlay_unchanged() {
    let mock_server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&mock_server)
        .await;

    let dir = scratch_dir("update-from-network-500");
    std::fs::create_dir_all(&dir).unwrap();
    let out_path = dir.join("out.json");
    let sentinel = "pre-existing-content-must-survive-a-failed-update";
    std::fs::write(&out_path, sentinel).unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_sluice"))
        .args([
            "models",
            "update",
            "--from-network",
            "--network-url",
            &mock_server.uri(),
            "--out",
            out_path.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        !output.status.success(),
        "500 from the mock must exit non-zero: {output:?}"
    );
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(!stderr.is_empty(), "expected an error message on stderr");

    let after = std::fs::read_to_string(&out_path).unwrap();
    assert_eq!(
        after, sentinel,
        "a failed --from-network update must not partially write the overlay file"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// FIX-4 (audit finding m18): `fetch_network` must reject a models.dev
/// response body larger than
/// [`sluice::registry::models_dev::MAX_MODELS_DEV_BYTES`] with a clean
/// `RegistryError::ResponseTooLarge`, never buffering the whole oversized
/// body into memory (the pre-fix behavior: unbounded `response.text()`).
/// The mock body here is genuinely larger than the cap (not just a spoofed
/// `content-length`), so it exercises the streamed running-total bound as
/// well as the upfront `content-length` check.
///
/// Lives here (integration binary) rather than in the `models_dev` unit
/// test module deliberately: an in-lib `#[tokio::test]` shares its process
/// with the crate's wasmtime epoch-thread tests, whose teardown SIGABRTs
/// when a tokio runtime is also present — a pre-existing full-suite issue
/// this test must not drag into `cargo test --lib`.
#[tokio::test]
async fn fetch_network_rejects_response_larger_than_size_cap() {
    use sluice::registry::models_dev::{fetch_network, MAX_MODELS_DEV_BYTES};
    use sluice::registry::RegistryError;

    let mock_server = MockServer::start().await;
    let oversized_body = "a".repeat(MAX_MODELS_DEV_BYTES + 1);
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_string(oversized_body))
        .mount(&mock_server)
        .await;

    let client = reqwest::Client::new();
    let err = fetch_network(&client, &mock_server.uri())
        .await
        .expect_err("oversized response body must be rejected");

    match err {
        RegistryError::ResponseTooLarge { limit, .. } => {
            assert_eq!(limit, MAX_MODELS_DEV_BYTES);
        }
        other => panic!("expected RegistryError::ResponseTooLarge, got {other:?}"),
    }
}

/// A normal, well-under-the-cap payload must still resolve through
/// `fetch_network` unchanged — the happy-path guard that the size-cap
/// plumbing didn't break normal fetches.
#[tokio::test]
async fn fetch_network_accepts_normal_size_payload() {
    use sluice::registry::models_dev::fetch_network;

    let mock_server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_string(NETWORK_FIXTURE_JSON))
        .mount(&mock_server)
        .await;

    let client = reqwest::Client::new();
    let registry = fetch_network(&client, &mock_server.uri())
        .await
        .expect("normal-size payload must resolve");
    assert!(registry.get("fixture-provider", "fixture-model").is_some());
}

/// FIX-4 (audit finding m18): a models.dev response larger than
/// `registry::models_dev::MAX_MODELS_DEV_BYTES` must be rejected cleanly
/// (non-zero exit, no partial overlay write) rather than buffering the
/// whole oversized body into memory. Mirrors the 500-status test above but
/// with a genuinely oversized 200 response body instead of a server error,
/// so this exercises the size cap specifically (not just the general
/// error-mapping path).
#[tokio::test]
async fn models_update_from_network_oversized_body_exits_nonzero_and_leaves_overlay_unchanged() {
    let mock_server = MockServer::start().await;
    let oversized_body = "a".repeat(sluice::registry::models_dev::MAX_MODELS_DEV_BYTES + 1);
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_string(oversized_body))
        .mount(&mock_server)
        .await;

    let dir = scratch_dir("update-from-network-oversized");
    std::fs::create_dir_all(&dir).unwrap();
    let out_path = dir.join("out.json");
    let sentinel = "pre-existing-content-must-survive-a-failed-update";
    std::fs::write(&out_path, sentinel).unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_sluice"))
        .args([
            "models",
            "update",
            "--from-network",
            "--network-url",
            &mock_server.uri(),
            "--out",
            out_path.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        !output.status.success(),
        "an oversized response body must exit non-zero: {output:?}"
    );
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(!stderr.is_empty(), "expected an error message on stderr");

    let after = std::fs::read_to_string(&out_path).unwrap();
    assert_eq!(
        after, sentinel,
        "an oversized --from-network response must not partially write the overlay file"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn models_update_from_network_500_writes_no_overlay_when_none_existed() {
    let mock_server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&mock_server)
        .await;

    let dir = scratch_dir("update-from-network-500-absent");
    std::fs::create_dir_all(&dir).unwrap();
    let out_path = dir.join("out.json");
    assert!(!out_path.exists());

    let output = Command::new(env!("CARGO_BIN_EXE_sluice"))
        .args([
            "models",
            "update",
            "--from-network",
            "--network-url",
            &mock_server.uri(),
            "--out",
            out_path.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(!output.status.success(), "{output:?}");
    assert!(
        !out_path.exists(),
        "a failed --from-network update must not create the overlay file"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
