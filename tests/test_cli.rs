//! Integration tests for `sluice test <route>` (design doc §14): a dry-run
//! probe through a route's `on_request` chain, driven as a real subprocess
//! (`CARGO_BIN_EXE_sluice`) mirroring the `tests/check_cli.rs`/
//! `tests/models_cli.rs` pattern, with a wiremock step server standing in
//! for the route's own url step.

use std::process::Command;

use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn write_tmp(name: &str, contents: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "sluice-test-cli-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(name);
    std::fs::write(&path, contents).unwrap();
    path
}

#[tokio::test]
async fn test_command_reports_step_and_would_forward_line() {
    let step_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/step"))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"action":"continue"}"#))
        .mount(&step_server)
        .await;

    let config = write_tmp(
        "route.toml",
        &format!(
            r#"
            [[route]]
            id = "claude"
            upstream = "https://api.anthropic.com"
              [[route.step]]
              name = "probe-step"
              type = "url"
              url = "{}/step"
        "#,
            step_server.uri()
        ),
    );

    let output = Command::new(env!("CARGO_BIN_EXE_sluice"))
        .args(["test", "claude", "--config", config.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");

    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(
        stdout.contains("probe-step (url/on_request) -> continue"),
        "expected step line in output, got:\n{stdout}"
    );
    assert!(
        stdout.contains("would forward to https://api.anthropic.com/"),
        "expected would-forward line in output, got:\n{stdout}"
    );
}

#[test]
fn test_command_exits_nonzero_for_unknown_route() {
    let config = write_tmp(
        "no-such-route.toml",
        r#"
        [[route]]
        id = "claude"
        upstream = "https://api.anthropic.com"
    "#,
    );

    let output = Command::new(env!("CARGO_BIN_EXE_sluice"))
        .args([
            "test",
            "does-not-exist",
            "--config",
            config.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("no route named"), "{stderr}");
}
