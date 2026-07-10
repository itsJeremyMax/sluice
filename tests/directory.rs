//! Integration tests for M7 Task 4: `sluice routes` and `sluice check
//! --config-dir`, both driven as real subprocesses (`CARGO_BIN_EXE_sluice`)
//! against a temporary `routes.d`-style config directory — mirroring the
//! `tests/check_cli.rs` pattern for the single-file case.

use std::process::Command;

fn make_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "sluice-directory-cli-test-{}-{}-{}",
        std::process::id(),
        tag,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(dir.join("routes.d")).unwrap();
    dir
}

#[test]
fn routes_prints_route_ids_and_a_step_name() {
    let dir = make_dir("routes");
    std::fs::write(
        dir.join("routes.d/a.toml"),
        r#"
        [[route]]
        id = "claude"
        upstream = "https://api.anthropic.com"
          [[route.step]]
          name = "redact"
          type = "url"
          url = "http://localhost:9000/redact"
    "#,
    )
    .unwrap();
    std::fs::write(
        dir.join("routes.d/b.toml"),
        r#"
        [[route]]
        id = "openai"
        upstream = "https://api.openai.com"
    "#,
    )
    .unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_sluice"))
        .args(["routes", "--config-dir", dir.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("claude"), "{stdout}");
    assert!(stdout.contains("openai"), "{stdout}");
    assert!(stdout.contains("redact"), "{stdout}");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn check_accepts_valid_config_dir() {
    let dir = make_dir("check-ok");
    std::fs::write(
        dir.join("routes.d/a.toml"),
        r#"
        [[route]]
        id = "claude"
        upstream = "https://api.anthropic.com"
    "#,
    )
    .unwrap();

    let status = Command::new(env!("CARGO_BIN_EXE_sluice"))
        .args(["check", "--config-dir", dir.to_str().unwrap()])
        .status()
        .unwrap();
    assert!(status.success());

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn check_rejects_nonexistent_config_dir() {
    let dir = std::env::temp_dir().join(format!(
        "sluice-directory-cli-test-missing-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));

    let output = Command::new(env!("CARGO_BIN_EXE_sluice"))
        .args(["check", "--config-dir", dir.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains(dir.to_str().unwrap()), "{stderr}");
}

#[test]
fn check_rejects_empty_config_dir() {
    let dir = make_dir("check-empty");
    // `make_dir` creates `routes.d` but leaves it empty and writes no
    // `gateway.toml`: zero routes overall, which must be rejected.

    let status = Command::new(env!("CARGO_BIN_EXE_sluice"))
        .args(["check", "--config-dir", dir.to_str().unwrap()])
        .status()
        .unwrap();
    assert!(!status.success());

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn check_rejects_config_dir_with_duplicate_route_id_across_files() {
    let dir = make_dir("check-dup");
    std::fs::write(
        dir.join("routes.d/a.toml"),
        r#"
        [[route]]
        id = "dup"
        upstream = "http://a"
    "#,
    )
    .unwrap();
    std::fs::write(
        dir.join("routes.d/b.toml"),
        r#"
        [[route]]
        id = "dup"
        upstream = "http://b"
    "#,
    )
    .unwrap();

    let status = Command::new(env!("CARGO_BIN_EXE_sluice"))
        .args(["check", "--config-dir", dir.to_str().unwrap()])
        .status()
        .unwrap();
    assert!(!status.success());

    let _ = std::fs::remove_dir_all(&dir);
}
