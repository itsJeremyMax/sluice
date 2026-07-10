use std::process::Command;

fn write_tmp(name: &str, contents: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("sluice-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(name);
    std::fs::write(&path, contents).unwrap();
    path
}

#[test]
fn check_accepts_valid_config() {
    let path = write_tmp(
        "ok.toml",
        r#"
        [[route]]
        id = "claude"
        upstream = "https://api.anthropic.com"
    "#,
    );
    let status = Command::new(env!("CARGO_BIN_EXE_sluice"))
        .args(["check", "--config", path.to_str().unwrap()])
        .status()
        .unwrap();
    assert!(status.success());
}

#[test]
fn check_rejects_config_with_no_routes() {
    let path = write_tmp(
        "no-routes.toml",
        r#"
        [gateway]
        listen = "127.0.0.1:9000"
    "#,
    );
    let status = Command::new(env!("CARGO_BIN_EXE_sluice"))
        .args(["check", "--config", path.to_str().unwrap()])
        .status()
        .unwrap();
    assert!(!status.success());
}

#[test]
fn check_rejects_duplicate_route_id() {
    let path = write_tmp(
        "dup.toml",
        r#"
        [[route]]
        id = "x"
        upstream = "http://a"
        [[route]]
        id = "x"
        upstream = "http://b"
    "#,
    );
    let status = Command::new(env!("CARGO_BIN_EXE_sluice"))
        .args(["check", "--config", path.to_str().unwrap()])
        .status()
        .unwrap();
    assert!(!status.success());
}
