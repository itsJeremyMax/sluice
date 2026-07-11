//! Integration tests for `sluice update [--check]`, driven as real
//! subprocesses (`CARGO_BIN_EXE_sluice`) against a wiremock stand-in for
//! GitHub releases, pointed at via the hidden `--releases-url` override —
//! the same pattern as `tests/models_cli.rs`'s `--network-url` tests.

use std::process::Command;

use sha2::{Digest, Sha256};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const CURRENT: &str = env!("CARGO_PKG_VERSION");

fn hex_digest(bytes: &[u8]) -> String {
    let mut out = String::new();
    for b in Sha256::digest(bytes) {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

/// Mount the `/latest` redirect pointing at `tag`.
async fn mount_latest(server: &MockServer, tag: &str) {
    Mock::given(method("GET"))
        .and(path("/latest"))
        .respond_with(
            ResponseTemplate::new(302)
                .insert_header("location", format!("{}/tag/{tag}", server.uri()).as_str()),
        )
        .mount(server)
        .await;
}

/// The bare-binary asset name for THIS test host's platform — must mirror
/// `sluice::update::asset_name`/`target_triple`.
fn host_asset(tag: &str) -> String {
    let target = match (std::env::consts::OS, std::env::consts::ARCH) {
        ("linux", "x86_64") => "x86_64-unknown-linux-gnu",
        ("linux", "aarch64") => "aarch64-unknown-linux-gnu",
        ("macos", "x86_64") => "x86_64-apple-darwin",
        ("macos", "aarch64") => "aarch64-apple-darwin",
        ("windows", "x86_64") => "x86_64-pc-windows-msvc",
        other => panic!("unsupported test platform {other:?}"),
    };
    let exe = if cfg!(windows) { ".exe" } else { "" };
    format!("sluice-{tag}-{target}{exe}")
}

/// Copy the built sluice binary to a scratch path so update tests can
/// mutate (or fail to mutate) a throwaway copy, never the real test binary.
fn scratch_binary(tag: &str) -> std::path::PathBuf {
    let dir =
        std::env::temp_dir().join(format!("sluice-update-test-{}-{}", std::process::id(), tag));
    std::fs::create_dir_all(&dir).unwrap();
    let dest = dir.join(if cfg!(windows) {
        "sluice.exe"
    } else {
        "sluice"
    });
    std::fs::copy(env!("CARGO_BIN_EXE_sluice"), &dest).unwrap();
    dest
}

#[tokio::test]
async fn check_reports_up_to_date_with_exit_zero() {
    let server = MockServer::start().await;
    mount_latest(&server, &format!("v{CURRENT}")).await;
    let output = Command::new(env!("CARGO_BIN_EXE_sluice"))
        .args(["update", "--check", "--releases-url", &server.uri()])
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("already up to date"), "stdout: {stdout}");
    assert_eq!(output.status.code(), Some(0));
}

#[tokio::test]
async fn check_reports_update_available_with_exit_ten() {
    let server = MockServer::start().await;
    mount_latest(&server, "v999.0.0").await;
    let output = Command::new(env!("CARGO_BIN_EXE_sluice"))
        .args(["update", "--check", "--releases-url", &server.uri()])
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("update available"), "stdout: {stdout}");
    assert_eq!(output.status.code(), Some(10));
}

#[tokio::test]
async fn update_swaps_binary_after_verified_download() {
    let server = MockServer::start().await;
    let tag = "v999.0.0";
    mount_latest(&server, tag).await;
    let asset = host_asset(tag);
    // The "new binary" is arbitrary bytes: the test only asserts the swap
    // wrote exactly these verified bytes, it never re-executes the file.
    let new_bytes = b"#!/bin/sh\necho fake-new-sluice\n".to_vec();
    Mock::given(method("GET"))
        .and(path(format!("/download/{tag}/{asset}.sha256")))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(format!("{}  {asset}\n", hex_digest(&new_bytes))),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/download/{tag}/{asset}")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(new_bytes.clone()))
        .mount(&server)
        .await;

    let bin = scratch_binary(tag);
    let output = Command::new(&bin)
        .args(["update", "--releases-url", &server.uri()])
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(std::fs::read(&bin).unwrap(), new_bytes);
}

#[tokio::test]
async fn update_refuses_checksum_mismatch_and_leaves_binary_untouched() {
    let server = MockServer::start().await;
    let tag = "v998.0.0";
    mount_latest(&server, tag).await;
    let asset = host_asset(tag);
    Mock::given(method("GET"))
        .and(path(format!("/download/{tag}/{asset}.sha256")))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(format!("{}  {asset}\n", hex_digest(b"other-bytes"))),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/download/{tag}/{asset}")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"payload".to_vec()))
        .mount(&server)
        .await;

    let bin = scratch_binary(tag);
    let before = std::fs::read(&bin).unwrap();
    let output = Command::new(&bin)
        .args(["update", "--releases-url", &server.uri()])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("checksum mismatch"), "stderr: {stderr}");
    assert_eq!(std::fs::read(&bin).unwrap(), before);
}

#[tokio::test]
async fn update_refuses_missing_checksum() {
    let server = MockServer::start().await;
    let tag = "v997.0.0";
    mount_latest(&server, tag).await;
    // No download mocks: the .sha256 GET 404s.
    let bin = scratch_binary(tag);
    let output = Command::new(&bin)
        .args(["update", "--releases-url", &server.uri()])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("no checksum published"), "stderr: {stderr}");
}
