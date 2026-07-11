//! `sluice update` — self-update against GitHub releases (see
//! docs/cli.md's update section). Resolution
//! follows the `releases/latest` redirect (no GitHub API); downloads are
//! the bare-binary assets and are ALWAYS sha256-verified.

use thiserror::Error;

/// GitHub releases base URL updates resolve against. Overridable only via
/// the hidden, test-only `--releases-url` flag (see `cli::Command::Update`).
pub const DEFAULT_RELEASES_URL: &str = "https://github.com/itsJeremyMax/sluice/releases";

/// `sluice update --check` exit code when a newer release exists. Distinct
/// from 1 (error) so scripts can branch on "behind" vs "broken".
pub const EXIT_UPDATE_AVAILABLE: u8 = 10;

#[derive(Debug, Error)]
pub enum UpdateError {
    #[error("could not resolve the latest release: {0}")]
    ResolveLatest(String),
    #[error("could not parse version '{0}'")]
    BadVersion(String),
    #[error("unsupported platform {os}/{arch}; download manually from {DEFAULT_RELEASES_URL}")]
    UnsupportedPlatform {
        os: &'static str,
        arch: &'static str,
    },
    #[error("download failed: {0}")]
    Download(String),
    #[error("no checksum published for {0}; refusing to install an unverified binary")]
    MissingChecksum(String),
    #[error("checksum mismatch for {asset}: expected {expected}, got {actual}")]
    ChecksumMismatch {
        asset: String,
        expected: String,
        actual: String,
    },
    #[error("failed to replace the running binary: {0}")]
    Replace(String),
}

/// Parse `"1.2.3"` or `"v1.2.3"` into a numerically comparable triple.
pub fn parse_version(s: &str) -> Result<(u64, u64, u64), UpdateError> {
    let bad = || UpdateError::BadVersion(s.to_string());
    let trimmed = s.trim();
    let trimmed = trimmed.strip_prefix('v').unwrap_or(trimmed);
    let parts: Vec<&str> = trimmed.split('.').collect();
    if parts.len() != 3 {
        return Err(bad());
    }
    let num = |p: &str| p.parse::<u64>().map_err(|_| bad());
    Ok((num(parts[0])?, num(parts[1])?, num(parts[2])?))
}

/// The release target triple for the platform THIS binary was built for,
/// matching the five targets `release.yml` builds.
pub fn target_triple() -> Result<&'static str, UpdateError> {
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("linux", "x86_64") => Ok("x86_64-unknown-linux-gnu"),
        ("linux", "aarch64") => Ok("aarch64-unknown-linux-gnu"),
        ("macos", "x86_64") => Ok("x86_64-apple-darwin"),
        ("macos", "aarch64") => Ok("aarch64-apple-darwin"),
        ("windows", "x86_64") => Ok("x86_64-pc-windows-msvc"),
        (os, arch) => Err(UpdateError::UnsupportedPlatform { os, arch }),
    }
}

/// Bare-binary release asset name: `sluice-<tag>-<target>` (`.exe` for
/// windows targets). MUST match `release.yml`'s packaging step.
pub fn asset_name(tag: &str, target: &str) -> String {
    let exe = if target.contains("windows") {
        ".exe"
    } else {
        ""
    };
    format!("sluice-{tag}-{target}{exe}")
}

use sha2::{Digest, Sha256};

/// A client that does NOT follow redirects — `resolve_latest` needs the
/// `Location` header itself, not the page it points at.
pub fn no_redirect_client() -> reqwest::Client {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("build reqwest client")
}

/// Resolve the latest release tag by requesting `<releases_url>/latest` and
/// reading the tag off the redirect `Location` (the same trick
/// `install.sh`'s `resolve_latest` uses — no GitHub API, no token). The
/// passed client must have redirects disabled (`no_redirect_client`).
pub async fn resolve_latest(
    client: &reqwest::Client,
    releases_url: &str,
) -> Result<String, UpdateError> {
    let url = format!("{releases_url}/latest");
    let resp = client
        .get(&url)
        .send()
        .await
        .map_err(|e| UpdateError::ResolveLatest(e.to_string()))?;
    let location = resp
        .headers()
        .get(reqwest::header::LOCATION)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| {
            UpdateError::ResolveLatest(format!(
                "expected a redirect from {url}, got status {}",
                resp.status()
            ))
        })?;
    let tag = location
        .rsplit("/tag/")
        .next()
        .filter(|t| !t.is_empty() && !t.contains('/'))
        .ok_or_else(|| {
            UpdateError::ResolveLatest(format!("no /tag/ in redirect location '{location}'"))
        })?;
    Ok(tag.to_string())
}

/// Download the bare-binary asset for `tag`/`target` and its `.sha256`,
/// verify the digest, and return the binary bytes. Strict: a missing
/// checksum file or a mismatch is an error — never returns unverified
/// bytes. The passed client must FOLLOW redirects (GitHub serves release
/// assets via a redirect to a CDN host).
pub async fn download_verified(
    client: &reqwest::Client,
    releases_url: &str,
    tag: &str,
    target: &str,
) -> Result<Vec<u8>, UpdateError> {
    let asset = asset_name(tag, target);
    let asset_url = format!("{releases_url}/download/{tag}/{asset}");

    let sum_resp = client
        .get(format!("{asset_url}.sha256"))
        .send()
        .await
        .map_err(|e| UpdateError::Download(e.to_string()))?;
    if !sum_resp.status().is_success() {
        return Err(UpdateError::MissingChecksum(asset));
    }
    let sum_text = sum_resp
        .text()
        .await
        .map_err(|e| UpdateError::Download(e.to_string()))?;
    // `sha256sum` format: "<hex>  <filename>" — first token is the digest.
    let expected = sum_text
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .to_lowercase();
    if expected.len() != 64 {
        return Err(UpdateError::MissingChecksum(asset));
    }

    let bin_resp = client
        .get(&asset_url)
        .send()
        .await
        .map_err(|e| UpdateError::Download(e.to_string()))?;
    if !bin_resp.status().is_success() {
        return Err(UpdateError::Download(format!(
            "GET {asset_url} returned {}",
            bin_resp.status()
        )));
    }
    let bytes = bin_resp
        .bytes()
        .await
        .map_err(|e| UpdateError::Download(e.to_string()))?
        .to_vec();

    let mut actual = String::new();
    for b in Sha256::digest(&bytes) {
        actual.push_str(&format!("{b:02x}"));
    }
    if actual != expected {
        return Err(UpdateError::ChecksumMismatch {
            asset,
            expected,
            actual,
        });
    }
    Ok(bytes)
}

/// Atomically replace the currently running executable with `bytes`: write
/// to a temp file next to the current exe (same filesystem, so the final
/// swap is a rename), mark executable on unix, then hand off to
/// `self_replace` (which owns the platform quirks, notably Windows'
/// can't-overwrite-a-running-exe dance).
pub fn replace_current_exe(bytes: &[u8]) -> Result<(), UpdateError> {
    let err = |e: std::io::Error| UpdateError::Replace(e.to_string());
    let exe = std::env::current_exe().map_err(err)?;
    let dir = exe
        .parent()
        .ok_or_else(|| UpdateError::Replace(format!("{} has no parent dir", exe.display())))?;
    let tmp = dir.join(format!(".sluice-update-{}", std::process::id()));
    std::fs::write(&tmp, bytes).map_err(err)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o755)).map_err(err)?;
    }
    let result = self_replace::self_replace(&tmp).map_err(err);
    let _ = std::fs::remove_file(&tmp);
    result
}

use std::process::ExitCode;

/// Run `sluice update [--check]`. Synchronous wrapper: spins up a
/// short-lived current-thread runtime for the network calls, mirroring
/// `main.rs`'s `resolve_models_source` pattern.
pub fn run(check_only: bool, releases_url: &str) -> ExitCode {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");
    rt.block_on(run_async(check_only, releases_url))
}

async fn run_async(check_only: bool, releases_url: &str) -> ExitCode {
    let current_str = env!("CARGO_PKG_VERSION");
    let current =
        parse_version(current_str).expect("CARGO_PKG_VERSION is always a valid x.y.z version");

    let tag = match resolve_latest(&no_redirect_client(), releases_url).await {
        Ok(tag) => tag,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };
    let latest = match parse_version(&tag) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };

    println!("current: {current_str}");
    println!("latest:  {}", tag.trim_start_matches('v'));

    if current == latest {
        println!("already up to date");
        return ExitCode::SUCCESS;
    }
    if current > latest {
        println!("ahead of latest release (dev build?)");
        return ExitCode::SUCCESS;
    }
    if check_only {
        println!("update available: run `sluice update`");
        return ExitCode::from(EXIT_UPDATE_AVAILABLE);
    }

    let target = match target_triple() {
        Ok(t) => t,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };
    println!("downloading {} ...", asset_name(&tag, target));
    let bytes = match download_verified(&reqwest::Client::new(), releases_url, &tag, target).await {
        Ok(b) => b,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };
    if let Err(e) = replace_current_exe(&bytes) {
        // Most common cause: binary lives somewhere not writable by this
        // user (e.g. /usr/local/bin without root).
        eprintln!("{e}");
        eprintln!(
            "hint: re-run with elevated permissions (e.g. sudo), or re-install via install.sh"
        );
        return ExitCode::FAILURE;
    }
    println!("updated to {tag}");
    ExitCode::SUCCESS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_plain_and_v_prefixed_versions() {
        assert_eq!(parse_version("0.1.0").unwrap(), (0, 1, 0));
        assert_eq!(parse_version("v1.2.30").unwrap(), (1, 2, 30));
    }

    #[test]
    fn rejects_garbage_versions() {
        assert!(parse_version("").is_err());
        assert!(parse_version("v1.2").is_err());
        assert!(parse_version("not-a-version").is_err());
    }

    #[test]
    fn version_tuples_order_numerically() {
        // (0,10,0) > (0,9,9) — tuple compare, not string compare.
        assert!(parse_version("0.10.0").unwrap() > parse_version("0.9.9").unwrap());
    }

    #[test]
    fn asset_name_carries_tag_and_target() {
        assert_eq!(
            asset_name("v0.2.0", "x86_64-apple-darwin"),
            "sluice-v0.2.0-x86_64-apple-darwin"
        );
        assert_eq!(
            asset_name("v0.2.0", "x86_64-pc-windows-msvc"),
            "sluice-v0.2.0-x86_64-pc-windows-msvc.exe"
        );
    }

    #[test]
    fn target_triple_resolves_on_supported_platforms() {
        // This test runs on a supported dev/CI platform by definition.
        let t = target_triple().unwrap();
        assert!(t.contains(std::env::consts::ARCH));
    }

    use sha2::{Digest, Sha256};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn hex_digest(bytes: &[u8]) -> String {
        let mut out = String::new();
        for b in Sha256::digest(bytes) {
            out.push_str(&format!("{b:02x}"));
        }
        out
    }

    #[tokio::test]
    async fn resolve_latest_reads_tag_from_redirect_location() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/latest"))
            .respond_with(
                ResponseTemplate::new(302)
                    .insert_header("location", format!("{}/tag/v9.9.9", server.uri()).as_str()),
            )
            .mount(&server)
            .await;
        let client = no_redirect_client();
        assert_eq!(
            resolve_latest(&client, &server.uri()).await.unwrap(),
            "v9.9.9"
        );
    }

    #[tokio::test]
    async fn download_verified_accepts_matching_checksum() {
        let server = MockServer::start().await;
        let target = "x86_64-apple-darwin";
        let asset = asset_name("v9.9.9", target);
        let body = b"fake-new-binary".to_vec();
        Mock::given(method("GET"))
            .and(path(format!("/download/v9.9.9/{asset}.sha256")))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(format!("{}  {asset}\n", hex_digest(&body))),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/download/v9.9.9/{asset}")))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(body.clone()))
            .mount(&server)
            .await;
        let client = reqwest::Client::new();
        let got = download_verified(&client, &server.uri(), "v9.9.9", target)
            .await
            .unwrap();
        assert_eq!(got, body);
    }

    #[tokio::test]
    async fn download_verified_rejects_checksum_mismatch() {
        let server = MockServer::start().await;
        let target = "x86_64-apple-darwin";
        let asset = asset_name("v9.9.9", target);
        Mock::given(method("GET"))
            .and(path(format!("/download/v9.9.9/{asset}.sha256")))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(format!("{}  {asset}\n", hex_digest(b"different-bytes"))),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/download/v9.9.9/{asset}")))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"fake-new-binary".to_vec()))
            .mount(&server)
            .await;
        let client = reqwest::Client::new();
        let err = download_verified(&client, &server.uri(), "v9.9.9", target)
            .await
            .unwrap_err();
        assert!(matches!(err, UpdateError::ChecksumMismatch { .. }));
    }

    #[tokio::test]
    async fn download_verified_refuses_missing_checksum() {
        let server = MockServer::start().await;
        // No mocks mounted: the .sha256 GET 404s.
        let client = reqwest::Client::new();
        let err = download_verified(&client, &server.uri(), "v9.9.9", "x86_64-apple-darwin")
            .await
            .unwrap_err();
        assert!(matches!(err, UpdateError::MissingChecksum(_)));
    }
}
