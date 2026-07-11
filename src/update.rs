//! `sluice update` — self-update against GitHub releases (design doc:
//! docs/superpowers/specs/2026-07-10-self-update-design.md). Resolution
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
}
