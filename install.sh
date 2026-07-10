#!/usr/bin/env bash
#
# sluice installer
#
# Installs, updates, or removes the `sluice` gateway binary from GitHub Releases.
# Works on Linux (x86_64, aarch64) and macOS (Intel, Apple Silicon).
#
# Quick install (latest release):
#   curl -fsSL https://raw.githubusercontent.com/itsJeremyMax/sluice/main/install.sh | bash
#
# Update to the newest release (re-run any time; a no-op if already current):
#   curl -fsSL https://raw.githubusercontent.com/itsJeremyMax/sluice/main/install.sh | bash
#
# Pin a version, choose a directory, or remove:
#   curl -fsSL .../install.sh | bash -s -- --version v0.1.0
#   curl -fsSL .../install.sh | bash -s -- --bin-dir "$HOME/.local/bin"
#   curl -fsSL .../install.sh | bash -s -- --uninstall
#
# Environment overrides: SLUICE_VERSION, SLUICE_INSTALL_DIR, SLUICE_REPO, NO_COLOR.

set -euo pipefail

REPO="${SLUICE_REPO:-itsJeremyMax/sluice}"
BINARY="sluice"

# ---- flags / config -------------------------------------------------------

VERSION="${SLUICE_VERSION:-}"      # empty means "latest"
BIN_DIR="${SLUICE_INSTALL_DIR:-}"  # empty means "auto-detect"
FORCE=0
VERIFY=1
ACTION="install"

usage() {
  cat <<'EOF'
sluice installer

Usage: install.sh [options]

Options:
  --version <tag>   Install a specific release tag (e.g. v0.1.0). Default: latest.
  --bin-dir <dir>   Install into <dir>. Default: /usr/local/bin if writable,
                    otherwise ~/.local/bin.
  --uninstall       Remove an installed sluice binary.
  --force           Reinstall even if the target version is already installed.
  --no-verify       Skip SHA-256 checksum verification (not recommended).
  --help            Show this help.

Environment:
  SLUICE_VERSION       Same as --version.
  SLUICE_INSTALL_DIR   Same as --bin-dir.
  SLUICE_REPO          GitHub owner/repo to pull releases from.
  NO_COLOR             Disable colored output.
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    --version)   VERSION="${2:-}"; shift 2 ;;
    --version=*) VERSION="${1#*=}"; shift ;;
    --bin-dir)   BIN_DIR="${2:-}"; shift 2 ;;
    --bin-dir=*) BIN_DIR="${1#*=}"; shift ;;
    --uninstall) ACTION="uninstall"; shift ;;
    --force)     FORCE=1; shift ;;
    --no-verify) VERIFY=0; shift ;;
    --help|-h)   usage; exit 0 ;;
    *) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

# ---- logging --------------------------------------------------------------

if [ -t 2 ] && [ -z "${NO_COLOR:-}" ]; then
  C_BLUE=$'\033[34m'; C_GREEN=$'\033[32m'; C_YELLOW=$'\033[33m'; C_RED=$'\033[31m'; C_DIM=$'\033[2m'; C_RESET=$'\033[0m'
else
  C_BLUE=''; C_GREEN=''; C_YELLOW=''; C_RED=''; C_DIM=''; C_RESET=''
fi

info()  { printf '%s==>%s %s\n' "$C_BLUE" "$C_RESET" "$*" >&2; }
ok()    { printf '%s==>%s %s\n' "$C_GREEN" "$C_RESET" "$*" >&2; }
warn()  { printf '%swarn:%s %s\n' "$C_YELLOW" "$C_RESET" "$*" >&2; }
die()   { printf '%serror:%s %s\n' "$C_RED" "$C_RESET" "$*" >&2; exit 1; }

need() { command -v "$1" >/dev/null 2>&1 || die "required tool not found: $1"; }

# ---- platform detection ---------------------------------------------------

detect_target() {
  local os arch
  os="$(uname -s)"
  arch="$(uname -m)"

  case "$os" in
    Linux)  os="unknown-linux-gnu" ;;
    Darwin) os="apple-darwin" ;;
    *) die "unsupported operating system: $os (this installer supports Linux and macOS; on Windows download the .zip from the releases page)" ;;
  esac

  case "$arch" in
    x86_64|amd64)  arch="x86_64" ;;
    aarch64|arm64) arch="aarch64" ;;
    *) die "unsupported architecture: $arch" ;;
  esac

  printf '%s-%s' "$arch" "$os"
}

# ---- downloader -----------------------------------------------------------

DL=""
if command -v curl >/dev/null 2>&1; then
  DL="curl"
elif command -v wget >/dev/null 2>&1; then
  DL="wget"
else
  die "need either curl or wget installed"
fi

# fetch <url> <dest-file>  (fails on HTTP errors)
fetch() {
  local url="$1" dest="$2"
  if [ "$DL" = "curl" ]; then
    curl -fSL --retry 3 -o "$dest" "$url"
  else
    wget -q -O "$dest" "$url"
  fi
}

# fetch <url> to stdout
fetch_stdout() {
  local url="$1"
  if [ "$DL" = "curl" ]; then
    curl -fsSL --retry 3 "$url"
  else
    wget -qO- "$url"
  fi
}

# ---- version resolution ---------------------------------------------------

resolve_latest() {
  # Follow the /releases/latest redirect and read the resolved tag from the URL,
  # so we do not need jq to parse the API JSON.
  local url tag
  url="https://github.com/${REPO}/releases/latest"
  if [ "$DL" = "curl" ]; then
    tag="$(curl -fsSLI -o /dev/null -w '%{url_effective}' "$url" | sed -n 's#.*/tag/##p')"
  else
    tag="$(wget -q -S --max-redirect=5 -O /dev/null "$url" 2>&1 | sed -n 's#.*Location:.*/tag/##p' | tail -n1 | tr -d '\r')"
  fi
  [ -n "$tag" ] || die "could not determine the latest release tag for ${REPO}. Pass --version <tag>, or check that a release has been published."
  printf '%s' "$tag"
}

# strip a leading 'v' to compare against `sluice --version` output
ver_num() { printf '%s' "${1#v}"; }

# ---- install-dir selection ------------------------------------------------

choose_bin_dir() {
  if [ -n "$BIN_DIR" ]; then
    printf '%s' "$BIN_DIR"
    return
  fi
  # Prefer a system-wide dir when we can write there (directly or via sudo),
  # else fall back to a per-user dir that needs no elevation.
  if [ -w /usr/local/bin ] 2>/dev/null; then
    printf '/usr/local/bin'
  elif [ "$(id -u)" -eq 0 ]; then
    printf '/usr/local/bin'
  elif command -v sudo >/dev/null 2>&1 && [ -d /usr/local/bin ]; then
    printf '/usr/local/bin'
  else
    printf '%s/.local/bin' "$HOME"
  fi
}

# run a command, elevating with sudo only when the destination needs it
maybe_sudo() {
  local dir="$1"; shift
  if [ -w "$dir" ] || [ "$(id -u)" -eq 0 ]; then
    "$@"
  elif command -v sudo >/dev/null 2>&1; then
    info "elevating with sudo to write to $dir"
    sudo "$@"
  else
    die "cannot write to $dir and sudo is unavailable; re-run with --bin-dir \"\$HOME/.local/bin\""
  fi
}

# ---- uninstall ------------------------------------------------------------

do_uninstall() {
  local found
  if [ -n "$BIN_DIR" ] && [ -x "$BIN_DIR/$BINARY" ]; then
    found="$BIN_DIR/$BINARY"
  else
    found="$(command -v "$BINARY" 2>/dev/null || true)"
  fi
  [ -n "$found" ] || die "$BINARY is not installed (nothing to remove)"
  info "removing $found"
  maybe_sudo "$(dirname "$found")" rm -f "$found"
  ok "$BINARY removed."
}

# ---- install --------------------------------------------------------------

do_install() {
  need uname
  need tar

  local target tag numver dest_dir dest tmp asset url current

  target="$(detect_target)"
  info "platform: $target"

  if [ -n "$VERSION" ]; then
    tag="$VERSION"
  else
    info "resolving latest release of $REPO"
    tag="$(resolve_latest)"
  fi
  numver="$(ver_num "$tag")"
  info "target version: $tag"

  dest_dir="$(choose_bin_dir)"
  dest="$dest_dir/$BINARY"

  # Update check: if the target version is already installed, stop unless forced.
  if [ "$FORCE" -eq 0 ] && [ -x "$dest" ]; then
    current="$("$dest" --version 2>/dev/null | awk '{print $NF}' || true)"
    if [ "$current" = "$numver" ]; then
      ok "$BINARY $numver is already installed at $dest (use --force to reinstall)."
      return
    fi
    [ -n "$current" ] && info "updating $BINARY $current -> $numver"
  fi

  asset="sluice-${target}.tar.gz"
  url="https://github.com/${REPO}/releases/download/${tag}/${asset}"

  tmp="$(mktemp -d)"
  trap 'rm -rf "$tmp"' EXIT

  info "downloading $asset"
  fetch "$url" "$tmp/$asset" || die "download failed: $url"

  if [ "$VERIFY" -eq 1 ]; then
    if fetch "$url.sha256" "$tmp/$asset.sha256" 2>/dev/null; then
      info "verifying checksum"
      local expected
      expected="$(awk '{print $1}' "$tmp/$asset.sha256")"
      local actual
      if command -v sha256sum >/dev/null 2>&1; then
        actual="$(sha256sum "$tmp/$asset" | awk '{print $1}')"
      elif command -v shasum >/dev/null 2>&1; then
        actual="$(shasum -a 256 "$tmp/$asset" | awk '{print $1}')"
      else
        warn "no sha256sum/shasum available; skipping verification"
        actual="$expected"
      fi
      [ "$actual" = "$expected" ] || die "checksum mismatch for $asset (expected $expected, got $actual)"
      ok "checksum verified"
    else
      warn "no published checksum for $asset; skipping verification"
    fi
  fi

  info "extracting"
  tar -xzf "$tmp/$asset" -C "$tmp"
  [ -f "$tmp/$BINARY" ] || die "archive did not contain a '$BINARY' binary"
  chmod +x "$tmp/$BINARY"

  info "installing to $dest"
  maybe_sudo "$dest_dir" mkdir -p "$dest_dir"
  maybe_sudo "$dest_dir" install -m 0755 "$tmp/$BINARY" "$dest"

  ok "$BINARY $numver installed to $dest"

  # PATH hint if the chosen dir is not reachable.
  case ":$PATH:" in
    *":$dest_dir:"*) : ;;
    *)
      warn "$dest_dir is not on your PATH. Add this to your shell profile:"
      printf '    export PATH="%s:$PATH"\n' "$dest_dir" >&2
      ;;
  esac

  if command -v "$BINARY" >/dev/null 2>&1; then
    info "run '$BINARY --help' to get started"
  fi
}

case "$ACTION" in
  install)   do_install ;;
  uninstall) do_uninstall ;;
esac
