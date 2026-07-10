//! Debounced hot-reload for the live config.
//!
//! [`ConfigSource`] remembers whether the gateway was started from a single
//! file or a directory, so the same source used for the initial load can be
//! re-run later on a filesystem-change notification. [`apply_reload`] is the
//! deterministic core: reload from the source, and only swap the live
//! `ArcSwap` on success — an invalid edit on disk is logged and ignored,
//! keeping the last-known-good config live (M7's "keep-live-on-error"
//! requirement). [`spawn_watcher`] wires that core up to a real `notify`
//! filesystem watcher with a debounce window, so a burst of saves (editors
//! that write-then-rename, `rsync`, etc.) triggers exactly one reload instead
//! of one per filesystem event.

use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use notify::{RecursiveMode, Watcher};

use super::load::{load_dir, load_file, ConfigError};
use super::Config;

/// Where the live config was loaded from, and how to reload it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigSource {
    File(PathBuf),
    Dir(PathBuf),
}

impl ConfigSource {
    /// Re-run the loader for this source. Shared by both the initial load
    /// (see `main`/`server::serve`) and every subsequent hot-reload, so the
    /// two can never drift in how they interpret a file vs. a directory.
    pub fn reload(&self) -> Result<Config, ConfigError> {
        match self {
            ConfigSource::File(path) => load_file(path),
            ConfigSource::Dir(dir) => load_dir(dir),
        }
    }

    /// The filesystem path a watcher should be pointed at.
    fn watch_path(&self) -> &Path {
        match self {
            ConfigSource::File(path) => path,
            ConfigSource::Dir(dir) => dir,
        }
    }
}

/// Reload `source` and, only on success, swap the result into `live`. On
/// failure, `live` is left completely untouched and the error is returned to
/// the caller to log — this is the "keep the last-known-good config live"
/// guarantee: a bad edit on disk never takes the gateway down or blanks out
/// routing.
pub fn apply_reload(source: &ConfigSource, live: &ArcSwap<Config>) -> Result<(), ConfigError> {
    let cfg = source.reload()?;
    live.store(Arc::new(cfg));
    Ok(())
}

/// Start a background `notify` watcher on `source`'s path (recursively, for
/// a directory) and, on filesystem events, debounce for `debounce` before
/// calling [`apply_reload`]. Runs on its own OS thread so it never depends on
/// (or blocks) the tokio runtime; failures to even start the watcher are
/// logged and the thread exits without panicking — the gateway keeps serving
/// whatever config it already has.
///
/// `on_reload` is invoked with the freshly-stored `Config` after each
/// SUCCESSFUL swap (never on a rejected reload), on this watcher thread. It is
/// the hook callers use to react to a swap; e.g. `server` prunes the shared
/// script-worker cache so workers orphaned by the reload are reaped (see
/// `proxy::prune_worker_cache`). A rejected reload leaves `live` untouched and
/// does not fire the callback.
pub fn spawn_watcher(
    source: ConfigSource,
    live: Arc<ArcSwap<Config>>,
    debounce: Duration,
    on_reload: impl Fn(&Config) + Send + 'static,
) {
    std::thread::spawn(move || {
        let (tx, rx) = mpsc::channel::<notify::Result<notify::Event>>();

        let mut watcher = match notify::recommended_watcher(move |res| {
            // The watcher's internal thread drives this callback; a closed
            // receiver just means our loop below has already exited.
            let _ = tx.send(res);
        }) {
            Ok(w) => w,
            Err(e) => {
                tracing::warn!("config watcher init failed: {e}; hot-reload disabled");
                return;
            }
        };

        let path = source.watch_path();
        let mode = if path.is_dir() {
            RecursiveMode::Recursive
        } else {
            RecursiveMode::NonRecursive
        };
        if let Err(e) = watcher.watch(path, mode) {
            tracing::warn!(
                "config watcher failed to watch {}: {e}; hot-reload disabled",
                path.display()
            );
            return;
        }

        // Block for the first event of a change, then drain (and reset the
        // debounce window on) any further events until things go quiet for
        // `debounce`, so a burst of writes collapses into a single reload.
        while let Ok(first) = rx.recv() {
            if let Err(e) = first {
                tracing::warn!("config watcher event error: {e}");
                continue;
            }
            loop {
                match rx.recv_timeout(debounce) {
                    Ok(_) => continue,
                    Err(RecvTimeoutError::Timeout) => break,
                    Err(RecvTimeoutError::Disconnected) => return,
                }
            }
            match apply_reload(&source, &live) {
                Ok(()) => {
                    tracing::info!("config reloaded");
                    // `apply_reload` is the only place this `live` is stored,
                    // and this thread is single-threaded, so the snapshot we
                    // load here is exactly the config we just swapped in.
                    on_reload(&live.load());
                }
                Err(e) => tracing::warn!("config reload rejected: {e}; keeping live config"),
            }
        }
        // `rx.recv()` only errs when every sender (i.e. `watcher`) has been
        // dropped, meaning the watcher itself has gone away; nothing left to
        // do but let the thread end.
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_temp_path(tag: &str) -> PathBuf {
        let unique = format!(
            "sluice-watch-test-{}-{}-{}",
            std::process::id(),
            tag,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        std::env::temp_dir().join(unique)
    }

    fn route_toml(id: &str) -> String {
        format!(
            r#"
            [[route]]
            id = "{id}"
            upstream = "http://u"
        "#
        )
    }

    #[test]
    fn apply_reload_swaps_live_config_on_valid_source() {
        let path = make_temp_path("valid");
        std::fs::write(&path, route_toml("a")).unwrap();
        let source = ConfigSource::File(path.clone());

        let initial = source.reload().unwrap();
        let live = ArcSwap::from_pointee(initial);

        // Rewrite the file with a new route id ("b") before reloading.
        std::fs::write(&path, route_toml("b")).unwrap();
        apply_reload(&source, &live).unwrap();

        let loaded = live.load();
        assert_eq!(loaded.routes.len(), 1);
        assert_eq!(loaded.routes[0].id, "b");

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn apply_reload_keeps_live_config_on_invalid_source_and_returns_err() {
        let path = make_temp_path("invalid");
        std::fs::write(&path, route_toml("b")).unwrap();
        let source = ConfigSource::File(path.clone());

        let initial = source.reload().unwrap();
        let live = ArcSwap::from_pointee(initial);

        // Now corrupt the file: not valid TOML at all.
        std::fs::write(&path, "this is not [ valid toml").unwrap();
        let err = apply_reload(&source, &live).unwrap_err();
        assert!(matches!(err, ConfigError::Parse(_)));

        // The live config must be completely unchanged.
        let loaded = live.load();
        assert_eq!(loaded.routes.len(), 1);
        assert_eq!(loaded.routes[0].id, "b");

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn apply_reload_dir_source_keeps_live_config_on_invalid_reload() {
        let dir = make_temp_path("dir-invalid");
        let routes_dir = dir.join("routes.d");
        std::fs::create_dir_all(&routes_dir).unwrap();
        std::fs::write(routes_dir.join("a.toml"), route_toml("dir-a")).unwrap();
        let source = ConfigSource::Dir(dir.clone());

        let initial = source.reload().unwrap();
        let live = ArcSwap::from_pointee(initial);

        // Introduce a duplicate route id across files: load_dir rejects this.
        std::fs::write(routes_dir.join("b.toml"), route_toml("dir-a")).unwrap();
        let err = apply_reload(&source, &live).unwrap_err();
        assert!(matches!(err, ConfigError::Invalid(_)));

        let loaded = live.load();
        assert_eq!(loaded.routes.len(), 1);
        assert_eq!(loaded.routes[0].id, "dir-a");

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Filesystem-event-driven end-to-end test: writes a new file after
    /// `spawn_watcher` starts and asserts the debounced reload swaps `live`.
    /// Marked `#[ignore]` because it depends on real OS filesystem event
    /// delivery timing, which is inherently less deterministic in CI than
    /// the `apply_reload` unit tests above (which are the primary coverage
    /// for this module's swap/keep-live-on-error contract).
    #[test]
    #[ignore]
    fn spawn_watcher_reloads_on_file_change() {
        let path = make_temp_path("fs-event");
        std::fs::write(&path, route_toml("first")).unwrap();
        let source = ConfigSource::File(path.clone());
        let initial = source.reload().unwrap();
        let live = Arc::new(ArcSwap::from_pointee(initial));

        spawn_watcher(source, live.clone(), Duration::from_millis(200), |_| {});

        // Give the watcher thread time to start and register.
        std::thread::sleep(Duration::from_millis(300));
        std::fs::write(&path, route_toml("second")).unwrap();

        // Generous wait: debounce window + watcher latency + reload time.
        std::thread::sleep(Duration::from_secs(2));

        let loaded = live.load();
        assert_eq!(loaded.routes[0].id, "second");

        let _ = std::fs::remove_file(&path);
    }
}
