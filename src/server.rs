use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::sync::Semaphore;

use crate::admin::{self, Ready};
use crate::config::watch::{spawn_watcher, ConfigSource};
use crate::config::{Config, GatewayDef};
use crate::loopback;
use crate::observability::{self, MetricsHandle};
use crate::proxy::{handle, handle_callback, ProxyState};
use crate::registry::Registry;

// M13 Task 2: one-process-many-listeners vs. `--gateway` per process
// ---------------------------------------------------------------------
// When a config defines `gateways.d` listeners, `serve` can either bind all
// of them in this one process (`--gateway` omitted) or just the one named
// by `--gateway <name>`. Binding several in one process is PORT/BIND
// separation only: every listener still shares this process's config
// `ArcSwap`, HTTP client, in-flight semaphore, model registry, and loopback
// continuation registry (see `derive_state`), and a panic, OOM, or deadlock
// in one request still takes every listener in the process down with it.
// Real fault isolation between gateways means running `--gateway <name>` as
// separate OS processes (optionally behind a supervisor/systemd), each with
// its own memory space and crash domain.

/// The debounce window `serve`'s hot-reload watcher waits for a burst of
/// filesystem events to go quiet before reloading. Chosen to comfortably
/// absorb editor write-then-rename saves and `rsync` bursts without making a
/// real edit feel sluggish; see `config::watch::spawn_watcher`'s doc comment
/// for the mechanics.
const RELOAD_DEBOUNCE: Duration = Duration::from_millis(200);

/// Run the gateway as a long-lived process: bind the listener, build the
/// live (hot-swappable) config state, start the background filesystem
/// watcher that keeps it reloading on edits to `source` (see
/// `config::watch::spawn_watcher`), and serve until `Ctrl-C`.
///
/// `source` records whether `cfg` came from a single file or a directory —
/// the watcher needs it to re-run the same loader `cfg` was originally
/// produced by. `check`/`routes` load config the same way but never call
/// `serve`, so they never spawn a watcher (offline by construction).
///
/// `selected_gateway` is the CLI's `--gateway <name>` (M13 Task 2): `None`
/// with no `gateways.d` listeners configured is the ordinary single-listener
/// case unchanged since M1; `None` with `gateways.d` listeners configured
/// binds every one of them in this one process (see this module's top doc
/// comment on why that's bind separation, not fault isolation); `Some(name)`
/// binds only that named gateway, erroring if no `gateways.d` file defines
/// it.
pub async fn serve(
    cfg: Config,
    source: ConfigSource,
    selected_gateway: Option<String>,
) -> anyhow::Result<()> {
    // Installed once per process (both are idempotent — see their own
    // doc comments — so this is also safe to call from test helpers that
    // construct additional gateways in the same process).
    observability::init_logging();
    let metrics = observability::init_metrics();
    let ready = Ready::new();

    let admin_listen = cfg.gateway.admin_listen.clone();
    let admin_token = cfg.gateway.admin_token.clone();

    if cfg.gateways.is_empty() && selected_gateway.is_none() {
        // Single-listener path: one data listener bound from `[gateway]
        // listen`, serving every configured route (`allowed_routes = None`)
        // — unchanged since before M13.
        let listener = TcpListener::bind(&cfg.gateway.listen).await?;
        let addr = listener.local_addr()?;
        println!("sluice listening on http://{addr}");

        let state = build_state(cfg);
        let prune_state = state.clone();
        spawn_watcher(source, state.config.clone(), RELOAD_DEBOUNCE, move |cfg| {
            crate::proxy::prune_worker_cache(&prune_state, cfg);
        });

        return serve_with_state(
            listener,
            state,
            admin_listen,
            admin_token,
            ready,
            metrics,
            async {
                let _ = tokio::signal::ctrl_c().await;
            },
        )
        .await;
    }

    // Multi-listener path (M13 Task 2): resolve which `gateways.d` entries
    // to bind (all of them, or just `--gateway <name>`), then bind each and
    // give it its own `ProxyState` sharing everything but `allowed_routes`
    // with the shared base state built below.
    let gateways = resolve_gateways(&cfg, selected_gateway.as_deref())?;

    let base_state = build_state(cfg);
    // Prune by the FULL reloaded config, not any per-listener route filter:
    // all derived listener states share this one `worker_cache`, so a worker
    // valid for any listener must survive the prune.
    let prune_state = base_state.clone();
    spawn_watcher(
        source,
        base_state.config.clone(),
        RELOAD_DEBOUNCE,
        move |cfg| {
            crate::proxy::prune_worker_cache(&prune_state, cfg);
        },
    );

    let mut listeners = Vec::with_capacity(gateways.len());
    for gw in &gateways {
        let listener = TcpListener::bind(&gw.listen).await?;
        let addr = listener.local_addr()?;
        println!("sluice gateway '{}' listening on http://{addr}", gw.name);
        let allowed: HashSet<String> = gw.routes.iter().cloned().collect();
        let state = derive_state(&base_state, Some(Arc::new(allowed)));
        listeners.push((listener, state));
    }

    serve_multi(
        listeners,
        admin_listen,
        admin_token,
        ready,
        metrics,
        async {
            let _ = tokio::signal::ctrl_c().await;
        },
    )
    .await
}

/// Resolve which of `cfg.gateways` (`gateways.d` listeners, M13) `serve`
/// should bind, given an optional `--gateway <name>` selection.
///
/// `None` resolves to every gateway `cfg.gateways` defines — bind them all
/// in this one process. `Some(name)` restricts the result to that single
/// named gateway; a name no `gateways.d/<name>.toml` defines is an error
/// (naming the unknown name) rather than a silent empty bind.
pub fn resolve_gateways(
    cfg: &Config,
    selected_gateway: Option<&str>,
) -> anyhow::Result<Vec<GatewayDef>> {
    match selected_gateway {
        Some(name) => {
            let gw = cfg
                .gateways
                .iter()
                .find(|g| g.name == name)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "unknown gateway '{name}': no gateways.d/{name}.toml defines it"
                    )
                })?;
            Ok(vec![gw.clone()])
        }
        None => Ok(cfg.gateways.clone()),
    }
}

/// Build the initial [`ProxyState`], wrapping `cfg` in an `ArcSwap` so it can
/// be hot-swapped later (see M7's config-reload work) without ever handing
/// out a config that changes mid-request — `proxy::handle` snapshots it once
/// per request via `load_full()`.
///
/// The in-flight semaphore's `max_inflight` permit count is derived from
/// `cfg` once, here, at startup: hot-reloading `max_inflight` is out of scope
/// (a later config swap changes routing/limits/etc. seen by *new* requests,
/// but not the size of this semaphore).
///
/// The model facts [`Registry`] is likewise built once, here, via
/// `Registry::load()` — unlike `config`, it is not hot-swappable in M9; a
/// refreshed registry only takes effect on the next process restart (see
/// `Registry::load`'s own doc comment for the local-file-vs-embedded-seed
/// precedence).
pub fn build_state(cfg: Config) -> Arc<ProxyState> {
    let inflight = Arc::new(Semaphore::new(cfg.gateway.max_inflight));
    let continuations = Arc::new(loopback::Registry::new());
    spawn_continuation_sweeper(continuations.clone());
    Arc::new(ProxyState {
        config: Arc::new(ArcSwap::from_pointee(cfg)),
        client: reqwest::Client::new(),
        inflight,
        registry: Arc::new(Registry::load()),
        continuations,
        allowed_routes: None,
        wasm_cache: Arc::new(dashmap::DashMap::new()),
        worker_cache: Arc::new(dashmap::DashMap::new()),
    })
}

/// Build a sibling [`ProxyState`] that shares every field of `base` — the
/// config `ArcSwap`, HTTP client, in-flight semaphore, model registry,
/// loopback continuation registry, compiled-wasm-module cache, and script
/// worker-process cache — except
/// `allowed_routes`. This is how a
/// multi-listener gateway process (M13 Task 2) gives each listener its own
/// route filter while still running as one process: a config swap, a
/// parked loopback continuation, and the in-flight ceiling are all shared
/// across every listener built this way. That sharing is exactly why this
/// is NOT a way to get crash isolation between listeners (see this module's
/// top doc comment) — a panic or resource exhaustion on one listener's
/// connections still affects every other listener sharing this state. Only
/// `--gateway <name>` run as separate OS processes gives you that.
///
/// Hot-reload asymmetry: the `allowed_routes` set baked in here (and the
/// `TcpListener` it's paired with by `serve`'s caller) are both derived once
/// at process startup from `gateways.d` and never revisited — unlike
/// `routes.d`, which `spawn_watcher` reloads into the shared `ArcSwap` live.
/// Adding/removing a `gateways.d/*.toml` listener, or editing an existing
/// one's `routes`, has no effect on a running process; it takes a restart
/// (same limitation as `build_state`'s `max_inflight` note above).
pub fn derive_state(
    base: &Arc<ProxyState>,
    allowed_routes: Option<Arc<HashSet<String>>>,
) -> Arc<ProxyState> {
    Arc::new(ProxyState {
        config: base.config.clone(),
        client: base.client.clone(),
        inflight: base.inflight.clone(),
        registry: base.registry.clone(),
        continuations: base.continuations.clone(),
        allowed_routes,
        wasm_cache: base.wasm_cache.clone(),
        worker_cache: base.worker_cache.clone(),
    })
}

/// How often the background sweeper below reaps loopback continuations, and
/// how old one must be to qualify. `CONTINUATION_SWEEP_TTL` is deliberately
/// well past `proxy::LOOPBACK_PARK_TIMEOUT` (60s): a continuation this old
/// has already outlived any handler that could still be legitimately parked
/// waiting on it, so it is unambiguously abandoned.
const CONTINUATION_SWEEP_INTERVAL: Duration = Duration::from_secs(30);
const CONTINUATION_SWEEP_TTL: Duration = Duration::from_secs(120);

/// Periodically drop loopback continuations older than
/// [`CONTINUATION_SWEEP_TTL`] (design doc §4.5, M12).
///
/// This is the backstop for a continuation whose parked handler was itself
/// cancelled before it could clean up after itself — e.g. the client
/// disconnected while parked, which drops the whole handler future
/// (`proxy::forward`'s doc comment covers why that cancellation is implicit
/// and free) without ever reaching `dispatch_loopback`'s own timeout/cleanup
/// branch. Without this sweeper, such a continuation's `oneshot::Sender`
/// (and whatever it's holding onto) would never be dropped, growing the
/// registry unboundedly over a long-running process's lifetime.
fn spawn_continuation_sweeper(continuations: Arc<loopback::Registry>) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(CONTINUATION_SWEEP_INTERVAL).await;
            continuations.sweep(CONTINUATION_SWEEP_TTL);
        }
    });
}

pub async fn serve_on(
    listener: TcpListener,
    cfg: Config,
    ready: Ready,
    metrics: MetricsHandle,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    let admin_listen = cfg.gateway.admin_listen.clone();
    let admin_token = cfg.gateway.admin_token.clone();
    let state = build_state(cfg);

    serve_with_state(
        listener,
        state,
        admin_listen,
        admin_token,
        ready,
        metrics,
        shutdown,
    )
    .await
}

/// Same as [`serve_on`], but takes an already-built [`ProxyState`] rather
/// than building one from a fresh `Config`. Exists so tests (and, later, the
/// hot-reload watcher's owner) can hold onto the `Arc<ProxyState>` — and
/// hence its `ArcSwap` — after the server starts, e.g. to call
/// `state.config.store(...)` and observe a subsequent request pick up the
/// change.
///
/// Single-listener only: this signature (one `TcpListener`, one
/// `ProxyState`) predates the M13 Task 2 multi-listener work and is kept
/// as-is so it and every caller (`serve_on`, and most of this crate's own
/// integration tests) are unaffected by it. See [`serve_multi`] for the
/// N-listener sibling `serve`'s own multi-gateway path drives.
pub async fn serve_with_state(
    listener: TcpListener,
    state: Arc<ProxyState>,
    admin_listen: String,
    admin_token: String,
    ready: Ready,
    metrics: MetricsHandle,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    // `shutdown` is a plain, single-consumer future (its only caller-side
    // requirement), but both the data-plane accept loop below and the
    // optional admin listener need to observe it independently. Bridge it
    // into a `watch` channel once here so each side gets its own receiver.
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(async move {
        shutdown.await;
        let _ = shutdown_tx.send(true);
    });

    spawn_admin(
        admin_listen,
        admin_token,
        ready.clone(),
        metrics,
        shutdown_rx.clone(),
    );

    // The data-plane `TcpListener` passed in is already bound (that's the
    // caller's job — see `serve` above and the integration test helpers),
    // so flip `/readyz` to 200 now, before entering the accept loop.
    ready.set_ready();

    let mut shutdown_rx = shutdown_rx;
    accept_loop(listener, state, &mut shutdown_rx).await
}

/// Like [`serve_with_state`], but drives several already-bound listeners in
/// one process instead of one — `serve`'s entrypoint for the M13 Task 2
/// multi-listener/multi-gateway case. Each `(TcpListener, Arc<ProxyState>)`
/// pair gets its own independent accept loop (ordinarily a `ProxyState`
/// built via [`derive_state`] so each carries a different `allowed_routes`),
/// all fed by the same shutdown signal, while the admin listener is still
/// bound exactly once — never duplicated per gateway.
///
/// Returns as soon as any one accept loop returns (success or error) — via
/// `futures_util::future::select_all` racing every listener's `JoinHandle`,
/// not a sequential await, so a later listener erroring out while an
/// earlier one is still healthy is noticed immediately rather than being
/// masked by blocking on the earlier (still-running) handle forever. The
/// sibling loops still running at that point are left running as detached
/// tasks rather than explicitly aborted (acceptable here: this only happens
/// on shutdown or on a `TcpListener::accept` error, neither of which this
/// M13 task needs to recover from more gracefully).
pub async fn serve_multi(
    listeners: Vec<(TcpListener, Arc<ProxyState>)>,
    admin_listen: String,
    admin_token: String,
    ready: Ready,
    metrics: MetricsHandle,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(async move {
        shutdown.await;
        let _ = shutdown_tx.send(true);
    });

    spawn_admin(
        admin_listen,
        admin_token,
        ready.clone(),
        metrics,
        shutdown_rx.clone(),
    );

    ready.set_ready();

    if listeners.is_empty() {
        // Nothing to race — `select_all` panics on an empty iterator.
        // `serve`'s own callers never hit this (see `resolve_gateways`: the
        // multi-listener path is only entered with at least one gateway),
        // but this function is `pub` and exercised directly by tests.
        return Ok(());
    }

    let mut handles = Vec::with_capacity(listeners.len());
    for (listener, state) in listeners {
        let mut listener_shutdown_rx = shutdown_rx.clone();
        handles.push(tokio::spawn(async move {
            accept_loop(listener, state, &mut listener_shutdown_rx).await
        }));
    }

    // Race every listener's accept loop: the first one to finish (cleanly on
    // shutdown, or with an error) decides `serve_multi`'s result. The other,
    // still-running handles are simply dropped here — dropping a
    // `tokio::task::JoinHandle` detaches the task rather than aborting it,
    // so they keep running as documented above.
    let (result, _index, _remaining) = futures_util::future::select_all(handles).await;
    result??;
    Ok(())
}

/// Spawn the admin listener (health/ready/metrics) as a background task if
/// `admin_listen` is non-empty — shared by [`serve_with_state`] and
/// [`serve_multi`] so the admin listener is bound exactly once regardless
/// of how many data-plane listeners the process is running.
fn spawn_admin(
    admin_listen: String,
    admin_token: String,
    ready: Ready,
    metrics: MetricsHandle,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    if admin_listen.is_empty() {
        return;
    }
    if admin_token.is_empty() {
        // Opt-in, not a hard failure (see `admin::authorized`: an empty
        // token means auth is disabled entirely, which is a legitimate
        // choice for e.g. an admin listener already bound to loopback-only
        // or an otherwise trusted network) — but this is exactly the
        // footgun Fix C (M14 final review) hardens against: silently
        // exposing `/metrics` and `/readyz` with no authentication at all.
        // Surface it loudly so an operator who didn't intend that notices.
        tracing::warn!(
            admin_listen = %admin_listen,
            "admin_listen is set but admin_token is empty: /metrics and /readyz on \
             this listener are UNAUTHENTICATED"
        );
    }
    tokio::spawn(async move {
        if let Err(err) =
            admin::serve_admin(&admin_listen, ready, metrics, admin_token, async move {
                let _ = shutdown_rx.changed().await;
            })
            .await
        {
            eprintln!("admin listener error: {err}");
        }
    });
}

/// One data-plane listener's accept loop: shared by [`serve_with_state`]
/// (one listener) and [`serve_multi`] (N listeners, one call per listener).
/// Routes each accepted connection's requests through `handle` (or
/// `handle_callback` for the loopback callback path — see the inline
/// comment below), and returns once `shutdown_rx` observes a change.
async fn accept_loop(
    listener: TcpListener,
    state: Arc<ProxyState>,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> anyhow::Result<()> {
    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => break,
            accepted = listener.accept() => {
                let (stream, _peer) = accepted?;
                let io = TokioIo::new(stream);
                let state = state.clone();
                tokio::spawn(async move {
                    let service = service_fn(move |req| {
                        let state = state.clone();
                        async move {
                            // The loopback callback (design doc §4.5, M12
                            // Task 3) shares this same data listener rather
                            // than a separate port. It is routed here BEFORE
                            // `handle`'s own routing/permit acquisition, and
                            // deliberately never goes through `handle` at
                            // all: `handle_callback` must acquire no
                            // `max_inflight` permit of its own (see its doc
                            // comment) since the parked original handler it
                            // resumes already holds one for the chain's whole
                            // lifetime — routing it through `handle` here
                            // would risk exactly the deadlock that split
                            // exists to avoid.
                            let live_config = state.config.load_full();
                            let resp = if req.uri().path() == live_config.gateway.callback_path {
                                handle_callback(state, req).await
                            } else {
                                handle(state, req).await
                            };
                            Ok::<_, std::convert::Infallible>(resp)
                        }
                    });
                    let _ = Builder::new(TokioExecutor::new())
                        .serve_connection(io, service)
                        .await;
                });
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    //! `serve_multi`'s whole point is racing several already-spawned
    //! `JoinHandle`s (one per listener's `accept_loop`) via
    //! `futures_util::future::select_all` rather than awaiting them in
    //! sequence. Reproducing an actual `TcpListener::accept` error
    //! deterministically (to drive `serve_multi` itself end-to-end) isn't
    //! practical from a test, so this instead pins down the `select_all`
    //! wiring directly: given one handle that never finishes (standing in
    //! for a healthy accept loop) and one that errors out immediately
    //! (standing in for a dead one), the race must resolve to the error
    //! promptly — not hang waiting on the healthy handle, which is exactly
    //! the bug this fixes (see this module's `serve_multi` doc comment).

    use std::time::Duration;

    #[tokio::test]
    async fn select_all_resolves_to_the_first_finished_handle_not_the_first_in_order() {
        // Ordered first in the Vec (like an "earlier" listener in
        // `serve_multi`), but never completes.
        let never_finishes = tokio::spawn(async {
            std::future::pending::<()>().await;
            Ok::<(), anyhow::Error>(())
        });
        // Ordered second (like a "later" listener), but fails right away.
        let fails_immediately =
            tokio::spawn(async { Err::<(), anyhow::Error>(anyhow::anyhow!("accept loop died")) });

        let handles = vec![never_finishes, fails_immediately];

        let (result, index, remaining) = tokio::time::timeout(
            Duration::from_secs(5),
            futures_util::future::select_all(handles),
        )
        .await
        .expect(
            "select_all must resolve as soon as the second handle errors, \
                     not hang on the first handle that never finishes",
        );

        assert_eq!(
            index, 1,
            "the failed handle must be the one select_all reports"
        );
        assert_eq!(
            remaining.len(),
            1,
            "the still-running handle is left in `remaining`"
        );
        let err = result
            .expect("join itself must succeed (no panic)")
            .unwrap_err();
        assert!(err.to_string().contains("accept loop died"));
    }
}
