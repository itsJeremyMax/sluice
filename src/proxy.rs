use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arc_swap::ArcSwap;
use base64::Engine;
use bytes::Bytes;
use futures_util::{stream, Stream, StreamExt};
use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode};
use tokio::sync::mpsc;
use tracing::Instrument;

use crate::config::{
    ChunkMode, Config, OnError, RouteAdapter, ScriptMode, Step, StepType, Translate, UrlMode,
};
use crate::directive::{apply_ops, Directive, RespSpec};
use crate::envelope::Envelope;
use crate::envelope::{build_request_envelope, build_response_envelope, build_stream_envelope};
use crate::http_msg::HttpMsg;
use crate::llm::adapter::{adapter_for, Adapter};
use crate::llm::{
    project_llm, resolve_facts, CanonicalStreamEvent, Llm, RequestCtx, StreamParseState,
    StreamRenderState, TranslationReport,
};
use crate::loopback::{self, token::ChainToken};
use crate::observability::{
    correlation_id_from, InflightGuard, METRIC_ABORT_TOTAL, METRIC_REQUESTS_TOTAL,
    METRIC_REQUEST_DURATION_SECONDS, METRIC_SHORT_CIRCUIT_TOTAL, METRIC_STEP_ERROR_TOTAL,
    METRIC_STREAM_SHED, METRIC_UPSTREAM_STATUS_TOTAL,
};
use crate::reconstruct;
use crate::registry::Registry;
use crate::router::{route_request, upstream_url_for};
use crate::sse::{SseEvent, SseFramer};
use crate::step::script::{ScriptOneshot, ScriptWorker};
use crate::step::url::UrlTransform;
use crate::step::wasm::WasmStep;
use crate::step::StepError;

/// Dispatch one step's invocation to the runtime matching its configured
/// `type`: `url` (HTTP POST to a step service, see [`UrlTransform`]),
/// `script` (spawn `cmd` as a oneshot subprocess, see [`ScriptOneshot`]), or
/// `wasm` (call a compiled guest module over the alloc/run ABI, see
/// [`WasmStep`]). Used uniformly by both the `on_request` and `on_response`
/// step loops below, so the three hooks share one dispatch path and one
/// directive handling story regardless of which runtime a given step
/// actually uses.
async fn run_step(state: &ProxyState, step: &Step, env: &Envelope) -> Result<Directive, StepError> {
    match step.type_ {
        StepType::Url => {
            let url = step.url.clone().expect("validated url present");
            UrlTransform::new(url, step.timeout_ms)
                .run(&state.client, env)
                .await
        }
        StepType::Script => match step.script_mode {
            ScriptMode::Oneshot => {
                let cmd = step.cmd.clone();
                ScriptOneshot::new(cmd, step.timeout_ms).run(env).await
            }
            ScriptMode::Worker => run_cached_worker_step(state, step, env).await,
        },
        StepType::Wasm => {
            let path = step.wasm.clone().expect("validated wasm path present");
            let wasm_step = get_or_compile_wasm(&state.wasm_cache, path).await?;
            wasm_step
                .run(env, Duration::from_millis(step.timeout_ms))
                .await
        }
    }
}

/// Look up (or compile-and-cache) the [`WasmStep`] for a `type = "wasm"`
/// step's module `path`, keyed by that path string in `ProxyState`'s
/// `wasm_cache`.
///
/// Compilation (`WasmStep::from_path`, which builds a wasmtime `Engine` and
/// compiles the `Module` — the expensive part) happens at most once per
/// distinct path, on whichever request first exercises that step; every
/// later call, on this or any other request, reuses the cached `Arc<WasmStep>`
/// and only pays for a fresh `Store`/`Instance` (see `WasmStep::run`).
/// Compilation runs inside `spawn_blocking` since `Module::from_file` is a
/// synchronous, potentially-slow call and must not block the async runtime
/// worker thread on a request's first hit.
///
/// Caching is by path only, not by file content or mtime: editing the
/// `.wasm` file at an already-cached path has no effect on a running
/// gateway (every subsequent call keeps using the module compiled the first
/// time that path was seen) until the process restarts. That mirrors how
/// `type = "script"`/`type = "url"` steps are configured (no live-reload of
/// the transform's own code either) and is an accepted limitation for M14,
/// not a bug.
///
/// A benign race is possible under concurrent first-use: two requests can
/// both miss the cache for the same never-before-seen path and both compile
/// the module before either insert finishes. `DashMap::entry` makes the
/// insert itself atomic, so whichever compile lands first wins and callers
/// always agree on a single cached `Arc<WasmStep>` afterward — the other
/// compile's result is simply dropped, an extra compile rather than a
/// correctness issue.
///
/// Deliberately does NOT take (or bake in) a `timeout_ms`: `WasmStep` itself
/// holds no timeout (see its doc comment) precisely because this cache is
/// keyed by path only, so two different step configs that happen to name
/// the same `.wasm` path share this one compiled `Arc<WasmStep>`. Baking in
/// whichever step's `timeout_ms` first compiled a path would silently apply
/// that timeout to every other step sharing the path too. Instead
/// `run_step` passes each call's own `step.timeout_ms` straight to
/// `WasmStep::run` at call time, after fetching the (timeout-agnostic)
/// cached step from here.
async fn get_or_compile_wasm(
    cache: &dashmap::DashMap<String, Arc<WasmStep>>,
    path: String,
) -> Result<Arc<WasmStep>, StepError> {
    if let Some(existing) = cache.get(&path) {
        return Ok(existing.clone());
    }
    let compile_path = path.clone();
    let compiled = tokio::task::spawn_blocking(move || {
        WasmStep::from_path(std::path::Path::new(&compile_path))
    })
    .await
    .map_err(|join_err| {
        StepError::WasmAbi(format!("wasm compile task did not complete: {join_err}"))
    })??;
    let compiled = Arc::new(compiled);
    let entry = cache.entry(path).or_insert_with(|| compiled.clone());
    Ok(entry.clone())
}

/// Cache key for a `script_mode = "worker"` step: the `cmd` argv joined with a
/// NUL byte (which can't appear inside an argv element), so distinct argvs map
/// to distinct keys and two steps with byte-identical argvs share one worker.
pub(crate) fn worker_cache_key(cmd: &[String]) -> String {
    cmd.join("\0")
}

/// Prune the shared script-worker cache after a config hot-reload so a worker
/// whose step was removed (or whose `cmd` changed, minting a new cache key) is
/// dropped and its `kill_on_drop` child reaped instead of leaking across
/// reloads.
///
/// The valid-key set is built from the FULL reloaded `config` (every
/// `type = "script", script_mode = "worker"` step across ALL routes), NOT from
/// any per-listener route filter. A multi-listener process shares one
/// `worker_cache` across route-filtered derived states, so pruning by a subset
/// would wrongly reap workers another listener still serves.
///
/// `DashMap::retain` drops the `Arc` for each removed entry. A worker no
/// request is mid-using then has its refcount hit zero and its child reaped; a
/// worker an in-flight request is still using survives (that request holds its
/// own `Arc` clone) and is reaped when the request finishes and its clone
/// drops.
pub(crate) fn prune_worker_cache(state: &ProxyState, config: &Config) {
    let valid: std::collections::HashSet<String> = config
        .routes
        .iter()
        .flat_map(|route| route.steps.iter())
        .filter(|step| {
            step.type_ == crate::config::StepType::Script
                && step.script_mode == crate::config::ScriptMode::Worker
        })
        .map(|step| worker_cache_key(&step.cmd))
        .collect();
    state.worker_cache.retain(|k, _| valid.contains(k));
}

/// Look up (or spawn-and-cache) the shared [`ScriptWorker`] for a
/// `script_mode = "worker"` step on `on_request`/`on_response`, keyed by its
/// command line in `ProxyState`'s `worker_cache`. Mirrors
/// [`get_or_compile_wasm`]: the worker process is spawned at most once per
/// distinct argv (on whichever request first exercises that step) and every
/// later call reuses the cached `Arc<Mutex<ScriptWorker>>`.
///
/// Like the wasm cache, a benign spawn race is possible under concurrent
/// first-use — `DashMap::entry` makes the insert atomic, so whichever spawn
/// lands first wins and the loser's freshly-spawned child is dropped (killed
/// via `kill_on_drop`), an extra spawn rather than a correctness issue.
fn get_or_spawn_worker(
    cache: &dashmap::DashMap<String, Arc<tokio::sync::Mutex<ScriptWorker>>>,
    key: &str,
    cmd: &[String],
    timeout_ms: u64,
) -> Result<Arc<tokio::sync::Mutex<ScriptWorker>>, StepError> {
    if let Some(existing) = cache.get(key) {
        return Ok(existing.clone());
    }
    let spawned = Arc::new(tokio::sync::Mutex::new(ScriptWorker::spawn(
        cmd, timeout_ms,
    )?));
    let entry = cache
        .entry(key.to_string())
        .or_insert_with(|| spawned.clone());
    Ok(entry.clone())
}

/// Dispatch a `script_mode = "worker"` step (on `on_request`/`on_response`)
/// through the shared cached worker: get-or-spawn the worker, lock it, and
/// `call` it with the serialized envelope.
///
/// Crash recovery: if `call` errors because the worker genuinely died between
/// requests (`StepError::Spawn` = broken pipe / EOF / short read / oversize
/// length prefix = the framing is desynced/dead), the dead entry is removed
/// from the cache and the worker is respawned ONCE, retrying the call a single
/// time. A second failure is returned as a `StepError` (honored via the step's
/// `on_error`); the gateway never loops respawning a worker that fails
/// deterministically.
///
/// Two error classes are deliberately NOT treated as death:
/// - `StepError::Timeout`: the worker is not retried within this request
///   (that would pay a second full timeout). `ScriptWorker::call` already
///   killed the child on timeout, so the entry now holds a dead worker; we
///   evict it here so the NEXT request spawns fresh instead of hitting the
///   corpse.
/// - `StepError::Decode`: the worker is ALIVE and healthy but emitted a
///   syntactically-invalid Directive frame. Killing/respawning it would churn
///   the process and discard its warm state over a deterministic bad output,
///   so we return the error unchanged (honored via the step's `on_error`)
///   WITHOUT evicting or respawning.
async fn run_cached_worker_step(
    state: &ProxyState,
    step: &Step,
    env: &Envelope,
) -> Result<Directive, StepError> {
    let key = worker_cache_key(&step.cmd);
    let envelope_json = serde_json::to_vec(env)?;

    let worker = get_or_spawn_worker(&state.worker_cache, &key, &step.cmd, step.timeout_ms)?;
    let first = {
        let mut guard = worker.lock().await;
        guard.call(&envelope_json).await
    };
    match first {
        Ok(directive) => Ok(directive),
        Err(StepError::Timeout) => {
            // `ScriptWorker::call` already killed the child on timeout, so the
            // cached entry now holds a dead worker. Evict this exact entry
            // (only if the map still points at it; a concurrent request may
            // already have replaced it) so the NEXT request spawns fresh, then
            // surface the timeout WITHOUT retrying (a retry would pay a second
            // full timeout budget).
            state
                .worker_cache
                .remove_if(&key, |_, v| Arc::ptr_eq(v, &worker));
            Err(StepError::Timeout)
        }
        Err(StepError::Decode(e)) => {
            // The worker is alive and healthy; it just emitted a bad Directive
            // frame. Do NOT evict or respawn: surface the decode error so the
            // step's `on_error` policy handles it while the warm worker lives
            // on to serve the next request.
            Err(StepError::Decode(e))
        }
        Err(_died) => {
            // Worker genuinely died mid-session (`StepError::Spawn`: broken
            // pipe / EOF / short read / oversize length prefix). Evict this
            // exact dead entry (only if the map still points at it; a
            // concurrent request may already have replaced it) and respawn
            // once, retrying the call a single time.
            state
                .worker_cache
                .remove_if(&key, |_, v| Arc::ptr_eq(v, &worker));
            let fresh = get_or_spawn_worker(&state.worker_cache, &key, &step.cmd, step.timeout_ms)?;
            let mut guard = fresh.lock().await;
            guard.call(&envelope_json).await
        }
    }
}

/// The boxed client-response body type. Owned by `reconstruct` (the module
/// that owns response construction); re-exported here since `proxy` remains
/// the module that wires requests through the pipeline.
pub use crate::reconstruct::ResponseBody;

pub struct ProxyState {
    /// The live config, swappable at runtime (see M7's hot-reload work).
    /// `proxy::handle` takes a single `load_full()` snapshot at the top of
    /// each request and threads that `Arc<Config>` through the whole
    /// request, so a swap mid-request never changes the config a request
    /// sees partway through.
    pub config: Arc<ArcSwap<Config>>,
    pub client: reqwest::Client,
    /// Global concurrency ceiling: one permit per in-flight request. When
    /// exhausted, `handle` sheds the request with 503 rather than queuing
    /// (see `try_acquire_owned` call at the top of `handle`).
    pub inflight: Arc<tokio::sync::Semaphore>,
    /// Model facts registry (design doc M9), built once at serve time via
    /// `Registry::load()`. Consulted per-request by `handle_inner` to fill in
    /// `Llm::facts` once a route's ingress adapter has parsed the model out
    /// of the request body.
    pub registry: Arc<Registry>,
    /// In-flight loopback continuations (design doc §4.5, M12): populated by
    /// `dispatch_loopback` when a chain parks, consumed by the callback (M12
    /// Task 3) when it resumes one. See `loopback::Registry`'s own doc
    /// comment for the full park/resume story.
    pub continuations: Arc<loopback::Registry>,
    /// Per-listener route filter for multi-listener gateways (M13 Task 2):
    /// `Some(set)` restricts this listener to serving only route ids in
    /// `set` — any other configured route 404s exactly as an undefined path
    /// would, on this listener only (see `server::derive_state`'s doc
    /// comment for how sibling listeners in the same process get their own
    /// sets). `None` (the ordinary single-listener case) serves every
    /// route the live config defines.
    pub allowed_routes: Option<Arc<std::collections::HashSet<String>>>,
    /// Compile-once cache for `type = "wasm"` steps (M14 Task 2), keyed by
    /// the step's configured `wasm` module path. See `get_or_compile_wasm`'s
    /// doc comment for the full caching story (compiled at most once per
    /// path, shared `Arc<WasmStep>` across every request and listener that
    /// hits that path, and — notably — no live-reload: a `.wasm` file
    /// edited at an already-cached path keeps serving the module compiled
    /// the first time that path was seen until the process restarts).
    pub wasm_cache: Arc<dashmap::DashMap<String, Arc<WasmStep>>>,
    /// Spawn-once cache for `type = "script"`, `script_mode = "worker"` steps
    /// running on `on_request`/`on_response` (M17 Task 2), keyed by the step's
    /// command line (the `cmd` argv joined with `'\0'`, so two steps with the
    /// same argv share one long-lived worker process while distinct argvs each
    /// get their own). Each entry is an `Arc<Mutex<ScriptWorker>>`: the mutex
    /// serializes the framed request/response exchange (`ScriptWorker::call`
    /// reads exactly one response frame per request, so two concurrent callers
    /// must not interleave on one process), and the `Arc` lets it be shared
    /// across requests and listeners. Mirrors `wasm_cache`'s
    /// compile/spawn-at-most-once-per-key story.
    ///
    /// A worker that dies mid-session surfaces a `call` error; `run_step`
    /// removes the dead entry and respawns once (see there). This cache is
    /// NOT used for `on_stream` mutate workers — those get a dedicated
    /// per-stream process (see `forward`'s on_stream branch) so a stateful
    /// worker's per-stream state can never interleave across concurrent
    /// streams.
    pub worker_cache: Arc<dashmap::DashMap<String, Arc<tokio::sync::Mutex<ScriptWorker>>>>,
}

/// Build a plain-text gateway-authored response (errors, 404s, etc). A thin
/// wrapper over the single reconstruction entrypoint so even gateway-authored
/// text still passes through [`reconstruct::sanitize_egress_headers`].
fn text_response(status: StatusCode, body: &str) -> Response<ResponseBody> {
    let mut headers = BTreeMap::new();
    headers.insert(
        "content-type".to_string(),
        "text/plain; charset=utf-8".to_string(),
    );
    reconstruct::client_response(status.as_u16(), &headers, Bytes::from(body.to_owned()), &[])
}

async fn request_to_httpmsg(
    req: Request<Incoming>,
    max_body_bytes: usize,
) -> Result<HttpMsg, Response<ResponseBody>> {
    let method = req.method().to_string();
    let path = req
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str().to_string())
        .unwrap_or_else(|| req.uri().path().to_string());
    let mut headers = std::collections::BTreeMap::new();
    for (name, value) in req.headers() {
        if let Ok(v) = value.to_str() {
            headers.insert(name.as_str().to_ascii_lowercase(), v.to_string());
        }
    }
    // Cap how much body the gateway will buffer: `Limited` surfaces a
    // `LengthLimitError` once more than `max_body_bytes` has been read,
    // which we translate into a 413 before anything reaches the upstream.
    let limited = http_body_util::Limited::new(req.into_body(), max_body_bytes);
    let collected = limited.collect().await.map_err(|err| {
        if err.is::<http_body_util::LengthLimitError>() {
            text_response(
                StatusCode::PAYLOAD_TOO_LARGE,
                "request body exceeds max_body_bytes",
            )
        } else {
            text_response(StatusCode::BAD_REQUEST, "failed to read request body")
        }
    })?;
    let body = collected.to_bytes();
    let body_b64 = base64::engine::general_purpose::STANDARD.encode(&body);
    Ok(HttpMsg {
        method,
        path,
        headers,
        body_b64,
    })
}

pub async fn handle(state: Arc<ProxyState>, req: Request<Incoming>) -> Response<ResponseBody> {
    // Snapshot the live config once, up front, and thread this single
    // `Arc<Config>` through the entire request (route lookup, limits,
    // expose_headers, upstream timeout, ...). A config swap that lands
    // mid-request (see `ProxyState::config`) must never change what this
    // request sees partway through — only fresh requests observe it.
    let config = state.config.load_full();

    // Shed load rather than queue it: a request that can't get a permit
    // right now is rejected immediately with 503 (no permit to attach, so
    // this path returns directly rather than going through `handle_inner`).
    let permit = match state.inflight.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            return text_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "too many in-flight requests",
            )
        }
    };

    // The live in-flight gauge's guard is constructed the instant the permit
    // is granted (matching the permit's lifetime exactly) and travels with
    // it into the response body below, so `sluice_inflight` reflects every
    // request from permit acquisition through full body delivery — not just
    // the `handle_inner` call — and decrements exactly once even on an early
    // return or panic, since `Drop` always runs.
    let inflight = InflightGuard::new();

    // `permit` is attached to the *final* response's body (see
    // `reconstruct::attach_permit`) rather than simply held in this frame,
    // so it stays alive for as long as the client is still receiving the
    // response body — including the whole duration of a streamed upstream
    // response — not just until `handle_inner` returns with headers ready.
    let resp = handle_inner(state, config, req).await;
    reconstruct::attach_permit(resp, permit, inflight)
}

async fn handle_inner(
    state: Arc<ProxyState>,
    config: Arc<Config>,
    req: Request<Incoming>,
) -> Response<ResponseBody> {
    let msg = match request_to_httpmsg(req, config.gateway.max_body_bytes).await {
        Ok(m) => m,
        Err(resp) => return resp,
    };

    let (route_id, upstream_base, steps, adapter, translate) =
        match route_request(&config, &msg.path) {
            Some(m) => (
                m.route.id.clone(),
                m.route.upstream.clone(),
                m.route.steps.clone(),
                m.route.adapter.clone(),
                m.route.translate.clone(),
            ),
            None => return text_response(StatusCode::NOT_FOUND, "no route matches this path"),
        };

    // Per-listener route filter (M13 Task 2): a multi-listener gateway
    // process gives each listener's `ProxyState` its own `allowed_routes`
    // set (see `server::derive_state`). A route that matched globally but
    // isn't in *this* listener's set is treated identically to a route that
    // doesn't exist at all — same 404, so a client on the wrong listener
    // can't distinguish "unrouted" from "routed but not exposed here".
    if let Some(allowed) = &state.allowed_routes {
        if !allowed.contains(&route_id) {
            return text_response(StatusCode::NOT_FOUND, "no route matches this path");
        }
    }

    // Parse the request body into the provider-agnostic `llm` view when this
    // route names an ingress adapter, enriching it with the model's resolved
    // facts. A body that fails to parse (malformed JSON, missing `model`, or
    // simply not an LLM-shaped body) is not a request failure — `llm` stays
    // `None` and the chain proceeds exactly as it would for any other route.
    let llm = build_llm_view(&state, &msg, adapter.as_ref(), translate.as_ref());

    // The ingress provider name (independent of whether `llm` above actually
    // parsed) is threaded down to `forward` so an `on_stream` observe tee can
    // resolve the same provider's `Adapter::parse_delta` for the *response*
    // stream even on a request whose body didn't parse as that provider's
    // request shape.
    let ingress = adapter.as_ref().and_then(|a| a.ingress.clone());

    // The correlation id ties this request's envelopes/logs together; it is
    // internal-only (never echoed to the client — see `reconstruct`'s
    // egress sanitizer) and is adopted from an inbound `x-request-id`
    // header when that value looks safe, else freshly generated.
    let correlation_id = correlation_id_from(msg.headers.get("x-request-id").map(String::as_str));

    // Every log line/span emitted while handling this request (including
    // per-step spans below) is nested under this one, so the route and
    // correlation id are attached to the whole request's worth of logs
    // without having to thread them through every call site.
    let span = tracing::info_span!("request", route = %route_id, correlation_id = %correlation_id);
    let start = Instant::now();

    let resp = run_from(
        state,
        config,
        route_id.clone(),
        upstream_base,
        steps,
        translate,
        correlation_id,
        llm,
        ingress,
        msg,
        serde_json::Map::new(),
        0,
        0,
    )
    .instrument(span)
    .await;

    // Recorded around the *whole* handling (routing, steps, and forward)
    // regardless of which branch inside `run_from` produced the
    // response, since this call site is the single point every path above
    // funnels back through. `outcome` is derived from the final *status*: an
    // `on_request`/`on_response` step error or fail-closed rejection surfaces
    // here as a 5xx `resp`, so `is_server_error()` captures it. Known blind
    // spot: an `on_stream` fail-closed abort happens *after* a 200 status +
    // headers are already committed and this `resp` has funnelled through, so
    // such a mid-stream truncation is counted `outcome="ok"` — it is instead
    // tracked by `METRIC_ABORT_TOTAL` / `METRIC_STEP_ERROR_TOTAL`, which fire
    // at the streaming call sites. This is inherent to scoring outcome at the
    // single pre-headers funnel point rather than a bug in the label.
    let outcome = if resp.status().is_server_error() {
        "error"
    } else {
        "ok"
    };
    metrics::counter!(METRIC_REQUESTS_TOTAL, "route" => route_id.clone(), "outcome" => outcome)
        .increment(1);
    metrics::histogram!(METRIC_REQUEST_DURATION_SECONDS, "route" => route_id)
        .record(start.elapsed().as_secs_f64());

    resp
}

/// Parse the request body into the provider-agnostic `llm` view. The
/// EFFECTIVE ingress provider is `[route.adapter] ingress = ...` when set,
/// else — as of the M15 Task 6 review fix — `[route.translate] from = ...`
/// on a translate route: a translate route's client body is always shaped
/// like `translate.from`'s dialect (that's what `translate_request` parses
/// it as too), so `on_request` steps get the same normalized `llm` view on a
/// translate route as they would if that route had named an ingress adapter
/// outright. Without this fallback, `on_request` guardrail/observe steps saw
/// no `llm` view at all on exactly the routes that translate. `adapter`
/// winning over `translate` when both are configured keeps existing
/// non-translate behavior identical.
///
/// Returns `None` — never an error response — when neither `adapter.ingress`
/// nor `translate.from` is configured, the resolved provider name is
/// unrecognized, or the body fails to parse: a route with no (or
/// non-matching) LLM shape simply carries no `llm` view through the chain
/// (design doc M9 Task 2).
///
/// As of M15 Task 2, the adapter parses into the lossless
/// [`crate::llm::CanonicalRequest`] IR (using a [`RequestCtx`] built from the
/// message's path/method — Google's adapter needs the path to read the
/// model id out of it) and [`project_llm`] projects that down to the `Llm`
/// view the rest of the chain already knows about, so this view's shape is
/// unchanged even though parsing now goes through the canonical IR.
fn build_llm_view(
    state: &ProxyState,
    msg: &HttpMsg,
    adapter: Option<&RouteAdapter>,
    translate: Option<&Translate>,
) -> Option<Llm> {
    let ingress = adapter
        .and_then(|a| a.ingress.as_deref())
        .or_else(|| translate.map(|t| t.from.as_str()))?;
    let adapter = adapter_for(ingress)?;
    let body = msg.body_bytes().ok()?;
    let ctx = RequestCtx {
        path: msg.path.clone(),
        method: msg.method.clone(),
    };
    match adapter.parse_request(&body, &ctx) {
        Ok(canonical) => {
            let mut llm = project_llm(&canonical, ingress);
            llm.facts = resolve_facts(&state.registry, ingress, &canonical.model);
            Some(llm)
        }
        Err(err) => {
            tracing::debug!("ingress adapter '{ingress}' failed to parse request body: {err}");
            None
        }
    }
}

/// Run the matched route's `on_request` step pipeline STARTING FROM
/// `resume_index` (a fresh request always passes `resume_index = 0, hop =
/// 0`; the loopback callback, M12 Task 3, resumes a parked chain by passing
/// the loopback step's own index + 1 and the next hop count — both read from
/// the server-held `loopback::Continuation` the callback took out of the
/// registry, not from the verified token's own `resume_index`/`hop` claims.
/// The token is still verified — that's what authenticates the callback as
/// naming a chain this gateway actually parked — but the claims it carries
/// are not trusted to drive control flow themselves; see
/// `loopback::Continuation::resume_index`), then forward to upstream. Split
/// out of `handle_inner` so the request span and the request-count/duration
/// metrics recorded by the caller have a single call site to wrap, no matter
/// which branch below (continue-to-forward, short-circuit, abort, a
/// fail-closed step error, or a loopback park) produces the final response.
///
/// `context` is threaded in (rather than always started fresh here) because
/// a resumed chain genuinely does pick up wherever the park left off: the
/// callback resumes with the `context` accumulated by the parked chain's own
/// earlier `on_request` steps — captured on the `Continuation` at dispatch
/// time (see `dispatch_loopback`) and cloned back in here — so no step's
/// `set_context` write made before the park is lost to whatever runs after
/// it. M12 Task 2 itself only ever calls this with a fresh, empty `context`
/// (see `handle_inner`), since a brand-new request has no earlier steps to
/// have accumulated one.
#[allow(clippy::too_many_arguments)]
async fn run_from(
    state: Arc<ProxyState>,
    config: Arc<Config>,
    route_id: String,
    upstream_base: String,
    steps: Vec<Step>,
    translate: Option<Translate>,
    correlation_id: String,
    llm: Option<Llm>,
    ingress: Option<String>,
    mut msg: HttpMsg,
    mut context: serde_json::Map<String, serde_json::Value>,
    resume_index: usize,
    hop: u32,
) -> Response<ResponseBody> {
    let cfg = &config;
    for (i, step) in steps.iter().enumerate() {
        if i < resume_index {
            continue;
        }
        if step.hook != crate::config::Hook::OnRequest {
            continue;
        }
        let self_name = step.effective_name(i);

        // `mode = "loopback"` `url` steps never run like an ordinary
        // transform: the step's tool doesn't hand back a directive inline,
        // it calls back later (M12 Task 3). Dispatch and park here instead
        // of falling through to `run_step` below — see `dispatch_loopback`.
        if step.type_ == StepType::Url && step.mode == UrlMode::Loopback {
            return dispatch_loopback(
                &state,
                cfg,
                &route_id,
                &correlation_id,
                &msg,
                step,
                &self_name,
                i,
                hop,
                &context,
            )
            .await;
        }

        let env = build_request_envelope(
            &route_id,
            &self_name,
            &msg,
            &context,
            &correlation_id,
            llm.as_ref(),
        );

        let step_span = tracing::info_span!("step", name = %self_name, hook = ?step.hook);
        let directive = run_step(&state, step, &env).instrument(step_span).await;

        match directive {
            Ok(Directive::Continue { ops }) => {
                if apply_ops(
                    &mut msg,
                    &mut context,
                    &self_name,
                    &ops,
                    cfg.gateway.max_context_bytes,
                )
                .is_err()
                {
                    metrics::counter!(METRIC_STEP_ERROR_TOTAL, "route" => route_id.clone())
                        .increment(1);
                    match step.on_error {
                        OnError::FailClosed => {
                            return text_response(
                                StatusCode::BAD_GATEWAY,
                                "step attempted an illegal mutation",
                            )
                        }
                        OnError::FailOpen => {}
                    }
                }
            }
            Ok(Directive::ShortCircuit { response }) => {
                metrics::counter!(METRIC_SHORT_CIRCUIT_TOTAL, "route" => route_id.clone())
                    .increment(1);
                return directive_response(response, &cfg.gateway.expose_headers);
            }
            Ok(Directive::Abort { response }) => {
                metrics::counter!(METRIC_ABORT_TOTAL, "route" => route_id.clone()).increment(1);
                return directive_response(response, &cfg.gateway.expose_headers);
            }
            // `emit`/`drop` are `on_stream` `mutate`-only (design doc M11
            // Task 3, see `proxy::run_mutate_chain`) — illegal at
            // `on_request`, routed through `on_error` like any other step
            // failure.
            Ok(Directive::Emit { .. }) | Ok(Directive::Drop { .. }) | Err(_) => {
                metrics::counter!(METRIC_STEP_ERROR_TOTAL, "route" => route_id.clone())
                    .increment(1);
                match step.on_error {
                    OnError::FailClosed => {
                        return text_response(StatusCode::BAD_GATEWAY, "step failed (fail_closed)")
                    }
                    OnError::FailOpen => {}
                }
            }
        }
    }

    // Cross-provider translation (M15 Task 6, buffered): AFTER the on_request
    // step loop (so steps still observe the CLIENT/`from` dialect) and BEFORE
    // building the upstream URL, rewrite `msg`'s body from `t.from`'s wire
    // shape into `t.to`'s and redirect the request to the target provider's
    // endpoint. `translate.is_none()` (every non-translate route) skips this
    // entirely — a zero-behavior-change no-op. The request-side fidelity
    // `report` is carried into `forward` so `report_header` can fold it
    // together with the response-side report into one `x-sluice-translation`.
    let mut request_report: Option<TranslationReport> = None;
    let upstream_url = if let Some(t) = translate.as_ref() {
        match translate_request(&mut msg, t, &upstream_base, &route_id) {
            Ok(rt) => {
                request_report = Some(rt.report);
                rt.upstream_url
            }
            Err(resp) => return resp,
        }
    } else {
        // Recompute the outbound URL from the (possibly step-mutated) path, so
        // a `set_path` op actually changes where the request is forwarded.
        upstream_url_for(&upstream_base, &msg.path)
    };
    let resp = forward(
        state.clone(),
        config,
        &upstream_url,
        &msg,
        &route_id,
        &steps,
        translate.as_ref(),
        request_report,
        &correlation_id,
        llm.as_ref(),
        ingress.as_deref(),
        &mut context,
    )
    .await;
    metrics::counter!(
        METRIC_UPSTREAM_STATUS_TOTAL,
        "route" => route_id,
        "status" => resp.status().as_u16().to_string(),
    )
    .increment(1);
    resp
}

/// How long [`dispatch_loopback`] will park waiting for a loopback chain's
/// callback before giving up and failing the client with a 504. This — not
/// the registry's `sweep` — is the mechanism that keeps a tool which never
/// calls back (crashes, hangs, or is simply misconfigured) from parking this
/// handler, and the `max_inflight` permit it holds, forever.
const LOOPBACK_PARK_TIMEOUT: Duration = Duration::from_secs(60);

/// How long a signed `x-chain-token` (design doc §4.5, M12) remains valid:
/// generous enough for a well-behaved tool to receive the loopback POST and
/// call back promptly, short enough that a leaked/replayed token can't be
/// used indefinitely.
///
/// Fix D (M14 final review, TTL alignment): kept EQUAL to
/// [`LOOPBACK_PARK_TIMEOUT`], never shorter. A token that expired before the
/// park timeout would let a well-behaved tool's callback arrive right at
/// (or just past) that shorter window and get spuriously rejected as
/// `TokenError::Expired` even though `dispatch_loopback` was still (or just
/// about done) parked waiting for exactly that callback — the token must
/// remain valid for at least as long as the gateway is actually willing to
/// wait for it. It's derived from `LOOPBACK_PARK_TIMEOUT` itself (rather
/// than a second independent literal) so the two can never drift apart
/// again the way the pre-fix 30s-token/60s-park split did.
const LOOPBACK_TOKEN_TTL_SECS: u64 = LOOPBACK_PARK_TIMEOUT.as_secs();

/// Dispatch a `mode = "loopback"` `on_request` step (design doc §4.5, M12):
/// sign a [`ChainToken`] naming where this chain should resume, register a
/// [`loopback::Continuation`] under its `cid`, fire the loopback POST to the
/// step's tool WITHOUT waiting for its response, and park this handler on
/// the continuation's `oneshot` receiver until either the callback (M12 Task
/// 3) resumes the chain and feeds back the real client response, or
/// [`LOOPBACK_PARK_TIMEOUT`] elapses.
///
/// Concurrency note: the tool's own POST is fired via `tokio::spawn` — a
/// tool that never responds to it (hangs, or the connection stalls) must
/// never block this function from reaching the park/timeout below, since
/// the callback that actually unparks this handler arrives on a wholly
/// different inbound connection/task, not as a response to this POST. The
/// `max_inflight` permit this handler's caller (`proxy::handle`) acquired
/// stays held for the whole duration of this park — it's attached to the
/// eventually-returned response's body the same as any other request (see
/// `reconstruct::attach_permit`), so a parked chain still counts against the
/// concurrency ceiling by design. Task 3's callback handler is documented to
/// acquire no permit of its own, precisely so resuming a chain can never
/// deadlock against `max_inflight` being exhausted by parked requests: if
/// resuming a chain required a fresh permit, and every permit were held by
/// requests parked waiting on exactly such a resume, the gateway would
/// deadlock solid.
#[allow(clippy::too_many_arguments)]
async fn dispatch_loopback(
    state: &Arc<ProxyState>,
    config: &Arc<Config>,
    route_id: &str,
    correlation_id: &str,
    msg: &HttpMsg,
    step: &Step,
    self_name: &str,
    index: usize,
    hop: u32,
    context: &serde_json::Map<String, serde_json::Value>,
) -> Response<ResponseBody> {
    let next_hop = hop + 1;
    if next_hop > config.gateway.max_hops {
        metrics::counter!(METRIC_ABORT_TOTAL, "route" => route_id.to_string()).increment(1);
        return text_response(
            StatusCode::LOOP_DETECTED,
            &format!(
                "loopback step '{self_name}' exceeded max_hops ({})",
                config.gateway.max_hops
            ),
        );
    }

    let cid = uuid::Uuid::new_v4().to_string();
    let now_unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let token = ChainToken {
        cid: cid.clone(),
        route_id: route_id.to_string(),
        resume_index: index + 1,
        hop: next_hop,
        expires_at_unix: now_unix + LOOPBACK_TOKEN_TTL_SECS,
    };
    let signed = token.sign(config.gateway.loopback_secret.as_bytes());

    let (tx, rx) = tokio::sync::oneshot::channel();
    let registered = state.continuations.register(
        cid.clone(),
        loopback::Continuation {
            tx,
            created: Instant::now(),
            route_id: route_id.to_string(),
            resume_index: index + 1,
            hop: next_hop,
            context: context.clone(),
            path: msg.path.clone(),
            correlation_id: correlation_id.to_string(),
            config: config.clone(),
        },
        config.gateway.max_parked,
    );
    if !registered {
        // Fix D (M14 final review): the registry already holds
        // `max_parked` concurrently-parked continuations — refuse to park
        // another rather than growing it without bound. Fail closed with
        // 503 (shed), the same status `handle`'s own `max_inflight` gate
        // uses for an analogous "the gateway is at capacity" condition.
        metrics::counter!(METRIC_ABORT_TOTAL, "route" => route_id.to_string()).increment(1);
        return text_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "too many parked loopback continuations",
        );
    }

    let url = step.url.clone().expect("validated url present");
    let body = msg
        .body_bytes()
        .map(Bytes::from)
        .unwrap_or_else(|_| Bytes::new());
    match reconstruct::upstream_request(&state.client, &msg.method, &url, &msg.headers, body) {
        Ok(builder) => {
            let builder = builder
                .header("x-chain-token", signed)
                .timeout(Duration::from_millis(step.timeout_ms));
            // Fire-and-forget: deliberately never awaited inline (see this
            // function's doc comment) — whatever the tool's own HTTP
            // response carries is not needed here; the real response comes
            // back later via the callback and this continuation's oneshot.
            tokio::spawn(async move {
                let _ = builder.send().await;
            });
        }
        Err(_) => {
            // Invalid method on the original request — can't even build the
            // loopback POST. Clean up the just-registered continuation
            // (nothing will ever consume it) and fail closed.
            state.continuations.take(&cid);
            return text_response(
                StatusCode::BAD_REQUEST,
                "invalid method for loopback dispatch",
            );
        }
    }

    match tokio::time::timeout(LOOPBACK_PARK_TIMEOUT, rx).await {
        Ok(Ok(resp)) => resp,
        Ok(Err(_)) => {
            // The continuation's `tx` was dropped without ever being sent
            // to (e.g. `Registry::sweep` reaped it) — the chain is lost.
            metrics::counter!(METRIC_STEP_ERROR_TOTAL, "route" => route_id.to_string())
                .increment(1);
            text_response(StatusCode::GATEWAY_TIMEOUT, "loopback continuation lost")
        }
        Err(_) => {
            // Timed out waiting. Remove our own registration so a late
            // callback can't resume into a receiver nobody is listening on
            // anymore — a benign, harmless race if the callback had already
            // taken it a moment earlier (`take` is idempotent: at most one
            // side ever gets `Some`).
            state.continuations.take(&cid);
            metrics::counter!(METRIC_STEP_ERROR_TOTAL, "route" => route_id.to_string())
                .increment(1);
            text_response(StatusCode::GATEWAY_TIMEOUT, "loopback continuation lost")
        }
    }
}

/// Resume a parked loopback chain (design doc §4.5, M12 Task 3): verifies the
/// `x-chain-token` the tool hands back (authenticating that this callback
/// names a chain this gateway actually parked), takes (removing) the
/// [`loopback::Continuation`] the token's `cid` names, resumes the chain from
/// where it parked — using `resume_index`, `hop`, and `context` read from
/// that server-held `Continuation`, not from the token's own claims; the
/// token's `resume_index`/`hop` claims are consulted only insofar as
/// verifying the MAC covers them, never used to drive control flow directly
/// — and feeds the resulting client response back through the continuation's
/// `oneshot` `tx`, which is what actually unparks `dispatch_loopback`'s
/// parked original handler. The TOOL that called this endpoint never sees
/// that response: this function always answers the tool's own HTTP request
/// with a bare 200 (or a 401/410 on a bad/absent token or an already-gone
/// continuation), entirely independent of how the resumed chain turns out.
///
/// Deliberately acquires NO `max_inflight` permit of its own — the parked
/// original handler already holds one for the whole chain's lifetime (see
/// `dispatch_loopback`'s doc comment for why a callback that needed a fresh
/// permit to resume would deadlock solid once every permit were held by
/// requests parked waiting on exactly such a resume). Callers (`server::serve_with_state`)
/// must route the callback path here BEFORE `handle`'s permit acquisition,
/// never through it.
pub async fn handle_callback(
    state: Arc<ProxyState>,
    req: Request<Incoming>,
) -> Response<ResponseBody> {
    // The token's signature must be checked against the CURRENTLY live
    // config's secret: the `cid` needed to look up which (possibly older)
    // config snapshot this chain actually pinned is itself inside the
    // signed, not-yet-trusted payload, so there is no way to consult that
    // snapshot's own secret before the MAC has been verified against
    // something. In the ordinary case (no `loopback_secret` rotation mid-chain)
    // this is the same secret the token was signed with; a secret rotated
    // out from under an in-flight chain fails this verification and the
    // client sees a clean 401 rather than the chain hanging — a documented,
    // fail-closed edge case rather than a bug.
    let live_secret = state.config.load_full().gateway.loopback_secret.clone();

    let token = match req
        .headers()
        .get("x-chain-token")
        .and_then(|v| v.to_str().ok())
    {
        Some(t) => t.to_string(),
        None => return text_response(StatusCode::UNAUTHORIZED, "missing x-chain-token"),
    };

    let now_unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let claims = match ChainToken::verify(&token, live_secret.as_bytes(), now_unix) {
        Ok(claims) => claims,
        Err(err) => {
            return text_response(
                StatusCode::UNAUTHORIZED,
                &format!("invalid chain token: {err}"),
            )
        }
    };

    // One-shot by construction (`Registry::take`): a replayed or duplicated
    // callback for the same `cid` finds nothing the second time, and a
    // continuation that already expired via `sweep` or the park timeout is
    // likewise simply gone.
    let continuation = match state.continuations.take(&claims.cid) {
        Some(c) => c,
        None => return text_response(StatusCode::GONE, "loopback continuation gone"),
    };

    // The route named by the token must be looked up in the continuation's
    // OWN pinned config snapshot (never the live one) — the whole point of
    // carrying `config` on the continuation is that a config swap mid-park
    // must not change what a resumed chain sees (see `loopback::Continuation`'s
    // doc comment).
    let route = match continuation
        .config
        .routes
        .iter()
        .find(|r| r.id == continuation.route_id)
    {
        Some(r) => r.clone(),
        None => {
            // Can't happen in practice: `continuation.config` is the exact
            // immutable snapshot that dispatched this very loopback step, so
            // it already contains this route by construction. Resume
            // defensively rather than panic if that invariant is ever
            // violated some other way.
            let _ = continuation.tx.send(text_response(
                StatusCode::BAD_GATEWAY,
                "loopback route vanished from its own pinned config snapshot",
            ));
            return text_response(StatusCode::OK, "ok");
        }
    };

    let mut msg = match request_to_httpmsg(req, continuation.config.gateway.max_body_bytes).await {
        Ok(m) => m,
        Err(resp) => {
            let _ = continuation.tx.send(resp);
            return text_response(StatusCode::OK, "ok");
        }
    };
    // The callback HTTP request's own `path` is always just
    // `callback_path` — a transport detail of how the tool called back into
    // the gateway, not the logical request path the chain is actually
    // processing. Restore the path the parked request pinned at dispatch
    // time (see `loopback::Continuation::path`'s doc comment) so upstream URL
    // reconstruction and any later steps' envelopes see the real route path,
    // not the callback endpoint's.
    msg.path = continuation.path.clone();

    let llm = build_llm_view(
        &state,
        &msg,
        route.adapter.as_ref(),
        route.translate.as_ref(),
    );
    let ingress = route.adapter.as_ref().and_then(|a| a.ingress.clone());

    let resp = run_from(
        state.clone(),
        continuation.config.clone(),
        continuation.route_id.clone(),
        route.upstream.clone(),
        route.steps.clone(),
        route.translate.clone(),
        continuation.correlation_id.clone(),
        llm,
        ingress,
        msg,
        continuation.context.clone(),
        continuation.resume_index,
        continuation.hop,
    )
    .await;

    if continuation.tx.send(resp).is_err() {
        // The parked handler is gone (its own park timeout fired, or the
        // client disconnected and the whole future was dropped) — the
        // resumed chain's response has nowhere to go. Not this function's
        // failure: the tool's own request still succeeded, so it still gets
        // its 200 below; only logged so an operator can see resumes that
        // raced a timeout/disconnect.
        tracing::warn!(
            cid = %claims.cid,
            "loopback callback resumed a chain but the parked handler was already gone"
        );
    }

    text_response(StatusCode::OK, "ok")
}

/// Decode a step's `ShortCircuit`/`Abort` response spec into a client
/// response, passing it through the same egress reconstruction (and
/// `expose_headers` allowlist) as every other client-bound response.
fn directive_response(response: RespSpec, expose_headers: &[String]) -> Response<ResponseBody> {
    let body = base64::engine::general_purpose::STANDARD
        .decode(&response.body_b64)
        .unwrap_or_default();
    reconstruct::client_response(
        response.status,
        &response.headers,
        Bytes::from(body),
        expose_headers,
    )
}

// Note: client-disconnect cancellation is handled for free by async
// future-drop — if the client goes away, hyper drops the future driving
// this handler (and hence this `forward` call), which drops the in-flight
// `builder.send()` future and cancels the underlying reqwest/upstream
// call. No explicit cancellation wiring is needed here.
#[allow(clippy::too_many_arguments)]
async fn forward(
    state: Arc<ProxyState>,
    config: Arc<Config>,
    upstream_url: &str,
    msg: &HttpMsg,
    route_id: &str,
    steps: &[Step],
    translate: Option<&Translate>,
    request_report: Option<TranslationReport>,
    correlation_id: &str,
    llm: Option<&Llm>,
    ingress: Option<&str>,
    context: &mut serde_json::Map<String, serde_json::Value>,
) -> Response<ResponseBody> {
    let body = msg
        .body_bytes()
        .map(Bytes::from)
        .unwrap_or_else(|_| Bytes::new());
    let builder = match reconstruct::upstream_request(
        &state.client,
        &msg.method,
        upstream_url,
        &msg.headers,
        body,
    ) {
        Ok(b) => b,
        // `msg.method` is always the string form of the `hyper::Method` the
        // original inbound request was parsed with (`request_to_httpmsg`),
        // and no `Op` mutates it (see `directive::Op` — there is no
        // `SetMethod` variant), so `reconstruct::upstream_request`'s
        // `reqwest::Method::from_bytes` parse can never fail here. This is
        // NOT the same as the reachable loopback-path 400 below, which
        // parses a *replayed* method string reconstructed from a signed
        // token and must stay a real error path.
        Err(_) => unreachable!("method came from a valid hyper Method"),
    };
    let builder = builder.timeout(std::time::Duration::from_millis(
        config.gateway.upstream_timeout_ms,
    ));

    let upstream = match builder.send().await {
        Ok(r) => r,
        Err(e) if e.is_timeout() => {
            return text_response(StatusCode::GATEWAY_TIMEOUT, "upstream request timed out")
        }
        Err(_) => return text_response(StatusCode::BAD_GATEWAY, "upstream request failed"),
    };

    let status = upstream.status().as_u16();
    let mut headers = BTreeMap::new();
    for (name, value) in upstream.headers() {
        if let Ok(v) = value.to_str() {
            headers.insert(name.as_str().to_ascii_lowercase(), v.to_string());
        }
    }

    // `on_response` steps: those declared with hook = on_response, dispatched
    // via `run_step` to either runtime (url or script). Indices are kept
    // against the *full* step list so `effective_name` matches the same
    // namespace an on_request step for the same config entry would get.
    let on_response_steps: Vec<(usize, &Step)> = steps
        .iter()
        .enumerate()
        .filter(|(_, s)| s.hook == crate::config::Hook::OnResponse)
        .collect();

    // `on_stream` steps: only relevant when there are no `on_response` steps
    // (an `on_response` route already buffers the whole body — see the
    // brief's "and no on_response steps" scoping; the two hooks combined on
    // one route are rejected at config load, see `config::load::validate`).
    // Split by `chunk_mode`: `observe` steps (M10) tee the stream unchanged;
    // `mutate` steps (M11 Task 3, M17 Task 2) gate what the client actually
    // receives. An on_stream step participates here if it is a URL step (any
    // chunk_mode, the M11 shape) or a `script_mode = "worker"` + `mutate`
    // script step (M17 Task 2): those are the two runtimes with a per-chunk
    // dispatch on the stream path. A `type = "script"` ONESHOT step, or a wasm
    // step, on on_stream is rejected at config load (see
    // `config::load::validate`), so it never reaches here; a worker OBSERVE
    // step has no observe-side runtime yet (observe remains URL-only) and is
    // deliberately excluded.
    let on_stream_steps: Vec<(usize, &Step)> = steps
        .iter()
        .enumerate()
        .filter(|(_, s)| {
            s.hook == crate::config::Hook::OnStream
                && (s.type_ == StepType::Url
                    || (s.type_ == StepType::Script
                        && s.script_mode == ScriptMode::Worker
                        && s.chunk_mode == ChunkMode::Mutate))
        })
        .collect();

    // A translate route (M15 Task 6) is buffered-only for this task: force it
    // down the buffered path below (never the streaming pass-through/on_stream
    // branch) so the whole upstream body can be parsed and re-rendered into
    // the client's `from` dialect before anything is sent. Streaming
    // translation is Task 7.
    if on_response_steps.is_empty() && translate.is_none() {
        let (mutate_steps, observe_steps): (Vec<_>, Vec<_>) = on_stream_steps
            .into_iter()
            .partition(|(_, s)| s.chunk_mode == ChunkMode::Mutate);

        if mutate_steps.is_empty() && observe_steps.is_empty() {
            // No on_response or on_stream steps: keep the existing streaming
            // pass-through, so routes that don't use either hook pay zero
            // buffering/framing cost.
            let stream = upstream.bytes_stream();
            return reconstruct::client_streaming_response(
                status,
                &headers,
                stream,
                &config.gateway.expose_headers,
            );
        }

        if !mutate_steps.is_empty() {
            // `on_stream` mutate: route each framed SSE event THROUGH the
            // ordered chain of mutate steps, in-path — the per-event step
            // call actually gates what (and whether) the client receives.
            // See `mutate_stream`/`run_mutate_chain` below.
            //
            // `mutate_steps` and `observe_steps` are mutually exclusive by
            // construction here: `config::load::validate` rejects any route
            // that configures BOTH an observe and a mutate `on_stream` step,
            // precisely because only one of the two branches below ever
            // runs — a route that got past validation with both would have
            // its observe steps silently never invoked. A single mutate
            // chain (one step, or several chained in order) is this task's
            // supported shape for the mutate branch; a fully independent
            // tee-plus-gate combination remains future work.
            // Build one dispatch target per mutate step, in chain order. A URL
            // step captures its destination; a `script_mode = "worker"` step
            // spawns a DEDICATED worker process HERE, at stream start (not from
            // the shared `worker_cache`), so concurrent client streams never
            // interleave a stateful worker's per-stream state. A worker that
            // fails to spawn at stream start is folded through the step's own
            // `on_error`: `fail_open` drops the step from this stream's chain
            // (it becomes a no-op, forwarding events unchanged); `fail_closed`
            // fails the whole response with 502 before any bytes are sent.
            let mut targets: Vec<MutateTarget> = Vec::with_capacity(mutate_steps.len());
            let mut worker_spawn_failed_closed = false;
            for (i, s) in &mutate_steps {
                let dispatch = match s.type_ {
                    StepType::Url => {
                        MutateDispatch::Url(s.url.clone().expect("validated url present"))
                    }
                    StepType::Script => match ScriptWorker::spawn(&s.cmd, s.timeout_ms) {
                        Ok(worker) => {
                            MutateDispatch::Worker(Arc::new(tokio::sync::Mutex::new(worker)))
                        }
                        Err(err) => {
                            tracing::warn!(
                                step = %s.effective_name(*i),
                                "on_stream worker failed to spawn at stream start: {err}"
                            );
                            metrics::counter!(
                                METRIC_STEP_ERROR_TOTAL,
                                "route" => route_id.to_string()
                            )
                            .increment(1);
                            match s.on_error {
                                OnError::FailOpen => continue,
                                OnError::FailClosed => {
                                    worker_spawn_failed_closed = true;
                                    break;
                                }
                            }
                        }
                    },
                    // Wasm on on_stream is rejected at config load; it never
                    // reaches this dispatch point.
                    StepType::Wasm => {
                        unreachable!("wasm on_stream steps are rejected at config load")
                    }
                };
                targets.push(MutateTarget {
                    self_name: s.effective_name(*i),
                    timeout_ms: s.timeout_ms,
                    on_error: s.on_error,
                    dispatch,
                });
            }
            if worker_spawn_failed_closed {
                metrics::counter!(METRIC_ABORT_TOTAL, "route" => route_id.to_string()).increment(1);
                return text_response(
                    StatusCode::BAD_GATEWAY,
                    "on_stream worker step failed to start",
                );
            }
            let stream_ctx = MutateContext {
                route_id: Arc::from(route_id),
                correlation_id: Arc::from(correlation_id),
                req: Arc::new(msg.clone()),
                llm: Arc::new(llm.cloned()),
            };
            let adapter = ingress.and_then(adapter_for);
            let stream = mutate_stream(
                upstream.bytes_stream(),
                stream_ctx,
                adapter,
                state.client.clone(),
                targets,
                config.gateway.max_context_bytes,
                config.gateway.max_event_bytes(),
            );
            return reconstruct::client_streaming_response(
                status,
                &headers,
                stream,
                &config.gateway.expose_headers,
            );
        }

        // `on_stream` observe only: tee the upstream stream to the client
        // unchanged while best-effort mirroring each complete SSE event (as
        // a normalized chunk envelope) into a bounded queue; a background
        // task drains that queue and fire-and-forget POSTs each envelope to
        // every observe step. Observe steps can NEVER delay, block, or
        // mutate what the client receives — their directive (whatever they
        // return) is not even parsed here. See `tee_observe_stream` and
        // `spawn_observe_poster` below.
        let targets: Vec<ObserveTarget> = observe_steps
            .iter()
            .map(|(i, s)| ObserveTarget {
                self_name: s.effective_name(*i),
                url: s.url.clone().expect("validated url present"),
                timeout_ms: s.timeout_ms,
            })
            .collect();
        let (tx, rx) = mpsc::channel(OBSERVE_QUEUE_CAPACITY);
        spawn_observe_poster(state.client.clone(), targets, rx);

        let observe_ctx = ObserveContext {
            route_id: Arc::from(route_id),
            correlation_id: Arc::from(correlation_id),
            req: Arc::new(msg.clone()),
            llm: Arc::new(llm.cloned()),
        };
        let adapter = ingress.and_then(adapter_for);
        let stream = tee_observe_stream(
            upstream.bytes_stream(),
            observe_ctx,
            adapter,
            tx,
            config.gateway.max_event_bytes(),
        );
        return reconstruct::client_streaming_response(
            status,
            &headers,
            stream,
            &config.gateway.expose_headers,
        );
    }

    // Cross-provider translation, STREAMING egress (M15 Task 7). A translate
    // route whose upstream answered with an SSE stream and that has NO
    // `on_response` steps translates the stream event-by-event, in path, from
    // the `to` (upstream) dialect into the client's `from` dialect — the
    // streaming mirror of `translate_response`'s buffered egress. Two shapes
    // deliberately keep the buffered path instead: a translate route WITH
    // `on_response` steps (those steps must observe/mutate a fully buffered
    // body, so it can't stream), and a translate route whose upstream is NOT a
    // stream (a normal buffered response body). Both fall through below.
    if let Some(t) = translate {
        if on_response_steps.is_empty() && is_event_stream_response(&headers) {
            // `from` renders to the client dialect, `to` parses the upstream
            // dialect. Unknown providers are impossible past config load; a
            // lookup miss logs and 502s rather than panicking.
            let (from, to) = match translate_adapters(t) {
                Ok(pair) => pair,
                Err(resp) => return resp,
            };
            // Obligation (b): seed the render state's model with the effective
            // upstream model, so a target preamble the renderer must
            // *synthesize* (an OpenAI-origin canonical stream carries no
            // `MessageStart`, so an Anthropic client's `message_start` is
            // invented here) still names a real model instead of an empty one.
            let seed_model = t
                .model
                .clone()
                .or_else(|| llm.map(|l| l.model.clone()))
                .unwrap_or_default();
            let stream = translate_stream(
                upstream.bytes_stream(),
                from,
                to,
                seed_model,
                config.gateway.max_event_bytes(),
            );

            // `report_header` on the streaming path: surface the request-side
            // render report (stream-side drops are logged, not folded into the
            // header) on a LOCAL copy of the expose allowlist, mirroring the
            // buffered path's auto-expose so `report_header = true` alone
            // surfaces the header without touching global `expose_headers`.
            let mut out_headers = headers.clone();
            let mut expose_owned = config.gateway.expose_headers.clone();
            if t.report_header {
                out_headers.insert(
                    "x-sluice-translation".to_string(),
                    summarize_report(&request_report.unwrap_or_default()),
                );
                expose_owned.push("x-sluice-translation".to_string());
            }
            return reconstruct::client_streaming_response(
                status,
                &out_headers,
                stream,
                &expose_owned,
            );
        }
    }

    // At least one on_response step: buffer the full upstream body (bounded
    // by `max_body_bytes`, same as the request-side cap) so steps can read
    // and mutate it via envelopes/ops before it reaches the client.
    let body = match buffer_or_overflow(
        upstream.bytes_stream(),
        config.gateway.max_response_bytes(),
    )
    .await
    {
        ResponseBuffer::Buffered(b) => b,
        ResponseBuffer::Upstream => {
            return text_response(
                StatusCode::BAD_GATEWAY,
                "failed to read upstream response body",
            )
        }
        ResponseBuffer::Overflow { prefix, rest } => {
            // A translate route MUST reconstruct the whole body into the
            // client dialect (the single-reconstruction guarantee), which is
            // impossible on an unbuffered stream. So on a translate route we
            // always Reject on overflow: streaming the raw `to`-dialect
            // upstream bytes to a `from`-dialect client would leak
            // untranslated bytes as HTTP 200 (dialect corruption). Only
            // non-translate routes may honor `stream_through`.
            let effective = if translate.is_some() {
                crate::config::Oversize::Reject
            } else {
                config.gateway.oversize
            };
            match effective {
                // `reject`: refuse the oversize response outright (the historical
                // behavior, and the forced policy for translate routes). The
                // prefix already read (and the still-open `rest` stream) are
                // dropped here.
                crate::config::Oversize::Reject => {
                    return text_response(
                        StatusCode::PAYLOAD_TOO_LARGE,
                        "response body exceeds max_response_bytes",
                    )
                }
                // `stream_through`: the body is too large to buffer, so hand it to
                // the client UNBUFFERED. Because `buffer_or_overflow` already
                // consumed a prefix of the upstream stream, reconstruct the full
                // body = that prefix followed by whatever remains (`rest`).
                // on_response steps CANNOT run on an unbuffered body, so they are
                // skipped for this response; the upstream status/headers are
                // preserved verbatim. Unreachable on a translate route (forced to
                // Reject above).
                crate::config::Oversize::StreamThrough => {
                    tracing::warn!(
                        route = %route_id,
                        max_response_bytes = config.gateway.max_response_bytes(),
                        "response exceeded max_response_bytes; on_response steps skipped, streaming through"
                    );
                    let combined = stream::once(async move { Ok::<Bytes, reqwest::Error>(prefix) })
                        .chain(rest);
                    return reconstruct::client_streaming_response(
                        status,
                        &headers,
                        combined,
                        &config.gateway.expose_headers,
                    );
                }
            }
        }
    };

    // `HttpMsg` has no status field (design doc M9's shape is request-first);
    // `status` is tracked here as a local var instead, and — per M10 scope —
    // is never mutated by ops. Only response headers/body and `context` are
    // mutable at on_response.
    let mut resp_msg = HttpMsg {
        method: String::new(),
        path: String::new(),
        headers,
        body_b64: String::new(),
    };
    resp_msg.set_body_bytes(&body);

    for (i, step) in on_response_steps {
        let self_name = step.effective_name(i);
        let env = build_response_envelope(
            route_id,
            &self_name,
            msg,
            &resp_msg,
            context,
            correlation_id,
            llm,
        );
        let step_span = tracing::info_span!("step", name = %self_name, hook = ?step.hook);
        let directive = run_step(&state, step, &env).instrument(step_span).await;

        match directive {
            Ok(Directive::Continue { ops }) => {
                if apply_ops(
                    &mut resp_msg,
                    context,
                    &self_name,
                    &ops,
                    config.gateway.max_context_bytes,
                )
                .is_err()
                {
                    metrics::counter!(METRIC_STEP_ERROR_TOTAL, "route" => route_id.to_string())
                        .increment(1);
                    match step.on_error {
                        OnError::FailClosed => {
                            return text_response(
                                StatusCode::BAD_GATEWAY,
                                "step attempted an illegal mutation",
                            )
                        }
                        OnError::FailOpen => {}
                    }
                }
            }
            Ok(Directive::Abort { response }) => {
                metrics::counter!(METRIC_ABORT_TOTAL, "route" => route_id.to_string()).increment(1);
                return directive_response(response, &config.gateway.expose_headers);
            }
            Ok(Directive::ShortCircuit { .. }) => {
                // Illegal at on_response (design doc §6.4): the upstream
                // response already exists, so short_circuit has no meaning
                // here. Treated as a step error, routed through on_error —
                // same as a transport/parse failure below.
                metrics::counter!(METRIC_STEP_ERROR_TOTAL, "route" => route_id.to_string())
                    .increment(1);
                match step.on_error {
                    OnError::FailClosed => {
                        return text_response(
                            StatusCode::BAD_GATEWAY,
                            "step returned an illegal directive (short_circuit) for on_response",
                        )
                    }
                    OnError::FailOpen => {}
                }
            }
            // `emit`/`drop` are `on_stream` `mutate`-only (design doc M11
            // Task 3) — illegal at `on_response`, routed through `on_error`
            // like any other illegal-directive/transport failure here.
            Ok(Directive::Emit { .. }) | Ok(Directive::Drop { .. }) | Err(_) => {
                metrics::counter!(METRIC_STEP_ERROR_TOTAL, "route" => route_id.to_string())
                    .increment(1);
                match step.on_error {
                    OnError::FailClosed => {
                        return text_response(StatusCode::BAD_GATEWAY, "step failed (fail_closed)")
                    }
                    OnError::FailOpen => {}
                }
            }
        }
    }

    let final_body = resp_msg.body_bytes().unwrap_or_default();

    // Cross-provider translation (M15 Task 6, buffered egress): the buffered
    // body is currently in the TARGET (`t.to`) dialect — the same dialect any
    // on_response steps above just observed and mutated (documented ordering:
    // steps see the upstream/target dialect, the client sees the `from`
    // dialect). Translate it back into the client's `from` dialect as the very
    // last step, preserving the upstream `status` verbatim.
    if let Some(t) = translate {
        return translate_response(
            status,
            &resp_msg.headers,
            &final_body,
            t,
            request_report,
            &config.gateway.expose_headers,
        );
    }

    reconstruct::client_response(
        status,
        &resp_msg.headers,
        Bytes::from(final_body),
        &config.gateway.expose_headers,
    )
}

/// The outcome of translating a buffered request body before forwarding it
/// (M15 Task 6). Carries both where to forward the (now `to`-dialect) body and
/// the request-side fidelity [`TranslationReport`] so `report_header` can fold
/// it together with the response-side report.
struct RequestTranslation {
    /// The upstream URL to forward the translated body to: the target
    /// provider's endpoint (see [`target_endpoint_path`]) joined onto the
    /// route's upstream base.
    upstream_url: String,
    /// Fields lost rendering the canonical request into `t.to`'s wire dialect.
    report: TranslationReport,
}

/// A boxed, dynamically-dispatched [`Adapter`] — the return type
/// [`adapter_for`] itself uses, named here purely to keep
/// [`translate_adapters`]'s signature (a pair of these) legible.
type BoxedAdapter = Box<dyn Adapter + Send + Sync>;

/// Resolve the `from`/`to` adapters named by a route's `[route.translate]`
/// config. Both provider names are validated at config load
/// (`config::load::validate`) — a lookup failure here means that invariant
/// regressed rather than anything a request itself did — so this logs and
/// fails closed with a 502 rather than panicking. Shared by
/// `translate_request`/`translate_response` (M15 Task 6 review Minor 4) so
/// the duplicated lookup-plus-log-plus-502 story lives in exactly one place.
#[allow(clippy::result_large_err)]
fn translate_adapters(
    t: &Translate,
) -> Result<(BoxedAdapter, BoxedAdapter), Response<ResponseBody>> {
    match (adapter_for(&t.from), adapter_for(&t.to)) {
        (Some(from), Some(to)) => Ok((from, to)),
        _ => {
            tracing::error!(
                "translation: unknown provider in translate config (from={}, to={})",
                t.from,
                t.to
            );
            Err(text_response(
                StatusCode::BAD_GATEWAY,
                "translation: unknown provider in route config",
            ))
        }
    }
}

/// The target provider's request endpoint path, used to redirect a translated
/// request from the `from` dialect's path (which the client hit) to the `to`
/// dialect's own endpoint. `model`/`stream` only matter for Google, whose
/// model id and streaming-ness travel in the URL path itself. Unknown `to`
/// names are impossible here (config validation rejects them; see
/// `config::load::validate`) — the fallback keeps this total rather than
/// panicking.
fn target_endpoint_path(to: &str, model: &str, stream: bool) -> String {
    match to {
        "anthropic" => "/v1/messages".to_string(),
        "openai" => "/v1/chat/completions".to_string(),
        "google" => format!(
            "/v1beta/models/{model}:{}",
            if stream {
                "streamGenerateContent"
            } else {
                "generateContent"
            }
        ),
        _ => "/".to_string(),
    }
}

/// Translate the buffered request `msg` in place from `t.from`'s wire dialect
/// into `t.to`'s (M15 Task 6, ingress). Returns the upstream URL the
/// translated body should be forwarded to and the render fidelity report.
///
/// A translate route is assumed to carry a `from`-shaped LLM body: if the
/// inbound body doesn't parse as `t.from`, that's a client error (a translate
/// route received an untranslatable body), answered with a documented 400 —
/// the untranslatable body is never forwarded to the upstream.
///
/// The `Err` variant is a fully-built `Response<ResponseBody>` (an early
/// client response), matching how the rest of `proxy`/`reconstruct` return
/// gateway-authored responses by value rather than boxing them.
#[allow(clippy::result_large_err)]
fn translate_request(
    msg: &mut HttpMsg,
    t: &Translate,
    upstream_base: &str,
    route_id: &str,
) -> Result<RequestTranslation, Response<ResponseBody>> {
    let (from, to) = translate_adapters(t)?;

    let body = msg.body_bytes().unwrap_or_default();
    let ctx = RequestCtx {
        path: msg.path.clone(),
        method: msg.method.clone(),
    };
    let mut canonical = match from.parse_request(&body, &ctx) {
        Ok(c) => c,
        Err(err) => {
            tracing::warn!(
                "translation: request body not parseable as '{}': {err}",
                t.from
            );
            return Err(text_response(
                StatusCode::BAD_REQUEST,
                "translation: request body is not valid for the route's source dialect",
            ));
        }
    };

    // A configured target model pins the model the upstream is called with
    // (validated to resolve under `to` at config load); otherwise the model
    // from the inbound body carries through untouched.
    if let Some(model) = &t.model {
        canonical.model = model.clone();
    }

    let mut report = TranslationReport::default();
    let rendered = match to.render_request(&canonical, &mut report) {
        Ok(r) => r,
        Err(err) => {
            tracing::error!(
                "translation: failed to render request into '{}': {err}",
                t.to
            );
            return Err(text_response(
                StatusCode::BAD_GATEWAY,
                "translation: failed to render request into target dialect",
            ));
        }
    };
    msg.set_body_bytes(&rendered);

    // `upstream_url_for` strips the leading route-id segment (it expects the
    // inbound gateway path shape `/<route-id>/<rest>`), so prepend the route
    // id to the target endpoint path — the client hit the `from` dialect's
    // path, but the upstream expects the `to` dialect's endpoint.
    let target = target_endpoint_path(&t.to, &canonical.model, canonical.stream);
    let upstream_url = upstream_url_for(upstream_base, &format!("/{route_id}{target}"));
    Ok(RequestTranslation {
        upstream_url,
        report,
    })
}

/// Translate a fully-buffered upstream response body from `t.to`'s wire
/// dialect back into the client's `t.from` dialect (M15 Task 6, egress) and
/// build the client response. The upstream `status` is preserved verbatim.
///
/// On an unparseable upstream body (not valid `t.to` JSON) this returns a
/// typed, logged 502 rather than ever forwarding a partial, native-looking
/// body the client would misread as its own dialect. If that unparseable
/// body looks like an SSE stream (`content-type: text/event-stream` — the
/// shape a `stream: true` translate request's upstream answers with), the 502
/// names that specifically rather than the generic "unparseable upstream
/// response". A streaming upstream only reaches this buffered egress when the
/// route also has `on_response` steps (a plain streaming translate route is
/// handled by `translate_stream`, M15 Task 7); combining `on_response`
/// buffering with a streaming upstream is what this branch rejects.
///
/// `request_report` (the ingress render's dropped fields, if any) is folded
/// together with this egress render's report so a single `x-sluice-translation`
/// header can summarize the whole round trip's fidelity. That header lives in
/// the internal `x-sluice-*` namespace, so ordinarily it only reaches the
/// client when `expose_headers` lists it (the same opt-in every diagnostic
/// `x-sluice-*` header uses) — EXCEPT when `t.report_header` is `true`, in
/// which case this route's own config has already opted this one header in
/// for this one response, so it's added to a local copy of the allowlist
/// passed to `reconstruct::client_response` rather than requiring the
/// operator to *also* list it in `[gateway] expose_headers` (M15 Task 6
/// review Important 1 — `report_header = true` alone must surface the
/// header; the global `expose_headers` config is never mutated).
fn translate_response(
    status: u16,
    headers: &BTreeMap<String, String>,
    body: &[u8],
    t: &Translate,
    request_report: Option<TranslationReport>,
    expose: &[String],
) -> Response<ResponseBody> {
    let (from, to) = match translate_adapters(t) {
        Ok(pair) => pair,
        Err(resp) => return resp,
    };

    let canonical = match to.parse_response(body) {
        Ok(c) => c,
        Err(err) => {
            if is_event_stream_response(headers) {
                // Streaming translation itself IS supported (M15 Task 7,
                // `translate_stream`), but only for a translate route with NO
                // `on_response` steps — those steps must observe/mutate a fully
                // buffered body, so this buffered egress path can't hand them a
                // live stream. A translate route that combines `on_response`
                // steps with a streaming upstream lands here; surface a
                // diagnosable 502 rather than the opaque "unparseable" one.
                tracing::warn!(
                    "translation: upstream returned an SSE stream for a translate route \
                     with on_response steps (from='{}', to='{}'); streaming translation is \
                     not supported alongside on_response buffering: {err}",
                    t.from,
                    t.to
                );
                return text_response(
                    StatusCode::BAD_GATEWAY,
                    "translation: streaming translation not supported with on_response steps",
                );
            }
            tracing::error!(
                "translation: unparseable upstream response from '{}': {err}",
                t.to
            );
            return text_response(
                StatusCode::BAD_GATEWAY,
                "translation: unparseable upstream response",
            );
        }
    };

    let mut report = request_report.unwrap_or_default();
    let rendered = match from.render_response(&canonical, &mut report) {
        Ok(r) => r,
        Err(err) => {
            tracing::error!(
                "translation: failed to render response into '{}': {err}",
                t.from
            );
            return text_response(
                StatusCode::BAD_GATEWAY,
                "translation: failed to render response into client dialect",
            );
        }
    };

    let mut out_headers = headers.clone();
    // A local copy of the expose allowlist, extended (never the global
    // config) so `report_header = true` alone surfaces the header to the
    // client without also requiring `expose_headers` to list it.
    let mut expose_owned = expose.to_vec();
    if t.report_header {
        out_headers.insert(
            "x-sluice-translation".to_string(),
            summarize_report(&report),
        );
        expose_owned.push("x-sluice-translation".to_string());
    }
    reconstruct::client_response(status, &out_headers, Bytes::from(rendered), &expose_owned)
}

/// True if `headers` (a case-insensitively-lowercased map, per `HttpMsg`'s
/// invariant) names an SSE `content-type` — the shape an upstream serving a
/// `stream: true` request answers with. Used only to make the 502 in
/// [`translate_response`]'s unparseable-body branch specifically diagnosable
/// (M15 Task 6 review Minor 3); matching on `content-type` rather than trying
/// to sniff the body itself keeps the detection cheap and unambiguous.
fn is_event_stream_response(headers: &BTreeMap<String, String>) -> bool {
    headers
        .get("content-type")
        .map(|v| v.to_ascii_lowercase().starts_with("text/event-stream"))
        .unwrap_or(false)
}

/// Summarize a [`TranslationReport`] into a single-line `x-sluice-translation`
/// header value: the dropped-field count plus the comma-joined field PATHS
/// only — never the dropped values themselves, which could carry request or
/// response content.
fn summarize_report(report: &TranslationReport) -> String {
    let paths: Vec<&str> = report.dropped.iter().map(|d| d.path.as_str()).collect();
    format!("dropped={}; fields={}", paths.len(), paths.join(","))
}

/// Parse one framed upstream `SseEvent` (in the `to` dialect) into a canonical
/// stream event and render it back out in the `from` dialect, appending the
/// rendered wire bytes to `out`. Every failure on the data path is contained
/// here (M15 Task 7 obligation 6): a chunk that doesn't parse to a canonical
/// event (`Ok(None)` housekeeping, or an `Err` from a malformed frame) simply
/// contributes nothing, and a render error is logged and skipped — a single
/// bad frame never aborts the stream or panics.
fn translate_stream_event(
    ev: &SseEvent,
    to: &(dyn Adapter + Send + Sync),
    from: &(dyn Adapter + Send + Sync),
    parse_state: &mut StreamParseState,
    render_state: &mut StreamRenderState,
    report: &mut TranslationReport,
    out: &mut Vec<u8>,
) {
    // One wire event can lift to more than one canonical event (e.g. a Gemini
    // terminal chunk carrying both final text AND stop_reason/usage): the
    // parser returns the first and queues the rest on `parse_state`, which we
    // drain here in order so none are lost.
    match to.parse_stream_event(ev, parse_state) {
        Ok(first) => {
            let mut events: Vec<CanonicalStreamEvent> = first.into_iter().collect();
            events.extend(parse_state.drain_pending());
            for canonical in events {
                match from.render_stream_event(&canonical, render_state, report) {
                    Ok(bytes) => out.extend_from_slice(&bytes),
                    Err(err) => {
                        tracing::warn!("translation: stream render error, event skipped: {err}")
                    }
                }
            }
        }
        Err(err) => tracing::warn!("translation: stream parse error, event skipped: {err}"),
    }
}

/// Wrap `upstream` (the raw `to`-dialect SSE byte stream) so each framed event
/// is translated into the client's `from` dialect and emitted as fully-framed
/// wire bytes (M15 Task 7, the streaming mirror of [`translate_response`]).
///
/// Return type matches [`tee_observe_stream`]/[`mutate_stream`] so
/// `reconstruct::client_streaming_response` accepts it unchanged. Per-stream
/// bookkeeping ([`StreamParseState`]/[`StreamRenderState`]) and the fidelity
/// [`TranslationReport`] are owned by the unfold state for the stream's whole
/// life. `report` is drained only to the log (stream-side drops aren't folded
/// into the response header, which is already sent by the time the body flows).
///
/// # Terminal synthesis (obligation a + c)
///
/// A coarse source (Google's Gemini stream) has no terminal event, so its
/// canonical stream never yields a [`CanonicalStreamEvent::MessageStop`] and a
/// target that requires one (Anthropic's `message_stop`, OpenAI's `[DONE]`)
/// would be left truncated. At upstream-stream-end, if no terminal was emitted
/// (tracked by the renderer on [`StreamRenderState::terminal_sent`]), a
/// `MessageStop` is synthesized and rendered so the target's required terminal
/// appears exactly once — and the same `terminal_sent` guard makes a source
/// that *did* send a terminal not get a second one.
fn translate_stream<S>(
    upstream: S,
    from: BoxedAdapter,
    to: BoxedAdapter,
    seed_model: String,
    max_event_bytes: usize,
) -> impl Stream<Item = Result<Bytes, reqwest::Error>>
where
    S: Stream<Item = Result<Bytes, reqwest::Error>> + Send + 'static,
{
    struct State<S> {
        inner: std::pin::Pin<Box<S>>,
        framer: SseFramer,
        from: BoxedAdapter,
        to: BoxedAdapter,
        parse_state: StreamParseState,
        render_state: StreamRenderState,
        report: TranslationReport,
        // Set once the upstream stream is exhausted and the trailing event +
        // terminal have been flushed, so the next poll ends the client stream.
        done: bool,
    }

    // Obligation (b): seed the model so a synthesized preamble names a real one.
    let render_state = StreamRenderState {
        model: seed_model,
        ..StreamRenderState::default()
    };

    let state = State {
        inner: Box::pin(upstream),
        framer: SseFramer::with_max_event_bytes(max_event_bytes),
        from,
        to,
        parse_state: StreamParseState::default(),
        render_state,
        report: TranslationReport::default(),
        done: false,
    };

    stream::unfold(state, |mut state| async move {
        loop {
            if state.done {
                return None;
            }
            match state.inner.next().await {
                Some(Ok(bytes)) => {
                    let mut out = Vec::new();
                    match state.framer.push(&bytes) {
                        Ok(events) => {
                            for ev in events {
                                translate_stream_event(
                                    &ev,
                                    state.to.as_ref(),
                                    state.from.as_ref(),
                                    &mut state.parse_state,
                                    &mut state.render_state,
                                    &mut state.report,
                                    &mut out,
                                );
                            }
                        }
                        // `SseError::EventTooLarge`: the framer already reset
                        // its own buffer; nothing salvageable to translate, and
                        // (per obligation 6) a malformed/oversized frame is
                        // skipped, never fatal.
                        Err(err) => {
                            tracing::warn!("translation: oversized SSE frame skipped: {err}")
                        }
                    }
                    // A chunk of pure housekeeping (e.g. an OpenAI role-only
                    // opener) translates to zero bytes; loop to read more rather
                    // than emit an empty frame.
                    if !out.is_empty() {
                        return Some((Ok(Bytes::from(out)), state));
                    }
                }
                Some(Err(e)) => {
                    // Transport error reading the upstream body: forward it once
                    // (mirrors `tee_observe_stream`/`mutate_stream`) and let the
                    // next poll decide whether the stream is now exhausted.
                    return Some((Err(e), state));
                }
                None => {
                    // Upstream ended: flush a trailing event the framer never
                    // saw terminated, then synthesize the target terminal if the
                    // source never produced one (obligations a + c).
                    let mut out = Vec::new();
                    if let Some(ev) = state.framer.finish() {
                        translate_stream_event(
                            &ev,
                            state.to.as_ref(),
                            state.from.as_ref(),
                            &mut state.parse_state,
                            &mut state.render_state,
                            &mut state.report,
                            &mut out,
                        );
                    }
                    if !state.render_state.terminal_sent {
                        match state.from.render_stream_event(
                            &CanonicalStreamEvent::MessageStop,
                            &mut state.render_state,
                            &mut state.report,
                        ) {
                            Ok(bytes) => out.extend_from_slice(&bytes),
                            Err(err) => tracing::warn!(
                                "translation: failed to synthesize terminal event: {err}"
                            ),
                        }
                    }
                    if !state.report.dropped.is_empty() {
                        tracing::info!(
                            "translation: stream-side dropped {} field(s): {}",
                            state.report.dropped.len(),
                            summarize_report(&state.report),
                        );
                    }
                    state.done = true;
                    if out.is_empty() {
                        return None;
                    }
                    return Some((Ok(Bytes::from(out)), state));
                }
            }
        }
    })
}

/// Bounded capacity of the `on_stream` observe tee queue (design doc M10
/// §9): chunk envelopes queued for delivery to observe steps beyond this
/// many in flight are DROPPED (shed) rather than the client stream ever
/// being slowed down to wait for room. See [`tee_observe_stream`].
const OBSERVE_QUEUE_CAPACITY: usize = 256;

/// Per-request material every chunk envelope needs, shared cheaply (via
/// `Arc`) across every SSE event of one response stream — only the chunk's
/// own JSON differs event to event.
#[derive(Clone)]
struct ObserveContext {
    route_id: Arc<str>,
    correlation_id: Arc<str>,
    req: Arc<HttpMsg>,
    llm: Arc<Option<Llm>>,
}

/// One chunk envelope's material, queued by [`tee_observe_stream`] for
/// [`spawn_observe_poster`]'s background task to turn into an `Envelope` and
/// POST.
struct ObserveItem {
    chunk_json: serde_json::Value,
    ctx: ObserveContext,
}

/// One `on_stream` observe step's identity and destination, captured by
/// value (rather than borrowed from `Step`/`Config`) so it can be moved into
/// the `'static` background task [`spawn_observe_poster`] spawns — that task
/// can easily still be draining chunks after `forward` (and the request that
/// produced them) has already returned.
struct ObserveTarget {
    self_name: String,
    url: String,
    timeout_ms: u64,
}

/// Wrap `upstream` so every `Bytes` item is forwarded to the client
/// immediately and unchanged — the client stream is never gated on step
/// work, full stop — while also being fed into an [`SseFramer`]; each
/// complete SSE event becomes a chunk envelope best-effort-queued via `tx`
/// for [`spawn_observe_poster`]'s background task. The returned stream's
/// `Item` type matches `upstream`'s exactly, so
/// `reconstruct::client_streaming_response` needs no changes to accept it —
/// this is deliberate: the client-facing type must stay untouched by the
/// tee, or the pass-through and observe code paths would diverge.
///
/// `final` is `true` only for the one event actually known to be last (via a
/// one-event lookahead: an event is only known to be non-final once a
/// subsequent one arrives), so both a stream that ends on a properly
/// `\n\n`-terminated event and one that ends mid-event (flushed via
/// `SseFramer::finish`) are flagged correctly.
fn tee_observe_stream<S>(
    upstream: S,
    ctx: ObserveContext,
    adapter: Option<Box<dyn Adapter + Send + Sync>>,
    tx: mpsc::Sender<ObserveItem>,
    max_event_bytes: usize,
) -> impl Stream<Item = Result<Bytes, reqwest::Error>>
where
    S: Stream<Item = Result<Bytes, reqwest::Error>> + Send + 'static,
{
    struct State<S> {
        inner: std::pin::Pin<Box<S>>,
        framer: SseFramer,
        // One-event lookahead: the most recently completed event, held back
        // until either a later event arrives (proving it wasn't final) or
        // the stream ends (proving it was).
        pending: Option<SseEvent>,
        seq: u64,
        ctx: ObserveContext,
        adapter: Option<Box<dyn Adapter + Send + Sync>>,
        tx: mpsc::Sender<ObserveItem>,
    }

    let state = State {
        inner: Box::pin(upstream),
        framer: SseFramer::with_max_event_bytes(max_event_bytes),
        pending: None,
        seq: 0,
        ctx,
        adapter,
        tx,
    };

    stream::unfold(state, |mut state| async move {
        match state.inner.next().await {
            Some(item) => {
                // Framing/enqueueing is a pure side effect on the observe
                // queue — `item` (the client-bound value) is forwarded
                // unchanged below regardless of what happens here.
                if let Ok(bytes) = &item {
                    if let Ok(events) = state.framer.push(bytes) {
                        for event in events {
                            if let Some(prev) = state.pending.replace(event) {
                                state.seq += 1;
                                let seq = state.seq;
                                enqueue_event(
                                    &state.tx,
                                    &state.ctx,
                                    state.adapter.as_deref(),
                                    seq,
                                    false,
                                    &prev,
                                );
                            }
                        }
                    }
                    // `SseError::EventTooLarge`: the framer already reset
                    // its own buffer; there is nothing salvageable to
                    // enqueue, and (per the doc comment above) the
                    // client-facing bytes are forwarded regardless.
                }
                Some((item, state))
            }
            None => {
                // Upstream ended: flush whatever the framer/lookahead were
                // still holding, correctly marking whichever event turns
                // out to be truly last as `final`.
                let trailing = state.framer.finish();
                match (state.pending.take(), trailing) {
                    (Some(prev), Some(last)) => {
                        state.seq += 1;
                        let seq = state.seq;
                        enqueue_event(
                            &state.tx,
                            &state.ctx,
                            state.adapter.as_deref(),
                            seq,
                            false,
                            &prev,
                        );
                        state.seq += 1;
                        let seq = state.seq;
                        enqueue_event(
                            &state.tx,
                            &state.ctx,
                            state.adapter.as_deref(),
                            seq,
                            true,
                            &last,
                        );
                    }
                    (Some(prev), None) => {
                        state.seq += 1;
                        let seq = state.seq;
                        enqueue_event(
                            &state.tx,
                            &state.ctx,
                            state.adapter.as_deref(),
                            seq,
                            true,
                            &prev,
                        );
                    }
                    (None, Some(last)) => {
                        state.seq += 1;
                        let seq = state.seq;
                        enqueue_event(
                            &state.tx,
                            &state.ctx,
                            state.adapter.as_deref(),
                            seq,
                            true,
                            &last,
                        );
                    }
                    (None, None) => {}
                }
                None
            }
        }
    })
}

/// Build one chunk envelope's JSON (`{ data_b64, seq, final, delta }`) and
/// best-effort enqueue it for [`spawn_observe_poster`] — DROPPING (shedding)
/// it and counting [`METRIC_STREAM_SHED`] if the bounded queue is full.
/// Never blocks: `try_send` is synchronous and non-blocking by
/// construction, which is exactly why the client-forwarding side of
/// [`tee_observe_stream`] can call this inline without an `.await`.
///
/// `data_b64` encodes the SSE event's normalized `data:` payload (the
/// [`SseEvent::data`] the framer already concatenated), not the literal
/// wire bytes of the event record — a documented simplification, since nothing
/// downstream needs byte-exact SSE framing back, only the event's content.
fn enqueue_event(
    tx: &mpsc::Sender<ObserveItem>,
    ctx: &ObserveContext,
    adapter: Option<&(dyn Adapter + Send + Sync)>,
    seq: u64,
    is_final: bool,
    event: &SseEvent,
) {
    let delta = adapter.and_then(|a| a.parse_delta(event));
    let chunk_json = serde_json::json!({
        "data_b64": base64::engine::general_purpose::STANDARD.encode(event.data.as_bytes()),
        "seq": seq,
        "final": is_final,
        "delta": delta,
    });
    match tx.try_send(ObserveItem {
        chunk_json,
        ctx: ctx.clone(),
    }) {
        Ok(()) => {}
        Err(mpsc::error::TrySendError::Full(_)) => {
            metrics::counter!(METRIC_STREAM_SHED).increment(1);
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            // The receiver (and hence `spawn_observe_poster`'s background
            // task) is gone — not expected in normal operation (that task
            // only exits once every `Sender` including this one is dropped,
            // which can't happen while `tee_observe_stream`'s state, which
            // owns this `tx`, is still alive), but if it ever does happen
            // (e.g. the poster task panicked) every subsequent event for
            // this request would otherwise be lost with zero visibility —
            // so this is logged rather than silently swallowed like a
            // routine shed.
            tracing::warn!("on_stream observe queue's receiver is gone; dropping chunk envelope");
        }
    }
}

/// Spawn the single background task that drains `rx` and fire-and-forget
/// POSTs each chunk envelope's JSON to every `on_stream` observe step's URL.
/// This task is entirely off the client's critical path (see
/// [`tee_observe_stream`]'s doc comment) — the only question is how it
/// paces itself against `rx`, and that's deliberate too: for a given item,
/// every target is POSTed *concurrently* (via `join_all`, each with its own
/// step-configured timeout), but the loop only advances to the *next* item
/// once the current one's POSTs have all completed or timed out. That means
/// a permanently slow/unresponsive observe step doesn't spawn an
/// ever-growing pile of outstanding requests (there is no per-item
/// `tokio::spawn` here) — it simply slows this drain loop down, which lets
/// [`enqueue_event`]'s bounded queue (and its shedding) absorb the backlog,
/// exactly the safety valve the design already provides. The step's
/// response — the ignored directive — is never even parsed: observe steps
/// cannot affect the client stream in any way (M11's `mutate` hook is the
/// only stream hook that can).
fn spawn_observe_poster(
    client: reqwest::Client,
    targets: Vec<ObserveTarget>,
    mut rx: mpsc::Receiver<ObserveItem>,
) {
    tokio::spawn(async move {
        while let Some(item) = rx.recv().await {
            let posts = targets.iter().map(|target| {
                let env = build_stream_envelope(
                    &item.ctx.route_id,
                    &target.self_name,
                    &item.ctx.req,
                    item.chunk_json.clone(),
                    // Observe steps never see a mutating context (see
                    // `build_stream_envelope`'s doc comment) — always empty.
                    &serde_json::Map::new(),
                    &item.ctx.correlation_id,
                    item.ctx.llm.as_ref().as_ref(),
                );
                let client = client.clone();
                let url = target.url.clone();
                let timeout = std::time::Duration::from_millis(target.timeout_ms);
                async move {
                    // Fire-and-forget: any response (and in particular the
                    // directive it might carry) is intentionally discarded.
                    let _ = client.post(&url).timeout(timeout).json(&env).send().await;
                }
            });
            futures_util::future::join_all(posts).await;
        }
    });
}

/// Per-stream material every chained mutate step's envelope needs, shared
/// cheaply (via `Arc`) across every SSE event of one response stream.
/// Deliberately a separate type from [`ObserveContext`] even though the
/// fields are identical — the `mutate` (M11) and `observe` (M10) `on_stream`
/// pipelines are kept as independent code paths (see `forward`'s on_stream
/// branch), so nothing here accidentally couples them.
#[derive(Clone)]
struct MutateContext {
    route_id: Arc<str>,
    correlation_id: Arc<str>,
    req: Arc<HttpMsg>,
    llm: Arc<Option<Llm>>,
}

/// One `on_stream` `mutate` step's identity, per-call timeout, and own
/// `on_error` policy, plus how it is dispatched ([`MutateDispatch`]) — all
/// captured by value (like [`ObserveTarget`]) so the whole chain can be moved
/// into the `'static` stream wrapper [`mutate_stream`] builds.
struct MutateTarget {
    self_name: String,
    timeout_ms: u64,
    on_error: OnError,
    dispatch: MutateDispatch,
}

/// How a [`MutateTarget`] runs its per-event step call (M17 Task 2). A `Url`
/// target POSTs each chunk envelope to a step service (the M11 form,
/// byte-identical to before this refactor); a `Worker` target frames each
/// chunk envelope to a DEDICATED, per-stream long-lived `script_mode =
/// "worker"` subprocess and reads a directive frame back.
///
/// The worker is deliberately per-stream (spawned in `forward`'s on_stream
/// branch and owned here) and NOT drawn from `ProxyState::worker_cache`: a
/// stateful worker accumulating per-stream state must not have two concurrent
/// client streams interleave their chunks onto one process. Its
/// `Arc<Mutex<..>>` is held only by this target, so it is dropped — and its
/// child killed via `kill_on_drop` — when the stream ends and `mutate_stream`'s
/// state (and thus the target) is dropped. The `Mutex` guards the framed
/// request/response exchange, which must not interleave even in the
/// single-stream case (the chain runs one event at a time, but the lock keeps
/// that invariant explicit and local).
enum MutateDispatch {
    Url(String),
    Worker(Arc<tokio::sync::Mutex<ScriptWorker>>),
}

/// Outcome of running one framed SSE event through the ordered chain of
/// `mutate` steps configured on a route. See [`run_mutate_chain`].
enum ChainOutcome {
    /// Forward these exact wire bytes (already re-framed as a `data:` SSE
    /// event) to the client.
    Forward(Bytes),
    /// Swallow this event: nothing is sent to the client, but the stream
    /// stays open for later events.
    Drop,
    /// Terminate the client stream immediately: either an explicit `abort`
    /// directive from a step, or a `fail_closed` step failure.
    Abort,
}

/// Render `payload` as an SSE `data:` event, terminated by the blank line
/// the framer requires to recognize it (`\n\n`). Per the design brief's
/// contract for `emit` ("forward the chunk's base64-decoded bytes to the
/// client re-framed as `data: <...>\n\n`"), each LINE of `payload` gets its
/// own `data:` prefix — a payload with embedded `\n` (e.g. multi-paragraph
/// LLM completion text, or an original event whose several `data:` lines
/// `SseFramer` already joined into one string) is re-split back into one
/// `data:` line per original line, exactly mirroring how `SseFramer::parse_event`
/// joins multiple `data:` lines with `\n` in the first place.
///
/// This is NOT cosmetic: emitting the whole payload as a single `data:`
/// line instead would put any embedded blank line (`\n\n`) directly into the
/// wire bytes, which `SseFramer` (on this gateway or a downstream client)
/// would then misparse as the event's own terminator — silently truncating
/// the event and turning everything after that blank line into a dangling,
/// unprefixed (and therefore discarded) line of a bogus next "event". Both
/// the `emit` path and the `fail_open` original-event pass-through
/// (`fail_event`) route through this function, so getting it wrong would
/// corrupt ordinary multi-paragraph content on the feature's main path, not
/// just an edge case.
fn wrap_sse_data(payload: &str) -> Bytes {
    let mut out = String::with_capacity(payload.len() + 8);
    for line in payload.split('\n') {
        out.push_str("data: ");
        out.push_str(line);
        out.push('\n');
    }
    out.push('\n');
    Bytes::from(out)
}

/// Apply a `mutate` step's `emit`/`drop` `ops` to the cross-event, cross-step
/// stream `context` map. Reuses [`apply_ops`] — the same op vocabulary and
/// atomicity contract `on_request`/`on_response` already use — against a
/// throwaway scratch [`HttpMsg`]: there is no request left to mutate at
/// `on_stream` (it already went to upstream long before this event arrived),
/// so only `Op::SetContext` has any observable effect here.
/// `set_header`/`set_body`/`set_path` ops are accepted (one shared contract
/// across every hook, rather than a stream-specific op subset) but silently
/// apply to, and are discarded with, the scratch message.
fn apply_stream_ops(
    context: &mut serde_json::Map<String, serde_json::Value>,
    self_name: &str,
    ops: &[crate::directive::Op],
    max_context_bytes: usize,
) -> Result<(), crate::directive::OpError> {
    let mut scratch = HttpMsg {
        method: String::new(),
        path: String::new(),
        headers: BTreeMap::new(),
        body_b64: String::new(),
    };
    apply_ops(&mut scratch, context, self_name, ops, max_context_bytes)
}

/// What a chain step's directive/ops-application resolved to when it did
/// NOT cleanly emit a rewrite: routed uniformly through `target.on_error`
/// (design doc M11 Task 3) — `fail_open` forwards the original upstream
/// event unchanged and stops the chain right here (later steps do not run
/// for this event); `fail_closed` aborts the whole client stream. This one
/// helper folds every failure shape (transport/decode error, an illegal
/// `continue`/`short_circuit` directive, a malformed/non-UTF8
/// `emit.chunk.data_b64`, or a `set_context` op that failed to apply) into
/// the same on_error decision, mirroring how `on_request`/`on_response`
/// already fold multiple failure modes into one fail_open/fail_closed
/// branch (see `run_pipeline`/`forward`).
fn fail_event(target: &MutateTarget, original: &str, route_id: &str) -> ChainOutcome {
    metrics::counter!(METRIC_STEP_ERROR_TOTAL, "route" => route_id.to_string()).increment(1);
    match target.on_error {
        OnError::FailOpen => ChainOutcome::Forward(wrap_sse_data(original)),
        OnError::FailClosed => {
            metrics::counter!(METRIC_ABORT_TOTAL, "route" => route_id.to_string()).increment(1);
            ChainOutcome::Abort
        }
    }
}

/// Run one framed SSE `event` through `targets` in order (design doc M11
/// Task 3): each step's directive is decoded and, per the brief, only
/// `emit`, `drop`, and `abort` are legal at `on_stream` — `continue` and
/// `short_circuit` are illegal here and routed through `on_error` exactly
/// like a transport/decode failure (see [`fail_event`]). `context` persists
/// across events *and* across every step within one event: the same map is
/// threaded through the whole stream's lifetime by [`mutate_stream`], which
/// owns it and passes it in here by `&mut` each call.
///
/// Chaining (multiple mutate steps on one route): an `emit`'s decoded bytes
/// become the *next* step's input — `delta` is re-parsed from that rewritten
/// text so a later step observes a delta consistent with what it's about to
/// receive, not the original upstream event. A `drop` by any step in the
/// chain ends the chain immediately for this event; later steps never run
/// for it. Per the brief, "supporting a single mutate step fully and
/// chaining sequentially for multiple is acceptable" — this is that
/// sequential-chain form (not a fan-out/merge of independent step outputs).
#[allow(clippy::too_many_arguments)]
async fn run_mutate_chain(
    client: &reqwest::Client,
    targets: &[MutateTarget],
    stream_ctx: &MutateContext,
    adapter: Option<&(dyn Adapter + Send + Sync)>,
    context: &mut serde_json::Map<String, serde_json::Value>,
    max_context_bytes: usize,
    seq: u64,
    is_final: bool,
    event: &SseEvent,
) -> ChainOutcome {
    let original = event.data.clone();
    let mut current = event.data.clone();

    for target in targets {
        let delta = adapter.and_then(|a| {
            a.parse_delta(&SseEvent {
                data: current.clone(),
            })
        });
        let chunk_json = serde_json::json!({
            "data_b64": base64::engine::general_purpose::STANDARD.encode(current.as_bytes()),
            "seq": seq,
            "final": is_final,
            "delta": delta,
        });
        let env = build_stream_envelope(
            &stream_ctx.route_id,
            &target.self_name,
            &stream_ctx.req,
            chunk_json,
            context,
            &stream_ctx.correlation_id,
            stream_ctx.llm.as_ref().as_ref(),
        );
        let step_span = tracing::info_span!(
            "step",
            name = %target.self_name,
            hook = ?crate::config::Hook::OnStream,
        );
        // Dispatch per target: a URL target POSTs the chunk envelope (the M11
        // path, unchanged); a worker target frames the SAME envelope JSON to
        // its dedicated per-stream subprocess and reads a directive back. Both
        // arms yield the same `Result<Directive, StepError>`, so the
        // emit/drop/abort/on_error handling below is shared byte-for-byte.
        let directive = async {
            match &target.dispatch {
                MutateDispatch::Url(url) => {
                    UrlTransform::new(url.clone(), target.timeout_ms)
                        .run(client, &env)
                        .await
                }
                MutateDispatch::Worker(worker) => {
                    let bytes = serde_json::to_vec(&env)?;
                    let mut guard = worker.lock().await;
                    guard.call(&bytes).await
                }
            }
        }
        .instrument(step_span)
        .await;

        match directive {
            Ok(Directive::Emit { chunk, ops }) => {
                let decoded = base64::engine::general_purpose::STANDARD
                    .decode(&chunk.data_b64)
                    .ok()
                    .and_then(|bytes| String::from_utf8(bytes).ok());
                match decoded {
                    Some(text) => {
                        if apply_stream_ops(context, &target.self_name, &ops, max_context_bytes)
                            .is_err()
                        {
                            return fail_event(target, &original, &stream_ctx.route_id);
                        }
                        current = text;
                    }
                    None => return fail_event(target, &original, &stream_ctx.route_id),
                }
            }
            Ok(Directive::Drop { ops }) => {
                if apply_stream_ops(context, &target.self_name, &ops, max_context_bytes).is_err() {
                    return fail_event(target, &original, &stream_ctx.route_id);
                }
                return ChainOutcome::Drop;
            }
            Ok(Directive::Abort { .. }) => {
                metrics::counter!(METRIC_ABORT_TOTAL, "route" => stream_ctx.route_id.to_string())
                    .increment(1);
                return ChainOutcome::Abort;
            }
            Ok(Directive::Continue { .. }) | Ok(Directive::ShortCircuit { .. }) => {
                return fail_event(target, &original, &stream_ctx.route_id);
            }
            Err(_) => return fail_event(target, &original, &stream_ctx.route_id),
        }
    }

    ChainOutcome::Forward(wrap_sse_data(&current))
}

/// Wrap `upstream` so every complete SSE event is routed THROUGH the route's
/// ordered `mutate` steps before anything reaches the client (design doc M11
/// Task 3) — unlike [`tee_observe_stream`], this per-event, awaited step call
/// actually gates what (and whether) the client receives: `emit` forwards
/// the (possibly rewritten) event, `drop` forwards nothing, `abort` ends the
/// client stream outright. This is the one point in the whole gateway where
/// forwarding a streamed response is intentionally NOT decoupled from step
/// latency — per the design brief, "per-event await here is INTENDED for
/// mutate (it gates the client)". The pooled `reqwest::Client` still means
/// no per-event TCP/TLS handshake; a framed single-connection worker
/// (batching many events over one long-lived step connection) is a
/// documented future optimization, not this task's form.
///
/// Same one-event lookahead as [`tee_observe_stream`] (`pending`, below): the
/// chunk envelope's `final` flag is only known once either a later event
/// arrives (proving the held-back one wasn't final) or the stream ends
/// (proving it was) — so exactly one event of latency is added regardless of
/// whether the upstream properly `\n\n`-terminates its last event.
///
/// Bytes that are never framed into a complete SSE event — a genuinely
/// non-SSE-shaped body, or the trailing partial tail some servers omit the
/// final terminator for — are forwarded to the client UNCHANGED, byte for
/// byte, once the upstream stream ends, bypassing the mutate chain entirely
/// (see [`SseFramer::finish_raw`]): mutate only ever runs the step chain
/// over fully framed events. A pathological upstream that never emits a
/// single `\n\n` at all accumulates entirely in the framer's buffer until
/// the stream ends (bounded only by `max_event_bytes`, the same cap any
/// single un-terminated event is already subject to) — not a new gap this
/// task introduces, and not the shape any of the three built-in provider
/// adapters' real SSE output takes.
fn mutate_stream<S>(
    upstream: S,
    stream_ctx: MutateContext,
    adapter: Option<Box<dyn Adapter + Send + Sync>>,
    client: reqwest::Client,
    targets: Vec<MutateTarget>,
    max_context_bytes: usize,
    max_event_bytes: usize,
) -> impl Stream<Item = Result<Bytes, reqwest::Error>>
where
    S: Stream<Item = Result<Bytes, reqwest::Error>> + Send + 'static,
{
    struct State<S> {
        inner: std::pin::Pin<Box<S>>,
        framer: SseFramer,
        // One-event lookahead, same purpose as `tee_observe_stream`'s
        // `pending`: the most recently completed event, held back until
        // either a later event arrives (proving it wasn't final) or the
        // stream ends (proving it was).
        pending: Option<SseEvent>,
        seq: u64,
        // Bytes already decided (via `run_mutate_chain`) and waiting to be
        // handed to the client on the next poll — decoupled from upstream
        // read cardinality, since one upstream chunk can complete zero, one,
        // or many events, and a `drop`ped event yields none at all.
        out: std::collections::VecDeque<Bytes>,
        ended: bool,
        client: reqwest::Client,
        targets: Vec<MutateTarget>,
        stream_ctx: MutateContext,
        adapter: Option<Box<dyn Adapter + Send + Sync>>,
        // Cross-event, cross-step stream context (design doc M11 Task 3):
        // owned here for the stream's whole lifetime, mutated in place by
        // each `run_mutate_chain` call.
        context: serde_json::Map<String, serde_json::Value>,
        max_context_bytes: usize,
    }

    let state = State {
        inner: Box::pin(upstream),
        framer: SseFramer::with_max_event_bytes(max_event_bytes),
        pending: None,
        seq: 0,
        out: std::collections::VecDeque::new(),
        ended: false,
        client,
        targets,
        stream_ctx,
        adapter,
        context: serde_json::Map::new(),
        max_context_bytes,
    };

    stream::unfold(state, |mut state| async move {
        loop {
            if let Some(bytes) = state.out.pop_front() {
                return Some((Ok(bytes), state));
            }
            if state.ended {
                return None;
            }

            match state.inner.next().await {
                Some(Ok(bytes)) => match state.framer.push(&bytes) {
                    Ok(events) => {
                        for event in events {
                            let Some(prev) = state.pending.replace(event) else {
                                continue;
                            };
                            state.seq += 1;
                            let seq = state.seq;
                            let outcome = run_mutate_chain(
                                &state.client,
                                &state.targets,
                                &state.stream_ctx,
                                state.adapter.as_deref(),
                                &mut state.context,
                                state.max_context_bytes,
                                seq,
                                false,
                                &prev,
                            )
                            .await;
                            match outcome {
                                ChainOutcome::Forward(b) => state.out.push_back(b),
                                ChainOutcome::Drop => {}
                                ChainOutcome::Abort => {
                                    state.ended = true;
                                    break;
                                }
                            }
                        }
                    }
                    Err(_) => {
                        // `SseError::EventTooLarge`: the framer already
                        // reset its own buffer (see `sse.rs`); nothing
                        // salvageable survives to forward for the oversized
                        // event, matching the framer's own documented
                        // "unrecoverable" contract.
                    }
                },
                Some(Err(e)) => {
                    // Transport error reading the upstream body: forward it
                    // once (mirrors `tee_observe_stream`'s pass-through of
                    // `Err` items) and let the next poll's `inner.next()`
                    // decide whether the underlying stream is now exhausted.
                    return Some((Err(e), state));
                }
                None => {
                    // Upstream ended: the held-back lookahead event (if any)
                    // is now known to be truly final, and whatever's left in
                    // the framer's buffer is an un-terminated trailing tail
                    // forwarded unchanged (see this function's doc comment).
                    if let Some(prev) = state.pending.take() {
                        state.seq += 1;
                        let seq = state.seq;
                        let outcome = run_mutate_chain(
                            &state.client,
                            &state.targets,
                            &state.stream_ctx,
                            state.adapter.as_deref(),
                            &mut state.context,
                            state.max_context_bytes,
                            seq,
                            true,
                            &prev,
                        )
                        .await;
                        if let ChainOutcome::Forward(b) = outcome {
                            state.out.push_back(b);
                        }
                        // `Drop`/`Abort` on the very last event: nothing more
                        // would follow regardless, so both end the stream
                        // the same way here (raw tail below, then `ended`).
                    }
                    if let Some(raw_tail) = state.framer.finish_raw() {
                        if !raw_tail.is_empty() {
                            state.out.push_back(Bytes::from(raw_tail));
                        }
                    }
                    state.ended = true;
                }
            }
        }
    })
}

/// Outcome of [`buffer_or_overflow`].
enum ResponseBuffer {
    /// The whole body fit within the cap and is buffered here.
    Buffered(Bytes),
    /// The body exceeded the cap. `prefix` is everything read up to (and
    /// including) the chunk that tripped the limit; `rest` is the still-open
    /// remainder of the upstream stream. Concatenating `prefix` then `rest`
    /// reproduces the full body without ever holding it all in memory — the
    /// `stream_through` policy uses this to forward an oversize body to the
    /// client unbuffered.
    Overflow {
        prefix: Bytes,
        rest: std::pin::Pin<Box<dyn Stream<Item = Result<Bytes, reqwest::Error>> + Send + Sync>>,
    },
    /// The upstream stream itself errored while being read.
    Upstream,
}

/// Drain `stream` into a single `Bytes`, but instead of failing when the
/// accumulated size exceeds `max_bytes`, return [`ResponseBuffer::Overflow`]
/// carrying the already-read prefix and the still-open remainder of the
/// stream. This bounds buffered memory the same way the request-side
/// `http_body_util::Limited` cap does, while leaving the caller free to either
/// reject the oversize body or stream it through unbuffered (the prefix has
/// been consumed from the stream, so the caller must re-prepend it).
async fn buffer_or_overflow<S>(stream: S, max_bytes: usize) -> ResponseBuffer
where
    S: Stream<Item = Result<Bytes, reqwest::Error>> + Send + Sync + 'static,
{
    let mut stream = Box::pin(stream);
    let mut buf: Vec<u8> = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = match chunk {
            Ok(c) => c,
            Err(_) => return ResponseBuffer::Upstream,
        };
        buf.extend_from_slice(&chunk);
        if buf.len() > max_bytes {
            return ResponseBuffer::Overflow {
                prefix: Bytes::from(buf),
                rest: stream,
            };
        }
    }
    ResponseBuffer::Buffered(Bytes::from(buf))
}

#[cfg(test)]
mod mutate_wire_format_tests {
    use super::wrap_sse_data;
    use crate::sse::SseFramer;

    #[test]
    fn single_line_payload_round_trips_through_the_framer() {
        let bytes = wrap_sse_data("hello");
        assert_eq!(bytes.as_ref(), b"data: hello\n\n");
        let mut framer = SseFramer::new();
        let events = framer.push(&bytes).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data, "hello");
    }

    /// A payload with an embedded blank line (e.g. multi-paragraph LLM
    /// completion text) must NOT be emitted as one bare `data:` line: doing
    /// so would put a literal `\n\n` in the wire bytes, which the framer
    /// (here, or on any downstream client) would misparse as the event's
    /// own terminator — truncating the event and turning the remainder into
    /// a dangling, discarded fragment. Each line must get its own `data:`
    /// prefix, exactly mirroring how `SseFramer::parse_event` joins
    /// multiple `data:` lines with `\n` when it first parsed them.
    #[test]
    fn multi_line_payload_with_embedded_blank_line_survives_round_trip() {
        let payload = "paragraph one\n\nparagraph two";
        let bytes = wrap_sse_data(payload);
        let mut framer = SseFramer::new();
        let events = framer.push(&bytes).unwrap();
        assert_eq!(
            events.len(),
            1,
            "an embedded blank line must not fragment into extra events: {events:?}"
        );
        assert_eq!(events[0].data, payload);
    }

    #[test]
    fn empty_payload_round_trips_as_empty_data_line() {
        let bytes = wrap_sse_data("");
        let mut framer = SseFramer::new();
        let events = framer.push(&bytes).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data, "");
    }
}

/// Worker-lifecycle tests for the cached `script_mode = "worker"` dispatch in
/// [`run_cached_worker_step`]: a timed-out worker is evicted (H2), a live
/// worker's bad-JSON output is NOT treated as death (H3), and the hot-reload
/// cache prune drops orphaned workers (H4).
#[cfg(test)]
mod worker_lifecycle_tests {
    use super::*;
    use crate::config::Hook;
    use std::process::Command as StdCommand;

    /// These tests drive a real `python3` worker over the framing protocol; on
    /// a host without `python3` they self-skip (mirroring `tests/scripts.rs`
    /// and the `src/step/script.rs` unit tests) rather than fail.
    fn have_python3() -> bool {
        StdCommand::new("python3")
            .arg("--version")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
    }

    fn write_worker(name: &str, program: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "sluice-worker-lifecycle-{}-{}",
            std::process::id(),
            name
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!("{name}.py"));
        std::fs::write(&path, program).unwrap();
        path
    }

    fn worker_step(cmd: Vec<String>, timeout_ms: u64) -> Step {
        Step {
            name: Some("w".into()),
            hook: Hook::OnRequest,
            type_: StepType::Script,
            url: None,
            mode: UrlMode::Transform,
            timeout_ms,
            on_error: OnError::FailOpen,
            chunk_mode: crate::config::ChunkMode::Observe,
            is_guardrail: false,
            script_mode: ScriptMode::Worker,
            cmd,
            wasm: None,
        }
    }

    fn minimal_state() -> Arc<ProxyState> {
        let cfg = crate::config::load::load_str(
            r#"
            [[route]]
            id = "x"
            upstream = "http://u"
        "#,
        )
        .unwrap();
        crate::server::build_state(cfg)
    }

    fn req_envelope() -> Envelope {
        let msg = HttpMsg {
            method: "POST".into(),
            path: "/x/v1/messages".into(),
            headers: BTreeMap::new(),
            body_b64: String::new(),
        };
        let ctx = serde_json::Map::new();
        build_request_envelope("x", "w", &msg, &ctx, "corr-1", None)
    }

    /// H2: a worker that times out is EVICTED from the cache (its child was
    /// already killed by `ScriptWorker::call`), so the NEXT request spawns a
    /// fresh worker rather than hitting the dead corpse. The worker sleeps far
    /// past the step timeout on its FIRST call (gated by a marker file), then
    /// behaves normally once respawned.
    #[tokio::test]
    async fn timed_out_worker_is_evicted_and_next_call_spawns_fresh() {
        if !have_python3() {
            eprintln!(
                "skipping timed_out_worker_is_evicted_and_next_call_spawns_fresh: no python3"
            );
            return;
        }
        let marker = std::env::temp_dir().join(format!(
            "sluice-worker-timeout-marker-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_file(&marker);
        let worker = write_worker(
            "sleep_first",
            r#"import sys, struct, json, os, time
marker = sys.argv[1]
while True:
    hdr = sys.stdin.buffer.read(4)
    if len(hdr) < 4:
        break
    n = struct.unpack('<I', hdr)[0]
    _ = sys.stdin.buffer.read(n)
    if not os.path.exists(marker):
        open(marker, 'w').close()
        time.sleep(30)
    resp = json.dumps({"action": "continue", "ops": [
        {"op": "set_header", "name": "x-alive", "value": "yes"}
    ]}).encode()
    sys.stdout.buffer.write(struct.pack('<I', len(resp)))
    sys.stdout.buffer.write(resp)
    sys.stdout.buffer.flush()
"#,
        );

        let state = minimal_state();
        let step = worker_step(
            vec![
                "python3".into(),
                worker.display().to_string(),
                marker.display().to_string(),
            ],
            500,
        );
        let key = worker_cache_key(&step.cmd);
        let env = req_envelope();

        let first = run_cached_worker_step(&state, &step, &env).await;
        assert!(
            matches!(first, Err(StepError::Timeout)),
            "the first call must time out, got {first:?}"
        );
        assert!(
            !state.worker_cache.contains_key(&key),
            "H2: a timed-out (killed) worker must be evicted so the next request spawns fresh"
        );

        let second = run_cached_worker_step(&state, &step, &env).await;
        let ok = matches!(&second, Ok(Directive::Continue { ops })
            if ops.iter().any(|op| matches!(op, crate::directive::Op::SetHeader { name, value }
                if name == "x-alive" && value == "yes")));
        assert!(
            ok,
            "after eviction a fresh worker must spawn and succeed, got {second:?}"
        );
        assert!(
            state.worker_cache.contains_key(&key),
            "the freshly spawned worker must be cached under the same key"
        );

        let _ = std::fs::remove_file(&marker);
        let _ = std::fs::remove_dir_all(worker.parent().unwrap());
    }

    /// H3: a LIVE worker that emits a syntactically-valid frame whose payload
    /// is not a valid `Directive` yields a `Decode` error, which is NOT treated
    /// as process death: the worker is neither evicted nor respawned, and the
    /// SAME process (proven by a persistent in-process counter) serves the next
    /// request. If it were wrongly respawned, the counter would reset and the
    /// second call would report `1` instead of `2`.
    #[tokio::test]
    async fn decoded_bad_json_does_not_kill_or_respawn_the_worker() {
        if !have_python3() {
            eprintln!("skipping decoded_bad_json_does_not_kill_or_respawn_the_worker: no python3");
            return;
        }
        let worker = write_worker(
            "bad_then_good",
            r#"import sys, struct, json
count = 0
while True:
    hdr = sys.stdin.buffer.read(4)
    if len(hdr) < 4:
        break
    n = struct.unpack('<I', hdr)[0]
    _ = sys.stdin.buffer.read(n)
    count += 1
    if count == 1:
        # valid JSON framing, but NOT a valid Directive (no "action" tag)
        resp = json.dumps({"not_a_directive": True}).encode()
    else:
        resp = json.dumps({"action": "continue", "ops": [
            {"op": "set_header", "name": "x-count", "value": str(count)}
        ]}).encode()
    sys.stdout.buffer.write(struct.pack('<I', len(resp)))
    sys.stdout.buffer.write(resp)
    sys.stdout.buffer.flush()
"#,
        );

        let state = minimal_state();
        let step = worker_step(vec!["python3".into(), worker.display().to_string()], 5000);
        let key = worker_cache_key(&step.cmd);
        let env = req_envelope();

        let first = run_cached_worker_step(&state, &step, &env).await;
        assert!(
            matches!(first, Err(StepError::Decode(_))),
            "a live worker's bad-JSON output must surface as Decode, got {first:?}"
        );
        let worker_after_first = state
            .worker_cache
            .get(&key)
            .map(|e| e.clone())
            .expect("H3: a Decode error must NOT evict the live worker");

        let second = run_cached_worker_step(&state, &step, &env).await;
        let count = match &second {
            Ok(Directive::Continue { ops }) => ops.iter().find_map(|op| match op {
                crate::directive::Op::SetHeader { name, value } if name == "x-count" => {
                    Some(value.clone())
                }
                _ => None,
            }),
            _ => None,
        };
        assert_eq!(
            count.as_deref(),
            Some("2"),
            "H3: the SAME warm process must serve the next call (counter -> 2), got {second:?}"
        );
        let worker_after_second = state
            .worker_cache
            .get(&key)
            .map(|e| e.clone())
            .expect("worker still cached");
        assert!(
            Arc::ptr_eq(&worker_after_first, &worker_after_second),
            "H3: the worker must not be respawned across a Decode error"
        );

        let _ = std::fs::remove_dir_all(worker.parent().unwrap());
    }

    /// H4: `prune_worker_cache` retains exactly the workers whose cache key is
    /// still produced by a `type = "script", script_mode = "worker"` step in
    /// the reloaded config, and drops every other entry (whose `Arc` drop reaps
    /// the child). Here two entries are cached; the reloaded config only keeps
    /// one, so the stale entry is pruned.
    #[tokio::test]
    async fn prune_retains_only_workers_present_in_reloaded_config() {
        let state = minimal_state();
        let kept_cmd = vec!["python3".to_string(), "kept.py".to_string()];
        let stale_cmd = vec!["python3".to_string(), "stale.py".to_string()];
        let kept_key = worker_cache_key(&kept_cmd);
        let stale_key = worker_cache_key(&stale_cmd);

        // Populate the cache with two dummy worker handles (never called, so
        // they never touch their stdin/stdout).
        let kept_worker = Arc::new(tokio::sync::Mutex::new(
            ScriptWorker::spawn(&["true".to_string()], 1000).unwrap(),
        ));
        let stale_worker = Arc::new(tokio::sync::Mutex::new(
            ScriptWorker::spawn(&["true".to_string()], 1000).unwrap(),
        ));
        state.worker_cache.insert(kept_key.clone(), kept_worker);
        state.worker_cache.insert(stale_key.clone(), stale_worker);
        assert_eq!(state.worker_cache.len(), 2);

        // A reloaded config that only defines the "kept" worker step.
        let reloaded = crate::config::load::load_str(
            r#"
            [gateway]
            allow_scripts = true

            [[route]]
            id = "x"
            upstream = "http://u"
              [[route.step]]
              name = "kept"
              type = "script"
              script_mode = "worker"
              cmd = ["python3", "kept.py"]
        "#,
        )
        .unwrap();

        prune_worker_cache(&state, &reloaded);

        assert!(
            state.worker_cache.contains_key(&kept_key),
            "a worker still present in the reloaded config must survive the prune"
        );
        assert!(
            !state.worker_cache.contains_key(&stale_key),
            "a worker orphaned by the reload must be pruned so its child is reaped"
        );
        assert_eq!(state.worker_cache.len(), 1);
    }
}
