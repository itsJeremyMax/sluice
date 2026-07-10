# Observability and operations

This is the reference for running sluice: what it emits (metrics, logs), the admin listener that exposes them, how load-shedding and limits behave under pressure, how config hot reload works, and the two operational workflows that sit outside the request path (the model registry refresh and loopback). For field-by-field config defaults see [configuration](configuration.md); for exact CLI flags see [CLI](cli.md).

## Metrics

Sluice installs a process-wide Prometheus recorder (`observability::init_metrics`) and exposes it as text exposition on the admin listener's `/metrics`. Every metric name and label below is read straight from `src/observability.rs` and the call sites in `src/proxy.rs`.

| Metric | Type | Labels | Measures |
|---|---|---|---|
| `sluice_requests_total` | counter | `route`, `outcome` (`ok` \| `error`) | Total requests handled, incremented once per request at the end of `handle_inner`, after the whole `on_request`/forward/`on_response` pipeline has produced a response. `outcome` is `error` when the final response status is a 5xx, else `ok`. |
| `sluice_inflight` | gauge | none | Requests currently in flight. Incremented the instant a request acquires its `max_inflight` permit and decremented only once the full response body has been delivered to the client (or dropped on disconnect): it covers the whole request lifetime, not just until headers are ready. |
| `sluice_request_duration_seconds` | histogram | `route` | Wall-clock duration of the whole request (routing, steps, and the upstream call), recorded alongside `sluice_requests_total`. |
| `sluice_upstream_status_total` | counter | `route`, `status` | One increment per call into `forward()`, labeled with the resulting status as a string: either the real upstream response status or a gateway-synthesized 502/504 on a transport failure or timeout. Only recorded for requests that actually reach `forward`; a request short-circuited, aborted, or parked on a loopback step before that point never touches this metric. |
| `sluice_short_circuit_total` | counter | `route` | Count of `on_request` steps that returned a `short_circuit` directive. |
| `sluice_abort_total` | counter | `route` | Count of requests aborted: an `on_request` step's `abort` directive, a loopback chain exceeding `max_hops`, a loopback dispatch refused because `max_parked` is already held, a loopback continuation lost to timeout or sweep, or an `on_stream` mutate worker that failed to spawn under `fail_closed`. |
| `sluice_step_error_total` | counter | `route` | Count of individual step failures: a decode/timeout/wasm error from `run_step`, an illegal context or body mutation a step attempted, or an `on_stream` worker that failed to spawn under `fail_open`. |
| `sluice_stream_shed_total` | counter | none | Count of `on_stream` observe chunk envelopes dropped because the bounded tee queue feeding the observe step's poster task was full. The client's own stream is never slowed down to make room for this mirrored copy; a burst under a slow or overloaded observe step sheds these instead. |

**Known coverage gap.** A request shed by `max_inflight` (see below) never increments `sluice_requests_total`, in any `outcome`, and never gets a `route` label at all. `proxy::handle` returns the 503 directly, before route resolution or `handle_inner` runs, so there is no route to label it with and no funnel point that would count it. If you alert on `sluice_requests_total{outcome="error"}` you will not see `max_inflight` shedding in it; watch `sluice_inflight` against your configured `max_inflight` (or scrape the 503 rate at your load balancer) to catch that case.

## The admin listener

A small, separate HTTP listener exposes `/healthz`, `/readyz`, and `/metrics`, kept apart from the data-plane listener so probes and scrapes never compete with proxied traffic for the same accept loop. Enable it by setting `[gateway] admin_listen` to a non-empty address; an empty string (the default) disables it entirely. See [configuration](configuration.md) for the full field list.

- **`/healthz`** always answers `200 ok`, with no auth check, since an orchestrator needs it reachable before any token is provisioned.
- **`/readyz`** answers `503 not ready` until the data-plane listener is bound and the accept loop is about to start, then `200 ok` for the rest of the process's life.
- **`/metrics`** renders the current Prometheus snapshot as text exposition (`content-type: text/plain; version=0.0.4`).

`/readyz` and `/metrics` are protected by an optional bearer token, `[gateway] admin_token`. An empty token (the default) disables auth on both; anything else requires an `Authorization: Bearer <token>` header matching exactly, compared in constant time (`subtle::ConstantTimeEq`) rather than a plain `==`, so response timing can't leak how many leading bytes of a guessed token were correct. If you set `admin_listen` but leave `admin_token` empty, the gateway logs a startup warning that `/metrics` and `/readyz` on that listener are unauthenticated: a deliberate footgun alarm, not a hard failure, since an admin listener already bound to loopback or an otherwise trusted network is a legitimate reason to leave it open.

```bash
curl http://127.0.0.1:9090/healthz

curl -H "Authorization: Bearer $SLUICE_ADMIN_TOKEN" http://127.0.0.1:9090/metrics
```

## Logging

`observability::init_logging` installs a JSON-formatted `tracing_subscriber` fmt layer with an `EnvFilter` (`RUST_LOG`, default `info`). Every log line is a JSON object, so it's machine-parseable without a separate log shipper regex.

Every request gets a correlation id: `correlation_id_from` adopts the inbound `x-request-id` header if it looks safe (ASCII printable, no control characters, 1 to 200 bytes), or generates a fresh UUID v4 otherwise. The id is internal-only (it is never echoed back to the client, the egress sanitizer strips it) and is attached to the request's `tracing::info_span!("request", route, correlation_id)`, which every step's own `info_span!("step", name, hook)` nests under. That means every log line and step span for one logical request, across every step call, carries the same `correlation_id` field, so you can `grep`/query by it across the whole request's lifetime even though each step call is its own HTTP round trip.

## Load-shedding and limits at runtime

Sluice fails closed and cleanly rather than queuing or hanging when it's at capacity. Field defaults for everything below live in [configuration](configuration.md).

- **`max_inflight`.** A global semaphore, one permit per in-flight request. `proxy::handle` calls `try_acquire_owned` up front; if no permit is free, the request is shed immediately with a clean `503 too many in-flight requests` rather than queued. The permit (and the `sluice_inflight` gauge guard) travels with the response body for the request's whole lifetime, including a streamed response, so a slow client holds its permit until its body finishes delivering, not just until headers are ready.
- **`upstream_timeout_ms`.** Applied to the outbound call to the upstream. A timeout produces a clean `504 upstream request timed out`; any other transport failure (connection refused, DNS failure, etc.) produces a `502 upstream request failed`.
- **Body and event caps.** `max_body_bytes` caps a buffered request or response body; `max_response_bytes` (falling back to `max_body_bytes` when unset) caps the buffered response path specifically (routes with `on_response` steps, or translate routes); `max_event_bytes` caps a single not-yet-terminated SSE event before the stream framer gives up on it. Exceeding a body cap under `oversize = "reject"` (the default) answers `413 payload too large`; `oversize = "stream_through"` instead streams the oversize body to the client unbuffered rather than holding it in memory.

All of the above shed or time out; none of them queue a request waiting for capacity to free up.

## Config hot reload

`sluice serve` wraps the live `Config` in an `ArcSwap` and starts a background filesystem watcher (`config::watch::spawn_watcher`) on the same file or directory the config was loaded from. Editing that file (or any file under a `--config-dir`) triggers a reload without a restart.

**Debounce.** The watcher waits 200ms (`RELOAD_DEBOUNCE`) after the first filesystem event before reloading, draining and resetting that window on every further event until things go quiet. This collapses a burst of writes (an editor's write-then-rename save, an `rsync`, several files in a directory changing together) into exactly one reload instead of one per event.

**Keep-live-on-error.** A reload only swaps the live config in on success. An invalid edit on disk (bad TOML, a validation failure) is logged with `tracing::warn!` and ignored; the last-known-good config stays live and the gateway keeps serving with it.

**In-flight requests are not dropped.** `proxy::handle` snapshots the config once, via `load_full()`, at the very top of each request, and threads that single `Arc<Config>` through the whole request. A swap that lands mid-request never changes what that request sees; only requests that start after the swap observe the new config.

**Worker pruning on reload.** `script_mode = "worker"` steps run through a shared, spawn-once cache of long-lived worker processes (`ProxyState::worker_cache`), keyed by each step's command line. After every successful reload, the gateway prunes that cache down to exactly the worker keys the new config still defines, across every route (not just the routes a given listener serves, in a multi-listener setup: the cache is shared, so pruning by a subset would wrongly reap a worker another listener still uses). A worker whose step was removed or whose `cmd` changed has its cache entry dropped; if no in-flight request is still using it, its process is reaped immediately via `kill_on_drop`, and if one is, it's reaped once that request's own `Arc` clone finishes and drops. This is what keeps worker processes from accumulating across repeated reloads.

**What is NOT hot-swappable.** `max_inflight` sizes the concurrency semaphore once, at process startup, from the config `serve` was launched with; a later reload changes routing and limits seen by new requests, but never resizes that semaphore. The model registry is likewise loaded once via `Registry::load()` at startup; a `sluice models update` run against a live gateway only takes effect the next time the process restarts, never live. A `gateways.d` listener's own bind address and route set are also fixed at startup for the same reason (see [configuration](configuration.md)).

## The model registry workflow

The model-facts registry (context window, advisory pricing, tool-call support) is a models.dev-derived table, resolved offline and loaded once at gateway startup. It is never fetched or refreshed on the request path: refreshing it is always an explicit, out-of-band step you run yourself, via the `sluice models` subcommands. Full flag reference: [CLI](cli.md).

```
sluice models list                      Print the currently loaded registry.
sluice models diff    --source <dir>    Show what a local models.dev-shaped directory would change.
sluice models diff    --from-network    Same, resolved from a live fetch of models.dev instead.
sluice models update  --source <dir>    Resolve a local source and write it as the local registry.
sluice models update  --from-network    Same, resolved from a live fetch of models.dev instead.
```

`models update` and `models diff` each require **exactly one** of `--source <dir>` or `--from-network`; clap rejects both together and rejects neither being given. `--source <dir>` reads an offline, models.dev-shaped directory (a `models/` directory of provider-agnostic base files, a `providers/<provider>/` directory of serving overlays). `--from-network` fetches the live `https://models.dev/api.json` dataset instead.

`models update` writes **only** the registry JSON file (default `sluice-models.json`, overridable with `--out`); it never touches your `sluice.toml` or routes. `Registry::load()`, called once at `serve` startup, prefers that local file if it exists and parses; otherwise it falls back to the small seed embedded in the binary at compile time. `models diff` never writes anything; it just reports what would change against whatever is currently loaded.

The `--from-network` fetch is bounded on two axes: a 30-second total-request timeout (applied directly on that GET, independent of any client-wide timeout config), and a 32 MiB response size cap (`MAX_MODELS_DEV_BYTES`), enforced both by an upfront `content-length` check and by a running total as the body streams in, so a lying or absent `content-length` header can't bypass it. A non-2xx response, a transport failure, an oversized body, or a non-UTF8 body all surface as a clean `RegistryError`, never a panic and never a partially-applied registry.

Pricing and limits are advisory: seed and refreshed data may lag what a provider actually charges or supports, and neither `sluice update`/`diff` nor the running gateway treat it as billing-grade truth. A models.dev field rename or shape change fails the importer loudly (a TOML/JSON deserialize error) rather than silently degrading a model's facts to `None`: schema drift breaks the importer, not the runtime.

The request-path-never-fetches invariant is absolute: nothing in `proxy::handle` or anywhere else on the request path performs a models.dev lookup or network call. The registry a running gateway uses is whatever `Registry::load()` resolved once at startup, full stop.

## Loopback

Loopback is for legacy tools that can't return a directive inline to a step call: instead of answering the gateway's POST directly, the tool needs to do its own thing and call the gateway back later. A `mode = "loopback"` `url` step signs a compact, HMAC-signed `x-chain-token` naming where the chain should resume, fires that token to the tool as a header on a POST it does not wait for a response to, and parks the handler (still holding its `max_inflight` permit) on an internal channel for up to 60 seconds. When the tool is ready, it calls the gateway back at `callback_path` (default `/__sluice/loopback`) carrying that same `x-chain-token`; the gateway verifies the token's signature and expiry, looks up the parked continuation by the token's `cid` (one-shot: a replayed callback for the same `cid` finds nothing), resumes the `on_request` chain from where it parked, and feeds the resulting response back to unpark the original handler. A chain that never gets a callback within the park window fails with a clean `504`; one that tries to hop through more loopback steps than `max_hops` allows is aborted with `508 loop detected`.

Loopback is **experimental and off by default**. It only activates once `[gateway] loopback_secret` is a non-empty string; leaving it empty (the default) means `sluice check`/`serve` rejects any `mode = "loopback"` step at config load. The `x-chain-token` itself is `base64url(json payload) + "." + base64url(HMAC-SHA256(payload, secret))`, verified in constant time before the payload is trusted or parsed, with an expiry equal to the park timeout (60s) so a well-behaved tool's callback can't be spuriously rejected as expired while the gateway is still parked waiting for it. `sluice token verify --secret <s> <token>` decodes and verifies a token entirely offline, using the same secret the gateway itself is configured with (no server, no network call), and prints the decoded claims (`cid`, `route_id`, `resume_index`, `hop`, `expires_at`) plus its validity, including for an expired-but-correctly-signed token.

The honest caveat: loopback is not hardened, and is not something to point at a tool or network you don't fully trust. A `loopback_secret` rotated out from under an in-flight chain fails that chain's callback verification and the client sees a clean `401` rather than a hang, which is a documented fail-closed edge case, not a guarantee of graceful rotation. `max_parked` caps how many chains can be parked concurrently (refusing new ones with a `503` once at the cap) so a tool that never calls back can't grow the continuation registry without bound, but that is a resource-exhaustion guard, not a statement that loopback is hardened for adversarial input.
