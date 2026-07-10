# Steps and directives

This is the detailed reference for how a step works: the envelope a step receives, the directive it answers with, and the three runtimes (`url`, `script`, `wasm`) that can carry that exchange. For the config fields that name a step (`hook`, `type`, `timeout_ms`, and so on), see [configuration](configuration.md). For how `[route.adapter]` and `[route.translate]` fill in the envelope's `llm` field, see [translation](translation.md).

## The envelope

Every step, regardless of runtime, receives the same JSON shape on stdin (script), as a wasm guest call's input bytes (wasm), or as a POST body (url):

```json
{
  "envelope_version": 1,
  "hook": "on_request",
  "route_id": "claude",
  "correlation_id": "a1b2c3d4-...",
  "self": "tagger",
  "request": {
    "method": "POST",
    "path": "/claude/v1/messages",
    "headers": { "content-type": "application/json" },
    "body_b64": "..."
  },
  "response": null,
  "chunk": null,
  "llm": null,
  "context": {}
}
```

Fields, all always present (some as `null`):

| Field | Type | Meaning |
|---|---|---|
| `envelope_version` | integer | Currently `1`. Bump only on a breaking shape change. |
| `hook` | string | Which hook is calling: `"on_request"`, `"on_response"`, or `"on_stream"`. |
| `route_id` | string | The route's configured id. |
| `correlation_id` | string | Internal id tying every envelope/log line for one request together. Adopted from an inbound `x-request-id` header when it looks safe, otherwise generated fresh. Never forwarded to the client or upstream. |
| `self` | string | This step's own effective name (`name`, or `"<type>:<index>"` if unnamed). This is the key a step's `set_context` op writes under. |
| `request` | object | The `HttpMsg` for the request: `method`, `path`, a lowercased header map, and `body_b64`. Present (and, on `on_request`, mutable via ops) on every hook, since even an `on_response`/`on_stream` step is still answering about some request. |
| `response` | object or `null` | The buffered upstream `HttpMsg` (headers/body only, no status field). Filled in only on `hook = "on_response"`; `null` everywhere else. |
| `chunk` | object or `null` | The current SSE event as `{ data_b64, seq, final, delta }`. Filled in only on `hook = "on_stream"`; `null` everywhere else. `delta` is a normalized view of the event when a route names an ingress adapter that recognizes it, else `null`. |
| `llm` | object or `null` | The provider-agnostic view of the call (`provider`, `model`, `messages`, `tools`, `max_tokens`, `stream`, `facts`), built once a route's ingress adapter (`[route.adapter] ingress = ...`, or `[route.translate] from = ...` on a translate route) recognizes the request body. `null` when no adapter is configured, the body doesn't parse as that provider's shape, or the route has nothing to do with LLM traffic. See [translation](translation.md) for how it's built. |
| `context` | object | The shared, per-step-namespaced map. See below. |

`request.headers`/`response.headers` are always lowercased keys (`HttpMsg` normalizes on write). `body_b64` is the message body, standard base64, never raw bytes.

### `HttpMsg` has no status field

A response's HTTP status is tracked outside the envelope. At `on_response`, only `response`'s headers and body (and `context`) are mutable through ops; there is no op to change the status code. `request.method`/`request.path` on the `response` object at `on_response` are meaningless placeholders (empty strings) since `response` reuses the same `HttpMsg` shape as `request` but the response itself has no method or path.

## `context`: the shared, namespaced map

`context` is a JSON object that survives across every step in the chain for one request (or, for `on_stream` mutate, for the whole stream). Each step can only write to its own key, named after its `self` value: a `set_context` op from step `redact` lands at `context.redact`, never at any other key. A step cannot overwrite another step's entry, only read it.

A later step sees every earlier step's writes:

```json
// after "redact" (self = "redact") runs set_context { "count": 3 }
{ "context": { "redact": { "count": 3 } } }

// a later step in the same chain reads context.redact.count
```

Each namespace entry is capped independently by `max_context_bytes` (default 64 KiB, see [configuration](configuration.md)): a `set_context` value whose serialized size exceeds the cap is rejected (the whole `apply_ops` call for that step fails atomically, see below), but that cap is per step, not on the map as a whole, so total `context` size still grows as more steps each write their own capped entry.

`context` is threaded differently per hook:

- `on_request` and `on_response` share one `context` map for the whole request (a step's `on_request` write is visible to a later `on_response` step on the same route).
- `on_stream` **mutate** steps share one `context` map for the whole stream's lifetime, across every event and every chained mutate step, but it starts empty for each new stream (an `on_request` write does not carry into `on_stream`).
- `on_stream` **observe** steps always see an empty `context`. Observe steps never mutate anything (their directive is ignored entirely, see below), so they have nothing to write and nothing earlier to read.

## The directive

A step answers with exactly one action. `Directive` is a tagged enum on the `action` field (`serde(tag = "action")`), decoded from whatever the step's runtime returns.

### `continue`

```json
{ "action": "continue", "ops": [
    { "op": "set_header",    "name": "x-tag", "value": "seen" },
    { "op": "delete_header", "name": "x-internal" },
    { "op": "set_body",      "body_b64": "..." },
    { "op": "set_path",      "path": "/claude/v2/messages" },
    { "op": "set_context",   "value": { "counted": true } }
] }
```

`ops` defaults to `[]` if omitted. Valid on `on_request` and `on_response`. The five ops:

| Op | Effect |
|---|---|
| `set_header` | Insert or replace a header, name lowercased. Rejected (see below) on a framing header. |
| `delete_header` | Remove a header, case-insensitive. Rejected on a framing header. |
| `set_body` | Replace the message body with `body_b64`. |
| `set_path` | Replace `request.path`. On `on_request`, this actually changes where the request is forwarded (the upstream URL is recomputed from the post-ops path). Meaningless on `on_response` (there is no outbound path to change). |
| `set_context` | Write `value` under this step's own `self` namespace in `context`, subject to the per-namespace `max_context_bytes` cap. |

Ops are applied atomically: `apply_ops` works against local clones of the message and context, and only overwrites the caller's originals on full success. If any op in the list fails (a protected header, an oversized `set_context`), none of that step's ops take effect, not even the ones before the failing one. The failure is then routed through the step's `on_error` policy exactly like any other step error.

### `short_circuit`

```json
{ "action": "short_circuit", "response": { "status": 200, "headers": {}, "body_b64": "..." } }
```

Answer the client immediately with `response`, skipping the upstream and any later steps. **Valid only on `on_request`.** A step that returns `short_circuit` on `on_response` is treated as an illegal directive: a step error, routed through that step's `on_error` (there is no upstream call left to "skip" once the response already exists). It is never legal on `on_stream`.

### `abort`

```json
{ "action": "abort", "response": { "status": 403, "headers": {}, "body_b64": "..." } }
```

Stop with an error response `response`. Valid on `on_request` and `on_response` (on `on_response`, `abort` replaces the buffered upstream response with `response` and skips any later `on_response` steps). On `on_stream` mutate, `abort` ends the client stream outright (no `response` body is meaningful there; the field is present on the wire shape but not used to build a new response mid-stream).

`RespSpec` (the shape of `response` on both `short_circuit` and `abort`) is `{ status: u16, headers: {} (default {}), body_b64: "" (default "") }`.

### `emit` and `drop`: `on_stream` mutate only

```json
{ "action": "emit", "chunk": { "data_b64": "..." }, "ops": [] }
{ "action": "drop", "ops": [] }
```

These two actions exist only for `on_stream` steps with `chunk_mode = "mutate"`. They are illegal everywhere else: an `on_request` or `on_response` step returning `emit` or `drop` is a step error, routed through `on_error`.

- `emit` forwards this SSE event to the client with its data replaced by the base64-decoded bytes in `chunk.data_b64`, applying `ops` first. In practice the only op with an observable effect at `on_stream` is `set_context` (there is no header/body/path to rewrite on a single chunk); the message-shaped ops are accepted but have no target to apply to at this hook.
- `drop` swallows this event: the client receives nothing for it, and the stream is not ended. `ops` are still applied first.
- `chunk.data_b64` is deliberately the only field `Directive::Emit` reads out of `chunk`; a step that echoes back the full chunk envelope shape (`seq`, `final`, `delta`) alongside `data_b64` still parses fine, those extra keys are ignored.

When multiple mutate steps are chained on one route, `emit`'s rewritten bytes become the next step's input: the next step's `chunk.data_b64` (and its re-derived `delta`) reflect the previous step's rewrite, not the original upstream event. A `drop` by any step in the chain ends the chain for that event immediately; later steps never see it.

### Which actions are legal on which hook

| Action | `on_request` | `on_response` | `on_stream` observe | `on_stream` mutate |
|---|---|---|---|---|
| `continue` | yes | yes | ignored (see below) | step error |
| `short_circuit` | yes | step error | ignored | step error |
| `abort` | yes | yes | ignored | yes |
| `emit` | step error | step error | ignored | yes |
| `drop` | step error | step error | ignored | yes |

"ignored" means literal: an `on_stream` observe step's directive, whatever it is, is never even parsed as a decision point. Observe is fire-and-forget informational output; see the hooks section below.

## Framing headers a step may not set

`set_header`/`delete_header` reject, case-insensitively, any of:

```
Connection
Transfer-Encoding
Content-Length
Keep-Alive
Upgrade
TE
```

This is the `HOP_BY_HOP` list in `src/reconstruct.rs`, the single place the gateway decides which headers survive onto an outbound message in either direction. A step that tries to set or delete one of these gets `OpError::ProtectedHeader`, which fails that step's whole ops batch (see the atomicity note above) and is routed through `on_error`.

The reason is structural, not a policy choice: the gateway always recomputes these from the final body and connection handling right before sending. `Content-Length` is set from the actual outbound body length (`reqwest` for the upstream request, the response builder for the client response); `Transfer-Encoding`/`Connection`/`Keep-Alive`/`Upgrade`/`TE` are hop-by-hop by definition and never forwarded verbatim. A step that wants to influence chunking or connection handling has no lever for it; those are the gateway's own concern.

Separately from this protected list, `sanitize_egress_headers` also always strips the gateway's own internal namespace (`x-sluice-*` and `x-chain-token`) before a message leaves the process in either direction, and drops `host` heading upstream. That stripping is not something ops can even attempt to bypass (it isn't an op-time check, it's an egress-time filter applied after every step has run), and `x-sluice-*` headers only reach the client at all when `[gateway] expose_headers` names them.

## The hooks in depth

### `on_request`

Runs once per request, in route order, over every step whose `hook = "on_request"` (the default when `hook` is omitted). Each step gets a fresh envelope built from the current (possibly already-edited) request and context. A `continue` step's ops are applied and the loop moves to the next step; `short_circuit`/`abort` end the loop immediately with a client response; a step error (transport failure, decode failure, an illegal directive for this hook, or a failed op) is routed through that step's `on_error`.

After the whole `on_request` chain finishes without short-circuiting or aborting, the gateway reconstructs the outbound request from the (possibly edited) envelope and forwards it upstream.

### `on_response`

Runs only when the route has at least one step with `hook = "on_response"`. The gateway first buffers the entire upstream response body (bounded by `max_response_bytes`), then runs each `on_response` step over an envelope carrying both the original `request` and the buffered `response`. `continue` mutates `response`'s headers/body and `context` (never status, never `request`); `abort` replaces the response outright; `short_circuit` and `emit`/`drop` are illegal here.

Buffering means an `on_response` route pays full-body latency: the client sees nothing until the whole upstream response has arrived and every `on_response` step has run. This is why `on_response` and `on_stream` cannot be configured on the same route (rejected at config load, see [configuration](configuration.md)): a route is either streaming-shaped or not.

### `on_stream` observe

Runs when the route's `on_stream` steps all have `chunk_mode = "observe"` (the default). Each complete SSE event is mirrored, unchanged, into a bounded queue; a background task drains that queue and fire-and-forget POSTs a chunk envelope to every observe step. The client's own stream is a straight tee of the upstream bytes and is never delayed, blocked, or altered by an observe step, no matter how slow that step is or what it returns. This is the point of observe: it can watch, log, or score a live stream, but it cannot gate it.

### `on_stream` mutate

Runs when the route's `on_stream` steps all have `chunk_mode = "mutate"`. Unlike observe, this is in-path: each framed SSE event is awaited through the ordered chain of mutate steps before it is forwarded (or dropped, or the stream aborted). This is the one place in the gateway where forwarding a stream is intentionally coupled to step latency, because gating the stream is the entire point.

A route cannot mix `chunk_mode = "observe"` and `chunk_mode = "mutate"` on its own `on_stream` steps (rejected at config load): the dispatch only ever runs one branch (mutate wins if present), so a mixed route would silently drop its observe steps rather than run them.

### Combination constraints

Enforced once, at config load (`sluice check` runs the same checks the server runs on startup):

- `on_response` and `on_stream` steps cannot be configured on the same route.
- `on_stream` `observe` and `on_stream` `mutate` cannot be configured on the same route.
- `[route.translate]` cannot be combined with `on_stream` steps; run guardrails or mutation for a translate route on the `on_request` or `on_response` hooks.
- A `type = "script"` step on `on_stream` must be `script_mode = "worker"` with `chunk_mode = "mutate"` (a oneshot process has no per-chunk runtime, and an observe worker is never dispatched).
- A `type = "wasm"` step cannot be configured on `on_stream` (the wasm ABI is request/response only).

## `on_error`: fail_closed vs fail_open

Every step has its own `on_error`, defaulting to `fail_closed`. It governs what happens when that step:

- returns a transport/decode error (the runtime itself failed: a bad HTTP response, a subprocess that produced non-JSON stdout, a wasm trap),
- times out,
- returns a directive that is illegal on the hook it ran on (`short_circuit` at `on_response`, `continue` at `on_stream` mutate, and so on), or
- returns `continue` with an op that fails to apply (a protected header, an oversized `set_context`).

`fail_closed` (the default) aborts the request/stream with a gateway-authored error response (502, or the stream ends). `fail_open` proceeds as if the step had returned `continue` with no ops: the request/response/event carries on unchanged to the next step. On `on_stream` mutate specifically, `fail_open` forwards the original (pre-step) event unchanged and stops the chain for that one event; later mutate steps do not run for it.

`is_guardrail = true` steps cannot be configured with `on_error = "fail_open"` (rejected at config load): a guardrail that fails open cannot enforce anything, so the gateway refuses to load that config at all rather than silently defeat the guardrail under load.

## The three step runtimes

`run_step` dispatches every step, on every hook, to one of three runtimes based on `type`. All three implement the same contract: take an `Envelope`, return a `Directive`, bounded by `timeout_ms`.

### `url`

A step POSTs the serialized envelope (as JSON) to the configured `url` and reads the response body back as a `Directive`. Any non-2xx status, a transport error, or a body that doesn't parse as a `Directive` is a step error. This is the default and the simplest runtime: any HTTP server in any language can be a step.

```json
POST http://127.0.0.1:9001/run
Content-Type: application/json

{ "envelope_version": 1, "hook": "on_request", ... }
```

```json
{ "action": "continue", "ops": [
  { "op": "set_header", "name": "x-tag", "value": "seen" }
] }
```

### `script`

Gated behind `[gateway] allow_scripts = true`; a `type = "script"` step in a config that leaves this `false` fails to load. `cmd` is the argv to spawn (`cmd[0]` plus its arguments).

**`script_mode = "oneshot"`** (the default): a fresh subprocess is spawned per call. The envelope's JSON is written to its stdin and stdin is closed (so the child sees EOF), while stdout/stderr are read concurrently (write and read run as one `tokio::join!`, not sequentially, since a write larger than the OS pipe buffer would otherwise deadlock against a child that writes its own output before draining stdin). Stdout is parsed as a `Directive`. Non-empty stderr is logged but never fails the step on its own. The whole exchange is bounded by `timeout_ms`; on timeout the child is killed and `StepError::Timeout` is returned.

```python
#!/usr/bin/env python3
import sys, json
env = json.load(sys.stdin)
json.dump({"action": "continue", "ops": [
    {"op": "set_header", "name": "x-tag", "value": "seen"}
]}, sys.stdout)
```

**`script_mode = "worker"`**: a single long-lived subprocess the gateway talks to repeatedly, one length-prefixed frame per call, instead of paying a spawn per call. This is what makes it possible to run a script on `on_stream` mutate, where a directive is needed per chunk.

Wire protocol, per call, over the worker's stdin/stdout:

- Request: a `u32` little-endian byte count, then that many bytes of envelope JSON, written to the child's stdin and flushed.
- Response: a `u32` little-endian byte count, then that many bytes of directive JSON, read back from the child's stdout with `read_exact` (so a response split across several OS reads is reassembled correctly).

A minimal Python worker loop:

```python
#!/usr/bin/env python3
import sys, struct, json

while True:
    hdr = sys.stdin.buffer.read(4)
    if len(hdr) < 4:
        break  # gateway closed the pipe
    n = struct.unpack('<I', hdr)[0]
    envelope = json.loads(sys.stdin.buffer.read(n))

    directive = {"action": "continue", "ops": [
        {"op": "set_header", "name": "x-worker", "value": "seen"}
    ]}

    body = json.dumps(directive).encode()
    sys.stdout.buffer.write(struct.pack('<I', len(body)))
    sys.stdout.buffer.write(body)
    sys.stdout.buffer.flush()
```

The response frame's declared length is checked against a 64 MiB hard cap before the gateway allocates a buffer for it, so a worker that writes a garbage or wrong-endian length prefix fails fast as a `StepError` instead of driving a multi-gigabyte allocation.

Worker lifecycle on `on_request`/`on_response`: workers are spawned at most once per distinct `cmd` argv and cached, shared across every request that hits a step with that same argv. If a call fails because the worker genuinely died (broken pipe, EOF, a short read, an oversized length prefix), the dead entry is evicted and the worker is respawned once, retrying that one call a single time; a second failure surfaces as a `StepError` handled by `on_error`. A timeout does not trigger a respawn-and-retry within the same request (that would pay a second full timeout budget); the child is killed, the cached entry is evicted so the *next* call spawns fresh, and the timeout is surfaced immediately. A worker that is alive but replies with malformed JSON is never killed or evicted for that alone: the decode error is just handed to `on_error`, keeping the process (and its state) alive.

Worker lifecycle on `on_stream` mutate is different: each stream gets its own dedicated worker process, spawned at stream start (not the shared cache used by `on_request`/`on_response`), so two concurrent client streams never interleave a stateful worker's per-stream state. If the worker fails to spawn at stream start, `on_error = "fail_open"` drops that step from the stream's chain (a no-op for the whole stream); `on_error = "fail_closed"` fails the whole response with 502 before any bytes are sent.

### `wasm`

A `type = "wasm"` step is a compiled `wasmtime` module, called per invocation over a small ptr/len ABI. The guest module must export:

- `memory`: the linear memory the host writes the envelope into and reads the directive back out of.
- `alloc(size: i32) -> i32`: returns a guest pointer to at least `size` bytes of scratch space.
- `run(ptr: i32, len: i32) -> i64`: given the pointer/length of the envelope JSON the host just wrote, returns a packed `(out_ptr as i64) << 32 | (out_len as i64)` describing where the guest wrote its directive JSON response.

Per call: the host allocates space in the guest via `alloc`, writes the serialized envelope into it, calls `run`, unpacks `out_ptr`/`out_len` from the returned `i64`, and reads that region back out as the directive JSON. `out_ptr`/`out_len` are entirely guest-controlled, so the host validates they fit within the guest's current memory before reading (and before allocating a host-side buffer for them); an out-of-bounds or absurd `(ptr, len)` is a `StepError::WasmAbi`, not a panic or an unbounded allocation.

Two resource limits apply to every call:

- **Memory cap**: a `wasmtime::StoreLimits` caps the guest's linear memory growth at 64 MiB. A guest that keeps calling `memory.grow` past that point gets growth failures inside the guest, not an unbounded host allocation.
- **Epoch timeout**: each `WasmStep` (one per distinct compiled module path, cached and shared) runs one dedicated background ticker thread that increments the `wasmtime::Engine`'s epoch every 10ms for the module's whole lifetime. Each call sets its own epoch deadline, relative to the engine's epoch at the moment that call's `Store` is created, to `ceil(timeout_ms / 10ms)` ticks (at least 1). A guest still running once its own deadline's tick count has elapsed is trapped from the inside with `Trap::Interrupt`, mapped to `StepError::WasmTimeout`. Deadlines are per call and relative, not shared, so two concurrent calls on the same cached module with different timeouts each time out purely on their own budget, never on each other's.

Compilation (the expensive part) happens once per distinct `wasm` module path, the first time any request exercises it, and the compiled `Engine`/`Module` are cached and reused; only a fresh `Store`/`Instance` is created per call. Editing the `.wasm` file at an already-cached path has no effect until the gateway restarts.

## Examples

A `url` step's directive, tagging a request:

```json
{ "action": "continue", "ops": [
  { "op": "set_header", "name": "x-tag", "value": "seen" }
] }
```

A Python oneshot script step, reading the envelope from stdin and answering on stdout:

```python
#!/usr/bin/env python3
import sys, json
env = json.load(sys.stdin)
json.dump({"action": "continue", "ops": [
    {"op": "set_header", "name": "x-tag", "value": "seen"}
]}, sys.stdout)
```

A worker's read/reply loop, framed with a `u32-le` length prefix each direction:

```python
#!/usr/bin/env python3
import sys, struct, json

while True:
    hdr = sys.stdin.buffer.read(4)
    if len(hdr) < 4:
        break
    n = struct.unpack('<I', hdr)[0]
    envelope = json.loads(sys.stdin.buffer.read(n))

    # ... decide a directive from envelope ...
    directive = {"action": "continue", "ops": []}

    body = json.dumps(directive).encode()
    sys.stdout.buffer.write(struct.pack('<I', len(body)))
    sys.stdout.buffer.write(body)
    sys.stdout.buffer.flush()
```
