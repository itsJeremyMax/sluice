# Configuration

This is the field-by-field reference for sluice's config: the `[gateway]` block, `[[route]]` blocks, and `[[route.step]]` blocks, plus everything `sluice check` rejects at load. For the overview and a quick start, see the top-level [README](../README.md).

## The two config shapes

Sluice loads config one of two ways:

- **A single file**, passed with `--config <file>` (default `sluice.toml`). It holds one `[gateway]` table and any number of `[[route]]` blocks.
- **A directory**, passed with `--config-dir <dir>`. The gateway reads:
  - `<dir>/gateway.toml`, optional, supplying the `[gateway]` table. If absent, `Gateway::default()` is used.
  - `<dir>/routes.d/*.toml`, each contributing `[[route]]` blocks. Files are read in lexical order by filename and their routes are concatenated in that order.
  - `<dir>/gateways.d/*.toml`, optional, each defining one named listener (see "Multi-listener gateways" below).

Every field in every one of these files is checked with `deny_unknown_fields`: a typo'd key fails to parse rather than being silently ignored.

A route id must be unique across the whole merged config. If two `routes.d` files define the same route id, the error names both files. A malformed or unparseable file anywhere in the directory aborts the whole load; there is no partial config.

`--config` and `--config-dir` are mutually exclusive on every subcommand that takes config.

### Multi-listener gateways (`gateways.d`)

Each `<dir>/gateways.d/<name>.toml` file defines one named listener that serves a subset of the configured routes. The gateway's name is the filename stem, not a field in the file. A file has two keys:

```toml
listen = "127.0.0.1:9001"
routes = ["claude", "gpt"]
```

`listen` is the address this named gateway binds. `routes` names the route ids (defined in `routes.d`) it exposes; every id must resolve to a real route. When `gateways.d` is used, the top-level `[gateway].listen` is not bound as its own listener; each `GatewayDef.listen` is what actually gets bound instead. Gateway names must be unique, and no two gateways may share a `listen` address, and a gateway's `listen` must not collide with `admin_listen`.

## `[gateway]`

All fields are optional; a bare `[[route]]` with no `[gateway]` table at all is valid and uses every default below. Types and defaults are verified against `src/config/mod.rs`.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `schema_version` | integer | `1` | Config schema version. Any value other than `1` is rejected at load; this build only supports version 1. |
| `listen` | string | `"127.0.0.1:8080"` | Address the main gateway listener binds to. Ignored in favor of `gateways.d`'s per-gateway `listen` values once any `gateways.d/*.toml` file exists. |
| `admin_listen` | string | `""` (empty) | Address the admin listener (`/healthz`, `/readyz`, `/metrics`) binds to. An empty string disables the admin listener entirely. Must differ from `listen`. |
| `admin_token` | string | `""` (empty) | Bearer token required on admin endpoints. An empty string means no auth is enforced, which only makes sense when `admin_listen` is also unset. |
| `max_body_bytes` | integer (bytes) | `8388608` (8 MiB) | Maximum size of a request or response body the gateway will buffer. Bodies over this are handled per `oversize`. |
| `max_response_bytes` | integer (bytes), optional | unset | Maximum size of a response body on the buffered response path (routes with `on_response` steps, or translate routes). Unset falls back to `max_body_bytes`, not to "unbounded." `0` is rejected at load. Read the resolved value with `Gateway::max_response_bytes()`. |
| `max_event_bytes` | integer (bytes), optional | unset | Maximum size of a single buffered, not-yet-terminated SSE event before the stream framer gives up on it. Unset falls back to the framer's own built-in ceiling (1 MiB). `0` is rejected at load. |
| `max_context_bytes` | integer (bytes) | `65536` (64 KiB) | Maximum size of a single step's own namespaced entry in the shared `context` map. The cap applies per step namespace, not to the map as a whole, so total `context` size can still grow as more steps each write their own capped entry. |
| `max_inflight` | integer | `1024` | Maximum number of concurrently in-flight requests before the gateway sheds load with a 503. `0` is rejected at load. |
| `upstream_timeout_ms` | integer (ms) | `60000` | Timeout for upstream requests. |
| `oversize` | `"reject"` \| `"stream_through"` | `"reject"` | Behavior when a body exceeds `max_body_bytes` (or `max_response_bytes` on the response path). `reject` answers with an error; `stream_through` streams the body to the client unbuffered instead of holding it in memory. |
| `expose_headers` | array of strings | `[]` | Allowlist of internal `x-sluice-*` / `x-chain-token` header names that survive the egress sanitizer toward the client. Everything not listed here is stripped before the response reaches the client. |
| `loopback_secret` | string | `""` (empty) | HMAC secret used to sign and verify the `x-chain-token` used by `mode = "loopback"` url steps. Empty (the default) disables loopback: any `mode = "loopback"` step is rejected at load unless this is non-empty. |
| `max_hops` | integer | `8` | Maximum number of loopback hops a single chain may take before the gateway refuses to resume it further. |
| `callback_path` | string | `"/__sluice/loopback"` | HTTP path the gateway listens on for loopback resume callbacks. |
| `max_parked` | integer | `1024` | Maximum number of loopback continuations that may be concurrently parked before the gateway refuses to park any more and answers with 503. |
| `allow_scripts` | boolean | `false` | Master switch for `type = "script"` steps. With this left `false`, any `type = "script"` step in the config is a validation error, not a silent no-op. |

## `[[route]]`

```toml
[[route]]
id       = "claude"                       # required, unique, no '/'
upstream = "https://api.anthropic.com"    # required
```

| Field | Type | Default | Meaning |
|---|---|---|---|
| `id` | string | required | The route's identifier. It's also the first path segment clients call, so it must be non-empty, must not contain `/`, and must be unique across the whole config. |
| `upstream` | string | required | The base URL requests are forwarded to. Must be non-empty. |
| `[route.adapter]` | table, optional | unset | Names the ingress/egress wire format this route speaks. See below. |
| `[route.translate]` | table, optional | unset | Cross-provider request/response translation for this route. See below. |
| `[[route.step]]` | array of tables | `[]` | The ordered chain of steps for this route. See the next section. |

### `[route.adapter]`

```toml
[route.adapter]
ingress = "anthropic"   # optional: anthropic | openai | google
egress  = "anthropic"   # optional; must equal ingress if set
```

`ingress` and `egress` each name a provider adapter by string (`"anthropic"`, `"openai"`, or `"google"`; unknown names are rejected at load). `ingress` is what parses the inbound request into the provider-agnostic `llm` view attached to the envelope. `egress` must be left unset or set equal to `ingress`; naming a different provider for `egress` is rejected at load. Cross-provider translation is configured with `[route.translate]` instead (see [translation](translation.md)).

### `[route.translate]`

```toml
[route.translate]
from          = "openai"      # required: anthropic | openai | google
to            = "anthropic"   # required; must differ from from
model         = "claude-opus-4-1-20250805"   # optional, must resolve in the model registry under `to`
report_header = false          # default shown
```

`from`/`to` name the source and target provider wire formats this route translates between; unlike `[route.adapter]`, they may differ. `model`, if set, pins the target-provider model id the translated request is sent with. `report_header` toggles a diagnostic response header describing translation fidelity. This is a summary; see [translation](translation.md) for the full mechanism and its limits.

## `[[route.step]]`

```toml
[[route.step]]
name       = "tagger"                      # optional; default "<type>:<index>"
hook       = "on_request"                  # default shown
type       = "url"                         # url | script | wasm
url        = "http://127.0.0.1:9001/run"   # required for type = url
timeout_ms = 1000                          # default shown
on_error   = "fail_closed"                 # default shown
```

| Field | Type | Default | Meaning |
|---|---|---|---|
| `name` | string, optional | `"<type>:<index>"` | The step's identity in the `context` map and in error messages. Must be unique within the route. |
| `hook` | `"on_request"` \| `"on_response"` \| `"on_stream"` | `"on_request"` | Which point in the request lifecycle this step runs at. |
| `type` | `"url"` \| `"script"` \| `"wasm"` | required | The step runtime. `script` requires `allow_scripts = true` in `[gateway]`. |
| `url` | string, optional | unset | Required when `type = "url"`: the HTTP endpoint the gateway POSTs the envelope to. |
| `mode` | `"transform"` \| `"loopback"` | `"transform"` | `type = "url"`-only. `loopback` parks the chain and resumes it later from a signed callback instead of expecting a directive inline; it's only valid on `hook = "on_request"` and requires a non-empty `loopback_secret`. |
| `timeout_ms` | integer (ms) | `1000` | How long the gateway waits for this step before treating it as failed. |
| `on_error` | `"fail_closed"` \| `"fail_open"` | `"fail_closed"` | What happens when the step errors or times out. `fail_closed` aborts the request; `fail_open` proceeds as if the step had returned `continue` with no edits. |
| `chunk_mode` | `"observe"` \| `"mutate"` | `"observe"` | Only meaningful when `hook = "on_stream"`. `observe` is read-only (a tee); `mutate` may rewrite chunks in place. |
| `is_guardrail` | boolean | `false` | Marks this step as a safety guardrail, which tightens validation: it cannot be `on_error = "fail_open"`, and on `hook = "on_stream"` it cannot be `chunk_mode = "observe"`. |
| `script_mode` | `"oneshot"` \| `"worker"` | `"oneshot"` | `type = "script"`-only. `oneshot` spawns a fresh subprocess per invocation. `worker` keeps one long-lived process alive across calls, which is what lets a script mutate a live stream. |
| `cmd` | array of strings, optional | `[]` | `type = "script"`-only: the argv to spawn. Required (non-empty) for script steps. |
| `wasm` | string, optional | unset | `type = "wasm"`-only: path to the compiled `.wasm`/`.wat` module. Required for wasm steps. |

Step semantics (the directive contract, chunk framing, loopback resume, worker framing, the WASM ABI) are covered in [steps and directives](steps-and-directives.md); this section only covers the config shape.

## Validation

`sluice check` runs the exact same `validate()` that `serve` runs at startup, so a bad config fails in CI instead of at deploy. Everything below is rejected at load, before any request is served.

**Gateway-level:**

- `schema_version` other than `1`.
- `max_response_bytes = 0` (would reject every response body; leave it unset instead).
- `max_event_bytes = 0` (would reject every SSE event; leave it unset instead).
- `max_inflight = 0`.
- `admin_listen` non-empty and equal to `listen`.
- A config that defines no `[[route]]` at all.

**Routes:**

- A route with an empty `id`.
- A route `id` containing `/`.
- Two routes sharing the same `id`, anywhere in the config. In a directory config, duplicates across `routes.d` files name both offending files.
- A route with an empty `upstream`.
- `[route.adapter].ingress` or `.egress` naming a provider that isn't `anthropic`, `openai`, or `google`.
- `[route.adapter].egress` set to a provider different from `.ingress` (it must equal `.ingress` or be left unset; use `[route.translate]` for cross-provider translation).
- `[route.translate].from` or `.to` naming an unknown provider.
- `[route.translate].from == .to` (a translate that doesn't translate is a config error).
- `[route.adapter].ingress` and `[route.translate].from` both set but disagreeing: the request-side `llm` view is built from `adapter.ingress`, so a mismatch would silently parse it with the wrong dialect and bypass any `on_request` guardrail relying on it.
- `[route.translate].model` set but not found in the model registry under `translate.to`.
- `[route.translate]` combined with a `mode = "loopback"` step on the same route (out of scope).
- `[route.translate]` combined with an `on_stream` step on the same route: the on_stream pipeline only runs on non-translate routes, so it would load clean and then silently never fire, dropping any guardrail on it.

**Steps:**

- Two steps in the same route sharing an effective `name` (explicit or the `"<type>:<index>"` default).
- `type = "url"` with no `url`.
- `mode = "loopback"` with an empty `loopback_secret`.
- `mode = "loopback"` on any hook other than `on_request`.
- `type = "script"` when `allow_scripts` is not `true`.
- `type = "script"` with an empty `cmd`.
- `type = "script"` whose `cmd[0]` is an explicit path (contains `/` or `\`) that doesn't exist on disk. A bare command name (no separator, e.g. `"jq"`) is not checked here; it's resolved against `PATH` at spawn time instead.
- `type = "script"` on `hook = "on_stream"` with `script_mode = "oneshot"` (the default): a oneshot process has no per-chunk runtime, so it would never actually run.
- `type = "script"` on `hook = "on_stream"` with `script_mode = "worker"` and `chunk_mode` other than `"mutate"`: the on_stream dispatch only calls a worker script step in mutate mode, so an observe worker step would load clean but never be invoked.
- `type = "wasm"` with no `wasm` path.
- `type = "wasm"` on `hook = "on_stream"` (the wasm ABI is request/response only, not streaming).
- `type = "wasm"` whose module fails to compile. The module is compiled at load time with the same loader the proxy uses at request time, so a config that passes this check compiles identically later; the error names both the file and the compiler's own message.
- `is_guardrail = true` on `hook = "on_stream"` with `chunk_mode = "observe"`: an observe-only guardrail can't enforce anything.
- `is_guardrail = true` with `on_error = "fail_open"`: a guardrail must fail closed.
- A route with both an `on_response` step and an `on_stream` step (the two response-side hooks are mutually exclusive on one route).
- A route whose `on_stream` steps mix `chunk_mode = "observe"` and `chunk_mode = "mutate"`: the stream dispatch only ever runs the mutate steps for such a route, so the observe step would load clean but silently never run. All-observe and all-mutate routes are both fine.

**`gateways.d` (directory config only):**

- Two `gateways.d/*.toml` files defining the same gateway name.
- Two gateways sharing the same `listen` address.
- A gateway's `listen` colliding with `[gateway].admin_listen`.
- A gateway file declaring an empty `routes` list.
- A gateway's `routes` referencing a route id that isn't defined anywhere in `routes.d`.

## Examples

A minimal route, defaults for everything else:

```toml
[[route]]
id       = "claude"
upstream = "https://api.anthropic.com"
```

A route with one step:

```toml
[[route]]
id       = "claude"
upstream = "https://api.anthropic.com"

  [[route.step]]
  name       = "tagger"
  hook       = "on_request"
  type       = "url"
  url        = "http://127.0.0.1:9001/run"
  timeout_ms = 1000
  on_error   = "fail_closed"
```

A directory layout:

```
conf.d/
  gateway.toml          # [gateway] table
  routes.d/
    01-claude.toml       # [[route]] id = "claude"
    02-gpt.toml           # [[route]] id = "gpt"
  gateways.d/
    public.toml          # listen = "...", routes = ["claude"]
    internal.toml         # listen = "...", routes = ["gpt"]
```

```bash
sluice check --config-dir conf.d
sluice serve --config-dir conf.d
```

Full, runnable configs live under [`../examples/`](../examples/).
