# Sluice

**One control point between your agents and the LLM APIs they call.** Budgets, guardrails, caching, cost metering, and cross-provider routing live in Sluice as an ordered chain of steps in a TOML file. No SDK, no code change in any agent. Turn a control on or off by editing one line.

```text
agent ──▶ sluice [ on_request ▸ upstream ▸ on_response | on_stream ] ──▶ Anthropic · OpenAI · Google
```

Your agents keep calling their provider the way they always have. Sluice sits in the path and runs your steps on the request going out and the response coming back. Traffic that is not an LLM call passes straight through as a plain byte pipe, so you pay only for what you turn on.

## A config at a glance

A real `sluice.toml`: proxy Anthropic behind a guardrail, and let OpenAI clients reach Claude through translation. This validates with `sluice check`.

```toml
[gateway]
listen              = "127.0.0.1:8080"   # where your agents connect
admin_listen        = "127.0.0.1:9090"   # /healthz, /readyz, /metrics
max_inflight        = 512                 # shed load with a 503 past this many concurrent requests
upstream_timeout_ms = 30000

# Proxy Anthropic, and vet every request with a guardrail first.
[[route]]
id       = "claude"
upstream = "https://api.anthropic.com"

  [route.adapter]
  ingress = "anthropic"            # parse the body into the provider-agnostic llm view

  [[route.step]]
  name         = "policy-check"
  type         = "url"
  url          = "http://127.0.0.1:9001/guardrail"
  is_guardrail = true              # must fail closed; the gateway enforces it
  timeout_ms   = 500

# Let OpenAI clients reach Claude, translated in both directions.
[[route]]
id       = "gpt"
upstream = "https://api.anthropic.com"

  [route.translate]
  from = "openai"
  to   = "anthropic"
```

## Install

Linux and macOS, x86_64 and arm64:

```bash
curl -fsSL https://raw.githubusercontent.com/itsJeremyMax/sluice/main/install.sh | bash
```

This fetches the latest release, verifies its SHA-256 checksum, and installs the `sluice` binary to `/usr/local/bin` (or `~/.local/bin` when that is not writable). Re-run the same command any time to update; it is a no-op when you already have the current version.

Pin a version, pick a directory, or remove:

```bash
curl -fsSL https://raw.githubusercontent.com/itsJeremyMax/sluice/main/install.sh | bash -s -- --version v0.1.0
curl -fsSL https://raw.githubusercontent.com/itsJeremyMax/sluice/main/install.sh | bash -s -- --bin-dir "$HOME/.local/bin"
curl -fsSL https://raw.githubusercontent.com/itsJeremyMax/sluice/main/install.sh | bash -s -- --uninstall
```

Prefer not to pipe to a shell? Grab a prebuilt binary from the [releases page](https://github.com/itsJeremyMax/sluice/releases), or build from source with `cargo build --release` (the binary lands at `target/release/sluice`).

## A tour in five configs

Sluice is configured, not coded. Here is the feature set as the TOML that switches each piece on. Start the gateway with `sluice serve --config sluice.toml`; validate any config first with `sluice check`.

**1. Start dumb: a transparent proxy.** Two lines is a working gateway.

```toml
[[route]]
id       = "claude"
upstream = "https://api.anthropic.com"
```

Requests to `http://127.0.0.1:8080/claude/v1/messages` now forward to `https://api.anthropic.com/v1/messages` and stream straight back. No step configured means no overhead.

**2. Block bad requests with a guardrail.** Add a step that points at a small HTTP service. It sees every request and can reject it before it leaves your network.

```toml
  [[route.step]]
  name         = "policy-check"
  type         = "url"
  url          = "http://127.0.0.1:9001/guardrail"
  is_guardrail = true          # a guardrail must fail closed; the gateway enforces it
```

The service receives the request as JSON and answers `continue`, `short_circuit`, or `abort`. Because a guardrail cannot be set to fail open, a crash in it blocks the request instead of leaking it through.

**3. Let OpenAI clients talk to Claude.** One block rewrites the request into the target provider's wire format on the way out, and translates the response back on the way in.

```toml
  [route.translate]
  from = "openai"        # what your clients speak
  to   = "anthropic"     # what the upstream speaks
```

Both directions run through one canonical shape, not a pile of per-pair shims. A field with no equivalent on the target is recorded in a translation report, never silently invented. Your clients only ever see OpenAI; the upstream only ever sees Anthropic.

**4. Cap the blast radius.** A looping agent should not be able to drain your quota or your memory.

```toml
[gateway]
max_inflight        = 256       # shed load with a clean 503 past this many concurrent requests
max_body_bytes      = 1048576   # reject bodies over 1 MiB ...
oversize            = "reject"  # ... or set "stream_through" to hand them to the client unbuffered
upstream_timeout_ms = 30000
```

**5. Rewrite a live stream, chunk by chunk.** A long-lived worker process mutates streamed output in place, with no fresh spawn per chunk.

```toml
[gateway]
allow_scripts = true            # script steps are opt-in

  [[route.step]]
  name        = "redactor"
  type        = "script"
  hook        = "on_stream"
  script_mode = "worker"        # one process, kept alive across chunks
  chunk_mode  = "mutate"        # may rewrite or drop each chunk
  cmd         = ["./redact.py"]
```

That is the shape of every feature: a few keys, checked at load, reversible by deleting them.

## Everything you can turn on

| Capability | Switch it on with | Reference |
|---|---|---|
| Transparent proxy | `[[route]]` id + upstream | this page |
| Request / response / stream steps | `[[route.step]] hook = ...` | [steps](docs/steps-and-directives.md) |
| Guardrails (fail-closed) | `is_guardrail = true` | [steps](docs/steps-and-directives.md) |
| Cross-provider translation | `[route.translate]` | [translation](docs/translation.md) |
| Provider-agnostic `llm` view | `[route.adapter] ingress = ...` | [config](docs/configuration.md) |
| Model registry (context, pricing, tools) | `sluice models list / diff / update` | [operations](docs/observability-and-operations.md) |
| Load-survival limits | `max_inflight`, `max_body_bytes`, `oversize`, `upstream_timeout_ms` | [config](docs/configuration.md) |
| Script steps (oneshot + worker) | `type = "script"` | [steps](docs/steps-and-directives.md) |
| Sandboxed WASM steps | `type = "wasm"` | [steps](docs/steps-and-directives.md) |
| Loopback for legacy tools | `mode = "loopback"` | [operations](docs/observability-and-operations.md) |
| Metrics and admin endpoints | `admin_listen`, `/metrics` | [operations](docs/observability-and-operations.md) |
| Hot reload | edit the file | [operations](docs/observability-and-operations.md) |
| Multi-listener split | `gateways.d/` | [config](docs/configuration.md) |

Everything above is checked at load. `sluice check` runs the exact validation `serve` runs at startup, so a bad config fails in CI instead of at deploy.

## Why not a plain HTTP proxy

A generic proxy sees bytes and paths. An agent gateway has to understand messages, tokens, models, and tool calls to be useful:

- Token budgets and rate limits enforced before the request leaves your network.
- Cost attribution per call, computed from the model and token counts.
- Guardrails that read the actual messages and tool calls, not just headers.
- One budget or guardrail that runs unchanged across Anthropic, OpenAI, and Google, because the request is normalized first.
- Model-aware routing driven by each model's real limits and pricing.

Sluice adds that understanding as optional steps. An LLM call is parsed into a provider-agnostic `llm` view (messages, tools, model, streaming flag) enriched with registry facts, so a step makes cross-provider decisions without caring which wire format it was handed.

## One file holds the order

Most pipelines are a linked list: each proxy hardcodes the next hop as its upstream, so the ordering is smeared across every tool, and pulling one out of the middle means rewiring its neighbor.

Sluice is a star. The gateway is the only node that forwards, and it holds the ordered list of steps. A step receives a request, changes it, and hands control back. It never learns who runs before or after it. That one rule, steps return rather than forward, is what lets you reorder or delete a step by editing a single file.

## How a request flows

1. A client calls `POST http://127.0.0.1:8080/claude/v1/messages`.
2. The first path segment (`claude`) selects the route; the rest (`/v1/messages`) is appended to that route's upstream.
3. The gateway wraps the request in an envelope (a JSON view) and hands it to each `on_request` step in order.
4. Each step returns a directive: continue with edits, answer immediately, or abort.
5. The gateway rebuilds a clean HTTP request from the edited envelope, forwards it, and streams the response back.
6. `on_response` steps see the full buffered response; `on_stream` steps see each chunk as it arrives.

The client and the upstream only ever see ordinary HTTP. The envelope exists on exactly one hop, gateway to step, and nowhere else.

## The envelope and the directive

Every step, in any language, receives the same shape and answers with a directive.

```json
{
  "hook": "on_request",
  "route_id": "claude",
  "self": "tagger",
  "request": { "method": "POST", "path": "/claude/v1/messages",
               "headers": { "content-type": "application/json" }, "body_b64": "..." },
  "response": null, "chunk": null, "llm": null, "context": {}
}
```

`response` fills in on the response hook, `chunk` on the stream hook, and `llm` once an adapter recognizes the call. A step with nothing to do with LLM traffic ignores all three. `self` is the step's own name, and the only key it may write under in the shared `context` map.

Edits are explicit ops, so leaving a field out means "do not touch it":

```jsonc
{ "action": "continue", "ops": [
    { "op": "set_header",    "name": "x-tag", "value": "seen" },
    { "op": "delete_header", "name": "x-internal" },
    { "op": "set_body",      "body_b64": "..." },
    { "op": "set_context",   "value": { "counted": true } }
] }

{ "action": "short_circuit", "response": { "status": 200, "headers": {}, "body_b64": "..." } }
{ "action": "abort",         "response": { "status": 403, "headers": {}, "body_b64": "..." } }

// on_stream mutate steps only
{ "action": "emit", "chunk": { "data_b64": "..." } }
{ "action": "drop" }
```

A whole step can be this small:

```python
#!/usr/bin/env python3
import sys, json
env = json.load(sys.stdin)
json.dump({"action": "continue", "ops": [
    {"op": "set_header", "name": "x-tag", "value": "seen"}
]}, sys.stdout)
```

The full contract (every op, chunk framing, worker framing, the WASM ABI) is in [steps and directives](docs/steps-and-directives.md).

## CLI

```
sluice serve          --config <file>   Run the gateway (the default command).
sluice check          --config <file>   Validate a config and exit non-zero on any error.
sluice routes         --config <file>   Print the effective, merged config: gateway, routes, steps.
sluice test <route>   --config <file>   Send a synthetic probe through a route's on_request chain and
                                         report each step's directive and timing, without live traffic.
sluice models list                      Print the currently loaded model registry.
sluice models diff     --source <dir>   Show what a models.dev-shaped source directory would change.
sluice models diff     --from-network   Same, resolved from a live fetch of models.dev instead.
sluice models update   --source <dir>   Resolve a models.dev-shaped source directory and write it as
                                         the local registry.
sluice models update   --from-network   Same, resolved from a live fetch of models.dev instead.
sluice token verify    --secret <s> <t> Decode and verify a signed x-chain-token (loopback), offline.
```

Every subcommand accepts `--config-dir <dir>` in place of `--config <file>`. `models update` / `models diff` take exactly one of `--source` or `--from-network`; the refresh is always an explicit step you run yourself, and nothing on the request path fetches models.dev.

## Honest limits

Some of this is narrower than it sounds, on purpose:

- **Translation** covers Anthropic, OpenAI, and Google, buffered and streaming, but a translate route cannot also run `on_stream` steps, so per-chunk work lives on `on_request` / `on_response`. Field fidelity is best-effort and reported, never invented.
- **The tokenizer** is approximate. Treat token counts, and the cost math built on them, as estimates rather than billing-grade numbers.
- **The model registry** ships a small embedded seed and refreshes only when you run `sluice models update`. Pricing is advisory, and nothing on the request path ever fetches it live.
- **Loopback** is experimental and off by default. It activates only once you set a non-empty `loopback_secret`, and it is not hardened. Don't point it at anything you don't fully trust.

Do not run this in front of production traffic without reading [the docs](docs/) and understanding what you turned on.

## Documentation

Detailed reference docs live in [docs/](docs/), and runnable annotated configs in [examples/](examples/):

- [Configuration](docs/configuration.md): every gateway, route, and step field, and every validation rule.
- [Steps and directives](docs/steps-and-directives.md): the envelope, the directive contract, and the url, script, and wasm runtimes.
- [Translation](docs/translation.md): cross-provider translation, the fidelity model, and its limits.
- [Observability and operations](docs/observability-and-operations.md): metrics, the admin listener, hot reload, the model registry, and loopback.
- [CLI](docs/cli.md): every subcommand and flag.

Each file in [examples/](examples/) is validated with `sluice check`. See [examples/README.md](examples/README.md) for what each one shows.

## Build and test

```bash
cargo build
cargo test          # unit tests plus the end-to-end integration suite
cargo clippy --all-targets -- -D warnings
cargo fmt --all --check
```

The integration tests spin up a real gateway on an ephemeral port against a mock upstream (or a mock step service), so they exercise the actual request path rather than mocking it away.

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE).
