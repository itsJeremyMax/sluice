# Sluice

Sluice is a self-hosted gateway that sits between your agents and the LLM APIs they call. It gives you one place to enforce budgets, run guardrails, cache, meter cost, and route across providers, without changing a line in any agent. You declare the controls as an ordered chain of steps in a single config file (or a directory of them). Add or remove one by editing that file, and nothing else changes.

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

Prefer not to pipe to a shell? Download a prebuilt binary for your platform from the [releases page](https://github.com/itsJeremyMax/sluice/releases), or build from source with `cargo build --release` (the binary lands at `target/release/sluice`).

## Why an agent needs a gateway

An agent that calls an LLM API directly has no brakes. Every call can overspend, leak a prompt, or fail when a provider goes down, and the code to prevent that tends to get copied into each tool. Sluice moves it to one control point you own and can change without redeploying anything:

- Token budgets and rate limits enforced before the request leaves your network, so a looping agent cannot drain your quota.
- Cost attribution per call, computed from the model and token counts, written where your billing can read it.
- Prompt and output guardrails that inspect the actual messages and tool calls, not just HTTP headers.
- Semantic caching keyed on what a prompt means rather than a byte-identical URL.
- One guardrail or budget that runs unchanged across Anthropic, OpenAI, and Google, because the request is normalized to a common shape first.
- Model-aware routing and fallback driven by each model's real limits and pricing.

A generic HTTP proxy cannot do any of this. It sees bytes and paths, while an agent gateway has to understand messages, tokens, models, and tool calls. Sluice adds that understanding as optional steps in the chain. Traffic that is not an LLM call still passes straight through as a plain byte pipe, so you pay only for what you turn on.

## Features

The gateway provides the capabilities below, each covered by an automated test suite:

**Routing and the chain.** First-segment routing from a single-file or directory-based config to an upstream, with an ordered per-route chain of steps and the full patch-directive contract (`continue`, `short_circuit`, `abort`, plus, on streams, `emit`/`drop`). One reconstruction/transparency boundary: the envelope exists on exactly one hop, gateway to step, and the client/upstream only ever see ordinary HTTP.

**Multi-step context.** Steps read and write a shared `context` map under their own namespace, so a later step in the chain can see what an earlier one decided.

**Load-survival limits.** Request and response body-size caps, a per-event cap on streamed SSE frames, upstream timeouts, and a concurrency ceiling (`max_inflight`) that sheds load with a clean 503 instead of queuing without bound. A response that outgrows its cap is either rejected or, if you opt into `oversize = "stream_through"`, handed to the client unbuffered rather than held in memory.

**Observability.** Structured JSON logs, a correlation id threaded through every step's span, Prometheus metrics, and a separate admin listener with `/healthz`, `/readyz`, and `/metrics`, optionally protected by a bearer token.

**Config.** A single TOML file, or a directory (`gateway.toml` + `routes.d/*.toml` + optional `gateways.d/*.toml`), with hot reload on filesystem change.

**The model registry.** A models.dev-derived table of per-model facts (context window, advisory pricing, tool-call support), inspectable and refreshable via `sluice models list/diff/update`, either from a local source directory or a live fetch of the models.dev dataset.

**The AI-native layer.** An ingress adapter for each of Anthropic, OpenAI, and Google parses the wire body into a provider-agnostic `llm` view (messages, tools, model, streaming flag) attached to the envelope, enriched with the model's registry facts. An approximate tokenizer and cost math give a step enough to work with. Guardrail and budget steps read this `llm` view to make cross-provider decisions without knowing which wire format they were handed.

**Cross-provider translation.** A route can declare `[route.translate] from = "anthropic", to = "openai"` and the gateway rewrites the request into the target provider's wire format on the way to the upstream, then translates the response back into the client's format, buffered or streaming. Both directions go through one canonical intermediate rather than a pile of per-pair shims, so a field that has no equivalent on the target is recorded in a translation report instead of being dropped in silence. The client only ever sees its own dialect, the upstream only ever sees the target dialect, and neither sees the intermediate.

**on_response steps**, which can inspect and rewrite the full buffered upstream response before it reaches the client.

**Streaming.** `on_stream` steps either observe chunks read-only (a tee that never slows the client down) or mutate them in place (gating what the client actually receives), over normalized deltas so a step doesn't need to parse each provider's own SSE shape.

**The script runtime.** A `type = "script"` step spawns a subprocess per invocation and talks to it over stdin/stdout, gated behind `allow_scripts = true` so it's opt-in. A `script_mode = "worker"` variant keeps one long-lived process alive across calls over a length-prefixed framing, which is what lets a script mutate a live stream (`on_stream` mutate) without paying a fresh spawn per chunk.

**The WASM runtime.** A `type = "wasm"` step runs a sandboxed, wasmtime-compiled guest module over a small ptr/len ABI, with a memory cap and epoch-based timeout so a runaway or malicious module can't take down the gateway.

**Loopback**, for legacy tools that can't return a directive inline: the gateway parks the chain, calls the tool, and resumes from a signed callback later.

**Multi-listener gateways**, so one process can expose different subsets of routes on different addresses via `gateways.d`.

## Honest limits

Some of the above is narrower than it sounds, on purpose:

- Cross-provider translation is real but narrower than the read-side layer around it. It covers Anthropic, OpenAI, and Google, buffered and streaming, but a translate route cannot also run `on_stream` steps, so guardrails and per-chunk mutation on a translated route sit on the `on_request`/`on_response` hooks instead. A translated response that outgrows `max_response_bytes` is rejected rather than streamed through untranslated, since streaming it raw would hand the client the wrong dialect. Field-level fidelity is best-effort: anything without an equivalent on the target provider is recorded in the translation report, not silently invented.
- The tokenizer is approximate, not an exact reimplementation of any provider's real tokenizer. Treat token counts and the cost math built on them as estimates, not billing-grade numbers.
- The model registry ships a small embedded seed and is refreshed explicitly via `sluice models update`, either against a local models.dev-shaped source directory (`--source`) or a live fetch of the models.dev dataset (`--from-network`). Either way it's a manual, out-of-band step you run yourself. Pricing is advisory, and nothing on the request path ever fetches it live.
- Loopback is EXPERIMENTAL and OFF BY DEFAULT. It only activates once you set a non-empty `loopback_secret`, and it is not hardened. Don't point it at anything you don't fully trust.

Do not run this in front of production traffic without reading the config section below and understanding what you've turned on.

## Why the order lives in one file

Most request pipelines are built as a linked list. Each proxy hardcodes the next hop's URL as its upstream, so the ordering is spread across every tool. Pulling one out of the middle means rewiring its predecessor, and forgetting to do so leaves a pointer aimed at nothing.

Sluice uses a star instead of a list. The gateway is the only node that forwards. It holds the ordered list of steps. A step receives a request, changes it, and hands control back to the gateway. It never learns who runs before or after it. That one rule, steps return rather than forward, is what lets you reorder or delete a step by editing a single file.

## How a request flows

1. A client calls the gateway, for example `POST http://127.0.0.1:8080/claude/v1/messages`.
2. The first path segment (`claude`) selects a route. The rest (`/v1/messages`) is appended to that route's upstream.
3. The gateway builds an envelope (a JSON view of the request) and hands it to each `on_request` step in order.
4. Each step returns a directive: keep going with a set of edits, answer immediately with a canned response, or abort.
5. The gateway reconstructs a clean HTTP request from the (possibly edited) envelope, forwards it to the upstream, and streams the response back to the client.
6. If the route has `on_response` steps, the full response is buffered and passed through them the same way before it reaches the client. If it has `on_stream` steps instead, each chunk of a streamed response passes through them as it arrives.

The client and the upstream only ever see ordinary HTTP. The envelope exists on exactly one hop, gateway to step, and nowhere else.

## Quick start

You need a recent stable Rust toolchain.

```bash
git clone <your-fork-url> sluice
cd sluice
cargo build --release
```

Write a config file, `sluice.toml`:

```toml
[[route]]
id       = "claude"
upstream = "https://api.anthropic.com"
```

Check that it is valid, then run the gateway:

```bash
cargo run -- check --config sluice.toml
cargo run -- serve --config sluice.toml
```

With that config, a request to `http://127.0.0.1:8080/claude/v1/messages` is forwarded to `https://api.anthropic.com/v1/messages` and the response streams straight back. No step is configured, so the gateway is a transparent pass-through.

## Adding a step

A step is a small HTTP service. The gateway POSTs it the envelope and reads back a directive. Point a route at one like this:

```toml
[[route]]
id       = "claude"
upstream = "https://api.anthropic.com"

  [[route.step]]
  name = "tagger"
  type = "url"
  url  = "http://127.0.0.1:9001/run"
```

Your service receives the envelope as JSON and returns a directive. Here is a step that tags every request with a header:

```json
{
  "action": "continue",
  "ops": [
    { "op": "set_header", "name": "x-tag", "value": "seen" }
  ]
}
```

The gateway applies the ops in order and forwards the tagged request upstream. Delete the `[[route.step]]` block and the route goes back to a plain pass-through. Nothing else in the file changes.

## The envelope

Every step receives the same shape, whatever language it is written in:

```json
{
  "envelope_version": 1,
  "hook": "on_request",
  "route_id": "claude",
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

`self` is the step's own name. It is how a step writes into `context`, which it can only touch under its own key. `response` is filled in on the response hook, `chunk` on the stream hook, and `llm` once a provider adapter recognizes the call and the route names it in `[route.adapter]`. A step that has nothing to do with LLM traffic can ignore all three and still work.

## The directive

A step answers with one of a small set of actions. Edits are explicit ops, so leaving a field out means "do not touch it," and removing a header takes a real `delete_header` op rather than dropping it from a map.

```jsonc
// keep going, with edits
{ "action": "continue", "ops": [
    { "op": "set_header",    "name": "x-tag", "value": "seen" },
    { "op": "delete_header", "name": "x-internal" },
    { "op": "set_body",      "body_b64": "..." },
    { "op": "set_path",      "path": "/claude/v2/messages" },
    { "op": "set_context",   "value": { "counted": true } }
] }

// answer now, skip the upstream and any later steps (on_request only)
{ "action": "short_circuit", "response": { "status": 200, "headers": {}, "body_b64": "..." } }

// stop with an error status
{ "action": "abort", "response": { "status": 403, "headers": {}, "body_b64": "..." } }

// on_stream mutate steps only: rewrite this chunk, or swallow it
{ "action": "emit", "chunk": { "data_b64": "..." } }
{ "action": "drop" }
```

Steps may not set the framing headers (`Connection`, `Transfer-Encoding`, `Content-Length`, `Keep-Alive`, `Upgrade`, `TE`); the gateway owns those and recomputes them when it rebuilds the outbound message.

A step written in Python is just as short:

```python
#!/usr/bin/env python3
import sys, json
env = json.load(sys.stdin)
json.dump({"action": "continue", "ops": [
    {"op": "set_header", "name": "x-tag", "value": "seen"}
]}, sys.stdout)
```

## Configuration

Config starts as one file and grows without a rewrite, or moves to a directory (`--config-dir`) once you have enough routes to want one file per route. A minimal route needs only an id and an upstream; everything else has a default. See `sluice routes` for the exact effective config a given file or directory resolves to, and [docs/configuration.md](docs/configuration.md) for the full field list (steps also support `hook`, `chunk_mode`, `is_guardrail`, `script_mode`/`cmd`, and `wasm`, on top of what's shown here).

```toml
[gateway]
schema_version = 1                 # required; an unknown major version is rejected at load
listen         = "127.0.0.1:8080"  # default shown

[[route]]
id       = "claude"                       # required; the first path segment; unique across the config
upstream = "https://api.anthropic.com"    # required

  [[route.step]]
  name       = "tagger"           # optional; defaults to "<type>:<index>"; unique within the route
  hook       = "on_request"       # default shown; on_response and on_stream are also wired
  type       = "url"              # url, script (needs allow_scripts = true), or wasm
  url        = "http://127.0.0.1:9001/run"   # required for type = url
  timeout_ms = 1000               # default shown
  on_error   = "fail_closed"      # default; fail_closed aborts on step error, fail_open proceeds
```

Validation happens once, at load time, not per request. `sluice check` runs the exact checks the server runs on startup, so a bad config fails in CI instead of at deploy. Route ids must be unique across the whole config, step names must be unique within a route, and unsupported combinations are rejected up front.

## CLI

```
sluice serve          --config <file>   Run the gateway (the default command).
sluice check          --config <file>   Validate a config and exit non-zero on any error.
sluice routes         --config <file>   Print the effective, merged config: gateway settings, routes, steps.
sluice test <route>   --config <file>   Send a synthetic probe through a route's on_request chain and report
                                         each step's directive and timing, without touching live traffic.
sluice models list                      Print the currently loaded model registry.
sluice models diff     --source <dir>   Show what a models.dev-shaped source directory would change.
sluice models diff     --from-network   Same, but resolved from a live fetch of models.dev instead.
sluice models update   --source <dir>   Resolve a models.dev-shaped source directory and write it as the
                                         local registry.
sluice models update   --from-network   Same, but resolved from a live fetch of models.dev instead.
sluice token verify    --secret <s> <t> Decode and verify a signed x-chain-token (loopback), offline.
```

Every subcommand above also accepts `--config-dir <dir>` instead of `--config <file>`.

`models update`/`models diff` take exactly one of `--source <dir>` (a local models.dev-shaped
checkout) or `--from-network` (a live fetch of the models.dev dataset), never both and never
neither. Either way the refresh is an explicit, opt-in step you run yourself; nothing on the
request path ever fetches models.dev.

## Documentation

Detailed reference docs live in [docs/](docs/), and runnable annotated configs in [examples/](examples/):

- [Configuration](docs/configuration.md): every gateway, route, and step field, and every validation rule.
- [Steps and directives](docs/steps-and-directives.md): the envelope, the directive contract, and the url, script, and wasm runtimes.
- [Translation](docs/translation.md): cross-provider translation, the fidelity model, and its current limits.
- [Observability and operations](docs/observability-and-operations.md): metrics, the admin listener, hot reload, the model registry, and loopback.
- [CLI](docs/cli.md): every subcommand and flag.

Each file in [examples/](examples/) is validated with `sluice check`. See [examples/README.md](examples/README.md) for what each one shows.

## Building and testing

```bash
cargo build
cargo test          # unit tests plus the end-to-end integration suite
cargo clippy --all-targets -- -D warnings
cargo fmt --all --check
```

The integration tests spin up a real gateway on an ephemeral port and a mock upstream (or a mock step service), so they exercise the actual request path rather than mocking it away.

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE).
