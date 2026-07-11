# Sluice example configs

Every file here validates against the config loader in this repo. Run the
`sluice check` command shown for each from the repo root, since a couple of
them (the wasm module path) resolve relative to the current working
directory rather than the config file's location. See the top-level
[README.md](../README.md) for the concepts (`route`, `step`, hook,
directive) these configs use.

- **minimal.toml**: the smallest valid config, a single passthrough route
  with no steps.

  ```bash
  sluice check --config examples/minimal.toml
  sluice serve --config examples/minimal.toml
  ```

- **guardrail-step.toml**: an `on_request` guardrail step (`is_guardrail =
  true`, `on_error = "fail_closed"`, `timeout_ms`), plus a `[gateway]` block
  with a separate admin listener and non-default `max_inflight` /
  `upstream_timeout_ms` limits.

  ```bash
  sluice check --config examples/guardrail-step.toml
  sluice serve --config examples/guardrail-step.toml
  ```

- **translation.toml**: a `[route.translate]` route that translates OpenAI
  wire format to Anthropic, with a real registry model pinned as the target
  and `report_header = true` so the response carries a fidelity header.

  ```bash
  sluice check --config examples/translation.toml
  sluice serve --config examples/translation.toml
  ```

- **script-worker-stream.toml**: a streaming route with a `type = "script"`
  step running as a long-lived worker (`script_mode = "worker"`) on
  `hook = "on_stream"` with `chunk_mode = "mutate"`, gated behind
  `allow_scripts = true`.

  ```bash
  sluice check --config examples/script-worker-stream.toml
  sluice serve --config examples/script-worker-stream.toml
  ```

- **wasm-step.toml**: a `type = "wasm"` step pointing at
  `noop-guardrail.wasm`, a placeholder module in this directory that
  satisfies the guest ABI (`memory`, `alloc`, `run` exports) and returns a
  no-op `{"action":"continue"}` directive. Swap it for a real compiled
  module.

  ```bash
  sluice check --config examples/wasm-step.toml
  sluice serve --config examples/wasm-step.toml
  ```

- **multi-listener/**: a config *directory* (`gateway.toml` +
  `routes.d/*.toml` + `gateways.d/*.toml`) where one process exposes
  different route subsets on different addresses: a public listener with
  only the `claude` route, and an internal listener with both `claude` and
  `internal-tools`.

  ```bash
  sluice check --config-dir examples/multi-listener
  sluice serve --config-dir examples/multi-listener
  ```

- **loopback.toml**: a `mode = "loopback"` `on_request` step for a legacy
  tool that can't return a directive inline, with `gateway.loopback_secret`
  set (loopback is off by default and rejected at load without it).

  ```bash
  sluice check --config examples/loopback.toml
  sluice serve --config examples/loopback.toml
  ```

None of these configs point at a real step service, script, or upstream
worth calling in `sluice serve`; run `sluice check` to see them validate,
and treat the `url`/`cmd` values as placeholders to replace with your own.
