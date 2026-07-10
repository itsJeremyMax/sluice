# Sluice documentation

Detailed reference for the Sluice gateway. Start with the [project README](../README.md) for the overview and a quick start, then use these documents for the full detail. Runnable configs live in [examples](../examples/).

## Contents

- [Configuration](configuration.md): the complete config reference. Every `[gateway]`, `[[route]]`, and `[[route.step]]` field with its type and default, the single-file and directory layouts, and every rule `sluice check` enforces at load.
- [Steps and directives](steps-and-directives.md): the step contract in depth. The envelope shape per hook, the directive actions and ops, the framing headers the gateway owns, the on_request/on_response/on_stream hooks, and the url, script (oneshot and worker), and wasm runtimes.
- [Translation](translation.md): cross-provider translation. The canonical intermediate, buffered and streaming translation, the fidelity model and the `x-sluice-translation` report, the `[route.translate]` fields, and the current limits.
- [Observability and operations](observability-and-operations.md): running the gateway. Every metric and its labels, the admin listener, structured logs, load-shedding, config hot reload, the model registry workflow, and loopback.
- [CLI](cli.md): every subcommand and flag, with usage and exit behavior.

## Examples

The [examples](../examples/) directory holds runnable, annotated configs, each one validated with `sluice check`:

- `minimal.toml`: a single passthrough route.
- `guardrail-step.toml`: an on_request url guardrail step with admin and limits.
- `translation.toml`: a cross-provider translate route.
- `script-worker-stream.toml`: a worker-mode script mutating a live stream.
- `wasm-step.toml`: a sandboxed wasm step.
- `multi-listener/`: one process serving different route subsets on different addresses.
- `loopback.toml`: a loopback step with a signed callback.

See [examples/README.md](../examples/README.md) for the exact command to run each one.
