# CLI

This is the command reference for the `sluice` binary: every subcommand, its flags, defaults, and exit behavior. For the config file itself see [configuration](configuration.md); for metrics, health checks, and the admin listener see [observability and operations](observability-and-operations.md).

## Global config flags

Every subcommand takes config one of two ways:

- `--config <file>` (default `sluice.toml`): a single config file.
- `--config-dir <dir>`: a config directory (`gateway.toml` + `routes.d/*.toml`, plus optional `gateways.d/*.toml`).

`--config` and `--config-dir` are mutually exclusive; passing both is a parse error (`ArgumentConflict`). This applies to `serve`, `check`, `routes`, and `test`. `models` and `token verify` do not take config at all: `models` reads the model registry directly, and `token verify` only needs the `--secret` you pass it.

## `serve`

Run the gateway. This is the default long-running process; there's no separate "start" wording, `serve` just blocks and serves traffic until the process exits.

```
sluice serve [--config <file> | --config-dir <dir>] [--gateway <name>]
```

- `--config <file>`: default `sluice.toml`.
- `--config-dir <dir>`: load from a config directory instead.
- `--gateway <name>`: serve only the named `gateways.d` listener instead of every configured listener. Requires a `--config-dir` config that defines a `gateways.d` gateway with this name; an unknown name is a startup error.

Config is loaded and validated before the listener binds, so a bad config fails immediately with the same error `sluice check` would report, and the process exits non-zero without ever opening a socket. Once serving, the config source (whichever of `--config`/`--config-dir` you passed) is watched for filesystem changes and hot-reloaded.

```bash
sluice serve --config sluice.toml
sluice serve --config-dir conf.d --gateway public
```

## `check`

Validate a config without serving.

```
sluice check [--config <file> | --config-dir <dir>]
```

Runs the exact load-and-validate path the server runs on startup: parsing, `deny_unknown_fields`, cross-route id uniqueness, step name uniqueness within a route, and every other check in `config::load::validate`. Prints `ok: <path> is valid` and exits 0 on success. On failure it prints the error to stderr and exits non-zero; nothing is written.

```bash
sluice check --config sluice.toml
sluice check --config-dir conf.d
```

## `routes`

Print the effective, merged configuration without serving: `[gateway]` settings, then each route's id, upstream, and steps (name, hook, type, mode, timeout, on_error), then any named `gateways.d` listeners if the config defines any.

```
sluice routes [--config <file> | --config-dir <dir>]
```

Useful for confirming what a directory of `routes.d/*.toml` files actually resolves to, since routes are concatenated across files in lexical filename order. Exits non-zero on a config error and prints nothing on success but the rendered text; a load failure writes only the error to stderr, no partial output.

```bash
sluice routes --config sluice.toml
```

## `test <route>`

Send a synthetic probe through a route's `on_request` step chain and report each step's directive and timing, without ever forwarding to the real upstream.

```
sluice test <route> [--config <file> | --config-dir <dir>]
```

`<route>` is required and must match a route id in the effective config; an unknown id exits non-zero with `no route named '<route>' in the effective config`. Otherwise `test` builds a small synthetic `POST /<route>/probe` envelope and walks the route's `on_request` steps in order, actually invoking each one (a `url` step is POSTed to, a `script` step is spawned as a oneshot subprocess, a `wasm` step is compiled and called fresh). For each step it prints the step's name, its type/hook, the directive it returned, and how long it took. A `mode = "loopback"` step is reported as skipped rather than invoked, since resuming it needs live infrastructure a one-shot probe doesn't have. If every step continues, it prints the upstream URL the request would have been forwarded to.

A step error, an illegal `ops` mutation, or an illegal directive (`emit`/`drop`, which are `on_stream`-only) stops the walk early and is printed inline, but does not make the command itself fail. The only failure this command's exit code reports is a config load error or the route not existing; the command exits 0 as long as the route was found and probed, whatever the probe itself turned up.

```bash
sluice test claude --config sluice.toml
```

## `models list`

Print the currently loaded model registry: the local registry file written by a previous `models update`, or the embedded seed if none exists.

```
sluice models list [--provider <id>] [--json]
```

- `--provider <id>`: only show models for this provider id (e.g. `anthropic`).
- `--json`: print a JSON array (sorted by model id) instead of a fixed-width text table.

The text table's columns are ID, PROVIDER, CONTEXT, COST_IN, COST_OUT, TOOL_CALL, sorted by id; missing numeric fields print as `-`.

```bash
sluice models list --provider anthropic
sluice models list --json
```

## `models diff`

Resolve a models.dev-shaped source and print what would change versus the currently loaded registry, without writing anything.

```
sluice models diff (--source <dir> | --from-network)
```

- `--source <dir>`: a local directory in models.dev's `models/` + `providers/` shape.
- `--from-network`: resolve from a live fetch of the models.dev dataset instead.

Exactly one of `--source` or `--from-network` is required; passing both, or neither, is a parse error (`ArgumentConflict` or `MissingRequiredArgument`). Output lists added and removed model ids (each tagged with its provider) and, for models present in both, which of `context`, `cost_input`, `cost_output` changed. If nothing differs it prints `no differences`. A resolution failure (bad directory, network error) prints the error and exits non-zero; nothing is written either way, since `diff` never writes.

```bash
sluice models diff --source ./models-dev-checkout
sluice models diff --from-network
```

## `models update`

Resolve a models.dev-shaped source and write it as the local registry file.

```
sluice models update (--source <dir> | --from-network) [--out <file>]
```

- `--source <dir>`: a local directory in models.dev's `models/` + `providers/` shape.
- `--from-network`: resolve from a live fetch of the models.dev dataset instead.
- `--out <file>`: where to write the resolved registry JSON. Default `sluice-models.json`, which matches the path `sluice models list` (and the gateway on startup) checks for a locally-refreshed registry.

Exactly one of `--source` or `--from-network` is required, same rule as `diff`. This writes only the `--out` file, never config or routes. On success it prints `wrote <n> model(s) to <path>` and exits 0. On a resolution failure it prints the error to stderr and exits non-zero without touching the output file: a failed resolution never reaches the write step, so a bad `--source` or a network error never alters `--out`. (The write itself is a plain file write, not an atomic temp-and-rename, so a failure mid-write could still leave a truncated file. Resolution failures, the common case, never reach that point.)

```bash
sluice models update --source ./models-dev-checkout
sluice models update --from-network --out sluice-models.json
```

Both `models diff` and `models update` also accept a hidden `--network-url <url>` flag alongside `--from-network`, which overrides the models.dev URL fetched. It exists only so integration tests can point the fetch at a mock server; it isn't part of the supported interface and doesn't appear in `--help`.

## `token verify`

Decode and verify a signed `x-chain-token` (the loopback resume token) against a secret, entirely offline.

```
sluice token verify --secret <secret> <token>
```

- `--secret <secret>`: required. The HMAC secret to verify against; must match the gateway's `[gateway] loopback_secret`.
- `<token>`: required positional. The signed token itself.

A chain token is a self-contained, HMAC-signed value, so verifying one needs only the secret, no running server and no network call. On a well-formed, correctly-signed token it prints the decoded claims (`cid`, `route_id`, `resume_index`, `hop`, `expires_at`) followed by `valid: true`, and exits 0. An expired-but-correctly-signed token still prints its claims, followed by `valid: false (expired)`, and exits non-zero. A bad signature prints `valid: false (bad mac)`; a malformed token prints `valid: false (bad format)`; both exit non-zero.

```bash
sluice token verify --secret "$LOOPBACK_SECRET" eyJjaWQi...
```

## `update`

Check for a newer sluice release, or install it.

```
sluice update [--check]
```

`sluice update --check` is read-only: it prints the current and latest
versions and exits `0` when up to date (or ahead, e.g. a dev build), `10`
when a newer release exists, and `1` on error. The distinct exit code
makes scripted checks easy: `sluice update --check || notify`.

`sluice update` (no flag) downloads the release binary for this platform,
verifies it against the `.sha256` checksum published with the release
(a missing or mismatched checksum aborts the update), and atomically
replaces the running binary. If the install location isn't writable,
re-run with elevated permissions or re-install via `install.sh`.

The latest version is resolved from GitHub's `releases/latest` redirect —
no API token needed. There is no automatic background checking: sluice
never checks for updates unless you run this command.

```bash
sluice update --check
sluice update
```
