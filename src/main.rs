use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use clap::Parser;

use sluice::cli::{Cli, Command, ModelsAction, TokenAction};
use sluice::config::load::ConfigError;
use sluice::config::watch::ConfigSource;
use sluice::config::{Config, Hook, OnError, Route, StepType, UrlMode};
use sluice::directive::{apply_ops, Directive};
use sluice::envelope::build_request_envelope;
use sluice::http_msg::HttpMsg;
use sluice::loopback::token::{ChainToken, TokenError};
use sluice::registry::diff::{diff as diff_registries, RegistryDiff};
use sluice::registry::models_dev::{fetch_network, MODELS_DEV_URL};
use sluice::registry::{ModelFacts, Registry, RegistryError};
use sluice::router::upstream_url_for;
use sluice::step::script::ScriptOneshot;
use sluice::step::url::UrlTransform;
use sluice::step::wasm::WasmStep;
use sluice::{config, server};

fn load(config: &Path, config_dir: &Option<PathBuf>) -> Result<Config, ConfigError> {
    match config_dir {
        Some(dir) => config::load::load_dir(dir),
        None => config::load::load_file(config),
    }
}

fn describe<'a>(config: &'a Path, config_dir: &'a Option<PathBuf>) -> &'a Path {
    config_dir.as_deref().unwrap_or(config)
}

/// The [`ConfigSource`] `serve` should hot-reload from, mirroring however
/// this process was actually invoked (`--config` vs. `--config-dir`) so a
/// later filesystem-change reload re-runs the very same loader that produced
/// the initial config.
fn source_of(config: &Path, config_dir: &Option<PathBuf>) -> ConfigSource {
    match config_dir {
        Some(dir) => ConfigSource::Dir(dir.clone()),
        None => ConfigSource::File(config.to_path_buf()),
    }
}

fn hook_str(hook: Hook) -> &'static str {
    match hook {
        Hook::OnRequest => "on_request",
        Hook::OnResponse => "on_response",
        Hook::OnStream => "on_stream",
    }
}

fn step_type_str(ty: StepType) -> &'static str {
    match ty {
        StepType::Url => "url",
        StepType::Script => "script",
        StepType::Wasm => "wasm",
    }
}

fn url_mode_str(mode: UrlMode) -> &'static str {
    match mode {
        UrlMode::Transform => "transform",
        UrlMode::Loopback => "loopback",
    }
}

fn on_error_str(on_error: OnError) -> &'static str {
    match on_error {
        OnError::FailOpen => "fail_open",
        OnError::FailClosed => "fail_closed",
    }
}

/// Render the effective, merged config as plain human-readable text: the
/// `[gateway]` settings, then each route's id/upstream and its steps'
/// effective identity/behavior. Used by `sluice routes`, entirely offline —
/// no server, no network.
fn render_routes(cfg: &Config) -> String {
    let mut out = String::new();
    let gw = &cfg.gateway;
    out.push_str("gateway:\n");
    out.push_str(&format!("  listen: {}\n", gw.listen));
    if !gw.admin_listen.is_empty() {
        out.push_str(&format!("  admin_listen: {}\n", gw.admin_listen));
    }
    out.push_str(&format!("  max_body_bytes: {}\n", gw.max_body_bytes));
    out.push_str(&format!("  max_inflight: {}\n", gw.max_inflight));
    out.push_str(&format!(
        "  upstream_timeout_ms: {}\n",
        gw.upstream_timeout_ms
    ));
    out.push_str(&format!("  max_context_bytes: {}\n", gw.max_context_bytes));

    out.push('\n');
    out.push_str(&format!("routes ({}):\n", cfg.routes.len()));
    for route in &cfg.routes {
        out.push_str(&format!("  - id: {}\n", route.id));
        out.push_str(&format!("    upstream: {}\n", route.upstream));
        if route.steps.is_empty() {
            continue;
        }
        out.push_str("    steps:\n");
        for (i, step) in route.steps.iter().enumerate() {
            out.push_str(&format!("      - name: {}\n", step.effective_name(i)));
            out.push_str(&format!("        hook: {}\n", hook_str(step.hook)));
            out.push_str(&format!("        type: {}\n", step_type_str(step.type_)));
            out.push_str(&format!("        mode: {}\n", url_mode_str(step.mode)));
            out.push_str(&format!("        timeout_ms: {}\n", step.timeout_ms));
            out.push_str(&format!(
                "        on_error: {}\n",
                on_error_str(step.on_error)
            ));
        }
    }

    // Named `gateways.d` listeners (M13): only present for a `--config-dir`
    // config that actually defines any, so this section is omitted
    // entirely for a single-file config (`cfg.gateways` always empty there).
    if !cfg.gateways.is_empty() {
        out.push('\n');
        out.push_str(&format!("gateways ({}):\n", cfg.gateways.len()));
        for gw in &cfg.gateways {
            out.push_str(&format!("  - name: {}\n", gw.name));
            out.push_str(&format!("    listen: {}\n", gw.listen));
            if gw.routes.is_empty() {
                continue;
            }
            out.push_str("    routes:\n");
            for route_id in &gw.routes {
                out.push_str(&format!("      - {route_id}\n"));
            }
        }
    }
    out
}

/// Render a slice of models as a readable, fixed-width text table (id,
/// provider, context, cost_input, cost_output, tool_call), sorted by id.
/// Used by `sluice models list` when `--json` isn't given.
fn render_models_table(models: &[&ModelFacts]) -> String {
    let mut sorted = models.to_vec();
    sorted.sort_by(|a, b| a.id.cmp(&b.id));

    let opt_u64 = |v: Option<u64>| v.map_or_else(|| "-".to_string(), |n| n.to_string());
    let opt_f64 = |v: Option<f64>| v.map_or_else(|| "-".to_string(), |n| n.to_string());

    let mut out = format!(
        "{:<32} {:<12} {:>10} {:>10} {:>10} {:>9}\n",
        "ID", "PROVIDER", "CONTEXT", "COST_IN", "COST_OUT", "TOOL_CALL"
    );
    for m in sorted {
        out.push_str(&format!(
            "{:<32} {:<12} {:>10} {:>10} {:>10} {:>9}\n",
            m.id,
            m.provider,
            opt_u64(m.context),
            opt_f64(m.cost_input),
            opt_f64(m.cost_output),
            m.tool_call,
        ));
    }
    out
}

/// Render a [`RegistryDiff`] as plain text: added/removed model ids, and for
/// changed ids, which of context/cost_input/cost_output differ. Never
/// writes anything — mirrors `sluice models diff`'s read-only contract.
fn render_diff(d: &RegistryDiff) -> String {
    if d.is_empty() {
        return "no differences\n".to_string();
    }
    let mut out = String::new();
    if !d.added.is_empty() {
        out.push_str("added:\n");
        for (provider, id) in &d.added {
            out.push_str(&format!("  + {id} ({provider})\n"));
        }
    }
    if !d.removed.is_empty() {
        out.push_str("removed:\n");
        for (provider, id) in &d.removed {
            out.push_str(&format!("  - {id} ({provider})\n"));
        }
    }
    if !d.changed.is_empty() {
        out.push_str("changed:\n");
        for c in &d.changed {
            let mut fields = Vec::new();
            if c.context_changed {
                fields.push("context");
            }
            if c.cost_input_changed {
                fields.push("cost_input");
            }
            if c.cost_output_changed {
                fields.push("cost_output");
            }
            out.push_str(&format!(
                "  ~ {} ({}) ({})\n",
                c.id,
                c.provider,
                fields.join(", ")
            ));
        }
    }
    out
}

/// Print a registry-source resolution error the same way `sluice check`/
/// `routes` report config errors: message to stderr, non-zero exit.
fn registry_error(context: &str, err: &RegistryError) -> std::process::ExitCode {
    eprintln!("{context}: {err}");
    std::process::ExitCode::FAILURE
}

/// Resolve a `models update`/`models diff` candidate registry from either a
/// local models.dev-shaped directory (`--source`, via [`Registry::from_dir`])
/// or a live network fetch (`--from-network`, via
/// [`sluice::registry::models_dev::fetch_network`]) — the SAME resolve/build
/// pipeline either way, per design §9.6: no parallel resolution path. CLI
/// parsing (`cli.rs`'s `conflicts_with`/`required_unless_present` group)
/// guarantees exactly one of `source`/`from_network` is set by the time this
/// runs, so `from_network == false` implies `source` is present.
///
/// The network fetch is async; since `run_models` (and `main`) stay
/// synchronous, this spins up a short-lived current-thread tokio runtime
/// just for the fetch, mirroring the `Command::Test` arm's `block_on`
/// pattern, with a fresh `reqwest::Client` per invocation (no pooling needed
/// for a single one-shot request).
fn resolve_models_source(
    source: Option<PathBuf>,
    from_network: bool,
    network_url: Option<&str>,
) -> Result<Registry, RegistryError> {
    if from_network {
        let base_url = network_url.unwrap_or(MODELS_DEV_URL);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build tokio runtime");
        let client = reqwest::Client::new();
        rt.block_on(fetch_network(&client, base_url))
    } else {
        let source = source
            .expect("CLI parsing guarantees --source is present when --from-network is not given");
        Registry::from_dir(&source)
    }
}

/// Error-message label for `resolve_models_source`'s two source kinds,
/// mirroring the pre-network-fetch message shape (`"failed to resolve
/// <path>"`) for `--source` and a network-specific label for
/// `--from-network`.
fn models_source_label(source: Option<&PathBuf>, from_network: bool) -> String {
    if from_network {
        "failed to fetch models.dev".to_string()
    } else {
        let source = source
            .expect("CLI parsing guarantees --source is present when --from-network is not given");
        format!("failed to resolve {}", source.display())
    }
}

/// Run a `sluice models <list|update|diff>` subcommand. Split out of
/// `main()`'s match arms purely for readability — each branch is otherwise
/// identical in shape to the `Check`/`Routes`/`Serve` arms.
fn run_models(action: ModelsAction) -> std::process::ExitCode {
    match action {
        ModelsAction::List { provider, json } => {
            let registry = Registry::load();
            let models: Vec<&ModelFacts> = registry
                .iter()
                .filter(|m| provider.as_deref().is_none_or(|p| m.provider == p))
                .collect();
            if json {
                match serde_json::to_string_pretty(&{
                    let mut sorted = models.clone();
                    sorted.sort_by(|a, b| a.id.cmp(&b.id));
                    sorted
                }) {
                    Ok(text) => {
                        println!("{text}");
                        std::process::ExitCode::SUCCESS
                    }
                    Err(err) => {
                        eprintln!("failed to serialize registry: {err}");
                        std::process::ExitCode::FAILURE
                    }
                }
            } else {
                print!("{}", render_models_table(&models));
                std::process::ExitCode::SUCCESS
            }
        }
        ModelsAction::Update {
            source,
            from_network,
            network_url,
            out,
        } => {
            let label = models_source_label(source.as_ref(), from_network);
            match resolve_models_source(source, from_network, network_url.as_deref()) {
                Ok(registry) => match registry.write_json_file(&out) {
                    Ok(()) => {
                        println!("wrote {} model(s) to {}", registry.len(), out.display());
                        std::process::ExitCode::SUCCESS
                    }
                    Err(err) => registry_error(&format!("failed to write {}", out.display()), &err),
                },
                Err(err) => registry_error(&label, &err),
            }
        }
        ModelsAction::Diff {
            source,
            from_network,
            network_url,
        } => {
            let label = models_source_label(source.as_ref(), from_network);
            match resolve_models_source(source, from_network, network_url.as_deref()) {
                Ok(candidate) => {
                    let current = Registry::load();
                    let d = diff_registries(&current, &candidate);
                    print!("{}", render_diff(&d));
                    std::process::ExitCode::SUCCESS
                }
                Err(err) => registry_error(&label, &err),
            }
        }
    }
}

/// Build the synthetic `on_request` probe message `sluice test` sends
/// through a route's chain (design doc §14): `POST /<route_id>/probe` with a
/// small JSON body and a single `content-type` header, mirroring what a real
/// client's first hop into the route would look like closely enough for
/// steps to see a plausible envelope, without needing any actual traffic.
fn probe_message(route_id: &str) -> HttpMsg {
    let mut msg = HttpMsg {
        method: "POST".to_string(),
        path: format!("/{route_id}/probe"),
        headers: BTreeMap::from([("content-type".to_string(), "application/json".to_string())]),
        body_b64: String::new(),
    };
    msg.set_body_bytes(b"{}");
    msg
}

/// Run one `on_request` step directly against its configured runtime
/// (`type = "url"` POSTs the envelope to the step's `url`, `type = "script"`
/// spawns `cmd` as a oneshot subprocess, `type = "wasm"` compiles-and-calls
/// the module fresh). Mirrors `proxy::run_step`'s dispatch, but deliberately
/// doesn't need a `ProxyState`/wasm cache: `sluice test` compiles a wasm
/// module fresh on the one call it makes, since there's no request volume
/// here to amortize a cache over.
///
/// Preconditions (`step.url`/`step.cmd`/`step.wasm` present as required by
/// `type_`) are guaranteed by `config::load::validate`, exactly as they are
/// for `proxy::run_step` — see that function's own `.expect(...)` calls.
async fn run_test_step(
    client: &reqwest::Client,
    step: &sluice::config::Step,
    env: &sluice::envelope::Envelope,
) -> Result<Directive, sluice::step::StepError> {
    match step.type_ {
        StepType::Url => {
            let url = step.url.clone().expect("validated url present");
            UrlTransform::new(url, step.timeout_ms)
                .run(client, env)
                .await
        }
        StepType::Script => {
            let cmd = step.cmd.clone();
            ScriptOneshot::new(cmd, step.timeout_ms).run(env).await
        }
        StepType::Wasm => {
            let path = step.wasm.clone().expect("validated wasm path present");
            let wasm_step = WasmStep::from_path(std::path::Path::new(&path))?;
            wasm_step
                .run(env, std::time::Duration::from_millis(step.timeout_ms))
                .await
        }
    }
}

/// Run `sluice test <route>` (design doc §14): walk `route`'s `on_request`
/// step chain IN ORDER, actually invoking each step (see `run_test_step`)
/// against a synthetic probe request (see `probe_message`) and reporting its
/// directive and timing, stopping before ever forwarding to the real
/// upstream. A `mode = "loopback"` step can't be probed this way (it parks
/// and resumes via a callback that needs live infra to fire) so it's
/// reported as skipped rather than invoked.
///
/// A best-effort diagnostic: unlike a live request, a step transport/decode
/// error, an illegal-mutation `ops` failure, or an illegal directive
/// (`emit`/`drop`, `on_request`-only) is reported and stops the walk early
/// (mirroring how a live request would fail_closed/short_circuit/abort) but
/// never itself makes this COMMAND fail — the only failure this command
/// reports via its exit code is the route not existing at all.
async fn run_test_probe(cfg: &Config, route: &Route) {
    let mut msg = probe_message(&route.id);
    let mut context = serde_json::Map::new();
    let client = reqwest::Client::new();
    let correlation_id = format!("test-{}", uuid::Uuid::new_v4());

    for (i, step) in route.steps.iter().enumerate() {
        if step.hook != Hook::OnRequest {
            continue;
        }
        let self_name = step.effective_name(i);
        let kind = format!("{}/{}", step_type_str(step.type_), hook_str(step.hook));

        if step.type_ == StepType::Url && step.mode == UrlMode::Loopback {
            println!("{self_name} ({kind}) -> skipped (loopback needs live infra)");
            continue;
        }

        let env =
            build_request_envelope(&route.id, &self_name, &msg, &context, &correlation_id, None);
        let start = std::time::Instant::now();
        let result = run_test_step(&client, step, &env).await;
        let elapsed_ms = start.elapsed().as_millis();

        match result {
            Ok(Directive::Continue { ops }) => {
                println!("{self_name} ({kind}) -> continue [{elapsed_ms}ms]");
                if let Err(err) = apply_ops(
                    &mut msg,
                    &mut context,
                    &self_name,
                    &ops,
                    cfg.gateway.max_context_bytes,
                ) {
                    println!("  note: ops rejected ({err}); chain stops here");
                    return;
                }
            }
            Ok(Directive::ShortCircuit { response }) => {
                println!(
                    "{self_name} ({kind}) -> short_circuit (status {}) [{elapsed_ms}ms]",
                    response.status
                );
                return;
            }
            Ok(Directive::Abort { response }) => {
                println!(
                    "{self_name} ({kind}) -> abort (status {}) [{elapsed_ms}ms]",
                    response.status
                );
                return;
            }
            Ok(Directive::Emit { .. }) | Ok(Directive::Drop { .. }) => {
                println!(
                    "{self_name} ({kind}) -> error (emit/drop is on_stream-only) [{elapsed_ms}ms]"
                );
                return;
            }
            Err(err) => {
                println!("{self_name} ({kind}) -> error: {err} [{elapsed_ms}ms]");
                return;
            }
        }
    }

    let upstream_url = upstream_url_for(&route.upstream, &msg.path);
    println!("would forward to {upstream_url}");
}

/// Run `sluice token verify --secret <s> <token>`: entirely offline (design
/// doc §4.5, M12) — decodes and MAC-verifies `token` against `secret`
/// without ever checking expiry via [`ChainToken::verify`] (which would
/// swallow an expired-but-correctly-signed token's claims), so the decoded
/// fields print even for an expired token, with expiry reported alongside
/// them rather than in place of them.
fn run_token_verify(secret: &str, token: &str) -> std::process::ExitCode {
    match ChainToken::decode(token, secret.as_bytes()) {
        Ok(claims) => {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let expired = claims.expires_at_unix <= now;
            println!("cid: {}", claims.cid);
            println!("route_id: {}", claims.route_id);
            println!("resume_index: {}", claims.resume_index);
            println!("hop: {}", claims.hop);
            println!("expires_at: {}", claims.expires_at_unix);
            if expired {
                println!("valid: false (expired)");
                std::process::ExitCode::FAILURE
            } else {
                println!("valid: true");
                std::process::ExitCode::SUCCESS
            }
        }
        Err(TokenError::BadMac) => {
            println!("valid: false (bad mac)");
            std::process::ExitCode::FAILURE
        }
        Err(TokenError::BadFormat) => {
            println!("valid: false (bad format)");
            std::process::ExitCode::FAILURE
        }
        // `decode` never checks expiry, so it never returns this variant —
        // matched anyway (rather than `unreachable!`) so this stays correct
        // by construction if `TokenError` ever grows another terminal
        // decode-time variant.
        Err(TokenError::Expired) => {
            println!("valid: false (expired)");
            std::process::ExitCode::FAILURE
        }
    }
}

fn main() -> std::process::ExitCode {
    let cli = Cli::parse();
    match cli.command {
        Command::Check { config, config_dir } => {
            let path = describe(&config, &config_dir);
            match load(&config, &config_dir) {
                Ok(_) => {
                    println!("ok: {} is valid", path.display());
                    std::process::ExitCode::SUCCESS
                }
                Err(err) => {
                    eprintln!("config error in {}:\n{err}", path.display());
                    std::process::ExitCode::FAILURE
                }
            }
        }
        Command::Routes { config, config_dir } => {
            let path = describe(&config, &config_dir);
            match load(&config, &config_dir) {
                Ok(cfg) => {
                    print!("{}", render_routes(&cfg));
                    std::process::ExitCode::SUCCESS
                }
                Err(err) => {
                    eprintln!("config error in {}:\n{err}", path.display());
                    std::process::ExitCode::FAILURE
                }
            }
        }
        Command::Models { action } => run_models(action),
        Command::Token { action } => match action {
            TokenAction::Verify { secret, token } => run_token_verify(&secret, &token),
        },
        Command::Test {
            route,
            config,
            config_dir,
        } => {
            let path = describe(&config, &config_dir);
            let cfg = match load(&config, &config_dir) {
                Ok(cfg) => cfg,
                Err(err) => {
                    eprintln!("config error in {}:\n{err}", path.display());
                    return std::process::ExitCode::FAILURE;
                }
            };
            let matched = match cfg.routes.iter().find(|r| r.id == route) {
                Some(r) => r.clone(),
                None => {
                    eprintln!("no route named '{route}' in the effective config");
                    return std::process::ExitCode::FAILURE;
                }
            };
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build tokio runtime");
            rt.block_on(run_test_probe(&cfg, &matched));
            std::process::ExitCode::SUCCESS
        }
        Command::Serve {
            config,
            config_dir,
            gateway,
        } => {
            let cfg = match load(&config, &config_dir) {
                Ok(cfg) => cfg,
                Err(err) => {
                    let path = describe(&config, &config_dir);
                    eprintln!("config error in {}:\n{err}", path.display());
                    return std::process::ExitCode::FAILURE;
                }
            };
            let source = source_of(&config, &config_dir);
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("build tokio runtime");
            match rt.block_on(server::serve(cfg, source, gateway)) {
                Ok(()) => std::process::ExitCode::SUCCESS,
                Err(err) => {
                    eprintln!("server error: {err}");
                    std::process::ExitCode::FAILURE
                }
            }
        }
    }
}
