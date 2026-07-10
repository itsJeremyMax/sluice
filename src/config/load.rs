use std::collections::HashSet;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use thiserror::Error;

use crate::llm::adapter::KNOWN_PROVIDERS;
use crate::registry::Registry;
use crate::step::wasm::WasmStep;

use super::{
    ChunkMode, Config, Gateway, GatewayDef, Hook, OnError, Route, ScriptMode, StepType, UrlMode,
};

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read file: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to parse TOML: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("{0}")]
    Invalid(String),
}

pub fn load_file(path: &Path) -> Result<Config, ConfigError> {
    let text = std::fs::read_to_string(path)?;
    load_str(&text)
}

pub fn load_str(text: &str) -> Result<Config, ConfigError> {
    let cfg: Config = toml::from_str(text)?;
    validate(&cfg)?;
    Ok(cfg)
}

/// A file that supplies only the `[gateway]` table (used for `<dir>/gateway.toml`).
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct GatewayFile {
    #[serde(default)]
    gateway: Gateway,
}

/// A file that supplies only `[[route]]` blocks (used for `<dir>/routes.d/*.toml`).
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RoutesFile {
    #[serde(default, rename = "route")]
    routes: Vec<Route>,
}

/// A file defining one named gateway listener (used for
/// `<dir>/gateways.d/*.toml`). The gateway's `name` is not read from the
/// file itself — it's the filename stem — so this struct only carries the
/// fields the file actually supplies.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct GatewaysFile {
    listen: String,
    #[serde(default)]
    routes: Vec<String>,
}

/// Load a config from a directory: `<dir>/gateway.toml` (optional, supplies
/// `[gateway]`) plus every `<dir>/routes.d/*.toml` (sorted by filename),
/// each contributing `[[route]]` blocks. The merged config is then
/// validated. A malformed/unparseable file aborts the whole load — no
/// partial config is ever returned.
pub fn load_dir(dir: &Path) -> Result<Config, ConfigError> {
    if !dir.is_dir() {
        return Err(ConfigError::Invalid(format!(
            "config dir '{}' does not exist or is not a directory",
            dir.display()
        )));
    }

    let gateway_path = dir.join("gateway.toml");
    let gateway = if gateway_path.is_file() {
        let text = std::fs::read_to_string(&gateway_path)?;
        let parsed: GatewayFile = toml::from_str(&text).map_err(|e| {
            ConfigError::Invalid(format!("failed to parse {}: {e}", gateway_path.display()))
        })?;
        parsed.gateway
    } else {
        Gateway::default()
    };

    let routes_dir = dir.join("routes.d");
    let mut route_files: Vec<PathBuf> = Vec::new();
    if routes_dir.is_dir() {
        for entry in std::fs::read_dir(&routes_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() && path.extension().is_some_and(|ext| ext == "toml") {
                route_files.push(path);
            }
        }
    }
    route_files.sort_by(|a, b| a.file_name().cmp(&b.file_name()));

    let mut routes: Vec<Route> = Vec::new();
    let mut route_origin: std::collections::HashMap<String, PathBuf> =
        std::collections::HashMap::new();
    for path in &route_files {
        let text = std::fs::read_to_string(path)?;
        let parsed: RoutesFile = toml::from_str(&text).map_err(|e| {
            ConfigError::Invalid(format!("failed to parse {}: {e}", path.display()))
        })?;
        for route in parsed.routes {
            if let Some(other) = route_origin.get(&route.id) {
                return Err(ConfigError::Invalid(format!(
                    "duplicate route id '{}' defined in both {} and {} (route ids must be globally unique)",
                    route.id,
                    other.display(),
                    path.display()
                )));
            }
            route_origin.insert(route.id.clone(), path.clone());
            routes.push(route);
        }
    }

    let gateways_dir = dir.join("gateways.d");
    let mut gateway_files: Vec<PathBuf> = Vec::new();
    if gateways_dir.is_dir() {
        for entry in std::fs::read_dir(&gateways_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() && path.extension().is_some_and(|ext| ext == "toml") {
                gateway_files.push(path);
            }
        }
    }
    gateway_files.sort_by(|a, b| a.file_name().cmp(&b.file_name()));

    let mut gateways: Vec<GatewayDef> = Vec::new();
    for path in &gateway_files {
        let text = std::fs::read_to_string(path)?;
        let parsed: GatewaysFile = toml::from_str(&text).map_err(|e| {
            ConfigError::Invalid(format!("failed to parse {}: {e}", path.display()))
        })?;
        let name = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or_default()
            .to_string();
        gateways.push(GatewayDef {
            name,
            listen: parsed.listen,
            routes: parsed.routes,
        });
    }

    let cfg = Config {
        gateway,
        routes,
        gateways,
    };
    validate(&cfg)?;
    Ok(cfg)
}

fn validate(cfg: &Config) -> Result<(), ConfigError> {
    if cfg.gateway.schema_version != 1 {
        return Err(ConfigError::Invalid(format!(
            "unsupported schema_version {}: this build supports major version 1",
            cfg.gateway.schema_version
        )));
    }

    if cfg.gateway.max_response_bytes == Some(0) {
        return Err(ConfigError::Invalid(
            "max_response_bytes must be >= 1 (0 would reject every response body)".into(),
        ));
    }

    if cfg.gateway.max_inflight == 0 {
        return Err(ConfigError::Invalid("max_inflight must be >= 1".into()));
    }

    if cfg.gateway.max_event_bytes == Some(0) {
        return Err(ConfigError::Invalid(
            "max_event_bytes must be >= 1 (0 would reject every SSE event)".into(),
        ));
    }

    if !cfg.gateway.admin_listen.is_empty() && cfg.gateway.admin_listen == cfg.gateway.listen {
        return Err(ConfigError::Invalid(
            "admin_listen must differ from listen".into(),
        ));
    }

    if cfg.routes.is_empty() {
        return Err(ConfigError::Invalid(
            "configuration defines no routes; at least one [[route]] is required".into(),
        ));
    }

    // `translate.model`, when set, must resolve in the model registry under
    // the target provider (see below) — the same resolution the running
    // gateway uses at request time (`Registry::load`: a locally-refreshed
    // registry file if present, else the embedded seed). Loaded lazily and
    // at most once for the whole config, not per-route, since `Registry::load`
    // reads (and may parse) a file from disk.
    let mut model_registry: Option<Registry> = None;

    let mut seen_ids: HashSet<&str> = HashSet::new();
    for route in &cfg.routes {
        if route.id.is_empty() {
            return Err(ConfigError::Invalid("a route has an empty id".into()));
        }
        if route.id.contains('/') {
            return Err(ConfigError::Invalid(format!(
                "route id '{}' must not contain '/'",
                route.id
            )));
        }
        if !seen_ids.insert(route.id.as_str()) {
            return Err(ConfigError::Invalid(format!(
                "duplicate route id '{}' (route ids must be globally unique)",
                route.id
            )));
        }
        if route.upstream.is_empty() {
            return Err(ConfigError::Invalid(format!(
                "route '{}' has an empty upstream",
                route.id
            )));
        }

        if let Some(adapter) = &route.adapter {
            if let Some(ingress) = &adapter.ingress {
                if !KNOWN_PROVIDERS.contains(&ingress.as_str()) {
                    return Err(ConfigError::Invalid(format!(
                        "route '{}' has unknown adapter.ingress '{}' (known: {})",
                        route.id,
                        ingress,
                        KNOWN_PROVIDERS.join(", ")
                    )));
                }
            }
            if let Some(egress) = &adapter.egress {
                if !KNOWN_PROVIDERS.contains(&egress.as_str()) {
                    return Err(ConfigError::Invalid(format!(
                        "route '{}' has unknown adapter.egress '{}' (known: {})",
                        route.id,
                        egress,
                        KNOWN_PROVIDERS.join(", ")
                    )));
                }
                if adapter.ingress.as_deref() != Some(egress.as_str()) {
                    return Err(ConfigError::Invalid(format!(
                        "route '{}': cross-provider egress translation is not supported yet",
                        route.id
                    )));
                }
            }
        }

        if let Some(t) = &route.translate {
            if !KNOWN_PROVIDERS.contains(&t.from.as_str()) {
                return Err(ConfigError::Invalid(format!(
                    "route '{}' has unknown translate.from '{}' (known: {})",
                    route.id,
                    t.from,
                    KNOWN_PROVIDERS.join(", ")
                )));
            }
            if !KNOWN_PROVIDERS.contains(&t.to.as_str()) {
                return Err(ConfigError::Invalid(format!(
                    "route '{}' has unknown translate.to '{}' (known: {})",
                    route.id,
                    t.to,
                    KNOWN_PROVIDERS.join(", ")
                )));
            }
            if t.from == t.to {
                return Err(ConfigError::Invalid(format!(
                    "route '{}': translate.from and translate.to are both '{}' (a translate that doesn't translate is a config error)",
                    route.id, t.from
                )));
            }
            // `build_llm_view` (proxy.rs) prefers `adapter.ingress` over
            // `translate.from` when both are set: the request-side `llm`
            // view is parsed using whichever one wins. If a route
            // configures BOTH and they differ, the llm view is parsed with
            // the wrong dialect (garbage or null), and any on_request
            // guardrail relying on it is silently bypassed. Reject the
            // mismatch outright; matching values (or no adapter at all)
            // stay valid.
            if let Some(ingress) = route.adapter.as_ref().and_then(|a| a.ingress.as_deref()) {
                if ingress != t.from {
                    return Err(ConfigError::Invalid(format!(
                        "route '{}': adapter.ingress '{}' and translate.from '{}' must match (the llm view uses adapter.ingress, so a mismatch silently disables request-side guardrails)",
                        route.id, ingress, t.from
                    )));
                }
            }
            if let Some(model) = &t.model {
                let registry = model_registry.get_or_insert_with(Registry::load);
                if registry.get(&t.to, model).is_none() {
                    return Err(ConfigError::Invalid(format!(
                        "route '{}': translate.model '{}' is not a known model for provider '{}' (translate.to)",
                        route.id, model, t.to
                    )));
                }
            }
            if route.steps.iter().any(|s| s.mode == UrlMode::Loopback) {
                return Err(ConfigError::Invalid(format!(
                    "route '{}': translate cannot be combined with a mode=loopback step (the loopback interaction is out of scope)",
                    route.id
                )));
            }
        }

        let mut seen_names: HashSet<String> = HashSet::new();
        let mut has_on_response = false;
        let mut has_on_stream = false;
        let mut has_on_stream_observe = false;
        let mut has_on_stream_mutate = false;
        for (i, step) in route.steps.iter().enumerate() {
            let name = step.effective_name(i);
            if !seen_names.insert(name.clone()) {
                return Err(ConfigError::Invalid(format!(
                    "route '{}' has duplicate step name '{}'",
                    route.id, name
                )));
            }

            match step.hook {
                Hook::OnResponse => has_on_response = true,
                Hook::OnStream => {
                    has_on_stream = true;
                    match step.chunk_mode {
                        ChunkMode::Observe => has_on_stream_observe = true,
                        ChunkMode::Mutate => has_on_stream_mutate = true,
                    }
                }
                Hook::OnRequest => {}
            }

            match step.type_ {
                StepType::Url => {
                    if step.url.is_none() {
                        return Err(ConfigError::Invalid(format!(
                            "route '{}' step '{}' is type=url but has no url",
                            route.id, name
                        )));
                    }
                    if step.mode == UrlMode::Loopback {
                        if cfg.gateway.loopback_secret.is_empty() {
                            return Err(ConfigError::Invalid(format!(
                                "route '{}' step '{}': mode=loopback requires a non-empty loopback_secret",
                                route.id, name
                            )));
                        }
                        if step.hook != Hook::OnRequest {
                            return Err(ConfigError::Invalid(format!(
                                "route '{}' step '{}' uses mode=loopback, which is only supported on hook=on_request",
                                route.id, name
                            )));
                        }
                    }
                }
                StepType::Script => {
                    if !cfg.gateway.allow_scripts {
                        return Err(ConfigError::Invalid(format!(
                            "route '{}' step '{}': script steps require allow_scripts = true",
                            route.id, name
                        )));
                    }
                    if step.cmd.is_empty() {
                        return Err(ConfigError::Invalid(format!(
                            "route '{}' step '{}' is type=script but has no cmd",
                            route.id, name
                        )));
                    }
                    // If `cmd[0]` names an explicit path (contains a path
                    // separator — absolute like `/usr/local/bin/redact` or
                    // relative like `./scripts/redact.sh`), check the file
                    // actually exists now rather than let a typo'd path load
                    // clean and fail every single invocation with a spawn
                    // error at runtime (silently no-op-ish for a
                    // `fail_open` step). A bare command name (e.g. `"jq"`,
                    // no separator) is deliberately NOT checked against
                    // `PATH` here: `PATH` is resolved by the OS at spawn
                    // time (`step::script::ScriptOneshot::run`), can differ
                    // between the process that validates a config and the
                    // one that later runs it, and checking it here would
                    // reject configs that are perfectly valid at the
                    // gateway's actual runtime.
                    let bin = &step.cmd[0];
                    if (bin.contains('/') || bin.contains('\\')) && !Path::new(bin).is_file() {
                        return Err(ConfigError::Invalid(format!(
                            "route '{}' step '{}': script cmd[0] '{}' does not exist",
                            route.id, name, bin
                        )));
                    }
                    // A script step on `on_stream` only has a runtime when it
                    // runs as a long-lived `script_mode = "worker"` process the
                    // gateway talks to per chunk (M17): the worker persists
                    // across chunks, so the gateway can frame each chunk's
                    // envelope to it and read a directive back. A oneshot script
                    // has no per-chunk runtime — there is no fresh-subprocess
                    // dispatch on the stream path — so a oneshot script on
                    // on_stream would silently never run. Reject the oneshot
                    // shape (default) on on_stream; allow the worker shape.
                    if step.hook == Hook::OnStream && step.script_mode == ScriptMode::Oneshot {
                        return Err(ConfigError::Invalid(format!(
                            "route '{}' step '{}': script steps are not supported on the on_stream hook yet",
                            route.id, name
                        )));
                    }
                    // A worker script on `on_stream` DOES have a per-chunk
                    // runtime (see above), but `proxy.rs`'s on_stream
                    // dispatch only ever calls a worker script step when
                    // `chunk_mode = mutate` (observe remains URL-only there
                    // for worker steps, M17 Task 2). A worker step on
                    // on_stream with `chunk_mode = observe` (the default)
                    // loads clean but is never dispatched. Reject it.
                    if step.hook == Hook::OnStream
                        && step.script_mode == ScriptMode::Worker
                        && step.chunk_mode != ChunkMode::Mutate
                    {
                        return Err(ConfigError::Invalid(format!(
                            "route '{}' step '{}': a worker script on on_stream requires chunk_mode = mutate (an observe-only worker step is never dispatched)",
                            route.id, name
                        )));
                    }
                }
                StepType::Wasm => {
                    if step.wasm.is_none() {
                        return Err(ConfigError::Invalid(format!(
                            "route '{}' step '{}' is type=wasm but has no wasm module path",
                            route.id, name
                        )));
                    }
                    // No stream-worker wasm ABI exists yet (M14): a wasm
                    // step configured on `on_stream` would, like a script
                    // step there, silently never run since `proxy.rs`'s
                    // on_stream dispatch is `type = "url"`-only. Reject it
                    // outright rather than let it load clean.
                    if step.hook == Hook::OnStream {
                        return Err(ConfigError::Invalid(format!(
                            "route '{}' step '{}': wasm steps are not supported on on_stream yet",
                            route.id, name
                        )));
                    }

                    // Compile the module now rather than let a
                    // nonexistent/garbage `.wasm` path load clean and then
                    // silently fail (or, for a `fail_open` step, silently
                    // no-op) on every request that reaches it. This calls
                    // the exact same `WasmStep::from_path` that the proxy's
                    // own module cache (`proxy::get_or_compile_wasm`) calls
                    // lazily on first request, so a config that passes this
                    // check compiles the identical way at runtime — the
                    // proxy still compiles lazily (compile-once, keyed by
                    // path) rather than reusing this validation-time
                    // compile directly (threading a compiled module through
                    // `Config`/`ProxyState` is out of scope here), but
                    // because both sites call the same function on the same
                    // path, there is no way for load-time validation to
                    // accept a module that then fails to compile at request
                    // time (short of the file changing on disk in between,
                    // which is true of any load-time file check).
                    let wasm_path = step.wasm.as_ref().expect("checked Some above");
                    WasmStep::from_path(Path::new(wasm_path)).map_err(|e| {
                        ConfigError::Invalid(format!(
                            "route '{}' step '{}': wasm module '{}' failed to compile: {}",
                            route.id, name, wasm_path, e
                        ))
                    })?;
                }
            }

            if step.is_guardrail {
                if step.hook == Hook::OnStream && step.chunk_mode == ChunkMode::Observe {
                    return Err(ConfigError::Invalid(format!(
                        "route '{}' step '{}': is_guardrail on hook=on_stream requires chunk_mode = mutate (an observe-only guardrail cannot enforce anything)",
                        route.id, name
                    )));
                }
                if step.on_error == OnError::FailOpen {
                    return Err(ConfigError::Invalid(format!(
                        "route '{}' step '{}': is_guardrail steps cannot use on_error = fail_open (a guardrail must fail closed)",
                        route.id, name
                    )));
                }
            }
        }

        if has_on_response && has_on_stream {
            return Err(ConfigError::Invalid(format!(
                "route '{}': on_response and on_stream steps cannot be combined on one route yet",
                route.id
            )));
        }

        // `proxy::forward` only runs the on_stream step pipeline on
        // NON-translate routes: the on_stream dispatch is gated on
        // `translate.is_none()`, and a translate route is forced down the
        // buffered (or, for streaming, translate-only) path instead, which
        // never looks at on_stream steps at all (streaming translate is a
        // future feature, out of scope here). A `[route.translate]` route
        // with an on_stream step therefore loads clean but silently drops
        // that step, including a guardrail. Reject the combination outright.
        if route.translate.is_some() && has_on_stream {
            return Err(ConfigError::Invalid(format!(
                "route '{}': on_stream steps cannot be combined with [route.translate] yet (they would be silently skipped on a translate route); run guardrails on on_request/on_response, or remove translate",
                route.id
            )));
        }

        // `proxy.rs`'s on_stream dispatch (`forward`) runs only the mutate
        // on_stream steps for a route — an observe-only step configured
        // alongside a mutate one on the SAME route loads clean but is
        // silently dropped at runtime, never invoked at all. Reject that
        // combination outright rather than let it load clean. All-observe
        // and all-mutate routes are both fine (and exercised by their own
        // tests below).
        if has_on_stream_observe && has_on_stream_mutate {
            return Err(ConfigError::Invalid(format!(
                "route '{}': a route cannot mix observe and mutate on_stream steps",
                route.id
            )));
        }
    }

    let route_ids: HashSet<&str> = cfg.routes.iter().map(|r| r.id.as_str()).collect();
    let mut seen_gateway_names: HashSet<&str> = HashSet::new();
    let mut seen_listens: std::collections::HashMap<&str, &str> = std::collections::HashMap::new();
    for gw in &cfg.gateways {
        if !seen_gateway_names.insert(gw.name.as_str()) {
            return Err(ConfigError::Invalid(format!(
                "duplicate gateway name '{}' (gateway names in gateways.d must be unique)",
                gw.name
            )));
        }

        if let Some(other_name) = seen_listens.get(gw.listen.as_str()) {
            return Err(ConfigError::Invalid(format!(
                "gateways '{}' and '{}' (gateways.d/*.toml) both listen on '{}' (gateway listen addresses must be unique)",
                other_name, gw.name, gw.listen
            )));
        }
        seen_listens.insert(gw.listen.as_str(), gw.name.as_str());

        // `[gateway].listen` is checked against `admin_listen` above, but
        // that check is moot once `gateways.d` exists: `[gateway].listen` is
        // then ignored entirely (see `serve`'s multi-listener path) and each
        // `GatewayDef.listen` is what's actually bound. Without this check,
        // an admin listener could silently collide with a gateway listener's
        // port and fail to bind at process start with no validation-time
        // warning.
        if !cfg.gateway.admin_listen.is_empty() && gw.listen == cfg.gateway.admin_listen {
            return Err(ConfigError::Invalid(format!(
                "gateway '{}' (gateways.d/{}.toml) and admin_listen both listen on '{}' (admin and gateway listen addresses must not collide)",
                gw.name, gw.name, gw.listen
            )));
        }

        if gw.routes.is_empty() {
            return Err(ConfigError::Invalid(format!(
                "gateway '{}' (gateways.d/{}.toml) declares no routes",
                gw.name, gw.name
            )));
        }

        for route_id in &gw.routes {
            if !route_ids.contains(route_id.as_str()) {
                return Err(ConfigError::Invalid(format!(
                    "gateway '{}' (gateways.d/{}.toml) references undefined route id '{}'",
                    gw.name, gw.name, route_id
                )));
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn minimal_config_loads_with_defaults() {
        let cfg = load_str(
            r#"
            [[route]]
            id = "claude"
            upstream = "https://api.anthropic.com"
        "#,
        )
        .unwrap();
        assert_eq!(cfg.gateway.listen, "127.0.0.1:8080");
        assert_eq!(cfg.routes.len(), 1);
        assert_eq!(cfg.routes[0].id, "claude");
    }

    #[test]
    fn rejects_unknown_schema_version() {
        let err = load_str(
            r#"
            [gateway]
            schema_version = 2
            [[route]]
            id = "x"
            upstream = "http://u"
        "#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("schema_version"));
    }

    #[test]
    fn rejects_duplicate_route_id() {
        let err = load_str(
            r#"
            [[route]]
            id = "dup"
            upstream = "http://a"
            [[route]]
            id = "dup"
            upstream = "http://b"
        "#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("duplicate route id"));
    }

    #[test]
    fn rejects_duplicate_step_name() {
        let err = load_str(
            r#"
            [[route]]
            id = "r"
            upstream = "http://u"
              [[route.step]]
              name = "s"
              type = "url"
              url = "http://step"
              [[route.step]]
              name = "s"
              type = "url"
              url = "http://step"
        "#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("duplicate step name"));
    }

    #[test]
    fn rejects_url_step_without_url() {
        let err = load_str(
            r#"
            [[route]]
            id = "r"
            upstream = "http://u"
              [[route.step]]
              type = "url"
        "#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("no url"));
    }

    #[test]
    fn accepts_oversize_stream_through() {
        let cfg = load_str(
            r#"
            [gateway]
            oversize = "stream_through"
            [[route]]
            id = "r"
            upstream = "http://u"
        "#,
        )
        .unwrap();
        assert_eq!(cfg.gateway.oversize, super::super::Oversize::StreamThrough);
    }

    #[test]
    fn rejects_max_response_bytes_zero() {
        let err = load_str(
            r#"
            [gateway]
            max_response_bytes = 0
            [[route]]
            id = "r"
            upstream = "http://u"
        "#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("max_response_bytes must be >= 1"));
    }

    #[test]
    fn accepts_oversize_reject() {
        let cfg = load_str(
            r#"
            [gateway]
            oversize = "reject"
            [[route]]
            id = "r"
            upstream = "http://u"
        "#,
        )
        .unwrap();
        assert_eq!(cfg.gateway.oversize, super::super::Oversize::Reject);
    }

    #[test]
    fn rejects_zero_max_inflight() {
        let err = load_str(
            r#"
            [gateway]
            max_inflight = 0
            [[route]]
            id = "r"
            upstream = "http://u"
        "#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("max_inflight must be >= 1"));
    }

    #[test]
    fn rejects_zero_max_event_bytes() {
        let err = load_str(
            r#"
            [gateway]
            max_event_bytes = 0
            [[route]]
            id = "r"
            upstream = "http://u"
        "#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("max_event_bytes must be >= 1"));
    }

    #[test]
    fn accepts_nonzero_max_event_bytes() {
        let cfg = load_str(
            r#"
            [gateway]
            max_event_bytes = 4096
            [[route]]
            id = "r"
            upstream = "http://u"
        "#,
        )
        .unwrap();
        assert_eq!(cfg.gateway.max_event_bytes(), 4096);
    }

    #[test]
    fn rejects_loopback_mode_without_secret() {
        let err = load_str(
            r#"
            [[route]]
            id = "r"
            upstream = "http://u"
              [[route.step]]
              type = "url"
              mode = "loopback"
              url = "http://step"
        "#,
        )
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("mode=loopback requires a non-empty loopback_secret"),
            "{err}"
        );
    }

    #[test]
    fn rejects_loopback_mode_on_on_response_hook() {
        let err = load_str(
            r#"
            [gateway]
            loopback_secret = "s3cr3t"
            [[route]]
            id = "r"
            upstream = "http://u"
              [[route.step]]
              type = "url"
              mode = "loopback"
              hook = "on_response"
              url = "http://step"
        "#,
        )
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("only supported on hook=on_request"),
            "{err}"
        );
    }

    #[test]
    fn rejects_loopback_mode_on_on_stream_hook() {
        let err = load_str(
            r#"
            [gateway]
            loopback_secret = "s3cr3t"
            [[route]]
            id = "r"
            upstream = "http://u"
              [[route.step]]
              type = "url"
              mode = "loopback"
              hook = "on_stream"
              url = "http://step"
        "#,
        )
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("only supported on hook=on_request"),
            "{err}"
        );
    }

    #[test]
    fn accepts_loopback_mode_on_request_with_secret() {
        let cfg = load_str(
            r#"
            [gateway]
            loopback_secret = "s3cr3t"
            [[route]]
            id = "r"
            upstream = "http://u"
              [[route.step]]
              type = "url"
              mode = "loopback"
              hook = "on_request"
              url = "http://step"
        "#,
        )
        .unwrap();
        assert_eq!(cfg.routes[0].steps[0].mode, UrlMode::Loopback);
    }

    #[test]
    fn rejects_script_step_without_allow_scripts() {
        let err = load_str(
            r#"
            [[route]]
            id = "r"
            upstream = "http://u"
              [[route.step]]
              type = "script"
        "#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("allow_scripts = true"), "{err}");
    }

    #[test]
    fn accepts_script_step_with_allow_scripts() {
        let cfg = load_str(
            r#"
            [gateway]
            allow_scripts = true
            [[route]]
            id = "r"
            upstream = "http://u"
              [[route.step]]
              type = "script"
              cmd = ["true"]
        "#,
        )
        .unwrap();
        assert_eq!(cfg.routes[0].steps.len(), 1);
    }

    #[test]
    fn rejects_script_step_without_cmd() {
        let err = load_str(
            r#"
            [gateway]
            allow_scripts = true
            [[route]]
            id = "r"
            upstream = "http://u"
              [[route.step]]
              type = "script"
        "#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("no cmd"), "{err}");
    }

    #[test]
    fn rejects_script_on_stream_hook() {
        let err = load_str(
            r#"
            [gateway]
            allow_scripts = true
            [[route]]
            id = "r"
            upstream = "http://u"
              [[route.step]]
              type = "script"
              hook = "on_stream"
              cmd = ["true"]
        "#,
        )
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("script steps are not supported on the on_stream hook yet"),
            "{err}"
        );
    }

    /// The oneshot-only rejection is now conditional (M17): a
    /// `script_mode = "worker"` step on `on_stream` with `chunk_mode = mutate`
    /// is ACCEPTED because the long-lived worker process (see
    /// `step::script::ScriptWorker`) does have a per-chunk runtime the gateway
    /// can talk to.
    #[test]
    fn accepts_script_worker_on_stream_mutate() {
        let cfg = load_str(
            r#"
            [gateway]
            allow_scripts = true
            [[route]]
            id = "r"
            upstream = "http://u"
              [[route.step]]
              type = "script"
              hook = "on_stream"
              chunk_mode = "mutate"
              script_mode = "worker"
              cmd = ["true"]
        "#,
        )
        .unwrap();
        assert_eq!(cfg.routes[0].steps.len(), 1);
        assert_eq!(cfg.routes[0].steps[0].script_mode, ScriptMode::Worker);
    }

    /// A oneshot script on `on_stream` (explicitly, not just by default) still
    /// has no per-chunk runtime and must be rejected (M17).
    #[test]
    fn rejects_script_oneshot_on_stream() {
        let err = load_str(
            r#"
            [gateway]
            allow_scripts = true
            [[route]]
            id = "r"
            upstream = "http://u"
              [[route.step]]
              type = "script"
              hook = "on_stream"
              script_mode = "oneshot"
              cmd = ["true"]
        "#,
        )
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("script steps are not supported on the on_stream hook yet"),
            "{err}"
        );
    }

    /// FIX-2 G2: a worker script on `on_stream` with an explicit
    /// `chunk_mode = "observe"` loads clean today but `proxy.rs`'s on_stream
    /// dispatch only ever calls a worker script step when `chunk_mode =
    /// mutate` (see the on_stream_steps filter in `proxy::forward`), so an
    /// observe worker step is configured but never invoked. Must be
    /// rejected at load.
    #[test]
    fn rejects_script_worker_on_stream_observe() {
        let err = load_str(
            r#"
            [gateway]
            allow_scripts = true
            [[route]]
            id = "r"
            upstream = "http://u"
              [[route.step]]
              name = "watcher"
              type = "script"
              hook = "on_stream"
              chunk_mode = "observe"
              script_mode = "worker"
              cmd = ["true"]
        "#,
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("r"), "{msg}");
        assert!(msg.contains("watcher"), "{msg}");
        assert!(msg.contains("chunk_mode = mutate"), "{msg}");
    }

    /// FIX-2 G2: same as above but relying on the default `chunk_mode`
    /// (observe is the default) rather than setting it explicitly.
    #[test]
    fn rejects_script_worker_on_stream_default_chunk_mode() {
        let err = load_str(
            r#"
            [gateway]
            allow_scripts = true
            [[route]]
            id = "r"
            upstream = "http://u"
              [[route.step]]
              name = "watcher"
              type = "script"
              hook = "on_stream"
              script_mode = "worker"
              cmd = ["true"]
        "#,
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("watcher"), "{msg}");
        assert!(msg.contains("chunk_mode = mutate"), "{msg}");
    }

    /// `script_mode = "worker"` on `on_request` is now accepted (M17): the
    /// worker runtime exists. (The oneshot vs. worker dispatch on non-stream
    /// hooks is wired up in a later task; the config shape loads clean here.)
    #[test]
    fn accepts_script_mode_worker_on_request() {
        let cfg = load_str(
            r#"
            [gateway]
            allow_scripts = true
            [[route]]
            id = "r"
            upstream = "http://u"
              [[route.step]]
              type = "script"
              hook = "on_request"
              script_mode = "worker"
              cmd = ["true"]
        "#,
        )
        .unwrap();
        assert_eq!(cfg.routes[0].steps.len(), 1);
        assert_eq!(cfg.routes[0].steps[0].script_mode, ScriptMode::Worker);
    }

    #[test]
    fn accepts_script_oneshot_on_request() {
        let cfg = load_str(
            r#"
            [gateway]
            allow_scripts = true
            [[route]]
            id = "r"
            upstream = "http://u"
              [[route.step]]
              type = "script"
              hook = "on_request"
              script_mode = "oneshot"
              cmd = ["true"]
        "#,
        )
        .unwrap();
        assert_eq!(cfg.routes[0].steps.len(), 1);
    }

    #[test]
    fn rejects_wasm_step_without_wasm_path() {
        let err = load_str(
            r#"
            [[route]]
            id = "r"
            upstream = "http://u"
              [[route.step]]
              type = "wasm"
        "#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("no wasm module path"), "{err}");
    }

    /// A trivially-valid guest module satisfying `WasmStep`'s alloc/run ABI
    /// (a `memory` export plus `alloc`/`run` exports) — enough for
    /// `WasmStep::from_path`'s compile step (exercised by `validate` for
    /// every `type = "wasm"` step) to succeed. Actually calling `run`
    /// end-to-end through the proxy is `tests/wasm.rs`'s job; this only
    /// needs to compile.
    fn minimal_valid_wasm_module_bytes() -> Vec<u8> {
        wat::parse_str(
            r#"(module
  (memory (export "memory") 1)
  (func (export "alloc") (param $size i32) (result i32)
    i32.const 0)
  (func (export "run") (param $ptr i32) (param $len i32) (result i64)
    i64.const 0))
"#,
        )
        .expect("WAT compiles to wasm bytes")
    }

    /// Compile [`minimal_valid_wasm_module_bytes`] to a real, uniquely-named
    /// temp `.wasm` file so a test's `wasm = "<path>"` names an actually
    /// compilable module, mirroring `tests/wasm.rs`'s own `write_wasm_module`
    /// helper.
    fn write_temp_wasm_module(tag: &str) -> PathBuf {
        let dir = make_temp_dir(&format!("wasm-{tag}"));
        let path = dir.join("module.wasm");
        std::fs::write(&path, minimal_valid_wasm_module_bytes()).unwrap();
        path
    }

    #[test]
    fn accepts_wasm_step_with_compilable_wasm_path() {
        let wasm_path = write_temp_wasm_module("accept");
        let cfg = load_str(&format!(
            r#"
            [[route]]
            id = "r"
            upstream = "http://u"
              [[route.step]]
              type = "wasm"
              wasm = "{}"
        "#,
            wasm_path.display()
        ))
        .unwrap();
        assert_eq!(cfg.routes[0].steps.len(), 1);

        let _ = std::fs::remove_dir_all(wasm_path.parent().unwrap());
    }

    /// The merge-blocking gap this fix closes (M14 final review, Fix B): a
    /// config naming a nonexistent/garbage `.wasm` path previously loaded
    /// clean and then silently failed (or, for a `fail_open` step, silently
    /// no-op'd) every request that reached it. `validate` must now compile
    /// the module at load time and reject a path that doesn't compile,
    /// naming both the offending file and wasmtime's own error.
    #[test]
    fn rejects_wasm_step_with_unloadable_module_path() {
        let err = load_str(
            r#"
            [[route]]
            id = "r"
            upstream = "http://u"
              [[route.step]]
              type = "wasm"
              wasm = "/no/such/module-xyz.wasm"
        "#,
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("/no/such/module-xyz.wasm"), "{msg}");
        assert!(msg.contains("failed to compile"), "{msg}");
    }

    #[test]
    fn rejects_wasm_on_stream_hook() {
        let err = load_str(
            r#"
            [[route]]
            id = "r"
            upstream = "http://u"
              [[route.step]]
              type = "wasm"
              hook = "on_stream"
              wasm = "modules/redact.wasm"
        "#,
        )
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("wasm steps are not supported on on_stream yet"),
            "{err}"
        );
    }

    /// `rejects_wasm_on_stream_hook` above deliberately names a
    /// non-compilable placeholder path (`modules/redact.wasm`) — proving
    /// the on_stream rejection fires BEFORE `validate` ever attempts to
    /// compile the module, so an on_stream wasm step is rejected for the
    /// right reason even when its path is also bogus.
    #[test]
    fn rejects_wasm_on_stream_hook_even_with_uncompilable_path() {
        let err = load_str(
            r#"
            [[route]]
            id = "r"
            upstream = "http://u"
              [[route.step]]
              type = "wasm"
              hook = "on_stream"
              wasm = "modules/redact.wasm"
        "#,
        )
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("wasm steps are not supported on on_stream yet"),
            "{err}"
        );
    }

    /// Fix B (script half): a `type = "script"` step whose `cmd[0]` is an
    /// explicit (absolute) path that doesn't exist must be rejected at load
    /// — previously this loaded clean and failed every invocation with a
    /// spawn error at runtime.
    #[test]
    fn rejects_script_step_with_missing_absolute_cmd_path() {
        let err = load_str(
            r#"
            [gateway]
            allow_scripts = true
            [[route]]
            id = "r"
            upstream = "http://u"
              [[route.step]]
              type = "script"
              cmd = ["/no/such/binary-xyz"]
        "#,
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("/no/such/binary-xyz"), "{msg}");
        assert!(msg.contains("does not exist"), "{msg}");
    }

    /// A bare command name (no path separator) is resolved via `PATH` at
    /// spawn time, not checked at config load — this must still load clean
    /// even though `does-not-exist-anywhere` is not a real binary anywhere
    /// on this machine's `PATH`.
    #[test]
    fn accepts_script_step_with_bare_command_name_regardless_of_path() {
        let cfg = load_str(
            r#"
            [gateway]
            allow_scripts = true
            [[route]]
            id = "r"
            upstream = "http://u"
              [[route.step]]
              type = "script"
              cmd = ["does-not-exist-anywhere"]
        "#,
        )
        .unwrap();
        assert_eq!(cfg.routes[0].steps.len(), 1);
    }

    #[test]
    fn rejects_guardrail_observe_on_stream() {
        let err = load_str(
            r#"
            [[route]]
            id = "r"
            upstream = "http://u"
              [[route.step]]
              type = "url"
              url = "http://step"
              hook = "on_stream"
              is_guardrail = true
        "#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("is_guardrail"), "{err}");
        assert!(err.to_string().contains("chunk_mode"), "{err}");
    }

    #[test]
    fn accepts_guardrail_mutate_on_stream() {
        let cfg = load_str(
            r#"
            [[route]]
            id = "r"
            upstream = "http://u"
              [[route.step]]
              type = "url"
              url = "http://step"
              hook = "on_stream"
              is_guardrail = true
              chunk_mode = "mutate"
        "#,
        )
        .unwrap();
        assert!(cfg.routes[0].steps[0].is_guardrail);
    }

    #[test]
    fn rejects_guardrail_fail_open() {
        let err = load_str(
            r#"
            [[route]]
            id = "r"
            upstream = "http://u"
              [[route.step]]
              type = "url"
              url = "http://step"
              is_guardrail = true
              on_error = "fail_open"
        "#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("fail_open"), "{err}");
    }

    #[test]
    fn accepts_guardrail_fail_closed() {
        let cfg = load_str(
            r#"
            [[route]]
            id = "r"
            upstream = "http://u"
              [[route.step]]
              type = "url"
              url = "http://step"
              is_guardrail = true
        "#,
        )
        .unwrap();
        assert!(cfg.routes[0].steps[0].is_guardrail);
    }

    #[test]
    fn rejects_on_response_and_on_stream_combined_on_one_route() {
        let err = load_str(
            r#"
            [[route]]
            id = "r"
            upstream = "http://u"
              [[route.step]]
              type = "url"
              url = "http://a"
              hook = "on_response"
              [[route.step]]
              type = "url"
              url = "http://b"
              hook = "on_stream"
        "#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("cannot be combined"), "{err}");
    }

    #[test]
    fn accepts_on_response_and_on_stream_on_different_routes() {
        let cfg = load_str(
            r#"
            [[route]]
            id = "r1"
            upstream = "http://a"
              [[route.step]]
              type = "url"
              url = "http://a"
              hook = "on_response"
            [[route]]
            id = "r2"
            upstream = "http://b"
              [[route.step]]
              type = "url"
              url = "http://b"
              hook = "on_stream"
        "#,
        )
        .unwrap();
        assert_eq!(cfg.routes.len(), 2);
    }

    /// Fix A (M14 final review merge-blocker): a route with BOTH a
    /// `chunk_mode = "observe"` and a `chunk_mode = "mutate"` on_stream step
    /// previously loaded clean — but `proxy::forward` only ever runs the
    /// mutate steps for such a route (see its doc comment), so the observe
    /// step would silently never be invoked. Must now be rejected at load.
    #[test]
    fn rejects_on_stream_route_mixing_observe_and_mutate() {
        let err = load_str(
            r#"
            [[route]]
            id = "r"
            upstream = "http://u"
              [[route.step]]
              name = "observer"
              type = "url"
              url = "http://a"
              hook = "on_stream"
              chunk_mode = "observe"
              [[route.step]]
              name = "mutator"
              type = "url"
              url = "http://b"
              hook = "on_stream"
              chunk_mode = "mutate"
        "#,
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("cannot mix observe and mutate"), "{msg}");
    }

    #[test]
    fn accepts_on_stream_route_with_only_observe_steps() {
        let cfg = load_str(
            r#"
            [[route]]
            id = "r"
            upstream = "http://u"
              [[route.step]]
              name = "observer1"
              type = "url"
              url = "http://a"
              hook = "on_stream"
              chunk_mode = "observe"
              [[route.step]]
              name = "observer2"
              type = "url"
              url = "http://b"
              hook = "on_stream"
              chunk_mode = "observe"
        "#,
        )
        .unwrap();
        assert_eq!(cfg.routes[0].steps.len(), 2);
    }

    #[test]
    fn accepts_on_stream_route_with_only_mutate_steps() {
        let cfg = load_str(
            r#"
            [[route]]
            id = "r"
            upstream = "http://u"
              [[route.step]]
              name = "mutator1"
              type = "url"
              url = "http://a"
              hook = "on_stream"
              chunk_mode = "mutate"
              [[route.step]]
              name = "mutator2"
              type = "url"
              url = "http://b"
              hook = "on_stream"
              chunk_mode = "mutate"
        "#,
        )
        .unwrap();
        assert_eq!(cfg.routes[0].steps.len(), 2);
    }

    #[test]
    fn rejects_admin_listen_equal_to_listen() {
        let err = load_str(
            r#"
            [gateway]
            listen = "127.0.0.1:9000"
            admin_listen = "127.0.0.1:9000"
            [[route]]
            id = "r"
            upstream = "http://u"
        "#,
        )
        .unwrap_err();
        assert!(err
            .to_string()
            .contains("admin_listen must differ from listen"));
    }

    #[test]
    fn rejects_config_with_no_routes() {
        let err = load_str(
            r#"
            [gateway]
            listen = "127.0.0.1:9000"
        "#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("no routes"), "{err}");
    }

    #[test]
    fn load_dir_rejects_nonexistent_path() {
        let dir = std::env::temp_dir().join(format!(
            "sluice-config-test-nonexistent-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let err = load_dir(&dir).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains(&dir.display().to_string()),
            "expected path in error: {msg}"
        );
        assert!(
            msg.contains("does not exist or is not a directory"),
            "{msg}"
        );
    }

    fn make_temp_dir(tag: &str) -> PathBuf {
        let unique = format!(
            "sluice-config-test-{}-{}-{}",
            std::process::id(),
            tag,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let dir = std::env::temp_dir().join(unique);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn load_dir_merges_gateway_and_routes_d() {
        let dir = make_temp_dir("merge");
        std::fs::write(
            dir.join("gateway.toml"),
            r#"
            [gateway]
            listen = "127.0.0.1:9100"
        "#,
        )
        .unwrap();
        let routes_dir = dir.join("routes.d");
        std::fs::create_dir_all(&routes_dir).unwrap();
        std::fs::write(
            routes_dir.join("a.toml"),
            r#"
            [[route]]
            id = "route-a"
            upstream = "http://a"
        "#,
        )
        .unwrap();
        std::fs::write(
            routes_dir.join("b.toml"),
            r#"
            [[route]]
            id = "route-b"
            upstream = "http://b"
        "#,
        )
        .unwrap();

        let cfg = load_dir(&dir).unwrap();
        assert_eq!(cfg.gateway.listen, "127.0.0.1:9100");
        assert_eq!(cfg.routes.len(), 2);
        assert_eq!(cfg.routes[0].id, "route-a");
        assert_eq!(cfg.routes[1].id, "route-b");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_dir_without_gateway_toml_uses_defaults() {
        let dir = make_temp_dir("no-gateway");
        let routes_dir = dir.join("routes.d");
        std::fs::create_dir_all(&routes_dir).unwrap();
        std::fs::write(
            routes_dir.join("a.toml"),
            r#"
            [[route]]
            id = "only"
            upstream = "http://a"
        "#,
        )
        .unwrap();

        let cfg = load_dir(&dir).unwrap();
        assert_eq!(cfg.gateway.listen, "127.0.0.1:8080");
        assert_eq!(cfg.routes.len(), 1);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_dir_empty_dir_rejects_zero_routes() {
        let dir = make_temp_dir("empty");

        let err = load_dir(&dir).unwrap_err();
        assert!(err.to_string().contains("no routes"), "{err}");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_dir_duplicate_route_id_across_files_names_both() {
        let dir = make_temp_dir("dup");
        let routes_dir = dir.join("routes.d");
        std::fs::create_dir_all(&routes_dir).unwrap();
        std::fs::write(
            routes_dir.join("a.toml"),
            r#"
            [[route]]
            id = "dup"
            upstream = "http://a"
        "#,
        )
        .unwrap();
        std::fs::write(
            routes_dir.join("b.toml"),
            r#"
            [[route]]
            id = "dup"
            upstream = "http://b"
        "#,
        )
        .unwrap();

        let err = load_dir(&dir).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("a.toml"), "expected 'a.toml' in: {msg}");
        assert!(msg.contains("b.toml"), "expected 'b.toml' in: {msg}");
        assert!(msg.contains("duplicate route id 'dup'"), "{msg}");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_dir_malformed_file_aborts_whole_load() {
        let dir = make_temp_dir("malformed");
        let routes_dir = dir.join("routes.d");
        std::fs::create_dir_all(&routes_dir).unwrap();
        std::fs::write(
            routes_dir.join("a.toml"),
            r#"
            [[route]]
            id = "good"
            upstream = "http://a"
        "#,
        )
        .unwrap();
        std::fs::write(routes_dir.join("z-bad.toml"), "this is not [ valid toml").unwrap();

        let err = load_dir(&dir).unwrap_err();
        assert!(err.to_string().contains("z-bad.toml"), "{err}");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_dir_orders_routes_lexically_by_filename() {
        let dir = make_temp_dir("order");
        let routes_dir = dir.join("routes.d");
        std::fs::create_dir_all(&routes_dir).unwrap();
        // Filenames sort lexically as "a_first.toml" < "b_second.toml", but
        // the route ids inside are reversed relative to that, so a
        // content-based (or non-deterministic directory-read) order would
        // produce a different result than a filename-lexical order.
        std::fs::write(
            routes_dir.join("a_first.toml"),
            r#"
            [[route]]
            id = "second"
            upstream = "http://a"
        "#,
        )
        .unwrap();
        std::fs::write(
            routes_dir.join("b_second.toml"),
            r#"
            [[route]]
            id = "first"
            upstream = "http://b"
        "#,
        )
        .unwrap();

        let cfg = load_dir(&dir).unwrap();
        assert_eq!(cfg.routes.len(), 2);
        assert_eq!(cfg.routes[0].id, "second");
        assert_eq!(cfg.routes[1].id, "first");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_dir_rejects_admin_listen_equal_to_listen() {
        let dir = make_temp_dir("admin-collision");
        std::fs::write(
            dir.join("gateway.toml"),
            r#"
            [gateway]
            listen = "127.0.0.1:9200"
            admin_listen = "127.0.0.1:9200"
        "#,
        )
        .unwrap();
        let routes_dir = dir.join("routes.d");
        std::fs::create_dir_all(&routes_dir).unwrap();
        std::fs::write(
            routes_dir.join("a.toml"),
            r#"
            [[route]]
            id = "r"
            upstream = "http://a"
        "#,
        )
        .unwrap();

        let err = load_dir(&dir).unwrap_err();
        assert!(err
            .to_string()
            .contains("admin_listen must differ from listen"));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn parses_route_adapter_ingress() {
        let cfg = load_str(
            r#"
            [[route]]
            id = "r"
            upstream = "http://u"
              [route.adapter]
              ingress = "anthropic"
        "#,
        )
        .unwrap();
        let adapter = cfg.routes[0].adapter.as_ref().expect("adapter present");
        assert_eq!(adapter.ingress.as_deref(), Some("anthropic"));
        assert_eq!(adapter.egress, None);
    }

    #[test]
    fn route_without_adapter_defaults_to_none() {
        let cfg = load_str(
            r#"
            [[route]]
            id = "r"
            upstream = "http://u"
        "#,
        )
        .unwrap();
        assert!(cfg.routes[0].adapter.is_none());
    }

    #[test]
    fn rejects_unknown_adapter_ingress() {
        let err = load_str(
            r#"
            [[route]]
            id = "r"
            upstream = "http://u"
              [route.adapter]
              ingress = "no-such-provider"
        "#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("unknown adapter.ingress"), "{err}");
    }

    #[test]
    fn rejects_unknown_adapter_egress() {
        let err = load_str(
            r#"
            [[route]]
            id = "r"
            upstream = "http://u"
              [route.adapter]
              ingress = "anthropic"
              egress = "no-such-provider"
        "#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("unknown adapter.egress"), "{err}");
    }

    #[test]
    fn rejects_egress_different_from_ingress() {
        let err = load_str(
            r#"
            [[route]]
            id = "r"
            upstream = "http://u"
              [route.adapter]
              ingress = "anthropic"
              egress = "openai"
        "#,
        )
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("cross-provider egress translation is not supported yet"),
            "{err}"
        );
    }

    #[test]
    fn accepts_egress_equal_to_ingress() {
        let cfg = load_str(
            r#"
            [[route]]
            id = "r"
            upstream = "http://u"
              [route.adapter]
              ingress = "anthropic"
              egress = "anthropic"
        "#,
        )
        .unwrap();
        let adapter = cfg.routes[0].adapter.as_ref().expect("adapter present");
        assert_eq!(adapter.egress.as_deref(), Some("anthropic"));
    }

    fn write_routes_ab(dir: &Path) {
        let routes_dir = dir.join("routes.d");
        std::fs::create_dir_all(&routes_dir).unwrap();
        std::fs::write(
            routes_dir.join("routes.toml"),
            r#"
            [[route]]
            id = "a"
            upstream = "http://a"
            [[route]]
            id = "b"
            upstream = "http://b"
        "#,
        )
        .unwrap();
    }

    #[test]
    fn load_dir_without_gateways_d_has_empty_gateways() {
        let dir = make_temp_dir("no-gateways-d");
        write_routes_ab(&dir);

        let cfg = load_dir(&dir).unwrap();
        assert!(cfg.gateways.is_empty());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_dir_reads_two_gateways_d_files() {
        let dir = make_temp_dir("gateways-two");
        write_routes_ab(&dir);
        let gateways_dir = dir.join("gateways.d");
        std::fs::create_dir_all(&gateways_dir).unwrap();
        std::fs::write(
            gateways_dir.join("public.toml"),
            r#"
            listen = "127.0.0.1:9001"
            routes = ["a"]
        "#,
        )
        .unwrap();
        std::fs::write(
            gateways_dir.join("internal.toml"),
            r#"
            listen = "127.0.0.1:9002"
            routes = ["b"]
        "#,
        )
        .unwrap();

        let cfg = load_dir(&dir).unwrap();
        assert_eq!(cfg.gateways.len(), 2);
        let by_name = |name: &str| cfg.gateways.iter().find(|g| g.name == name).unwrap();
        let internal = by_name("internal");
        assert_eq!(internal.listen, "127.0.0.1:9002");
        assert_eq!(internal.routes, vec!["b".to_string()]);
        let public = by_name("public");
        assert_eq!(public.listen, "127.0.0.1:9001");
        assert_eq!(public.routes, vec!["a".to_string()]);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_dir_gateway_rejects_unknown_route_id_naming_file_and_id() {
        let dir = make_temp_dir("gateways-bad-route");
        write_routes_ab(&dir);
        let gateways_dir = dir.join("gateways.d");
        std::fs::create_dir_all(&gateways_dir).unwrap();
        std::fs::write(
            gateways_dir.join("public.toml"),
            r#"
            listen = "127.0.0.1:9001"
            routes = ["a", "no-such-route"]
        "#,
        )
        .unwrap();

        let err = load_dir(&dir).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("public.toml"), "{msg}");
        assert!(msg.contains("no-such-route"), "{msg}");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_dir_rejects_two_gateways_with_same_listen() {
        let dir = make_temp_dir("gateways-same-listen");
        write_routes_ab(&dir);
        let gateways_dir = dir.join("gateways.d");
        std::fs::create_dir_all(&gateways_dir).unwrap();
        std::fs::write(
            gateways_dir.join("public.toml"),
            r#"
            listen = "127.0.0.1:9001"
            routes = ["a"]
        "#,
        )
        .unwrap();
        std::fs::write(
            gateways_dir.join("internal.toml"),
            r#"
            listen = "127.0.0.1:9001"
            routes = ["b"]
        "#,
        )
        .unwrap();

        let err = load_dir(&dir).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("public"), "{msg}");
        assert!(msg.contains("internal"), "{msg}");
        assert!(msg.contains("127.0.0.1:9001"), "{msg}");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_dir_rejects_admin_listen_colliding_with_gateway_listen() {
        let dir = make_temp_dir("gateways-admin-collision");
        std::fs::write(
            dir.join("gateway.toml"),
            r#"
            [gateway]
            admin_listen = "127.0.0.1:9001"
        "#,
        )
        .unwrap();
        write_routes_ab(&dir);
        let gateways_dir = dir.join("gateways.d");
        std::fs::create_dir_all(&gateways_dir).unwrap();
        std::fs::write(
            gateways_dir.join("public.toml"),
            r#"
            listen = "127.0.0.1:9001"
            routes = ["a"]
        "#,
        )
        .unwrap();

        let err = load_dir(&dir).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("public.toml"), "{msg}");
        assert!(msg.contains("admin_listen"), "{msg}");
        assert!(msg.contains("127.0.0.1:9001"), "{msg}");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_dir_gateway_with_no_routes_is_rejected() {
        let dir = make_temp_dir("gateways-no-routes");
        write_routes_ab(&dir);
        let gateways_dir = dir.join("gateways.d");
        std::fs::create_dir_all(&gateways_dir).unwrap();
        std::fs::write(
            gateways_dir.join("empty.toml"),
            r#"
            listen = "127.0.0.1:9001"
        "#,
        )
        .unwrap();

        let err = load_dir(&dir).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("empty.toml") || msg.contains("'empty'"),
            "{msg}"
        );
        assert!(msg.contains("declares no routes"), "{msg}");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn route_without_translate_defaults_to_none() {
        let cfg = load_str(
            r#"
            [[route]]
            id = "r"
            upstream = "http://u"
        "#,
        )
        .unwrap();
        assert!(cfg.routes[0].translate.is_none());
    }

    #[test]
    fn accepts_valid_translate_route_round_trips_fields() {
        let cfg = load_str(
            r#"
            [[route]]
            id = "r"
            upstream = "http://u"
              [route.translate]
              from = "openai"
              to = "anthropic"
              model = "claude-opus-4-1-20250805"
              report_header = true
        "#,
        )
        .unwrap();
        let t = cfg.routes[0].translate.as_ref().expect("translate present");
        assert_eq!(t.from, "openai");
        assert_eq!(t.to, "anthropic");
        assert_eq!(t.model.as_deref(), Some("claude-opus-4-1-20250805"));
        assert!(t.report_header);
    }

    #[test]
    fn accepts_translate_without_model_defaults_report_header_false() {
        let cfg = load_str(
            r#"
            [[route]]
            id = "r"
            upstream = "http://u"
              [route.translate]
              from = "openai"
              to = "anthropic"
        "#,
        )
        .unwrap();
        let t = cfg.routes[0].translate.as_ref().expect("translate present");
        assert_eq!(t.model, None);
        assert!(!t.report_header);
    }

    #[test]
    fn rejects_translate_unknown_from_provider() {
        // M15.5 review Minor: the route id is deliberately distinctive
        // (not the earlier "r") so `msg.contains(route id)` actually proves
        // the id was surfaced in the error, rather than trivially matching
        // because "r" appears somewhere in ordinary English prose.
        let err = load_str(
            r#"
            [[route]]
            id = "xlate-route"
            upstream = "http://u"
              [route.translate]
              from = "no-such-provider"
              to = "anthropic"
        "#,
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("xlate-route"), "{msg}");
        assert!(msg.contains("no-such-provider"), "{msg}");
    }

    #[test]
    fn rejects_translate_unknown_to_provider() {
        let err = load_str(
            r#"
            [[route]]
            id = "r"
            upstream = "http://u"
              [route.translate]
              from = "openai"
              to = "no-such-provider"
        "#,
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("no-such-provider"), "{msg}");
    }

    #[test]
    fn rejects_translate_from_equal_to() {
        let err = load_str(
            r#"
            [[route]]
            id = "xlate-route"
            upstream = "http://u"
              [route.translate]
              from = "anthropic"
              to = "anthropic"
        "#,
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("xlate-route"), "{msg}");
        assert!(
            msg.contains("from") && msg.contains("to"),
            "expected message to mention from/to: {msg}"
        );
    }

    #[test]
    fn rejects_translate_model_not_in_registry_under_to() {
        let err = load_str(
            r#"
            [[route]]
            id = "xlate-route"
            upstream = "http://u"
              [route.translate]
              from = "openai"
              to = "anthropic"
              model = "no-such-model-xyz"
        "#,
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("xlate-route"), "{msg}");
        assert!(msg.contains("no-such-model-xyz"), "{msg}");
        assert!(msg.contains("anthropic"), "{msg}");
    }

    #[test]
    fn accepts_translate_model_present_in_registry_under_to() {
        let cfg = load_str(
            r#"
            [[route]]
            id = "r"
            upstream = "http://u"
              [route.translate]
              from = "openai"
              to = "anthropic"
              model = "claude-opus-4-1-20250805"
        "#,
        )
        .unwrap();
        let t = cfg.routes[0].translate.as_ref().expect("translate present");
        assert_eq!(t.model.as_deref(), Some("claude-opus-4-1-20250805"));
    }

    #[test]
    fn rejects_translate_combined_with_loopback_step() {
        let err = load_str(
            r#"
            [gateway]
            loopback_secret = "s3cr3t"
            [[route]]
            id = "xlate-route"
            upstream = "http://u"
              [route.translate]
              from = "openai"
              to = "anthropic"
              [[route.step]]
              type = "url"
              mode = "loopback"
              hook = "on_request"
              url = "http://step"
        "#,
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("xlate-route"), "{msg}");
        assert!(msg.contains("loopback"), "{msg}");
    }

    /// FIX-2 G1: `proxy::forward` only runs on_stream steps on non-translate
    /// routes (the on_stream pipeline is gated on `translate.is_none()`, and
    /// the streaming-translate branch ignores on_stream steps entirely), so
    /// a `[route.translate]` route with an on_stream step loads clean but
    /// silently drops that step, including a guardrail. Must be rejected at
    /// load.
    #[test]
    fn rejects_translate_route_with_on_stream_step() {
        let err = load_str(
            r#"
            [[route]]
            id = "xlate-route"
            upstream = "http://u"
              [route.translate]
              from = "openai"
              to = "anthropic"
              [[route.step]]
              name = "guardrail"
              type = "url"
              url = "http://step"
              hook = "on_stream"
              is_guardrail = true
              chunk_mode = "mutate"
        "#,
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("xlate-route"), "{msg}");
        assert!(msg.contains("on_stream"), "{msg}");
        assert!(msg.contains("translate"), "{msg}");
    }

    /// FIX-2 G1: a translate route with no on_stream steps at all is
    /// unaffected by the new gate.
    #[test]
    fn accepts_translate_route_with_no_on_stream_steps() {
        let cfg = load_str(
            r#"
            [[route]]
            id = "xlate-route"
            upstream = "http://u"
              [route.translate]
              from = "openai"
              to = "anthropic"
              [[route.step]]
              name = "checker"
              type = "url"
              url = "http://step"
              hook = "on_request"
        "#,
        )
        .unwrap();
        assert!(cfg.routes[0].translate.is_some());
    }

    /// FIX-2 G1: a non-translate route with an on_stream step is unaffected
    /// by the new gate (this is the existing, still-supported shape).
    #[test]
    fn accepts_non_translate_route_with_on_stream_step() {
        let cfg = load_str(
            r#"
            [[route]]
            id = "r"
            upstream = "http://u"
              [[route.step]]
              name = "watcher"
              type = "url"
              url = "http://step"
              hook = "on_stream"
              chunk_mode = "observe"
        "#,
        )
        .unwrap();
        assert!(cfg.routes[0].translate.is_none());
    }

    /// FIX-2 G3: `build_llm_view` (proxy.rs) prefers `adapter.ingress` over
    /// `translate.from` when both are set. If a route configures both and
    /// they differ, the request-side `llm` view is parsed with the wrong
    /// dialect, so any on_request guardrail relying on it is silently
    /// looking at garbage. Must be rejected at load.
    #[test]
    fn rejects_adapter_ingress_translate_from_mismatch() {
        let err = load_str(
            r#"
            [[route]]
            id = "xlate-route"
            upstream = "http://u"
              [route.adapter]
              ingress = "openai"
              [route.translate]
              from = "anthropic"
              to = "openai"
        "#,
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("xlate-route"), "{msg}");
        assert!(msg.contains("adapter.ingress"), "{msg}");
        assert!(msg.contains("translate.from"), "{msg}");
    }

    /// FIX-2 G3: matching `adapter.ingress` and `translate.from` stays valid
    /// (this is a legitimate way to be explicit about the ingress dialect on
    /// a translate route).
    #[test]
    fn accepts_adapter_ingress_translate_from_match() {
        let cfg = load_str(
            r#"
            [[route]]
            id = "xlate-route"
            upstream = "http://u"
              [route.adapter]
              ingress = "anthropic"
              [route.translate]
              from = "anthropic"
              to = "openai"
        "#,
        )
        .unwrap();
        assert_eq!(
            cfg.routes[0].adapter.as_ref().unwrap().ingress.as_deref(),
            Some("anthropic")
        );
    }

    /// FIX-2 G3: a translate route with no `[route.adapter]` at all is
    /// unaffected by the new gate (this is the existing, common shape).
    #[test]
    fn accepts_translate_route_without_adapter() {
        let cfg = load_str(
            r#"
            [[route]]
            id = "xlate-route"
            upstream = "http://u"
              [route.translate]
              from = "anthropic"
              to = "openai"
        "#,
        )
        .unwrap();
        assert!(cfg.routes[0].adapter.is_none());
    }

    #[test]
    fn load_dir_gateway_file_deny_unknown_fields() {
        let dir = make_temp_dir("gateways-unknown-field");
        write_routes_ab(&dir);
        let gateways_dir = dir.join("gateways.d");
        std::fs::create_dir_all(&gateways_dir).unwrap();
        std::fs::write(
            gateways_dir.join("public.toml"),
            r#"
            listen = "127.0.0.1:9001"
            routes = ["a"]
            bogus = "nope"
        "#,
        )
        .unwrap();

        let err = load_dir(&dir).unwrap_err();
        assert!(err.to_string().contains("public.toml"), "{err}");

        let _ = std::fs::remove_dir_all(&dir);
    }
}
