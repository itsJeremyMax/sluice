pub mod load;
pub mod watch;

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub gateway: Gateway,
    #[serde(default, rename = "route")]
    pub routes: Vec<Route>,
    /// Named listeners populated from `<dir>/gateways.d/*.toml` (M13). Not
    /// parsed from a single-file `[gateway]`/`[[route]]` config — it's only
    /// ever filled in by `config::load::load_dir`, so this stays empty for
    /// `load_file`/`load_str`.
    #[serde(skip)]
    pub gateways: Vec<GatewayDef>,
}

/// A named listener that serves a subset of the configured routes (M13).
/// `name` is the filename stem of the `gateways.d/<name>.toml` file that
/// defined it; `listen` is the address it binds; `routes` names the route
/// ids it exposes (each must be a defined route id — see
/// `config::load::validate`).
#[derive(Debug, Clone, Default)]
pub struct GatewayDef {
    pub name: String,
    pub listen: String,
    pub routes: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Gateway {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    /// Master switch for `type = "script"` steps (design doc §4.5, M11).
    /// Scripts run as subprocesses the gateway spawns per the step's `cmd`;
    /// leaving this `false` (the default) means any `type = "script"` step
    /// in the config is a validation error, not a silent no-op.
    #[serde(default)]
    pub allow_scripts: bool,
    #[serde(default = "default_listen")]
    pub listen: String,
    /// Allowlist of internal `x-sluice-*`/`x-chain-token` header names that
    /// survive the egress sanitizer toward the client (never upstream).
    /// See `reconstruct::sanitize_egress_headers` (design doc §7).
    #[serde(default = "default_expose_headers")]
    pub expose_headers: Vec<String>,
    /// Maximum size (in bytes) of a single step's own namespaced entry in
    /// the `context` map — the cap is enforced per-step-namespace, not on
    /// the map as a whole. The total `context` size can still grow without
    /// bound as more steps each write their own (individually-capped) entry.
    #[serde(default = "default_max_context_bytes")]
    pub max_context_bytes: usize,
    /// Maximum size (in bytes) of a request or response body the gateway
    /// will buffer. Bodies larger than this are handled per `oversize`.
    #[serde(default = "default_max_body_bytes")]
    pub max_body_bytes: usize,
    /// Behavior when a body exceeds `max_body_bytes`.
    #[serde(default = "default_oversize")]
    pub oversize: Oversize,
    /// Maximum number of concurrently in-flight requests the gateway will
    /// accept before shedding load.
    #[serde(default = "default_max_inflight")]
    pub max_inflight: usize,
    /// Timeout (in milliseconds) for upstream requests.
    #[serde(default = "default_upstream_timeout_ms")]
    pub upstream_timeout_ms: u64,
    /// Address the admin listener (health/ready/metrics) binds to. Empty
    /// string disables the admin listener entirely.
    #[serde(default = "default_admin_listen")]
    pub admin_listen: String,
    /// Bearer token required to access admin endpoints. Empty string means
    /// no auth is enforced (only sensible when `admin_listen` is also unset).
    #[serde(default = "default_admin_token")]
    pub admin_token: String,
    /// HMAC secret used to sign/verify the `x-chain-token` used by
    /// `mode = "loopback"` url steps (M12). Empty string (the default)
    /// means loopback is disabled: `config::load::validate` rejects any
    /// `mode = "loopback"` step unless this is non-empty.
    #[serde(default = "default_loopback_secret")]
    pub loopback_secret: String,
    /// Maximum number of loopback hops a single chain may take before the
    /// gateway refuses to resume it further (M12).
    #[serde(default = "default_max_hops")]
    pub max_hops: u32,
    /// HTTP path the gateway listens on for loopback resume callbacks (M12).
    #[serde(default = "default_callback_path")]
    pub callback_path: String,
    /// Maximum number of loopback continuations (design doc §4.5, M12) that
    /// may be concurrently parked in `loopback::Registry` before
    /// `proxy::dispatch_loopback` refuses to park any more and answers with
    /// 503 instead. Without a cap, a client that keeps opening new chains
    /// against a `mode = "loopback"` step whose tool never calls back can
    /// grow the registry without bound for as long as
    /// `server::CONTINUATION_SWEEP_TTL` allows an entry to live.
    #[serde(default = "default_max_parked")]
    pub max_parked: usize,
    /// Maximum size (in bytes) of a single buffered, not-yet-terminated SSE
    /// event before [`crate::sse::SseFramer`] gives up on it and returns
    /// [`crate::sse::SseError::EventTooLarge`] (design doc M16). `None` (the
    /// default — left unset in config) means "use the framer's own built-in
    /// ceiling", [`crate::sse::DEFAULT_MAX_EVENT_BYTES`] (1 MiB) — see
    /// [`Gateway::max_event_bytes`] for the resolved value. `Some(0)` is
    /// rejected at load (`config::load::validate`).
    #[serde(default)]
    pub max_event_bytes: Option<usize>,
    /// Maximum size (in bytes) of a response body the gateway will buffer on
    /// the buffered response path (routes with `on_response` steps, or
    /// translate routes). `None` (the default — left unset in config) falls
    /// back to [`Gateway::max_body_bytes`] so behavior is unchanged from
    /// before this field existed; it does NOT mean "unbounded". Bodies larger
    /// than the resolved value are handled per `oversize` (`reject` errors,
    /// `stream_through` streams the body through unbuffered). `Some(0)` is
    /// rejected at load (`config::load::validate`). See
    /// [`Gateway::max_response_bytes`] for the resolved value.
    #[serde(default)]
    pub max_response_bytes: Option<usize>,
}

impl Default for Gateway {
    fn default() -> Self {
        Self {
            schema_version: default_schema_version(),
            allow_scripts: false,
            listen: default_listen(),
            expose_headers: default_expose_headers(),
            max_context_bytes: default_max_context_bytes(),
            max_body_bytes: default_max_body_bytes(),
            oversize: default_oversize(),
            max_inflight: default_max_inflight(),
            upstream_timeout_ms: default_upstream_timeout_ms(),
            admin_listen: default_admin_listen(),
            admin_token: default_admin_token(),
            loopback_secret: default_loopback_secret(),
            max_hops: default_max_hops(),
            callback_path: default_callback_path(),
            max_parked: default_max_parked(),
            max_event_bytes: None,
            max_response_bytes: None,
        }
    }
}

impl Gateway {
    /// The effective response-body buffering ceiling: the configured
    /// `max_response_bytes` if set, else [`Gateway::max_body_bytes`] — an
    /// unset field preserves the prior behavior exactly (the buffered
    /// response path used `max_body_bytes` before this field existed).
    pub fn max_response_bytes(&self) -> usize {
        self.max_response_bytes.unwrap_or(self.max_body_bytes)
    }

    /// The effective per-event SSE size ceiling: the configured
    /// `max_event_bytes` if set, else [`crate::sse::DEFAULT_MAX_EVENT_BYTES`]
    /// (1 MiB) — the same constant `SseFramer::new()` used before this field
    /// existed, so an unset field preserves prior behavior exactly.
    pub fn max_event_bytes(&self) -> usize {
        self.max_event_bytes
            .unwrap_or(crate::sse::DEFAULT_MAX_EVENT_BYTES)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Route {
    pub id: String,
    pub upstream: String,
    #[serde(default, rename = "step")]
    pub steps: Vec<Step>,
    #[serde(default)]
    pub adapter: Option<RouteAdapter>,
    /// `[route.translate]`: cross-provider request/response translation for
    /// this route (M15). Unlike `RouteAdapter`'s same-provider
    /// ingress/egress pairing, `translate.from`/`translate.to` may name
    /// *different* providers — the actual proxy-time translation is wired up
    /// in a later M15 task; this task only carries the config shape and its
    /// load-time validation (see `config::load::validate`).
    #[serde(default)]
    pub translate: Option<Translate>,
}

/// `[route.translate]`: names the source (`from`) and target (`to`) provider
/// wire formats this route translates between (M15 design doc). `model`, if
/// set, pins the target-provider model id the translated request is sent
/// with; it must resolve in the model registry under `to` (see
/// `config::load::validate`). `report_header` toggles whether the gateway
/// adds a diagnostic header describing the translation fidelity to the
/// response (default `false`).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Translate {
    pub from: String,
    pub to: String,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub report_header: bool,
}

/// `[route.adapter]`: names the ingress/egress provider wire formats this
/// route speaks, resolved by name via `llm::adapter::adapter_for` (design
/// doc M9). `egress` translation to a *different* provider than `ingress` is
/// out of scope for M9 (see `config::load::validate`); leaving `egress`
/// unset (or equal to `ingress`) is the only supported shape today.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteAdapter {
    #[serde(default)]
    pub ingress: Option<String>,
    #[serde(default)]
    pub egress: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Step {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default = "default_hook")]
    pub hook: Hook,
    #[serde(rename = "type")]
    pub type_: StepType,
    #[serde(default)]
    pub url: Option<String>,
    /// `type = "url"`-only mode (`transform` vs. `loopback`). Deliberately
    /// kept separate from `script_mode` below rather than made a shared
    /// polymorphic field, so each step type has its own unambiguous,
    /// strongly-typed mode enum (design doc §4.5, M11 brief).
    #[serde(default = "default_url_mode")]
    pub mode: UrlMode,
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    #[serde(default = "default_on_error")]
    pub on_error: OnError,
    /// Whether an `on_stream` step observes chunks read-only or may rewrite
    /// them in place. Only meaningful when `hook = on_stream`; ignored
    /// otherwise.
    #[serde(default)]
    pub chunk_mode: ChunkMode,
    /// Marks this step as a safety guardrail. Guardrail steps have tighter
    /// validation constraints (see `config::load::validate`): they must not
    /// merely observe streamed chunks, and they must not be configured to
    /// fail open.
    #[serde(default)]
    pub is_guardrail: bool,
    /// `type = "script"`-only mode: whether the script process is spawned
    /// fresh per invocation (`oneshot`) or is a long-lived worker the
    /// gateway talks to repeatedly (`worker`). A separate field from `mode`
    /// (which stays url-only) rather than an untagged/polymorphic reuse of
    /// `mode`, per the M11 brief.
    #[serde(default)]
    pub script_mode: ScriptMode,
    /// `type = "script"`-only: the command (argv) to spawn. Populated now so
    /// config authors can write complete script steps; actually running the
    /// command is Task 2/3, not this task.
    #[serde(default)]
    pub cmd: Vec<String>,
    /// `type = "wasm"`-only: the path to the compiled `.wasm`/`.wat` module
    /// this step runs (see `step::wasm::WasmStep`). `validate` rejects
    /// `type = "wasm"` steps that leave this unset; it is ignored for other
    /// step types.
    #[serde(default)]
    pub wasm: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
#[allow(clippy::enum_variant_names)] // "On*" is the deliberate hook-name convention (brief-mandated)
pub enum Hook {
    OnRequest,
    OnResponse,
    OnStream,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepType {
    Url,
    Script,
    Wasm,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UrlMode {
    Transform,
    Loopback,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnError {
    FailOpen,
    FailClosed,
}

/// Whether an `on_stream` step may only read chunks (`Observe`, the
/// default) or may rewrite them in place (`Mutate`). See design doc §4.5.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChunkMode {
    #[default]
    Observe,
    Mutate,
}

/// `type = "script"` process lifecycle: a fresh subprocess per invocation
/// (`Oneshot`, the default) or a long-lived worker process (`Worker`). See
/// design doc §4.5.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScriptMode {
    #[default]
    Oneshot,
    Worker,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Oversize {
    Reject,
    StreamThrough,
}

fn default_schema_version() -> u32 {
    1
}
fn default_listen() -> String {
    "127.0.0.1:8080".to_string()
}
fn default_hook() -> Hook {
    Hook::OnRequest
}
fn default_url_mode() -> UrlMode {
    UrlMode::Transform
}
fn default_timeout_ms() -> u64 {
    1000
}
fn default_on_error() -> OnError {
    OnError::FailClosed
}
fn default_expose_headers() -> Vec<String> {
    Vec::new()
}
fn default_max_context_bytes() -> usize {
    65536
}
fn default_max_body_bytes() -> usize {
    8_388_608
}
fn default_oversize() -> Oversize {
    Oversize::Reject
}
fn default_max_inflight() -> usize {
    1024
}
fn default_upstream_timeout_ms() -> u64 {
    60_000
}
fn default_admin_listen() -> String {
    String::new()
}
fn default_admin_token() -> String {
    String::new()
}
fn default_loopback_secret() -> String {
    String::new()
}
fn default_max_hops() -> u32 {
    8
}
fn default_callback_path() -> String {
    "/__sluice/loopback".to_string()
}
fn default_max_parked() -> usize {
    1024
}

impl Step {
    /// Effective namespace/identity: explicit name, else `<type>:<index>`.
    pub fn effective_name(&self, index: usize) -> String {
        match &self.name {
            Some(n) => n.clone(),
            None => {
                let ty = match self.type_ {
                    StepType::Url => "url",
                    StepType::Script => "script",
                    StepType::Wasm => "wasm",
                };
                format!("{ty}:{index}")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gateway_default_listen_and_version() {
        let g = Gateway::default();
        assert_eq!(g.listen, "127.0.0.1:8080");
        assert_eq!(g.schema_version, 1);
    }

    #[test]
    fn gateway_expose_headers_defaults_empty() {
        let g = Gateway::default();
        assert!(g.expose_headers.is_empty());
    }

    #[test]
    fn gateway_expose_headers_parses_provided_list() {
        let cfg: Config = toml::from_str(
            r#"
            [gateway]
            expose_headers = ["x-sluice-timing", "x-sluice-cost"]
            [[route]]
            id = "r"
            upstream = "http://u"
        "#,
        )
        .unwrap();
        assert_eq!(
            cfg.gateway.expose_headers,
            vec!["x-sluice-timing".to_string(), "x-sluice-cost".to_string()]
        );
    }

    #[test]
    fn gateway_max_context_bytes_defaults_to_65536() {
        let g = Gateway::default();
        assert_eq!(g.max_context_bytes, 65536);
    }

    #[test]
    fn gateway_max_context_bytes_parses_provided_value() {
        let cfg: Config = toml::from_str(
            r#"
            [gateway]
            max_context_bytes = 1024
            [[route]]
            id = "r"
            upstream = "http://u"
        "#,
        )
        .unwrap();
        assert_eq!(cfg.gateway.max_context_bytes, 1024);
    }

    #[test]
    fn gateway_load_survival_defaults() {
        let g = Gateway::default();
        assert_eq!(g.max_body_bytes, 8_388_608);
        assert_eq!(g.oversize, Oversize::Reject);
        assert_eq!(g.max_inflight, 1024);
        assert_eq!(g.upstream_timeout_ms, 60_000);
    }

    #[test]
    fn gateway_max_response_bytes_falls_back_to_max_body_bytes() {
        let g = Gateway::default();
        assert_eq!(g.max_response_bytes, None);
        assert_eq!(g.max_response_bytes(), g.max_body_bytes);
    }

    #[test]
    fn gateway_max_response_bytes_uses_configured_value_when_set() {
        let cfg: Config = toml::from_str(
            r#"
            [gateway]
            max_body_bytes = 1000
            max_response_bytes = 42
            [[route]]
            id = "r"
            upstream = "http://u"
        "#,
        )
        .unwrap();
        assert_eq!(cfg.gateway.max_response_bytes, Some(42));
        assert_eq!(cfg.gateway.max_response_bytes(), 42);
    }

    #[test]
    fn gateway_allow_scripts_defaults_false() {
        let g = Gateway::default();
        assert!(!g.allow_scripts);
    }

    #[test]
    fn gateway_allow_scripts_parses_provided_value() {
        let cfg: Config = toml::from_str(
            r#"
            [gateway]
            allow_scripts = true
            [[route]]
            id = "r"
            upstream = "http://u"
        "#,
        )
        .unwrap();
        assert!(cfg.gateway.allow_scripts);
    }

    #[test]
    fn step_chunk_mode_is_guardrail_script_mode_default() {
        let cfg: Config = toml::from_str(
            r#"
            [[route]]
            id = "r"
            upstream = "http://u"
              [[route.step]]
              type = "url"
              url = "http://step"
        "#,
        )
        .unwrap();
        let step = &cfg.routes[0].steps[0];
        assert_eq!(step.chunk_mode, ChunkMode::Observe);
        assert!(!step.is_guardrail);
        assert_eq!(step.script_mode, ScriptMode::Oneshot);
        assert!(step.cmd.is_empty());
        assert_eq!(step.wasm, None);
    }

    #[test]
    fn step_wasm_field_parses_provided_path() {
        let cfg: Config = toml::from_str(
            r#"
            [[route]]
            id = "r"
            upstream = "http://u"
              [[route.step]]
              type = "wasm"
              wasm = "modules/redact.wasm"
        "#,
        )
        .unwrap();
        let step = &cfg.routes[0].steps[0];
        assert_eq!(step.type_, StepType::Wasm);
        assert_eq!(step.wasm.as_deref(), Some("modules/redact.wasm"));
    }

    #[test]
    fn step_chunk_mode_is_guardrail_script_mode_parse_explicit() {
        let cfg: Config = toml::from_str(
            r#"
            [gateway]
            allow_scripts = true
            [[route]]
            id = "r"
            upstream = "http://u"
              [[route.step]]
              hook = "on_stream"
              type = "script"
              chunk_mode = "mutate"
              is_guardrail = true
              script_mode = "worker"
              cmd = ["/bin/guard"]
        "#,
        )
        .unwrap();
        let step = &cfg.routes[0].steps[0];
        assert_eq!(step.chunk_mode, ChunkMode::Mutate);
        assert!(step.is_guardrail);
        assert_eq!(step.script_mode, ScriptMode::Worker);
        assert_eq!(step.cmd, vec!["/bin/guard".to_string()]);
    }

    #[test]
    fn gateway_admin_fields_default_empty() {
        let g = Gateway::default();
        assert_eq!(g.admin_listen, "");
        assert_eq!(g.admin_token, "");
    }

    #[test]
    fn gateway_loopback_fields_have_expected_defaults() {
        let g = Gateway::default();
        assert_eq!(g.loopback_secret, "");
        assert_eq!(g.max_hops, 8);
        assert_eq!(g.callback_path, "/__sluice/loopback");
        assert_eq!(g.max_parked, 1024);
    }

    #[test]
    fn gateway_max_event_bytes_defaults_none_and_helper_returns_sse_default() {
        let g = Gateway::default();
        assert_eq!(g.max_event_bytes, None);
        assert_eq!(g.max_event_bytes(), crate::sse::DEFAULT_MAX_EVENT_BYTES);
    }

    #[test]
    fn gateway_max_event_bytes_parses_provided_value() {
        let cfg: Config = toml::from_str(
            r#"
            [gateway]
            max_event_bytes = 4096
            [[route]]
            id = "r"
            upstream = "http://u"
        "#,
        )
        .unwrap();
        assert_eq!(cfg.gateway.max_event_bytes, Some(4096));
        assert_eq!(cfg.gateway.max_event_bytes(), 4096);
    }

    #[test]
    fn gateway_max_parked_parses_provided_value() {
        let cfg: Config = toml::from_str(
            r#"
            [gateway]
            max_parked = 4
            [[route]]
            id = "r"
            upstream = "http://u"
        "#,
        )
        .unwrap();
        assert_eq!(cfg.gateway.max_parked, 4);
    }

    #[test]
    fn gateway_loopback_fields_parse_provided_values() {
        let cfg: Config = toml::from_str(
            r#"
            [gateway]
            loopback_secret = "s3cr3t"
            max_hops = 3
            callback_path = "/custom/loopback"
            [[route]]
            id = "r"
            upstream = "http://u"
        "#,
        )
        .unwrap();
        assert_eq!(cfg.gateway.loopback_secret, "s3cr3t");
        assert_eq!(cfg.gateway.max_hops, 3);
        assert_eq!(cfg.gateway.callback_path, "/custom/loopback");
    }

    #[test]
    fn effective_name_prefers_explicit() {
        let s = Step {
            name: Some("redact".into()),
            hook: Hook::OnRequest,
            type_: StepType::Url,
            url: Some("http://x".into()),
            mode: UrlMode::Transform,
            timeout_ms: 1000,
            on_error: OnError::FailClosed,
            chunk_mode: ChunkMode::Observe,
            is_guardrail: false,
            script_mode: ScriptMode::Oneshot,
            cmd: Vec::new(),
            wasm: None,
        };
        assert_eq!(s.effective_name(3), "redact");
    }

    #[test]
    fn effective_name_falls_back_to_type_index() {
        let s = Step {
            name: None,
            hook: Hook::OnRequest,
            type_: StepType::Url,
            url: Some("http://x".into()),
            mode: UrlMode::Transform,
            timeout_ms: 1000,
            on_error: OnError::FailClosed,
            chunk_mode: ChunkMode::Observe,
            is_guardrail: false,
            script_mode: ScriptMode::Oneshot,
            cmd: Vec::new(),
            wasm: None,
        };
        assert_eq!(s.effective_name(2), "url:2");
    }
}
