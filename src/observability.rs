//! Observability foundations: structured JSON logging, correlation ids, and
//! Prometheus metrics.
//!
//! `init_logging` wires up a JSON-formatted `tracing` subscriber so every
//! request/step span the gateway emits is machine-parseable. Correlation
//! ids let a single logical request be traced across the gateway and its
//! step services even though each step call is a separate HTTP round trip
//! (see `envelope::Envelope::correlation_id`).
//!
//! `init_metrics` installs the global Prometheus recorder used by the
//! `metrics` facade macros (`counter!`, `gauge!`, `histogram!`) and returns a
//! [`MetricsHandle`] that can render the current metric snapshot as
//! Prometheus text exposition for a scrape endpoint.

use std::sync::OnceLock;

use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

/// Total requests handled, labeled by route and outcome.
pub const METRIC_REQUESTS_TOTAL: &str = "sluice_requests_total";
/// Number of requests currently in flight.
pub const METRIC_INFLIGHT: &str = "sluice_inflight";
/// Request duration histogram (seconds), labeled by `route`.
pub const METRIC_REQUEST_DURATION_SECONDS: &str = "sluice_request_duration_seconds";
/// Upstream response status counter, labeled by `route` and `status`.
pub const METRIC_UPSTREAM_STATUS_TOTAL: &str = "sluice_upstream_status_total";
/// Count of requests short-circuited (e.g. by a circuit breaker or cache).
pub const METRIC_SHORT_CIRCUIT_TOTAL: &str = "sluice_short_circuit_total";
/// Count of requests aborted before completion.
pub const METRIC_ABORT_TOTAL: &str = "sluice_abort_total";
/// Count of errors raised by individual pipeline steps.
pub const METRIC_STEP_ERROR_TOTAL: &str = "sluice_step_error_total";
/// Count of `on_stream` observe chunk envelopes DROPPED because the bounded
/// tee queue was full (design doc M10 §9) — the client stream is never
/// slowed down to make room, so a burst of events under a slow/overloaded
/// observe step sheds instead.
pub const METRIC_STREAM_SHED: &str = "sluice_stream_shed_total";

/// RAII guard for the live in-flight gauge ([`METRIC_INFLIGHT`]): increments
/// the gauge by 1.0 on construction and decrements it by 1.0 on [`Drop`].
///
/// Constructed the moment `proxy::handle` is granted its concurrency-limit
/// permit, and folded into the same response-body wrapper the permit itself
/// travels in (`reconstruct::attach_permit`/`GuardedBody`) so both share the
/// exact same lifetime: the gauge only returns to baseline once the final
/// response body has been fully delivered to the client or dropped (client
/// disconnect), covering the *whole* request lifetime — not just until
/// `handle_inner` returns headers — and decrementing exactly once even on an
/// early return or panic, since `Drop` always runs.
pub struct InflightGuard {
    _private: (),
}

impl InflightGuard {
    /// Construct a new guard, incrementing [`METRIC_INFLIGHT`] by 1.0.
    pub fn new() -> Self {
        metrics::gauge!(METRIC_INFLIGHT).increment(1.0);
        Self { _private: () }
    }
}

impl Default for InflightGuard {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        metrics::gauge!(METRIC_INFLIGHT).decrement(1.0);
    }
}

/// Process-wide cache of the installed Prometheus handle. `install_recorder`
/// errors if a global recorder is already set (e.g. because an earlier test
/// in the same process already installed one), so we install at most once
/// and hand out clones of the cached handle thereafter.
static METRICS_HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

/// A handle to the process's Prometheus recorder, used to render the
/// current metric snapshot as Prometheus text exposition.
#[derive(Clone, Debug)]
pub struct MetricsHandle {
    inner: PrometheusHandle,
}

impl MetricsHandle {
    /// Render the current metrics snapshot in Prometheus text exposition
    /// format.
    pub fn render(&self) -> String {
        self.inner.render()
    }
}

/// Install the global Prometheus recorder (once per process) and return a
/// handle to it. Safe to call repeatedly, including across tests that share
/// a process: only the first call installs the recorder; later calls clone
/// the cached handle instead of panicking on `install_recorder`'s "already
/// installed" error.
pub fn init_metrics() -> MetricsHandle {
    let handle = METRICS_HANDLE.get_or_init(|| {
        PrometheusBuilder::new()
            .install_recorder()
            .expect("failed to install Prometheus recorder")
    });
    MetricsHandle {
        inner: handle.clone(),
    }
}

/// Install a JSON `tracing_subscriber` fmt layer with an `EnvFilter`
/// (default `info` when `RUST_LOG` is unset). Uses `try_init` so calling
/// this more than once (e.g. across tests in the same process) never
/// panics — only the first call actually installs the global subscriber.
pub fn init_logging() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .json()
        .with_env_filter(filter)
        .try_init();
}

/// A correlation id is considered valid when it is ASCII printable (no
/// control characters), and between 1 and 200 bytes long. This bounds the
/// size of an attacker-supplied inbound id before it's echoed into logs
/// and downstream step envelopes.
pub fn is_valid_correlation_id(s: &str) -> bool {
    let len = s.len();
    if !(1..=200).contains(&len) {
        return false;
    }
    s.chars().all(|c| c.is_ascii() && !c.is_ascii_control())
}

/// Adopt a valid inbound correlation id, or generate a fresh one. Called
/// once per request with the inbound `x-request-id` header value (if any);
/// the result is threaded through the request's envelopes and log spans.
pub fn correlation_id_from(inbound: Option<&str>) -> String {
    match inbound {
        Some(s) if is_valid_correlation_id(s) => s.to_string(),
        _ => uuid::Uuid::new_v4().to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn init_metrics_renders_recorded_counter() {
        let handle = init_metrics();
        metrics::counter!(METRIC_REQUESTS_TOTAL).increment(1);
        let rendered = handle.render();
        assert!(
            rendered.contains(METRIC_REQUESTS_TOTAL),
            "expected render() to contain {METRIC_REQUESTS_TOTAL:?}, got: {rendered}"
        );
    }

    #[test]
    fn init_metrics_is_safe_to_call_repeatedly() {
        let first = init_metrics();
        let second = init_metrics();
        first.render();
        second.render();
    }

    #[test]
    fn inflight_guard_moves_gauge_and_returns_to_baseline_on_drop() {
        // Route this guard's gauge writes to a thread-local recorder we fully
        // own, rather than the process-wide one `init_metrics()` installs.
        // `sluice_inflight` is unlabeled and shared, and other tests in this
        // binary (`reconstruct::tests`) also construct `InflightGuard`, so an
        // exact rendered value against the global gauge would race them. A
        // local recorder makes the +1/-1 assertions deterministic.
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        metrics::with_local_recorder(&recorder, || {
            {
                let _guard = InflightGuard::new();
                let rendered = handle.render();
                assert!(
                    rendered.contains(&format!("{METRIC_INFLIGHT} 1")),
                    "expected inflight gauge to read 1 while guard is alive, got: {rendered}"
                );
            }
            let rendered = handle.render();
            assert!(
                rendered.contains(&format!("{METRIC_INFLIGHT} 0")),
                "expected inflight gauge to return to baseline after guard dropped, got: {rendered}"
            );
        });
    }

    #[test]
    fn init_logging_does_not_panic_when_called_repeatedly() {
        init_logging();
        init_logging();
    }

    #[test]
    fn valid_correlation_id_accepts_ascii_printable() {
        assert!(is_valid_correlation_id("abc-123_XYZ"));
        assert!(is_valid_correlation_id("a"));
        assert!(is_valid_correlation_id("has spaces and punctuation!"));
    }

    #[test]
    fn valid_correlation_id_rejects_empty() {
        assert!(!is_valid_correlation_id(""));
    }

    #[test]
    fn valid_correlation_id_rejects_over_200_bytes() {
        let too_long = "a".repeat(201);
        assert!(!is_valid_correlation_id(&too_long));
        let exactly_200 = "a".repeat(200);
        assert!(is_valid_correlation_id(&exactly_200));
    }

    #[test]
    fn valid_correlation_id_rejects_control_chars() {
        assert!(!is_valid_correlation_id("abc\ndef"));
        assert!(!is_valid_correlation_id("abc\tdef"));
        assert!(!is_valid_correlation_id("abc\r\ndef"));
        assert!(!is_valid_correlation_id("\u{7f}"));
    }

    #[test]
    fn valid_correlation_id_rejects_non_ascii() {
        assert!(!is_valid_correlation_id("héllo"));
    }

    #[test]
    fn correlation_id_from_adopts_valid_inbound() {
        assert_eq!(correlation_id_from(Some("req-42")), "req-42");
    }

    #[test]
    fn correlation_id_from_generates_when_none() {
        let id = correlation_id_from(None);
        assert!(is_valid_correlation_id(&id));
        assert!(uuid::Uuid::parse_str(&id).is_ok());
    }

    #[test]
    fn correlation_id_from_generates_when_inbound_invalid() {
        let too_long = "a".repeat(201);
        let generated = correlation_id_from(Some(&too_long));
        assert_ne!(generated, too_long);
        assert!(uuid::Uuid::parse_str(&generated).is_ok());

        let with_control = "bad\nid";
        let generated = correlation_id_from(Some(with_control));
        assert_ne!(generated, with_control);
        assert!(uuid::Uuid::parse_str(&generated).is_ok());
    }

    #[test]
    fn correlation_id_from_generates_when_inbound_empty() {
        let generated = correlation_id_from(Some(""));
        assert!(uuid::Uuid::parse_str(&generated).is_ok());
    }
}
