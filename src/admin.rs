//! Admin service: a small, separate HTTP listener exposing `/healthz`,
//! `/readyz`, and `/metrics`. Kept apart from the data-plane listener
//! (`server::serve_on`) so liveness/readiness probes and metrics scrapes
//! never compete with proxied traffic for the same accept loop, and so an
//! operator can put the admin port on a different network/ACL than the
//! data plane.
//!
//! `/readyz` and `/metrics` are protected by an optional bearer token (see
//! [`authorized`]); `/healthz` is always open since orchestrators need it
//! reachable before any credentials are provisioned.

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder;
use subtle::ConstantTimeEq;
use tokio::net::TcpListener;

use crate::observability::MetricsHandle;

/// Boxed body type for admin responses. Deliberately local to this module
/// rather than reusing `reconstruct::ResponseBody`: admin responses are
/// gateway-authored text, not proxied upstream data, so they don't need
/// the transparency reconstruction pipeline.
pub type AdminBody = BoxBody<Bytes, std::convert::Infallible>;

/// Shared readiness flag. Cloning shares the same underlying flag; flip it
/// with [`Ready::set_ready`] once the data-plane listener is bound, so
/// `/readyz` reports 503 during startup and 200 once traffic can flow.
#[derive(Clone)]
pub struct Ready(Arc<AtomicBool>);

impl Ready {
    pub fn new() -> Self {
        Ready(Arc::new(AtomicBool::new(false)))
    }

    pub fn set_ready(&self) {
        self.0.store(true, Ordering::SeqCst);
    }

    pub fn is_ready(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
}

impl Default for Ready {
    fn default() -> Self {
        Self::new()
    }
}

/// Decide whether a request to `path` is authorized.
///
/// `/healthz` is always authorized (orchestrators must be able to reach it
/// before any token is configured/distributed). For every other path
/// (namely `/readyz` and `/metrics`), the request is authorized if `token`
/// is empty (auth disabled) or if `auth_header` is exactly
/// `Some("Bearer <token>")`.
///
/// The actual byte comparison is constant-time (`subtle::ConstantTimeEq`),
/// never a plain `==`/`PartialEq` on the strings: the bearer token is a
/// secret compared against attacker-supplied input on every request to a
/// protected admin path, so a short-circuiting `==` would let response
/// timing leak how many leading bytes of a guessed token were correct. The
/// length check ahead of `ct_eq` is not itself a timing leak worth
/// avoiding — `expected`'s length is a public constant (the configured
/// token's own length), not secret material a timing attack against
/// `auth_header` needs to recover.
pub fn authorized(path: &str, token: &str, auth_header: Option<&str>) -> bool {
    if path == "/healthz" || token.is_empty() {
        return true;
    }
    let expected = format!("Bearer {token}");
    match auth_header {
        Some(actual) if actual.len() == expected.len() => {
            actual.as_bytes().ct_eq(expected.as_bytes()).into()
        }
        _ => false,
    }
}

fn text_response(
    status: StatusCode,
    content_type: &str,
    body: impl Into<Bytes>,
) -> Response<AdminBody> {
    Response::builder()
        .status(status)
        .header("content-type", content_type)
        .body(Full::new(body.into()).boxed())
        .expect("static admin response is always valid")
}

/// Pure routing decision: given a path, the current readiness flag, and a
/// metrics snapshot, build the response body. Kept separate from auth so it
/// can be unit-tested without constructing a hyper `Request`.
fn route_admin(path: &str, ready: &Ready, metrics: &MetricsHandle) -> Response<AdminBody> {
    match path {
        "/healthz" => text_response(StatusCode::OK, "text/plain; charset=utf-8", "ok"),
        "/readyz" => {
            if ready.is_ready() {
                text_response(StatusCode::OK, "text/plain; charset=utf-8", "ok")
            } else {
                text_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "text/plain; charset=utf-8",
                    "not ready",
                )
            }
        }
        "/metrics" => text_response(
            StatusCode::OK,
            "text/plain; version=0.0.4",
            metrics.render(),
        ),
        _ => text_response(
            StatusCode::NOT_FOUND,
            "text/plain; charset=utf-8",
            "not found",
        ),
    }
}

/// Handle one admin request: enforce [`authorized`], then dispatch via
/// [`route_admin`].
pub async fn handle(
    req: Request<Incoming>,
    ready: Ready,
    metrics: MetricsHandle,
    token: String,
) -> Response<AdminBody> {
    let path = req.uri().path().to_string();
    let auth_header = req
        .headers()
        .get(hyper::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());

    if !authorized(&path, &token, auth_header) {
        return text_response(
            StatusCode::UNAUTHORIZED,
            "text/plain; charset=utf-8",
            "unauthorized",
        );
    }

    route_admin(&path, &ready, &metrics)
}

/// Bind and serve the admin listener, mirroring `server::serve_on`'s
/// bind/accept-loop/serve_connection shape.
pub async fn serve_admin(
    listen: &str,
    ready: Ready,
    metrics: MetricsHandle,
    token: String,
    shutdown: impl Future<Output = ()>,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(listen).await?;

    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            accepted = listener.accept() => {
                let (stream, _peer) = accepted?;
                let io = TokioIo::new(stream);
                let ready = ready.clone();
                let metrics = metrics.clone();
                let token = token.clone();
                tokio::spawn(async move {
                    let service = service_fn(move |req| {
                        let ready = ready.clone();
                        let metrics = metrics.clone();
                        let token = token.clone();
                        async move {
                            Ok::<_, std::convert::Infallible>(handle(req, ready, metrics, token).await)
                        }
                    });
                    let _ = Builder::new(TokioExecutor::new())
                        .serve_connection(io, service)
                        .await;
                });
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observability::init_metrics;

    #[test]
    fn healthz_always_authorized() {
        assert!(authorized("/healthz", "", None));
        assert!(authorized("/healthz", "secret", None));
        assert!(authorized("/healthz", "secret", Some("Bearer wrong")));
    }

    #[test]
    fn readyz_and_metrics_authorized_when_no_token_configured() {
        assert!(authorized("/readyz", "", None));
        assert!(authorized("/metrics", "", None));
    }

    #[test]
    fn readyz_and_metrics_unauthorized_when_token_set_and_no_header() {
        assert!(!authorized("/readyz", "secret", None));
        assert!(!authorized("/metrics", "secret", None));
    }

    #[test]
    fn readyz_and_metrics_authorized_with_correct_bearer() {
        assert!(authorized("/readyz", "secret", Some("Bearer secret")));
        assert!(authorized("/metrics", "secret", Some("Bearer secret")));
    }

    #[test]
    fn readyz_and_metrics_unauthorized_with_wrong_bearer() {
        assert!(!authorized("/readyz", "secret", Some("Bearer nope")));
        assert!(!authorized(
            "/metrics",
            "secret",
            Some("Bearer secretwrong")
        ));
        assert!(!authorized("/metrics", "secret", Some("secret")));
    }

    /// Fix C (constant-time compare): a wrong header of the EXACT SAME
    /// length as the expected `Bearer <token>` string must still be
    /// rejected — this is the case that would slip through a naive
    /// length-first-return-early shortcut that skipped the byte comparison
    /// whenever lengths matched, and it's the specific shape `ct_eq` (not
    /// `==`) exists to compare safely.
    #[test]
    fn rejects_same_length_wrong_bearer() {
        assert!(!authorized("/readyz", "secret", Some("Bearer wrongo")));
    }

    /// Differs from the expected `Bearer <token>` in only its LAST byte —
    /// the shape most likely to expose a short-circuiting comparison (every
    /// byte but the last matches).
    #[test]
    fn rejects_bearer_differing_only_in_last_byte() {
        assert!(!authorized("/readyz", "secret", Some("Bearer secreT")));
    }

    #[test]
    fn functional_correctness_of_constant_time_compare_accept_and_reject() {
        // Same-length correct token: accepted.
        assert!(authorized(
            "/metrics",
            "a-long-shared-secret",
            Some("Bearer a-long-shared-secret")
        ));
        // Same-length incorrect token (last char differs): rejected.
        assert!(!authorized(
            "/metrics",
            "a-long-shared-secret",
            Some("Bearer a-long-shared-secreX")
        ));
        // Different-length incorrect token: rejected.
        assert!(!authorized(
            "/metrics",
            "a-long-shared-secret",
            Some("Bearer short")
        ));
        // No header at all: rejected.
        assert!(!authorized("/metrics", "a-long-shared-secret", None));
    }

    #[test]
    fn route_admin_healthz_returns_200_ok() {
        let ready = Ready::new();
        let metrics = init_metrics();
        let resp = route_admin("/healthz", &ready, &metrics);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn route_admin_readyz_returns_503_before_ready() {
        let ready = Ready::new();
        let metrics = init_metrics();
        let resp = route_admin("/readyz", &ready, &metrics);
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn route_admin_readyz_returns_200_once_ready() {
        let ready = Ready::new();
        ready.set_ready();
        let metrics = init_metrics();
        let resp = route_admin("/readyz", &ready, &metrics);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn route_admin_metrics_returns_200_with_prometheus_content_type() {
        let ready = Ready::new();
        let metrics = init_metrics();
        let resp = route_admin("/metrics", &ready, &metrics);
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get("content-type").unwrap(),
            "text/plain; version=0.0.4"
        );
    }

    #[test]
    fn route_admin_unknown_path_returns_404() {
        let ready = Ready::new();
        let metrics = init_metrics();
        let resp = route_admin("/nope", &ready, &metrics);
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn ready_defaults_to_not_ready() {
        let ready = Ready::new();
        assert!(!ready.is_ready());
        ready.set_ready();
        assert!(ready.is_ready());
    }

    #[test]
    fn ready_clone_shares_state() {
        let ready = Ready::new();
        let clone = ready.clone();
        clone.set_ready();
        assert!(ready.is_ready());
    }
}
