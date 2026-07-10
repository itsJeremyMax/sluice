//! The transparency boundary (design doc §7): the single place that decides
//! which headers survive onto an outbound message, in either direction.
//!
//! Nothing should write a header onto an outbound request/response without
//! passing through [`sanitize_egress_headers`] first.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use http_body_util::{combinators::BoxBody, BodyExt, Full, StreamBody};
use hyper::body::{Body, Frame, SizeHint};
use hyper::header::{HeaderName, HeaderValue, CONTENT_LENGTH};
use hyper::http::response::Builder;
use hyper::{Response, StatusCode};
use tokio::sync::OwnedSemaphorePermit;

use crate::observability::InflightGuard;

/// Hop-by-hop / framing headers the gateway owns. Never forwarded verbatim in
/// either direction; framing headers (e.g. `content-length`) are recomputed
/// by the builder from the final body instead.
pub const HOP_BY_HOP: &[&str] = &[
    "connection",
    "transfer-encoding",
    "content-length",
    "keep-alive",
    "upgrade",
    "te",
];

/// True if `name` is in Sluice's internal header namespace: `x-sluice-*` or
/// exactly `x-chain-token`. Comparison is case-insensitive.
pub fn is_internal_header(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    lower.starts_with("x-sluice-") || lower == "x-chain-token"
}

/// Case-insensitive membership test against [`HOP_BY_HOP`].
pub fn is_hop_by_hop(name: &str) -> bool {
    HOP_BY_HOP.contains(&name.to_ascii_lowercase().as_str())
}

/// Which side of the gateway an outbound message is headed toward.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// Gateway -> client (the response path, including short_circuit/abort).
    ToClient,
    /// Gateway -> upstream (the request path).
    ToUpstream,
}

/// The single egress header sanitizer. Returns the header pairs from `src`
/// that are allowed to leave the gateway process in direction `dir`:
///
/// - hop-by-hop headers are always dropped (framing is recomputed by the
///   caller from the final body, e.g. `Content-Length`);
/// - `host` is dropped when `dir == ToUpstream` (kept `ToClient`, since it's
///   an ordinary header there);
/// - `x-chain-token` is always dropped in both directions — it is a
///   gateway-internal credential, never relaxable by `expose_headers`;
/// - `x-sluice-*` namespace headers are dropped unless `dir == ToClient` and
///   the lowercased name appears in `expose` (case-insensitive) — the
///   allowlist only ever relaxes the client direction, never the upstream
///   one.
pub fn sanitize_egress_headers(
    src: &BTreeMap<String, String>,
    dir: Direction,
    expose: &[String],
) -> Vec<(String, String)> {
    src.iter()
        .filter(|(name, _)| {
            let lower = name.to_ascii_lowercase();
            if is_hop_by_hop(&lower) {
                return false;
            }
            if dir == Direction::ToUpstream && lower == "host" {
                return false;
            }
            // Both `x-chain-token` and `x-sluice-*` share the internal
            // namespace test in `is_internal_header`; only `x-sluice-*` ever
            // gets the ToClient+expose exception below (a chain token never
            // starts with `x-sluice-`, so `sluice_exposed` is unconditionally
            // `false` for it, keeping it dropped in both directions exactly
            // as before de-duplication).
            if is_internal_header(&lower) {
                let sluice_exposed = dir == Direction::ToClient
                    && lower.starts_with("x-sluice-")
                    && expose.iter().any(|e| e.eq_ignore_ascii_case(&lower));
                if !sluice_exposed {
                    return false;
                }
            }
            true
        })
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// The boxed body type used for every response the gateway sends to the
/// client. Errors are `Infallible`: failures on the source side (a broken
/// upstream stream, a bad step) are converted to empty frames rather than
/// propagated, so the body itself can never fail.
pub type ResponseBody = BoxBody<Bytes, Infallible>;

/// A response body that carries an owned concurrency permit (and the
/// live-inflight gauge guard tied to it) alongside the real body, releasing
/// both only when the body itself is dropped.
///
/// This is how `max_inflight` is made to cover the *entire* request
/// lifetime for the gateway's dominant (streaming) workload: without this
/// wrapper, `proxy::handle`'s permit would drop as soon as the function
/// returned a `Response<ResponseBody>` — i.e. as soon as upstream response
/// *headers* arrived — while the body could still be streaming to the
/// client for an arbitrarily long time afterward. By moving the permit (and
/// `_inflight`) into the body, both stay held for as long as hyper is still
/// driving frames out of (or, on client disconnect, holding a reference to)
/// this body, and are released the instant the body is fully delivered or
/// dropped — so `sluice_inflight` tracks the true in-flight count, not just
/// "until headers were ready".
///
/// `poll_frame`/`size_hint`/`is_end_stream` are pure delegation to the inner
/// body; the permit/gauge fields never affect polling, only lifetime.
struct GuardedBody {
    inner: ResponseBody,
    _permit: OwnedSemaphorePermit,
    _inflight: InflightGuard,
}

impl GuardedBody {
    fn new(inner: ResponseBody, permit: OwnedSemaphorePermit, inflight: InflightGuard) -> Self {
        Self {
            inner,
            _permit: permit,
            _inflight: inflight,
        }
    }
}

impl Body for GuardedBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        // `ResponseBody` is `Pin<Box<dyn Body>>`, which is unconditionally
        // `Unpin`, so `self.get_mut()` is safe: we never move `inner` out,
        // only reborrow it to delegate the poll.
        Pin::new(&mut self.get_mut().inner).poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

/// Attach `permit` (and its paired `inflight` gauge guard) to `resp`'s body
/// so both are held until the body is fully streamed to the client or
/// dropped (client disconnect). This is the single place `proxy::handle`
/// routes every return path through, so the concurrency ceiling — and the
/// `sluice_inflight` gauge — cover the whole request lifetime rather than
/// just "until response headers are ready".
pub fn attach_permit(
    resp: Response<ResponseBody>,
    permit: OwnedSemaphorePermit,
    inflight: InflightGuard,
) -> Response<ResponseBody> {
    let (parts, body) = resp.into_parts();
    Response::from_parts(
        parts,
        BodyExt::boxed(GuardedBody::new(body, permit, inflight)),
    )
}

/// Build a bare, header-only 502 response. Used as the panic-safe fallback
/// when constructing the real response head/body somehow fails; it carries
/// no caller-controlled input, so it cannot fail itself.
fn fallback_502() -> Response<ResponseBody> {
    Response::builder()
        .status(StatusCode::BAD_GATEWAY)
        .body(Full::new(Bytes::new()).boxed())
        .expect("bare status-only response can never fail to build")
}

/// Single authority for the response *head*: clamps `status` into a valid
/// range (falling back to `502` when it isn't), then applies
/// [`sanitize_egress_headers`] for [`Direction::ToClient`], skipping any
/// header whose name or value cannot be encoded rather than panicking.
///
/// Shared by [`client_response`] and [`client_streaming_response`] so header
/// sanitization happens in exactly one place.
fn build_client_head(
    status: u16,
    headers: &BTreeMap<String, String>,
    expose: &[String],
) -> Builder {
    let status = StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY);
    let mut builder = Response::builder().status(status);
    for (name, value) in sanitize_egress_headers(headers, Direction::ToClient, expose) {
        let Ok(header_name) = HeaderName::from_bytes(name.as_bytes()) else {
            eprintln!("reconstruct: skipping invalid header name {name:?}");
            continue;
        };
        let Ok(header_value) = HeaderValue::from_str(&value) else {
            eprintln!("reconstruct: skipping invalid header value for {name:?}");
            continue;
        };
        builder = builder.header(header_name, header_value);
    }
    builder
}

/// Build a fully-buffered client response: sanitized headers, a recomputed
/// `content-length` from `body.len()`, and the raw body bytes verbatim
/// (never envelope JSON). Panic-safe: see [`build_client_head`] and
/// [`fallback_502`].
pub fn client_response(
    status: u16,
    headers: &BTreeMap<String, String>,
    body: Bytes,
    expose: &[String],
) -> Response<ResponseBody> {
    let builder = build_client_head(status, headers, expose).header(CONTENT_LENGTH, body.len());
    match builder.body(Full::new(body).boxed()) {
        Ok(resp) => resp,
        Err(_) => fallback_502(),
    }
}

/// Errors from reconstructing an outbound message.
#[derive(Debug, thiserror::Error)]
pub enum ReconstructError {
    /// The method string could not be parsed as an HTTP method.
    #[error("invalid method: {0}")]
    Method(String),
}

/// Build the outbound upstream request: parses `method`, applies
/// [`sanitize_egress_headers`] for [`Direction::ToUpstream`] (internal
/// namespace always stripped, hop-by-hop + `host` dropped — `expose` is
/// irrelevant upstream, so an empty allowlist is passed), and sets `body`
/// (reqwest recomputes `content-length` from it, so framing is correct by
/// construction). Header values that fail [`HeaderValue`] parsing are
/// skipped, not panicked. Returns the builder; the caller adds
/// `.timeout`/`.send`.
pub fn upstream_request(
    client: &reqwest::Client,
    method: &str,
    url: &str,
    headers: &BTreeMap<String, String>,
    body: Bytes,
) -> Result<reqwest::RequestBuilder, ReconstructError> {
    let parsed_method = reqwest::Method::from_bytes(method.as_bytes())
        .map_err(|_| ReconstructError::Method(method.to_string()))?;

    let mut builder = client.request(parsed_method, url).body(body);

    for (name, value) in sanitize_egress_headers(headers, Direction::ToUpstream, &[]) {
        let Ok(header_name) = HeaderName::from_bytes(name.as_bytes()) else {
            eprintln!("reconstruct: skipping invalid header name {name:?}");
            continue;
        };
        let Ok(header_value) = HeaderValue::from_str(&value) else {
            eprintln!("reconstruct: skipping invalid header value for {name:?}");
            continue;
        };
        builder = builder.header(header_name, header_value);
    }

    Ok(builder)
}

/// Build a streamed client response: sanitized headers, no `content-length`
/// (the transfer is chunked), and a body that forwards `body_stream` frames
/// as they arrive. A stream error is treated as an empty frame rather than
/// propagated or panicking, since [`ResponseBody`]'s error type is
/// `Infallible`.
pub fn client_streaming_response<S, E>(
    status: u16,
    headers: &BTreeMap<String, String>,
    body_stream: S,
    expose: &[String],
) -> Response<ResponseBody>
where
    S: Stream<Item = Result<Bytes, E>> + Send + Sync + 'static,
{
    let builder = build_client_head(status, headers, expose);
    let body = BodyExt::boxed(StreamBody::new(body_stream.map(|item| {
        let bytes = item.unwrap_or_default();
        Ok::<_, Infallible>(Frame::data(bytes))
    })));
    match builder.body(body) {
        Ok(resp) => resp,
        Err(_) => fallback_502(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn one(name: &str, value: &str) -> BTreeMap<String, String> {
        let mut m = BTreeMap::new();
        m.insert(name.to_string(), value.to_string());
        m
    }

    #[test]
    fn internal_header_dropped_to_client_with_empty_expose() {
        let src = one("x-sluice-timing", "42ms");
        let out = sanitize_egress_headers(&src, Direction::ToClient, &[]);
        assert!(out.is_empty());
    }

    #[test]
    fn internal_header_kept_to_client_when_exposed() {
        let src = one("x-sluice-timing", "42ms");
        let expose = vec!["x-sluice-timing".to_string()];
        let out = sanitize_egress_headers(&src, Direction::ToClient, &expose);
        assert_eq!(
            out,
            vec![("x-sluice-timing".to_string(), "42ms".to_string())]
        );
    }

    #[test]
    fn internal_header_always_dropped_to_upstream_even_if_exposed() {
        let src = one("x-sluice-timing", "42ms");
        let expose = vec!["x-sluice-timing".to_string()];
        let out = sanitize_egress_headers(&src, Direction::ToUpstream, &expose);
        assert!(out.is_empty());
    }

    #[test]
    fn chain_token_always_dropped_both_directions() {
        let src = one("x-chain-token", "secret");
        assert!(sanitize_egress_headers(&src, Direction::ToClient, &[]).is_empty());
        let expose = vec!["x-chain-token".to_string()];
        assert!(sanitize_egress_headers(&src, Direction::ToClient, &expose).is_empty());
        assert!(sanitize_egress_headers(&src, Direction::ToUpstream, &expose).is_empty());
    }

    #[test]
    fn hop_by_hop_headers_dropped_both_directions() {
        for name in ["connection", "content-length", "transfer-encoding"] {
            let src = one(name, "x");
            assert!(
                sanitize_egress_headers(&src, Direction::ToClient, &[]).is_empty(),
                "{name} should be dropped ToClient"
            );
            assert!(
                sanitize_egress_headers(&src, Direction::ToUpstream, &[]).is_empty(),
                "{name} should be dropped ToUpstream"
            );
        }
    }

    #[test]
    fn host_dropped_to_upstream_kept_to_client() {
        let src = one("host", "example.com");
        assert!(sanitize_egress_headers(&src, Direction::ToUpstream, &[]).is_empty());
        assert_eq!(
            sanitize_egress_headers(&src, Direction::ToClient, &[]),
            vec![("host".to_string(), "example.com".to_string())]
        );
    }

    #[test]
    fn normal_header_survives_both_directions() {
        let src = one("content-type", "application/json");
        assert_eq!(
            sanitize_egress_headers(&src, Direction::ToClient, &[]),
            vec![("content-type".to_string(), "application/json".to_string())]
        );
        assert_eq!(
            sanitize_egress_headers(&src, Direction::ToUpstream, &[]),
            vec![("content-type".to_string(), "application/json".to_string())]
        );
    }

    #[test]
    fn expose_match_is_case_insensitive() {
        // `HttpMsg` always stores header names lowercase (see http_msg::set_header),
        // so the case variance under test is in the `expose` allowlist entry.
        let src = one("x-sluice-timing", "42ms");
        let expose = vec!["X-SLUICE-TIMING".to_string()];
        let out = sanitize_egress_headers(&src, Direction::ToClient, &expose);
        assert_eq!(
            out,
            vec![("x-sluice-timing".to_string(), "42ms".to_string())]
        );
    }

    #[test]
    fn sanitize_strips_internal_headers_via_shared_helper_and_keeps_normal_header() {
        // Proves `sanitize_egress_headers` goes through the same
        // `is_internal_header` membership test the standalone helper uses
        // (no drift between the two), covering both internal-namespace
        // shapes (`x-sluice-*` and the exact `x-chain-token`) alongside an
        // ordinary header that must survive.
        let mut src = BTreeMap::new();
        src.insert("x-sluice-foo".to_string(), "internal".to_string());
        src.insert("x-chain-token".to_string(), "secret".to_string());
        src.insert("content-type".to_string(), "application/json".to_string());

        let out = sanitize_egress_headers(&src, Direction::ToClient, &[]);
        assert_eq!(
            out,
            vec![("content-type".to_string(), "application/json".to_string())]
        );
    }

    #[test]
    fn is_internal_header_matches_namespace_and_chain_token() {
        assert!(is_internal_header("x-sluice-timing"));
        assert!(is_internal_header("X-SLUICE-COST"));
        assert!(is_internal_header("x-chain-token"));
        assert!(!is_internal_header("x-custom"));
    }

    #[test]
    fn is_hop_by_hop_case_insensitive_and_excludes_host() {
        assert!(is_hop_by_hop("Content-Length"));
        assert!(is_hop_by_hop("CONNECTION"));
        assert!(!is_hop_by_hop("host"));
        assert!(!is_hop_by_hop("x-test"));
    }

    async fn body_bytes(resp: Response<ResponseBody>) -> Bytes {
        resp.into_body()
            .collect()
            .await
            .expect("boxed body with Infallible error cannot fail to collect")
            .to_bytes()
    }

    /// Unit-level proof for the M5 review finding: the permit `GuardedBody`
    /// carries must remain held for as long as the body is alive, and be
    /// released the instant it is dropped — regardless of whether that drop
    /// happens because the body was fully streamed or because the client
    /// disconnected mid-stream. `try_acquire` on a `Semaphore::new(1)` is a
    /// direct, deterministic probe of permit availability with no timing
    /// dependency.
    #[test]
    fn guarded_body_holds_permit_until_dropped() {
        let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(1));
        let permit = semaphore
            .clone()
            .try_acquire_owned()
            .expect("fresh semaphore has a permit available");
        let inner: ResponseBody = Full::new(Bytes::from_static(b"hello")).boxed();
        let guarded = GuardedBody::new(inner, permit, InflightGuard::new());

        assert!(
            semaphore.try_acquire().is_err(),
            "permit must still be held while GuardedBody is alive"
        );

        drop(guarded);

        assert!(
            semaphore.try_acquire().is_ok(),
            "permit must be released once GuardedBody is dropped"
        );
    }

    /// Same guarantee, exercised through the actual entrypoint `proxy::handle`
    /// uses (`attach_permit`) and the real collection path (`BodyExt::collect`,
    /// same as hyper driving the body to a client): the permit is only
    /// released once the response body has been fully delivered.
    #[tokio::test]
    async fn attach_permit_releases_only_after_body_fully_collected() {
        let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(1));
        let permit = semaphore
            .clone()
            .try_acquire_owned()
            .expect("fresh semaphore has a permit available");

        let resp = client_response(200, &BTreeMap::new(), Bytes::from_static(b"hello"), &[]);
        let guarded_resp = attach_permit(resp, permit, InflightGuard::new());

        assert!(
            semaphore.try_acquire().is_err(),
            "permit must be held while the guarded response's body has not been collected"
        );

        let collected = body_bytes(guarded_resp).await;
        assert_eq!(collected, Bytes::from_static(b"hello"));

        assert!(
            semaphore.try_acquire().is_ok(),
            "permit must be released once the response body has been fully delivered"
        );
    }

    #[tokio::test]
    async fn client_response_keeps_normal_header_strips_internal_and_sets_content_length() {
        let mut headers = BTreeMap::new();
        headers.insert("content-type".to_string(), "text/plain".to_string());
        headers.insert("x-sluice-x".to_string(), "internal".to_string());
        let body = Bytes::from_static(b"hello world");

        let resp = client_response(200, &headers, body.clone(), &[]);

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.headers().get("content-type").unwrap(), "text/plain");
        assert!(resp.headers().get("x-sluice-x").is_none());
        assert_eq!(
            resp.headers().get(CONTENT_LENGTH).unwrap(),
            &body.len().to_string()
        );
        assert_eq!(body_bytes(resp).await, body);
    }

    #[tokio::test]
    async fn client_response_clamps_out_of_range_status_to_502() {
        let headers = BTreeMap::new();
        for bad_status in [0u16, 1000u16] {
            let resp = client_response(bad_status, &headers, Bytes::new(), &[]);
            assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
        }
    }

    #[tokio::test]
    async fn client_response_skips_header_with_invalid_value_without_panicking() {
        let mut headers = BTreeMap::new();
        headers.insert("x-bad".to_string(), "a\nb".to_string());
        headers.insert("x-good".to_string(), "fine".to_string());

        let resp = client_response(200, &headers, Bytes::from_static(b"body"), &[]);

        assert_eq!(resp.status(), StatusCode::OK);
        assert!(resp.headers().get("x-bad").is_none());
        assert_eq!(resp.headers().get("x-good").unwrap(), "fine");
    }

    #[tokio::test]
    async fn client_streaming_response_strips_internal_header_and_concatenates_frames() {
        let mut headers = BTreeMap::new();
        headers.insert("x-sluice-x".to_string(), "internal".to_string());
        headers.insert("content-type".to_string(), "text/plain".to_string());
        let frames: Vec<Result<Bytes, Infallible>> = vec![
            Ok(Bytes::from_static(b"hello ")),
            Ok(Bytes::from_static(b"world")),
        ];
        let stream = futures_util::stream::iter(frames);

        let resp = client_streaming_response(200, &headers, stream, &[]);

        assert_eq!(resp.status(), StatusCode::OK);
        assert!(resp.headers().get("x-sluice-x").is_none());
        assert!(resp.headers().get(CONTENT_LENGTH).is_none());
        assert_eq!(body_bytes(resp).await, Bytes::from_static(b"hello world"));
    }

    #[tokio::test]
    async fn upstream_request_strips_host_and_internal_keeps_content_type() {
        let mut headers = BTreeMap::new();
        headers.insert("host".to_string(), "example.com".to_string());
        headers.insert("x-sluice-x".to_string(), "internal".to_string());
        headers.insert("content-type".to_string(), "application/json".to_string());
        let client = reqwest::Client::new();

        let builder = upstream_request(
            &client,
            "GET",
            "http://upstream.invalid/path",
            &headers,
            Bytes::new(),
        )
        .expect("valid method builds");
        let request = builder.build().expect("request builds");

        assert!(request.headers().get("host").is_none());
        assert!(request.headers().get("x-sluice-x").is_none());
        assert_eq!(
            request.headers().get("content-type").unwrap(),
            "application/json"
        );
    }

    #[tokio::test]
    async fn upstream_request_invalid_method_errors() {
        let client = reqwest::Client::new();

        let err = upstream_request(
            &client,
            "BAD METHOD",
            "http://upstream.invalid/path",
            &BTreeMap::new(),
            Bytes::new(),
        )
        .unwrap_err();

        assert!(matches!(err, ReconstructError::Method(m) if m == "BAD METHOD"));
    }

    #[tokio::test]
    async fn upstream_request_sets_body() {
        let client = reqwest::Client::new();
        let body = Bytes::from_static(b"hello upstream");

        let builder = upstream_request(
            &client,
            "POST",
            "http://upstream.invalid/path",
            &BTreeMap::new(),
            body.clone(),
        )
        .expect("valid method builds");
        let request = builder.build().expect("request builds");

        let request_body = request.body().expect("body is set");
        assert_eq!(request_body.as_bytes(), Some(body.as_ref()));
    }
}
