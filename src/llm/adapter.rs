//! Per-provider request/response adapters: parse a provider's wire-format
//! request and response bodies into the gateway's lossless
//! [`super::CanonicalRequest`]/[`super::CanonicalResponse`] IR (design doc
//! M15 Task 2). Steps that only need the lossy read-side view get there via
//! [`super::project_llm`], not by talking to an `Adapter` directly. Anthropic
//! `/v1/messages`, OpenAI `/v1/chat/completions` (see
//! [`super::openai::OpenAiAdapter`]), and Google `generateContent` (see
//! [`super::google::GoogleAdapter`]) all parse for real. This module
//! establishes the trait and the by-name lookup so config validation and the
//! proxy can already recognize provider names.

use serde_json::{Map, Value};
use thiserror::Error;

use super::anthropic::AnthropicAdapter;
use super::delta::Delta;
use super::google::GoogleAdapter;
use super::openai::OpenAiAdapter;
use super::{
    CanonicalRequest, CanonicalResponse, CanonicalStreamEvent, RequestCtx, StreamParseState,
    StreamRenderState, TranslationReport,
};
use crate::sse::SseEvent;

/// Collect every top-level key of a JSON object not in `known` into a fresh
/// `Map`, tagging each key with the parsing adapter's own `provider` name
/// (`"<provider>.<key>"`, e.g. `"anthropic.metadata"`, `"openai.user"`,
/// `"google.safetySettings"`). Shared by all three built-in adapters
/// (`anthropic`, `openai`, `google`) to build their `extra`/losslessness
/// escape valves for request and response parsing — the logic is
/// provider-agnostic; only each adapter's `KNOWN_*_FIELDS` list and
/// `provider` argument differ. The tag is what lets
/// [`emit_or_drop_extra`] later tell a same-provider render (re-emit
/// losslessly) from a cross-provider one (drop with a report entry) — see
/// its doc comment.
pub(crate) fn collect_extra(value: &Value, known: &[&str], provider: &str) -> Map<String, Value> {
    value
        .as_object()
        .map(|obj| {
            obj.iter()
                .filter(|(k, _)| !known.contains(&k.as_str()))
                .map(|(k, v)| (format!("{provider}.{k}"), v.clone()))
                .collect()
        })
        .unwrap_or_default()
}

/// Re-emit or drop every entry of a [`CanonicalRequest::extra`]/
/// [`CanonicalResponse::extra`] map during render, into `out` (the
/// top-level JSON object being built for `target_provider`'s wire format).
/// Shared by all three built-in adapters' render functions.
///
/// Each key is tagged `"<src>.<key>"` by [`collect_extra`] at parse time.
/// When `src == target_provider` — a same-provider round trip — the field
/// genuinely came from this exact dialect, so it's safe to re-emit
/// verbatim: the key is stripped of its tag and inserted into `out`,
/// unless `out` already has a field by that name (the renderer's own
/// typed fields always win; the collision is recorded as a note rather
/// than silently dropped, so it isn't invisible). When `src` differs —
/// including a legacy/untagged key with no `.` at all, which parses as an
/// empty `src` that can never equal a real provider name — the field is
/// provider-specific to a *different* dialect and has no verified home in
/// `target_provider`'s wire format, so it's recorded as dropped via
/// [`TranslationReport::drop`] under an `"extra.<key>"` path (the tag
/// stripped back off, so the report reads the same regardless of source).
pub(crate) fn emit_or_drop_extra(
    target_provider: &str,
    extra: &Map<String, Value>,
    out: &mut Map<String, Value>,
    report: &mut TranslationReport,
) {
    for (tagged_key, value) in extra {
        let (src, key) = tagged_key
            .split_once('.')
            .unwrap_or(("", tagged_key.as_str()));
        if src == target_provider {
            if out.contains_key(key) {
                report.notes.push(format!(
                    "extra.{key}: collides with a field this renderer already set; \
                     the renderer's own value was kept, extra value dropped"
                ));
            } else {
                out.insert(key.to_string(), value.clone());
            }
        } else {
            report.drop(
                format!("extra.{key}"),
                format!("{src}-specific field not representable in {target_provider}"),
            );
        }
    }
}

/// The known adapter/provider names. Used both by [`adapter_for`] and by
/// config validation (`config::load::validate`) to reject unknown names
/// consistently.
pub const KNOWN_PROVIDERS: &[&str] = &["anthropic", "openai", "google"];

/// Failure parsing a provider's wire-format request or response body.
#[derive(Debug, Error)]
pub enum AdapterError {
    #[error("adapter '{0}' request parsing is not implemented yet")]
    NotImplemented(String),
    #[error("malformed request body: {0}")]
    Malformed(String),
}

/// Parses a single provider's wire-format request/response bodies into the
/// canonical LLM IR. Object-safe (no generic methods, no `Self` return) so
/// it can be looked up by name at runtime via [`adapter_for`].
pub trait Adapter {
    /// The provider name this adapter handles (e.g. `"anthropic"`).
    fn provider(&self) -> &str;

    /// Parse a raw request body into the lossless [`CanonicalRequest`]
    /// shape. `ctx` carries the request's HTTP path/method — needed by
    /// Google, whose model id normally travels in the URL path
    /// (`/v1beta/models/<model>:<method>`) rather than the JSON body. All
    /// three built-in adapters (`anthropic`, `openai`, `google`) parse for
    /// real; `AdapterError::NotImplemented` is reserved for future adapters
    /// not yet wired up.
    fn parse_request(
        &self,
        body: &[u8],
        ctx: &RequestCtx,
    ) -> Result<CanonicalRequest, AdapterError>;

    /// Parse a raw (non-streaming) response body into the lossless
    /// [`CanonicalResponse`] shape.
    fn parse_response(&self, body: &[u8]) -> Result<CanonicalResponse, AdapterError>;

    /// Parse a single SSE event from this provider's streaming response into
    /// the normalized [`Delta`] shape. Returns `None` for events that carry
    /// nothing this gateway normalizes (housekeeping events, a `[DONE]`
    /// sentinel, malformed JSON, etc.) — this is a lossy best-effort parse,
    /// not a full re-derivation of the provider's wire format. Defaults to
    /// `None` so adapters that don't support streaming yet compile without
    /// implementing it; all three built-in adapters (`anthropic`, `openai`,
    /// `google`) override this as of M10 Task 2.
    fn parse_delta(&self, _event: &SseEvent) -> Option<Delta> {
        None
    }

    /// Serialize a lossless [`CanonicalRequest`] back into this provider's
    /// wire-format request body (design doc M15 Task 3 — the render
    /// direction, the mirror image of [`Adapter::parse_request`]). Every
    /// canonical field that has a home in this provider's dialect is
    /// written; every field that doesn't (e.g. `top_k` rendered to OpenAI,
    /// which has no such parameter) is recorded in `report` via
    /// [`TranslationReport::drop`] rather than silently discarded — see the
    /// per-pair fidelity notes on [`super::canonical`]'s module doc comment.
    /// Never fails on a canonical value it simply can't map faithfully
    /// (that's what `report` is for); `Err` is reserved for inputs so
    /// malformed serialization itself can't proceed (there are none today —
    /// `CanonicalRequest`'s fields are already typed/validated by
    /// construction — but the `Result` keeps this symmetric with
    /// `parse_request` and leaves room for a future adapter that needs it).
    fn render_request(
        &self,
        req: &CanonicalRequest,
        report: &mut TranslationReport,
    ) -> Result<Vec<u8>, AdapterError>;

    /// Serialize a lossless [`CanonicalResponse`] back into this provider's
    /// wire-format (non-streaming) response body — the render-direction
    /// mirror of [`Adapter::parse_response`]. Same fidelity contract as
    /// [`Adapter::render_request`]: unmappable fields are recorded in
    /// `report`, never silently dropped or panicked on.
    fn render_response(
        &self,
        resp: &CanonicalResponse,
        report: &mut TranslationReport,
    ) -> Result<Vec<u8>, AdapterError>;

    /// Parse one streaming SSE event from this provider's wire format up into
    /// at most one canonical [`CanonicalStreamEvent`] (design doc M15 Task 4,
    /// the streaming mirror of [`Adapter::parse_request`]). `st` carries the
    /// per-stream bookkeeping described on [`StreamParseState`] — allocate one
    /// [`StreamParseState`] per stream and thread the same `&mut` through
    /// every event, in order.
    ///
    /// Returns `Ok(None)` for a wire event that carries nothing canonical: an
    /// empty keep-alive/`:` comment (which [`crate::sse::SseEvent`] surfaces
    /// as empty `data`), an Anthropic `ping`, an OpenAI role-only first chunk
    /// (its role/model isn't lifted to canonical here — a documented
    /// granularity gap that render bridges by *synthesizing* the target
    /// preamble; see [`StreamRenderState`]), or a chunk this adapter doesn't
    /// recognize. Malformed chunk JSON is treated as `Ok(None)` rather than an
    /// error — a single corrupt frame shouldn't abort a whole stream — so the
    /// `Err` arm exists only for symmetry with the buffered parse methods
    /// (no built-in adapter returns it today). Never panics on any input.
    ///
    /// Defaults to `Ok(None)` so an adapter that doesn't translate streams
    /// compiles without implementing it; all three built-in adapters override
    /// it.
    fn parse_stream_event(
        &self,
        _event: &SseEvent,
        _st: &mut StreamParseState,
    ) -> Result<Option<CanonicalStreamEvent>, AdapterError> {
        Ok(None)
    }

    /// Serialize one canonical [`CanonicalStreamEvent`] down into this
    /// provider's *fully-framed* streaming wire bytes — ready to write to the
    /// client as-is (design doc M15 Task 4, the streaming mirror of
    /// [`Adapter::render_response`]). Returns raw bytes rather than
    /// [`crate::sse::SseEvent`]s precisely because an `SseEvent` can only
    /// carry the `data:` payload, whereas Anthropic's stream also relies on
    /// the SSE `event:` type line this method emits (e.g.
    /// `event: content_block_delta\ndata: {...}\n\n`); OpenAI/Google frames
    /// are `data: {...}\n\n`, and OpenAI's terminal is `data: [DONE]\n\n`.
    ///
    /// One canonical event may render to zero, one, or several framed events:
    /// zero (an empty `Vec`) when the target dialect emits nothing for it
    /// (e.g. OpenAI has no analog of a text `content_block_start`), or several
    /// when the target's granularity requires *synthesizing* lifecycle events
    /// the source never sent (e.g. an Anthropic renderer emits a synthetic
    /// `message_start` + `content_block_start` ahead of the first delta of an
    /// OpenAI-origin stream). `st` carries the per-stream bookkeeping on
    /// [`StreamRenderState`] that makes that synthesis correct across events;
    /// thread one `&mut` per stream. `report` records any fidelity downgrades
    /// (e.g. a stop reason with no exact target equivalent), matching the
    /// buffered render methods' contract. Never panics on any input.
    ///
    /// Defaults to an empty `Vec` so an adapter that doesn't translate streams
    /// compiles without implementing it; all three built-in adapters override
    /// it.
    fn render_stream_event(
        &self,
        _event: &CanonicalStreamEvent,
        _st: &mut StreamRenderState,
        _report: &mut TranslationReport,
    ) -> Result<Vec<u8>, AdapterError> {
        Ok(Vec::new())
    }
}

/// Look up the built-in adapter for a provider name. Unknown name → `None`.
/// Returns `Box<dyn Adapter + Send + Sync>` (rather than plain
/// `Box<dyn Adapter>`) so the boxed adapter can be moved into, and held
/// across `.await` points inside, a `Send + Sync` future — needed by
/// `proxy::tee_observe_stream`, whose returned stream must itself be
/// `Send + Sync` to satisfy `reconstruct::client_streaming_response`. All
/// three built-in adapters are trivial unit structs and therefore
/// `Send + Sync` for free.
pub fn adapter_for(name: &str) -> Option<Box<dyn Adapter + Send + Sync>> {
    match name {
        "anthropic" => Some(Box::new(AnthropicAdapter)),
        "openai" => Some(Box::new(OpenAiAdapter)),
        "google" => Some(Box::new(GoogleAdapter)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adapter_for_recognizes_known_providers() {
        for name in KNOWN_PROVIDERS {
            let adapter = adapter_for(name).unwrap_or_else(|| panic!("{name} must be known"));
            assert_eq!(adapter.provider(), *name);
        }
    }

    #[test]
    fn adapter_for_unknown_provider_returns_none() {
        assert!(adapter_for("no-such-provider").is_none());
    }

    fn ctx(path: &str) -> RequestCtx {
        RequestCtx {
            path: path.to_string(),
            method: "POST".to_string(),
        }
    }

    #[test]
    fn all_known_adapters_parse_their_wire_format_for_real() {
        // All three built-in adapters parse for real (no stubs remain);
        // `AdapterError::NotImplemented` is unreachable through
        // `adapter_for`.
        let anthropic = adapter_for("anthropic").unwrap();
        assert!(anthropic
            .parse_request(
                br#"{"model": "claude-opus-4-1-20250805", "messages": []}"#,
                &ctx("/v1/messages"),
            )
            .is_ok());

        let openai = adapter_for("openai").unwrap();
        assert!(openai
            .parse_request(
                br#"{"model": "gpt-4o", "messages": []}"#,
                &ctx("/v1/chat/completions"),
            )
            .is_ok());

        let google = adapter_for("google").unwrap();
        assert!(google
            .parse_request(
                br#"{"contents": []}"#,
                &ctx("/v1beta/models/gemini-1.5-pro:generateContent"),
            )
            .is_ok());
    }
}
