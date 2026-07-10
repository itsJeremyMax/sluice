use serde::{Deserialize, Serialize};

use crate::http_msg::HttpMsg;
use crate::llm::Llm;

pub const ENVELOPE_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    pub envelope_version: u32,
    pub hook: String,
    pub route_id: String,
    pub correlation_id: String,
    #[serde(rename = "self")]
    pub self_: Option<String>,
    pub request: HttpMsg,
    pub response: Option<HttpMsg>,
    pub chunk: Option<serde_json::Value>,
    pub llm: Option<Llm>,
    pub context: serde_json::Map<String, serde_json::Value>,
}

pub fn build_request_envelope(
    route_id: &str,
    self_name: &str,
    req: &HttpMsg,
    context: &serde_json::Map<String, serde_json::Value>,
    correlation_id: &str,
    llm: Option<&Llm>,
) -> Envelope {
    Envelope {
        envelope_version: ENVELOPE_VERSION,
        hook: "on_request".to_string(),
        route_id: route_id.to_string(),
        correlation_id: correlation_id.to_string(),
        self_: Some(self_name.to_string()),
        request: req.clone(),
        response: None,
        chunk: None,
        llm: llm.cloned(),
        context: context.clone(),
    }
}

/// Build an `on_response` envelope: `request` carries the (possibly
/// on_request-mutated) request that produced this response, `response`
/// carries the buffered upstream response a step's ops will mutate in place.
/// Response *status* is not represented in `HttpMsg` and is not mutable via
/// ops in M10 — only response headers/body and `context` are (see
/// `proxy::forward`'s on_response loop).
pub fn build_response_envelope(
    route_id: &str,
    self_name: &str,
    req: &HttpMsg,
    resp: &HttpMsg,
    context: &serde_json::Map<String, serde_json::Value>,
    correlation_id: &str,
    llm: Option<&Llm>,
) -> Envelope {
    Envelope {
        envelope_version: ENVELOPE_VERSION,
        hook: "on_response".to_string(),
        route_id: route_id.to_string(),
        correlation_id: correlation_id.to_string(),
        self_: Some(self_name.to_string()),
        request: req.clone(),
        response: Some(resp.clone()),
        chunk: None,
        llm: llm.cloned(),
        context: context.clone(),
    }
}

/// Build an `on_stream` chunk envelope: `chunk` carries the caller-built
/// `{ data_b64, seq, final, delta }` JSON for a single SSE event. There is no
/// `response` at `on_stream` — headers/status live in the client stream
/// itself, not in this envelope.
///
/// `context` is caller-supplied (design doc M11 Task 3): `on_stream`
/// `observe` steps (M10, see `proxy::spawn_observe_poster`) always pass an
/// empty map — observe steps see the request and the chunk, not any
/// mutating context — while `on_stream` `mutate` steps (M11, see
/// `proxy::run_mutate_chain`) thread through the one context map that
/// persists across every event *and* every chained step for the whole
/// stream's lifetime, exactly like `on_request`/`on_response`'s context but
/// scoped to this one stream rather than this one request.
///
/// Observe steps' directive is always ignored (see `proxy::forward`'s
/// `on_stream` branch) — for them this envelope is fire-and-forget
/// informational output, never a decision point. Mutate steps' directive
/// (`emit`/`drop`/`abort`) is exactly what gates the client stream.
pub fn build_stream_envelope(
    route_id: &str,
    self_name: &str,
    req: &HttpMsg,
    chunk_json: serde_json::Value,
    context: &serde_json::Map<String, serde_json::Value>,
    correlation_id: &str,
    llm: Option<&Llm>,
) -> Envelope {
    Envelope {
        envelope_version: ENVELOPE_VERSION,
        hook: "on_stream".to_string(),
        route_id: route_id.to_string(),
        correlation_id: correlation_id.to_string(),
        self_: Some(self_name.to_string()),
        request: req.clone(),
        response: None,
        chunk: Some(chunk_json),
        llm: llm.cloned(),
        context: context.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn msg() -> HttpMsg {
        HttpMsg {
            method: "POST".into(),
            path: "/v1/messages".into(),
            headers: BTreeMap::new(),
            body_b64: String::new(),
        }
    }

    #[test]
    fn builds_on_request_envelope_with_version_and_self() {
        let env = build_request_envelope(
            "claude",
            "redact",
            &msg(),
            &serde_json::Map::new(),
            "corr-1",
            None,
        );
        assert_eq!(env.envelope_version, ENVELOPE_VERSION);
        assert_eq!(env.hook, "on_request");
        assert_eq!(env.route_id, "claude");
        assert_eq!(env.self_.as_deref(), Some("redact"));
        assert!(env.response.is_none());
    }

    #[test]
    fn serializes_self_field_as_self() {
        let env = build_request_envelope(
            "claude",
            "redact",
            &msg(),
            &serde_json::Map::new(),
            "corr-1",
            None,
        );
        let v = serde_json::to_value(&env).unwrap();
        assert_eq!(v["self"], "redact");
        assert_eq!(v["envelope_version"], 1);
    }

    #[test]
    fn clones_provided_context_into_envelope() {
        let mut context = serde_json::Map::new();
        context.insert("a".to_string(), serde_json::json!({"x": 1}));
        let env = build_request_envelope("claude", "redact", &msg(), &context, "corr-1", None);
        assert_eq!(env.context.get("a"), Some(&serde_json::json!({"x": 1})));
    }

    #[test]
    fn carries_correlation_id_into_envelope() {
        let env = build_request_envelope(
            "claude",
            "redact",
            &msg(),
            &serde_json::Map::new(),
            "corr-xyz",
            None,
        );
        assert_eq!(env.correlation_id, "corr-xyz");
        let v = serde_json::to_value(&env).unwrap();
        assert_eq!(v["correlation_id"], "corr-xyz");
    }

    #[test]
    fn clones_provided_llm_into_envelope() {
        let llm = crate::llm::Llm {
            provider: "anthropic".to_string(),
            model: "claude-opus-4-1-20250805".to_string(),
            messages: vec![crate::llm::Message {
                role: "user".to_string(),
                content: "hi".to_string(),
            }],
            tools: vec![],
            max_tokens: Some(1024),
            stream: false,
            facts: None,
        };
        let env = build_request_envelope(
            "claude",
            "redact",
            &msg(),
            &serde_json::Map::new(),
            "corr-1",
            Some(&llm),
        );
        assert_eq!(env.llm, Some(llm));
    }

    #[test]
    fn defaults_llm_to_none_when_not_provided() {
        let env = build_request_envelope(
            "claude",
            "redact",
            &msg(),
            &serde_json::Map::new(),
            "corr-1",
            None,
        );
        assert!(env.llm.is_none());
    }

    #[test]
    fn builds_on_response_envelope_with_request_and_response() {
        let req = msg();
        let mut resp = msg();
        resp.method = String::new();
        resp.path = String::new();
        let env = build_response_envelope(
            "claude",
            "cost-tag",
            &req,
            &resp,
            &serde_json::Map::new(),
            "corr-1",
            None,
        );
        assert_eq!(env.hook, "on_response");
        assert_eq!(env.request, req);
        assert_eq!(env.response, Some(resp));
        assert_eq!(env.self_.as_deref(), Some("cost-tag"));
    }

    #[test]
    fn on_response_envelope_serializes_hook_field() {
        let env = build_response_envelope(
            "claude",
            "cost-tag",
            &msg(),
            &msg(),
            &serde_json::Map::new(),
            "corr-1",
            None,
        );
        let v = serde_json::to_value(&env).unwrap();
        assert_eq!(v["hook"], "on_response");
        assert!(v["response"].is_object());
    }

    #[test]
    fn builds_on_stream_envelope_with_chunk_and_no_response() {
        let chunk = serde_json::json!({
            "data_b64": "aGVsbG8=",
            "seq": 1,
            "final": false,
            "delta": {"kind": "text", "text": "hi", "tool_call": null, "usage": null, "finish": null},
        });
        let env = build_stream_envelope(
            "claude",
            "watcher",
            &msg(),
            chunk.clone(),
            &serde_json::Map::new(),
            "corr-1",
            None,
        );
        assert_eq!(env.hook, "on_stream");
        assert_eq!(env.self_.as_deref(), Some("watcher"));
        assert!(env.response.is_none());
        assert_eq!(env.chunk, Some(chunk));
    }

    #[test]
    fn on_stream_envelope_serializes_hook_and_chunk_fields() {
        let chunk = serde_json::json!({"data_b64": "aGk=", "seq": 1, "final": true, "delta": null});
        let env = build_stream_envelope(
            "claude",
            "watcher",
            &msg(),
            chunk,
            &serde_json::Map::new(),
            "corr-1",
            None,
        );
        let v = serde_json::to_value(&env).unwrap();
        assert_eq!(v["hook"], "on_stream");
        assert_eq!(v["chunk"]["seq"], 1);
        assert_eq!(v["chunk"]["final"], true);
        assert!(v["response"].is_null());
    }

    #[test]
    fn on_stream_envelope_threads_provided_context() {
        let mut context = serde_json::Map::new();
        context.insert("guard".to_string(), serde_json::json!({"count": 2}));
        let chunk =
            serde_json::json!({"data_b64": "aGk=", "seq": 1, "final": false, "delta": null});
        let env = build_stream_envelope("claude", "guard", &msg(), chunk, &context, "corr-1", None);
        assert_eq!(
            env.context.get("guard"),
            Some(&serde_json::json!({"count": 2}))
        );
    }
}
