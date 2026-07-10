//! Integration tests for the M15 Task 3 render direction: serializing a
//! [`CanonicalRequest`]/[`CanonicalResponse`] back into each provider's wire
//! format via `Adapter::render_request`/`render_response`, and the
//! [`TranslationReport`] fidelity bookkeeping that goes with it. Complements
//! each adapter's own unit tests (in `src/llm/{anthropic,openai,google}.rs`)
//! with cross-cutting scenarios that exercise more than one adapter at once:
//! same-provider round trips, and translating a request/response from one
//! provider's dialect into another's.

use sluice::llm::adapter::adapter_for;
use sluice::llm::{
    CanonicalMessage, CanonicalRequest, CanonicalResponse, CanonicalStreamEvent, CanonicalTool,
    CanonicalUsage, ContentBlock, RequestCtx, Role, Sampling, StopReason, StreamParseState,
    StreamRenderState, ToolChoice, TranslationReport,
};
use sluice::sse::SseEvent;

fn ctx(path: &str) -> RequestCtx {
    RequestCtx {
        path: path.to_string(),
        method: "POST".to_string(),
    }
}

// -- (a) same-provider round trip: parse -> render -> parse is semantically
// equal -------------------------------------------------------------------

/// A representative Anthropic `/v1/messages` request body: system prompt,
/// a tool, tool_choice, full sampling (including `top_k`, which Anthropic
/// supports), and a tool-use/tool-result exchange in the content blocks —
/// every field this adapter's render can carry losslessly for Anthropic.
const ANTHROPIC_REQUEST_BODY: &[u8] = br#"{
    "model": "claude-opus-4-1-20250805",
    "system": "You are a helpful assistant.",
    "messages": [
        {"role": "user", "content": "What's the weather in NYC?"},
        {"role": "assistant", "content": [
            {"type": "tool_use", "id": "call_1", "name": "get_weather", "input": {"location": "NYC"}}
        ]},
        {"role": "user", "content": [
            {"type": "tool_result", "tool_use_id": "call_1", "content": "72F and sunny", "is_error": false}
        ]}
    ],
    "tools": [
        {"name": "get_weather", "description": "Look up the weather", "input_schema": {"type": "object"}}
    ],
    "tool_choice": {"type": "auto"},
    "max_tokens": 1024,
    "temperature": 0.7,
    "top_p": 0.9,
    "top_k": 40,
    "stop_sequences": ["STOP"],
    "stream": false
}"#;

#[test]
fn anthropic_request_round_trip_is_semantically_equal() {
    let adapter = adapter_for("anthropic").unwrap();
    let original = adapter
        .parse_request(ANTHROPIC_REQUEST_BODY, &ctx("/v1/messages"))
        .unwrap();

    let mut report = TranslationReport::default();
    let rendered = adapter.render_request(&original, &mut report).unwrap();
    assert!(
        report.dropped.is_empty(),
        "same-provider round trip must not drop anything: {:?}",
        report.dropped
    );

    let reparsed = adapter
        .parse_request(&rendered, &ctx("/v1/messages"))
        .unwrap();
    assert_eq!(reparsed, original);
}

// "id"/"type"/"role" (unlike a real Anthropic response body would also
// carry) aren't in this adapter's KNOWN_RESPONSE_FIELDS, so they land in
// `extra`, tagged `"anthropic.<field>"` by `collect_extra`. Since render now
// re-emits an `extra` entry whenever its tag matches the render target (see
// `adapter::emit_or_drop_extra`), these survive a same-provider round trip
// losslessly instead of breaking the "reparsed == original" assertion below
// — this is exactly the M15 Task 3 review Finding 1 fix, exercised here with
// a representative field (`id`) in
// `anthropic_response_round_trip_is_semantically_equal`.
const ANTHROPIC_RESPONSE_BODY: &[u8] = br#"{
    "id": "msg_1",
    "type": "message",
    "role": "assistant",
    "model": "claude-opus-4-1-20250805",
    "content": [
        {"type": "text", "text": "The weather in NYC is "},
        {"type": "tool_use", "id": "call_1", "name": "get_weather", "input": {"location": "NYC"}}
    ],
    "stop_reason": "tool_use",
    "usage": {"input_tokens": 10, "output_tokens": 20}
}"#;

#[test]
fn anthropic_response_round_trip_is_semantically_equal() {
    let adapter = adapter_for("anthropic").unwrap();
    let original = adapter.parse_response(ANTHROPIC_RESPONSE_BODY).unwrap();
    assert_eq!(
        original.extra.get("anthropic.id"),
        Some(&serde_json::json!("msg_1"))
    );

    let mut report = TranslationReport::default();
    let rendered = adapter.render_response(&original, &mut report).unwrap();
    assert!(
        report.dropped.is_empty(),
        "same-provider round trip must not drop anything, including extra: {:?}",
        report.dropped
    );

    // The representative extra field genuinely reached the wire, not just
    // the reparsed canonical shape.
    let value: serde_json::Value = serde_json::from_slice(&rendered).unwrap();
    assert_eq!(value["id"], serde_json::json!("msg_1"));

    let reparsed = adapter.parse_response(&rendered).unwrap();
    assert_eq!(reparsed, original);
}

/// An OpenAI request with no `top_k` (OpenAI has none, so a round trip that
/// included one could never be lossless — that gap is covered separately by
/// `anthropic_to_openai_drops_top_k_and_maps_system_and_tools` below).
const OPENAI_REQUEST_BODY: &[u8] = br#"{
    "model": "gpt-4o",
    "messages": [
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": "What's the weather in NYC?"},
        {"role": "assistant", "content": null, "tool_calls": [
            {"id": "call_1", "type": "function", "function": {"name": "get_weather", "arguments": "{\"location\":\"NYC\"}"}}
        ]},
        {"role": "tool", "tool_call_id": "call_1", "content": "72F and sunny"}
    ],
    "tools": [
        {"type": "function", "function": {"name": "get_weather", "description": "Look up the weather", "parameters": {"type": "object"}}}
    ],
    "tool_choice": "auto",
    "max_tokens": 1024,
    "temperature": 0.7,
    "top_p": 0.9,
    "stop": ["STOP"],
    "stream": false
}"#;

#[test]
fn openai_request_round_trip_is_semantically_equal() {
    let adapter = adapter_for("openai").unwrap();
    let original = adapter
        .parse_request(OPENAI_REQUEST_BODY, &ctx("/v1/chat/completions"))
        .unwrap();

    let mut report = TranslationReport::default();
    let rendered = adapter.render_request(&original, &mut report).unwrap();
    assert!(
        report.dropped.is_empty(),
        "same-provider round trip must not drop anything: {:?}",
        report.dropped
    );

    let reparsed = adapter
        .parse_request(&rendered, &ctx("/v1/chat/completions"))
        .unwrap();
    assert_eq!(reparsed, original);
}

// "id"/"object"/"created" (unlike a real OpenAI response body would also
// carry) aren't in this adapter's KNOWN_RESPONSE_FIELDS, so they land in
// `extra`, tagged `"openai.<field>"`; see the comment on
// ANTHROPIC_RESPONSE_BODY above for why that now survives a same-provider
// round trip instead of being dropped. `created` must be present here (a
// real OpenAI response always carries it) or render's FIX-6 discriminator
// synthesis would add a fresh `openai.created` extra entry on reparse that
// isn't in `original`, breaking the round-trip equality assertion below.
const OPENAI_RESPONSE_BODY: &[u8] = br#"{
    "id": "chatcmpl-1",
    "object": "chat.completion",
    "created": 1700000000,
    "model": "gpt-4o",
    "choices": [{
        "index": 0,
        "message": {"role": "assistant", "content": "hello there"},
        "finish_reason": "stop"
    }],
    "usage": {"prompt_tokens": 10, "completion_tokens": 20, "total_tokens": 30}
}"#;

#[test]
fn openai_response_round_trip_is_semantically_equal() {
    let adapter = adapter_for("openai").unwrap();
    let original = adapter.parse_response(OPENAI_RESPONSE_BODY).unwrap();
    assert_eq!(
        original.extra.get("openai.object"),
        Some(&serde_json::json!("chat.completion"))
    );

    let mut report = TranslationReport::default();
    let rendered = adapter.render_response(&original, &mut report).unwrap();
    assert!(
        report.dropped.is_empty(),
        "same-provider round trip must not drop anything, including extra: {:?}",
        report.dropped
    );

    let value: serde_json::Value = serde_json::from_slice(&rendered).unwrap();
    assert_eq!(value["object"], serde_json::json!("chat.completion"));

    let reparsed = adapter.parse_response(&rendered).unwrap();
    assert_eq!(reparsed, original);
}

/// A Google request with no `tool_choice` (this codebase's Google adapter
/// never parses one — see `canonical`'s module doc comment's fidelity
/// table) and `stream: false` (Google signals streaming via the URL path,
/// which a request body round trip through this adapter can't carry).
const GOOGLE_REQUEST_BODY: &[u8] = br#"{
    "contents": [
        {"role": "user", "parts": [{"text": "What's the weather in NYC?"}]},
        {"role": "model", "parts": [{"functionCall": {"name": "get_weather", "args": {"location": "NYC"}}}]},
        {"role": "function", "parts": [{"functionResponse": {"name": "get_weather", "response": {"result": "72F and sunny"}}}]}
    ],
    "systemInstruction": {"parts": [{"text": "You are a helpful assistant."}]},
    "tools": [
        {"functionDeclarations": [
            {"name": "get_weather", "description": "Look up the weather", "parameters": {"type": "object"}}
        ]}
    ],
    "generationConfig": {
        "temperature": 0.7,
        "topP": 0.9,
        "topK": 40,
        "maxOutputTokens": 1024,
        "stopSequences": ["STOP"]
    }
}"#;

#[test]
fn google_request_round_trip_is_semantically_equal() {
    let adapter = adapter_for("google").unwrap();
    let path = "/v1beta/models/gemini-1.5-pro:generateContent";
    let original = adapter
        .parse_request(GOOGLE_REQUEST_BODY, &ctx(path))
        .unwrap();

    let mut report = TranslationReport::default();
    let rendered = adapter.render_request(&original, &mut report).unwrap();
    assert!(
        report.dropped.is_empty(),
        "same-provider round trip must not drop anything: {:?}",
        report.dropped
    );

    // render_request writes `model` as a body fallback field (it has no
    // RequestCtx to put a path on) — parsing it back with the same
    // models/<model> path segment exercises the same "path wins" precedence
    // parse_request always uses, and the fallback field still lines up.
    let reparsed = adapter.parse_request(&rendered, &ctx(path)).unwrap();
    assert_eq!(reparsed, original);
}

// "promptFeedback" isn't in this adapter's KNOWN_RESPONSE_FIELDS, so it
// lands in `extra` tagged `"google.promptFeedback"`; see the comment on
// ANTHROPIC_RESPONSE_BODY above for why that now survives a same-provider
// round trip instead of being dropped.
const GOOGLE_RESPONSE_BODY: &[u8] = br#"{
    "candidates": [{
        "content": {"parts": [{"text": "hello there"}], "role": "model"},
        "finishReason": "STOP",
        "index": 0
    }],
    "usageMetadata": {"promptTokenCount": 10, "candidatesTokenCount": 20, "totalTokenCount": 30},
    "modelVersion": "gemini-1.5-pro",
    "promptFeedback": {"blockReason": "NONE"}
}"#;

#[test]
fn google_response_round_trip_is_semantically_equal() {
    let adapter = adapter_for("google").unwrap();
    let original = adapter.parse_response(GOOGLE_RESPONSE_BODY).unwrap();
    assert_eq!(
        original.extra.get("google.promptFeedback"),
        Some(&serde_json::json!({"blockReason": "NONE"}))
    );

    let mut report = TranslationReport::default();
    let rendered = adapter.render_response(&original, &mut report).unwrap();
    assert!(
        report.dropped.is_empty(),
        "same-provider round trip must not drop anything, including extra: {:?}",
        report.dropped
    );

    let value: serde_json::Value = serde_json::from_slice(&rendered).unwrap();
    assert_eq!(
        value["promptFeedback"],
        serde_json::json!({"blockReason": "NONE"})
    );

    let reparsed = adapter.parse_response(&rendered).unwrap();
    assert_eq!(reparsed, original);
}

// -- (b) Anthropic request -> canonical -> OpenAI request -------------------

#[test]
fn anthropic_to_openai_drops_top_k_and_maps_system_and_tools() {
    let anthropic = adapter_for("anthropic").unwrap();
    let openai = adapter_for("openai").unwrap();

    let canonical = anthropic
        .parse_request(ANTHROPIC_REQUEST_BODY, &ctx("/v1/messages"))
        .unwrap();
    assert_eq!(canonical.sampling.top_k, Some(40));
    assert_eq!(
        canonical.system.as_deref(),
        Some("You are a helpful assistant.")
    );

    let mut report = TranslationReport::default();
    let rendered = openai.render_request(&canonical, &mut report).unwrap();

    // top_k has no OpenAI equivalent — must show up in the report.
    assert!(
        report
            .dropped
            .iter()
            .any(|d| d.path == "sampling.top_k" && d.reason.contains("top_k")),
        "expected sampling.top_k to be reported dropped, got: {:?}",
        report.dropped
    );

    let value: serde_json::Value = serde_json::from_slice(&rendered).unwrap();

    // System became a leading role: "system" message.
    assert_eq!(value["messages"][0]["role"], serde_json::json!("system"));
    assert_eq!(
        value["messages"][0]["content"],
        serde_json::json!("You are a helpful assistant.")
    );

    // Tools became OpenAI's nested tools[].function shape.
    assert_eq!(
        value["tools"][0]["function"]["name"],
        serde_json::json!("get_weather")
    );

    // The canonical max-tokens value renders as `max_completion_tokens` (the
    // key OpenAI reasoning models require); the o1/o3-rejected `max_tokens` is
    // never emitted.
    assert_eq!(value["max_completion_tokens"], serde_json::json!(1024));
    assert!(value.get("max_tokens").is_none());

    // top_k itself must not appear on the rendered OpenAI body.
    assert!(value.get("top_k").is_none());

    // The rendered body must still be valid OpenAI JSON the adapter can
    // parse back in (even though it's now a lossy translation, not a
    // round trip) — never a garbage/partial body.
    let reparsed = openai
        .parse_request(&rendered, &ctx("/v1/chat/completions"))
        .unwrap();
    assert_eq!(reparsed.sampling.top_k, None);
    assert_eq!(
        reparsed.system.as_deref(),
        Some("You are a helpful assistant.")
    );
    assert_eq!(reparsed.tools.len(), 1);
    assert_eq!(reparsed.tools[0].name, "get_weather");
}

// -- (c) OpenAI response -> canonical -> Anthropic response ------------------

#[test]
fn openai_to_anthropic_response_maps_content_stop_reason_and_usage() {
    let openai = adapter_for("openai").unwrap();
    let anthropic = adapter_for("anthropic").unwrap();

    let body = br#"{
        "model": "gpt-4o",
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": null,
                "tool_calls": [{"id": "call_1", "type": "function", "function": {"name": "get_weather", "arguments": "{\"location\":\"NYC\"}"}}]
            },
            "finish_reason": "tool_calls"
        }],
        "usage": {"prompt_tokens": 15, "completion_tokens": 25, "total_tokens": 40}
    }"#;

    let canonical = openai.parse_response(body).unwrap();
    assert_eq!(canonical.stop_reason, Some(StopReason::ToolUse));
    assert_eq!(
        canonical.content,
        vec![ContentBlock::ToolUse {
            id: "call_1".to_string(),
            name: "get_weather".to_string(),
            input: serde_json::json!({"location": "NYC"}),
        }]
    );

    let mut report = TranslationReport::default();
    let rendered = anthropic.render_response(&canonical, &mut report).unwrap();
    // Every canonical field here has a home in Anthropic's response shape.
    assert!(
        report.dropped.is_empty(),
        "unexpected drops: {:?}",
        report.dropped
    );

    let value: serde_json::Value = serde_json::from_slice(&rendered).unwrap();
    assert_eq!(value["stop_reason"], serde_json::json!("tool_use"));
    assert_eq!(
        value["content"][0],
        serde_json::json!({
            "type": "tool_use",
            "id": "call_1",
            "name": "get_weather",
            "input": {"location": "NYC"},
        })
    );
    assert_eq!(
        value["usage"],
        serde_json::json!({"input_tokens": 15, "output_tokens": 25})
    );

    // Confirm it re-parses back into the same canonical shape (content
    // blocks + stop_reason + usage), completing the cross-provider mapping.
    let reparsed = anthropic.parse_response(&rendered).unwrap();
    assert_eq!(reparsed.content, canonical.content);
    assert_eq!(reparsed.stop_reason, canonical.stop_reason);
    assert_eq!(reparsed.usage, canonical.usage);
}

// -- (d) extra fields: source-tagged, so same-provider survives and
// cross-provider drops with a report entry (M15 Task 3 review Finding 1) ---

/// A same-provider parse -> render round trip re-emits an `extra` field
/// losslessly: `collect_extra` tags it `"anthropic.metadata"` at parse time,
/// and `emit_or_drop_extra` re-emits it (stripped back to `metadata`) since
/// the render target matches the tag. Contrast with
/// `extra_fields_from_a_different_provider_are_dropped_on_render` below,
/// where the same field, tagged for a *different* provider, is dropped
/// instead.
#[test]
fn same_provider_extra_fields_survive_render() {
    let adapter = adapter_for("anthropic").unwrap();

    let body = br#"{
        "model": "claude-opus-4-1-20250805",
        "messages": [],
        "metadata": {"user_id": "u_123"}
    }"#;
    let canonical = adapter.parse_request(body, &ctx("/v1/messages")).unwrap();
    assert_eq!(
        canonical.extra.get("anthropic.metadata"),
        Some(&serde_json::json!({"user_id": "u_123"}))
    );

    let mut report = TranslationReport::default();
    let rendered = adapter.render_request(&canonical, &mut report).unwrap();

    assert!(
        report.dropped.is_empty(),
        "same-provider extra must survive: {:?}",
        report.dropped
    );

    // The field genuinely reached the rendered wire body, not just the
    // reparsed canonical shape.
    let value: serde_json::Value = serde_json::from_slice(&rendered).unwrap();
    assert_eq!(value["metadata"], serde_json::json!({"user_id": "u_123"}));

    let reparsed = adapter
        .parse_request(&rendered, &ctx("/v1/messages"))
        .unwrap();
    assert_eq!(reparsed, canonical);
}

/// The cross-provider counterpart: a field parsed by Anthropic
/// (`"anthropic.metadata"`) has no verified home in OpenAI's dialect when
/// rendered there, so it's dropped with a report entry instead of being
/// guessed at or silently discarded.
#[test]
fn extra_fields_from_a_different_provider_are_dropped_on_render() {
    let anthropic = adapter_for("anthropic").unwrap();
    let openai = adapter_for("openai").unwrap();

    let body = br#"{
        "model": "claude-opus-4-1-20250805",
        "messages": [],
        "metadata": {"user_id": "u_123"}
    }"#;
    let canonical = anthropic.parse_request(body, &ctx("/v1/messages")).unwrap();
    assert_eq!(
        canonical.extra.get("anthropic.metadata"),
        Some(&serde_json::json!({"user_id": "u_123"}))
    );

    let mut report = TranslationReport::default();
    let rendered = openai.render_request(&canonical, &mut report).unwrap();

    assert_eq!(
        report
            .dropped
            .iter()
            .filter(|d| d.path == "extra.metadata")
            .count(),
        1
    );
    assert!(report
        .dropped
        .iter()
        .any(|d| d.path == "extra.metadata" && d.reason.contains("anthropic")));

    // And the field genuinely does not appear on the rendered body.
    let value: serde_json::Value = serde_json::from_slice(&rendered).unwrap();
    assert!(value.get("metadata").is_none());
}

/// Google rendering a `Some(tool_choice)` is the other concrete
/// "known non-mappable field" this codebase has: the Google adapter has
/// never parsed `tool_choice`/`toolConfig` (see `canonical`'s fidelity
/// table), so render always drops one rather than inventing a wire shape.
#[test]
fn google_tool_choice_is_recorded_as_dropped_on_render() {
    let adapter = adapter_for("google").unwrap();
    let req = CanonicalRequest {
        model: "gemini-1.5-pro".to_string(),
        messages: vec![CanonicalMessage {
            role: Role::User,
            content: vec![ContentBlock::Text {
                text: "hi".to_string(),
            }],
        }],
        system: None,
        tools: vec![CanonicalTool {
            name: "get_weather".to_string(),
            description: None,
            input_schema: serde_json::Value::Null,
        }],
        tool_choice: Some(ToolChoice::Auto),
        sampling: Sampling::default(),
        stream: false,
        extra: Default::default(),
    };

    let mut report = TranslationReport::default();
    adapter.render_request(&req, &mut report).unwrap();

    assert_eq!(report.dropped.len(), 1);
    assert_eq!(report.dropped[0].path, "tool_choice");
}

// -- (e) Google function-role request -> canonical -> OpenAI request -------
// (M15 Task 3 review Finding 2)

/// Google's `"function"` role maps to canonical [`Role::Tool`] without
/// policing which part types the message actually carries (see
/// `google::parse_role`/`parts_to_blocks`) — so a `"function"`-role message
/// whose part is a `functionCall` (not the usual `functionResponse`) parses
/// into a `Role::Tool` canonical message holding a `ToolUse` block, an
/// unusual combination no built-in adapter's own parse side produces on its
/// own. Rendering that combination to OpenAI must not emit
/// `{"role": "tool", "tool_calls": [...]}` (structurally invalid: no
/// `tool_call_id`) — it must render as a valid `role: "assistant"` message
/// with `tool_calls` instead.
#[test]
fn google_function_role_tool_use_translates_to_valid_openai_assistant_tool_call() {
    let google = adapter_for("google").unwrap();
    let openai = adapter_for("openai").unwrap();

    let body = br#"{
        "contents": [
            {"role": "function", "parts": [
                {"functionCall": {"name": "get_weather", "args": {"location": "NYC"}}}
            ]}
        ]
    }"#;
    let canonical = google
        .parse_request(body, &ctx("/v1beta/models/gemini-1.5-pro:generateContent"))
        .unwrap();
    assert_eq!(canonical.messages[0].role, Role::Tool);
    assert_eq!(
        canonical.messages[0].content,
        vec![ContentBlock::ToolUse {
            id: "get_weather".to_string(),
            name: "get_weather".to_string(),
            input: serde_json::json!({"location": "NYC"}),
        }]
    );

    let mut report = TranslationReport::default();
    let rendered = openai.render_request(&canonical, &mut report).unwrap();
    let value: serde_json::Value = serde_json::from_slice(&rendered).unwrap();

    let rendered_message = &value["messages"][0];
    assert_eq!(rendered_message["role"], serde_json::json!("assistant"));
    assert_eq!(
        rendered_message["tool_calls"][0]["function"]["name"],
        serde_json::json!("get_weather")
    );

    // No message anywhere in the rendered body is role:"tool" without a
    // tool_call_id — the invariant this fix guarantees.
    for message in value["messages"].as_array().unwrap() {
        if message["role"] == serde_json::json!("tool") {
            assert!(
                message.get("tool_call_id").is_some(),
                "a role:\"tool\" message must always carry tool_call_id: {message:?}"
            );
        }
    }

    // The rendered body is valid, parseable OpenAI JSON.
    let reparsed = openai
        .parse_request(&rendered, &ctx("/v1/chat/completions"))
        .unwrap();
    assert_eq!(
        reparsed.messages[0].content,
        vec![ContentBlock::ToolUse {
            id: "get_weather".to_string(),
            name: "get_weather".to_string(),
            input: serde_json::json!({"location": "NYC"}),
        }]
    );
}

// -- misc: never panics on unusual canonical values --------------------------

#[test]
fn render_never_panics_on_hand_built_canonical_values() {
    let req = CanonicalRequest {
        model: String::new(),
        messages: vec![
            CanonicalMessage {
                role: Role::System,
                content: vec![ContentBlock::Image {
                    media_type: "image/png".to_string(),
                    data: "abc".to_string(),
                }],
            },
            CanonicalMessage {
                role: Role::Tool,
                content: vec![ContentBlock::ToolResult {
                    id: "call_1".to_string(),
                    content: "not json".to_string(),
                    is_error: true,
                }],
            },
        ],
        system: None,
        tools: vec![],
        tool_choice: Some(ToolChoice::Tool("get_weather".to_string())),
        sampling: Sampling {
            top_k: Some(1),
            ..Sampling::default()
        },
        stream: true,
        extra: Default::default(),
    };
    let resp = CanonicalResponse {
        model: String::new(),
        content: vec![ContentBlock::ToolResult {
            id: "x".to_string(),
            content: "y".to_string(),
            is_error: true,
        }],
        stop_reason: Some(StopReason::Other("weird".to_string())),
        usage: Some(CanonicalUsage {
            input_tokens: None,
            output_tokens: None,
        }),
        extra: Default::default(),
    };

    for name in ["anthropic", "openai", "google"] {
        let adapter = adapter_for(name).unwrap();
        let mut report = TranslationReport::default();
        adapter
            .render_request(&req, &mut report)
            .unwrap_or_else(|e| panic!("{name} render_request must not error: {e}"));
        let mut report = TranslationReport::default();
        adapter
            .render_response(&resp, &mut report)
            .unwrap_or_else(|e| panic!("{name} render_response must not error: {e}"));
    }
}

// -- (d) streaming translation state machines (M15 Task 4) ----------------
//
// These golden tests exercise `Adapter::parse_stream_event` (a provider's SSE
// event stream lifted up into `CanonicalStreamEvent`s) and
// `Adapter::render_stream_event` (canonical events serialized down into
// another provider's fully-framed SSE wire bytes). The interesting behavior
// is the *granularity bridging*: OpenAI/Google pack a message's start and its
// first delta into one coarse chunk, while Anthropic splits every lifecycle
// step into its own event, so the state machines must synthesize (render
// side) or collapse (also render side) the events one dialect has and the
// other lacks. Chunk *bodies* are compared as normalized JSON (key order /
// whitespace would make byte comparison brittle), but the SSE *framing* — the
// `event:` type lines Anthropic needs, and OpenAI's `[DONE]` terminal — is
// asserted exactly, since that framing is precisely what a streaming client
// dispatches on.

/// One parsed SSE frame: the optional `event:` type line (only Anthropic
/// emits it) and the raw `data:` payload.
#[derive(Debug)]
struct Frame {
    event: Option<String>,
    data: String,
}

/// Split fully-framed SSE wire bytes into their individual `\n\n`-terminated
/// frames, extracting each frame's `event:`/`data:` lines. Mirrors how a real
/// SSE client (and `crate::sse::SseFramer`) delimits events.
fn frames(bytes: &[u8]) -> Vec<Frame> {
    let text = String::from_utf8(bytes.to_vec()).expect("render output must be UTF-8");
    let mut out = Vec::new();
    for block in text.split("\n\n") {
        if block.trim().is_empty() {
            continue;
        }
        let mut event = None;
        let mut data = None;
        for line in block.split('\n') {
            if let Some(rest) = line.strip_prefix("event: ") {
                event = Some(rest.to_string());
            } else if let Some(rest) = line.strip_prefix("data: ") {
                data = Some(rest.to_string());
            }
        }
        out.push(Frame {
            event,
            data: data.unwrap_or_default(),
        });
    }
    out
}

/// The `data:` payload of each frame parsed as JSON. Panics on a non-JSON
/// payload (e.g. `[DONE]`), so only call it on frames known to carry JSON.
fn frame_json(frame: &Frame) -> serde_json::Value {
    serde_json::from_str(&frame.data)
        .unwrap_or_else(|e| panic!("frame data must be JSON: {:?} ({e})", frame.data))
}

/// Drive a provider's streaming parser over an ordered list of `data:`
/// payloads, collecting every canonical event it surfaces (dropping the
/// `Ok(None)` housekeeping events). One [`StreamParseState`] threads the whole
/// stream, exactly as a real proxy would.
fn parse_stream(provider: &str, datas: &[&str]) -> Vec<CanonicalStreamEvent> {
    let adapter = adapter_for(provider).unwrap();
    let mut st = StreamParseState::default();
    let mut out = Vec::new();
    for data in datas {
        let event = SseEvent {
            data: (*data).to_string(),
        };
        if let Some(canonical) = adapter
            .parse_stream_event(&event, &mut st)
            .unwrap_or_else(|e| panic!("{provider} parse_stream_event must not error: {e}"))
        {
            out.push(canonical);
        }
        // Drain any events a single wire event expanded into beyond the first,
        // exactly as `proxy::translate_stream_event` does per event.
        out.extend(st.drain_pending());
    }
    out
}

/// Drive a provider's streaming renderer over an ordered list of canonical
/// events, concatenating all the wire bytes it produces. One
/// [`StreamRenderState`] threads the whole stream.
fn render_stream(provider: &str, events: &[CanonicalStreamEvent]) -> (Vec<u8>, TranslationReport) {
    let adapter = adapter_for(provider).unwrap();
    let mut st = StreamRenderState::default();
    let mut report = TranslationReport::default();
    let mut out = Vec::new();
    for event in events {
        let bytes = adapter
            .render_stream_event(event, &mut st, &mut report)
            .unwrap_or_else(|e| panic!("{provider} render_stream_event must not error: {e}"));
        out.extend(bytes);
    }
    (out, report)
}

/// A realistic OpenAI text stream: a role-only opening chunk, two content
/// chunks, a finish chunk, and the `[DONE]` sentinel.
const OPENAI_TEXT_CHUNKS: &[&str] = &[
    r#"{"object":"chat.completion.chunk","model":"gpt-4o","choices":[{"index":0,"delta":{"role":"assistant"},"finish_reason":null}]}"#,
    r#"{"object":"chat.completion.chunk","model":"gpt-4o","choices":[{"index":0,"delta":{"content":"Hello"},"finish_reason":null}]}"#,
    r#"{"object":"chat.completion.chunk","model":"gpt-4o","choices":[{"index":0,"delta":{"content":" world"},"finish_reason":null}]}"#,
    r#"{"object":"chat.completion.chunk","model":"gpt-4o","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}"#,
    "[DONE]",
];

/// (1) An OpenAI chunk sequence parses to the expected canonical events. The
/// role-only chunk is `Ok(None)` (its role/model aren't lifted — a documented
/// gap the Anthropic renderer bridges by synthesizing a `message_start`), the
/// two content chunks become `ContentBlockDelta`s on the single streamed text
/// block (index 0), the finish chunk becomes a `MessageDelta`, and `[DONE]`
/// becomes `MessageStop`. Note the canonical sequence is *coarse* — no
/// `MessageStart`, no `ContentBlockStart`/`Stop` — exactly what OpenAI's wire
/// format can express.
#[test]
fn openai_text_stream_parses_to_expected_canonical_events() {
    let events = parse_stream("openai", OPENAI_TEXT_CHUNKS);
    assert_eq!(
        events,
        vec![
            CanonicalStreamEvent::ContentBlockDelta {
                index: 0,
                text: Some("Hello".to_string()),
                partial_json: None,
            },
            CanonicalStreamEvent::ContentBlockDelta {
                index: 0,
                text: Some(" world".to_string()),
                partial_json: None,
            },
            CanonicalStreamEvent::MessageDelta {
                stop_reason: Some(StopReason::EndTurn),
                usage: None,
            },
            CanonicalStreamEvent::MessageStop,
        ]
    );
}

/// (2) That coarse canonical sequence renders to the full, granular Anthropic
/// SSE event sequence: a synthetic `message_start` and text
/// `content_block_start` appear ahead of the first delta, and a synthetic
/// `content_block_stop` is injected before `message_delta` — none of which
/// existed in the canonical input. The `event:` framing is asserted exactly;
/// the JSON bodies by value.
#[test]
fn openai_text_canonical_renders_to_anthropic_frame_sequence() {
    let canonical = parse_stream("openai", OPENAI_TEXT_CHUNKS);
    let (bytes, report) = render_stream("anthropic", &canonical);
    assert!(report.dropped.is_empty(), "{:?}", report.dropped);

    let frames = frames(&bytes);
    let event_types: Vec<&str> = frames
        .iter()
        .map(|f| {
            f.event
                .as_deref()
                .expect("anthropic frames carry event: lines")
        })
        .collect();
    assert_eq!(
        event_types,
        vec![
            "message_start",
            "content_block_start",
            "content_block_delta",
            "content_block_delta",
            "content_block_stop",
            "message_delta",
            "message_stop",
        ]
    );

    // The synthesized message_start carries an empty model (OpenAI's stream
    // never surfaced one to lift) and the assistant role.
    assert_eq!(
        frame_json(&frames[0]),
        serde_json::json!({
            "type": "message_start",
            "message": {
                "type": "message",
                "role": "assistant",
                "model": "",
                "content": [],
                "stop_reason": null,
                "usage": {"input_tokens": 0, "output_tokens": 0},
            },
        })
    );
    assert_eq!(
        frame_json(&frames[1]),
        serde_json::json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "text", "text": ""},
        })
    );
    assert_eq!(
        frame_json(&frames[2]),
        serde_json::json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "text_delta", "text": "Hello"},
        })
    );
    assert_eq!(
        frame_json(&frames[3]),
        serde_json::json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "text_delta", "text": " world"},
        })
    );
    assert_eq!(
        frame_json(&frames[4]),
        serde_json::json!({"type": "content_block_stop", "index": 0})
    );
    assert_eq!(
        frame_json(&frames[5]),
        serde_json::json!({"type": "message_delta", "delta": {"stop_reason": "end_turn"}})
    );
    assert_eq!(
        frame_json(&frames[6]),
        serde_json::json!({"type": "message_stop"})
    );
}

/// A realistic Anthropic text stream, including a `ping` (which must parse to
/// nothing canonical). Carries a real model on `message_start` and usage on
/// `message_delta`, both of which must survive translation to OpenAI.
const ANTHROPIC_TEXT_EVENTS: &[&str] = &[
    r#"{"type":"message_start","message":{"type":"message","role":"assistant","model":"claude-opus-4-1-20250805","content":[],"stop_reason":null,"usage":{"input_tokens":10,"output_tokens":0}}}"#,
    r#"{"type":"ping"}"#,
    r#"{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}"#,
    r#"{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hello"}}"#,
    r#"{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":" world"}}"#,
    r#"{"type":"content_block_stop","index":0}"#,
    r#"{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":20}}"#,
    r#"{"type":"message_stop"}"#,
];

/// (3) The reverse direction: an Anthropic event sequence parses to the full
/// canonical lifecycle (the `ping` dropped), then renders to OpenAI chunks
/// collapsed into OpenAI's coarser shape — a role-bearing first chunk, content
/// chunks, a finish chunk, a usage-only chunk, and the exact `data: [DONE]`
/// terminal. The model captured from Anthropic's `message_start` rides through
/// onto every OpenAI chunk.
#[test]
fn anthropic_text_stream_translates_to_openai_chunks_and_done() {
    let canonical = parse_stream("anthropic", ANTHROPIC_TEXT_EVENTS);
    assert_eq!(
        canonical,
        vec![
            CanonicalStreamEvent::MessageStart {
                model: "claude-opus-4-1-20250805".to_string(),
                role: Role::Assistant,
            },
            CanonicalStreamEvent::ContentBlockStart {
                index: 0,
                block: ContentBlock::Text {
                    text: String::new(),
                },
            },
            CanonicalStreamEvent::ContentBlockDelta {
                index: 0,
                text: Some("Hello".to_string()),
                partial_json: None,
            },
            CanonicalStreamEvent::ContentBlockDelta {
                index: 0,
                text: Some(" world".to_string()),
                partial_json: None,
            },
            CanonicalStreamEvent::ContentBlockStop { index: 0 },
            CanonicalStreamEvent::MessageDelta {
                stop_reason: Some(StopReason::EndTurn),
                usage: Some(CanonicalUsage {
                    input_tokens: None,
                    output_tokens: Some(20),
                }),
            },
            CanonicalStreamEvent::MessageStop,
        ]
    );

    let (bytes, _report) = render_stream("openai", &canonical);
    let frames = frames(&bytes);

    // Every non-terminal frame is a chat.completion.chunk; the last is [DONE].
    assert_eq!(frames.len(), 6);
    assert!(frames.iter().all(|f| f.event.is_none()));

    assert_eq!(
        frame_json(&frames[0]),
        serde_json::json!({
            "object": "chat.completion.chunk",
            "model": "claude-opus-4-1-20250805",
            "choices": [{"index": 0, "delta": {"role": "assistant"}, "finish_reason": null}],
        })
    );
    assert_eq!(
        frame_json(&frames[1]),
        serde_json::json!({
            "object": "chat.completion.chunk",
            "model": "claude-opus-4-1-20250805",
            "choices": [{"index": 0, "delta": {"content": "Hello"}, "finish_reason": null}],
        })
    );
    assert_eq!(
        frame_json(&frames[2]),
        serde_json::json!({
            "object": "chat.completion.chunk",
            "model": "claude-opus-4-1-20250805",
            "choices": [{"index": 0, "delta": {"content": " world"}, "finish_reason": null}],
        })
    );
    assert_eq!(
        frame_json(&frames[3]),
        serde_json::json!({
            "object": "chat.completion.chunk",
            "model": "claude-opus-4-1-20250805",
            "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
        })
    );
    assert_eq!(
        frame_json(&frames[4]),
        serde_json::json!({
            "object": "chat.completion.chunk",
            "model": "claude-opus-4-1-20250805",
            "choices": [],
            "usage": {"completion_tokens": 20},
        })
    );
    // The terminal is asserted byte-exactly, not as JSON.
    assert!(frames[5].event.is_none());
    assert_eq!(frames[5].data, "[DONE]");
}

/// A realistic OpenAI tool-call stream: role chunk, an opening tool_call
/// fragment (carrying id + name, empty arguments), two argument fragments, a
/// `tool_calls` finish, and `[DONE]`.
const OPENAI_TOOL_CHUNKS: &[&str] = &[
    r#"{"object":"chat.completion.chunk","model":"gpt-4o","choices":[{"index":0,"delta":{"role":"assistant"},"finish_reason":null}]}"#,
    r#"{"object":"chat.completion.chunk","model":"gpt-4o","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"get_weather","arguments":""}}]},"finish_reason":null}]}"#,
    r#"{"object":"chat.completion.chunk","model":"gpt-4o","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"location\":"}}]},"finish_reason":null}]}"#,
    r#"{"object":"chat.completion.chunk","model":"gpt-4o","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"NYC\"}"}}]},"finish_reason":null}]}"#,
    r#"{"object":"chat.completion.chunk","model":"gpt-4o","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}"#,
    "[DONE]",
];

/// (4) A tool-call stream round-trips OpenAI `tool_calls` <-> Anthropic
/// `input_json_delta` with the tool id, name, and streamed arguments all
/// preserved. Path: OpenAI chunks -> canonical -> Anthropic frames -> canonical
/// again -> OpenAI chunks. The first tool fragment (id + name) becomes a
/// `ContentBlockStart`; each later fragment an `input_json_delta`
/// `ContentBlockDelta`, which reassembled reconstructs the original arguments
/// JSON.
#[test]
fn tool_call_stream_round_trips_openai_anthropic_preserving_name_and_id() {
    // OpenAI -> canonical.
    let canonical = parse_stream("openai", OPENAI_TOOL_CHUNKS);
    assert_eq!(
        canonical,
        vec![
            CanonicalStreamEvent::ContentBlockStart {
                index: 0,
                block: ContentBlock::ToolUse {
                    id: "call_1".to_string(),
                    name: "get_weather".to_string(),
                    input: serde_json::Value::Null,
                },
            },
            CanonicalStreamEvent::ContentBlockDelta {
                index: 0,
                text: None,
                partial_json: Some(r#"{"location":"#.to_string()),
            },
            CanonicalStreamEvent::ContentBlockDelta {
                index: 0,
                text: None,
                partial_json: Some(r#""NYC"}"#.to_string()),
            },
            CanonicalStreamEvent::MessageDelta {
                stop_reason: Some(StopReason::ToolUse),
                usage: None,
            },
            CanonicalStreamEvent::MessageStop,
        ]
    );

    // canonical -> Anthropic frames. The tool call streams as an
    // input_json_delta sequence, and the tool_use start carries id + name.
    let (anthropic_bytes, report) = render_stream("anthropic", &canonical);
    assert!(report.dropped.is_empty(), "{:?}", report.dropped);
    let anthropic_frames = frames(&anthropic_bytes);
    let event_types: Vec<&str> = anthropic_frames
        .iter()
        .map(|f| f.event.as_deref().unwrap())
        .collect();
    assert_eq!(
        event_types,
        vec![
            "message_start",
            "content_block_start",
            "content_block_delta",
            "content_block_delta",
            "content_block_stop",
            "message_delta",
            "message_stop",
        ]
    );
    assert_eq!(
        frame_json(&anthropic_frames[1]),
        serde_json::json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "tool_use", "id": "call_1", "name": "get_weather", "input": {}},
        })
    );
    assert_eq!(
        frame_json(&anthropic_frames[2])["delta"],
        serde_json::json!({"type": "input_json_delta", "partial_json": r#"{"location":"#})
    );

    // Anthropic frames -> canonical again: id + name survive on the start, and
    // the argument fragments reassemble into the original arguments object.
    let anthropic_datas: Vec<&str> = anthropic_frames.iter().map(|f| f.data.as_str()).collect();
    let round_tripped = parse_stream("anthropic", &anthropic_datas);

    let tool_start = round_tripped
        .iter()
        .find_map(|e| match e {
            CanonicalStreamEvent::ContentBlockStart { block, .. } => Some(block),
            _ => None,
        })
        .expect("a tool_use content_block_start must survive the round trip");
    match tool_start {
        ContentBlock::ToolUse { id, name, .. } => {
            assert_eq!(id, "call_1");
            assert_eq!(name, "get_weather");
        }
        other => panic!("expected a ToolUse block, got {other:?}"),
    }

    let reassembled: String = round_tripped
        .iter()
        .filter_map(|e| match e {
            CanonicalStreamEvent::ContentBlockDelta {
                partial_json: Some(fragment),
                ..
            } => Some(fragment.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&reassembled).unwrap(),
        serde_json::json!({"location": "NYC"})
    );

    // And back out to OpenAI: the reconstructed tool_calls chunk names the
    // same tool with the same id, and its argument fragments still reassemble.
    let (openai_bytes, _report) = render_stream("openai", &round_tripped);
    let openai_frames = frames(&openai_bytes);
    // The terminal [DONE] frame isn't JSON; consider only the chunk frames.
    let openai_json: Vec<serde_json::Value> = openai_frames
        .iter()
        .filter(|f| f.data != "[DONE]")
        .map(frame_json)
        .collect();
    let tool_start_chunk = openai_json
        .iter()
        .find(|v| v["choices"][0]["delta"].get("tool_calls").is_some())
        .expect("a tool_calls chunk must be emitted");
    let call = &tool_start_chunk["choices"][0]["delta"]["tool_calls"][0];
    assert_eq!(call["id"], serde_json::json!("call_1"));
    assert_eq!(call["function"]["name"], serde_json::json!("get_weather"));

    let openai_args: String = openai_json
        .iter()
        .filter_map(|v| {
            v["choices"][0]["delta"]["tool_calls"][0]["function"]["arguments"]
                .as_str()
                .map(str::to_string)
        })
        .collect();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&openai_args).unwrap(),
        serde_json::json!({"location": "NYC"})
    );
}

/// A Google `streamGenerateContent` text stream round-trips to Anthropic
/// frames: Gemini has no explicit message-start or terminal, so the Anthropic
/// renderer synthesizes both, and the finishReason/usage chunk becomes a
/// `message_delta`. Exercises the third adapter's streaming direction.
#[test]
fn google_text_stream_translates_to_anthropic_frames() {
    let google_chunks: &[&str] = &[
        r#"{"candidates":[{"content":{"parts":[{"text":"Hello"}],"role":"model"},"index":0}]}"#,
        r#"{"candidates":[{"content":{"parts":[{"text":" world"}],"role":"model"},"index":0}]}"#,
        r#"{"candidates":[{"finishReason":"STOP","index":0}],"usageMetadata":{"promptTokenCount":5,"candidatesTokenCount":7}}"#,
    ];
    let canonical = parse_stream("google", google_chunks);
    assert_eq!(
        canonical,
        vec![
            CanonicalStreamEvent::ContentBlockDelta {
                index: 0,
                text: Some("Hello".to_string()),
                partial_json: None,
            },
            CanonicalStreamEvent::ContentBlockDelta {
                index: 0,
                text: Some(" world".to_string()),
                partial_json: None,
            },
            CanonicalStreamEvent::MessageDelta {
                stop_reason: Some(StopReason::EndTurn),
                usage: Some(CanonicalUsage {
                    input_tokens: Some(5),
                    output_tokens: Some(7),
                }),
            },
        ]
    );

    let (bytes, report) = render_stream("anthropic", &canonical);
    assert!(report.dropped.is_empty(), "{:?}", report.dropped);
    let event_types: Vec<String> = frames(&bytes)
        .iter()
        .map(|f| f.event.clone().unwrap())
        .collect();
    assert_eq!(
        event_types,
        vec![
            "message_start",
            "content_block_start",
            "content_block_delta",
            "content_block_delta",
            "content_block_stop",
            "message_delta",
        ]
    );
}

/// F1a regression: Gemini's `streamGenerateContent` commonly packs the final
/// text part into the SAME terminal chunk that carries `finishReason` +
/// `usageMetadata`. The parser must surface BOTH the trailing text AND the
/// stop_reason/usage from that one chunk, content first. The old parser checked
/// `usageMetadata` first and returned the `MessageDelta` early, silently
/// dropping the tail of every Gemini-origin assistant message.
#[test]
fn google_terminal_chunk_keeps_colocated_text_and_usage() {
    let google_chunks: &[&str] = &[
        r#"{"candidates":[{"content":{"parts":[{"text":"final words"}],"role":"model"},"finishReason":"STOP","index":0}],"usageMetadata":{"promptTokenCount":5,"candidatesTokenCount":7}}"#,
    ];
    let canonical = parse_stream("google", google_chunks);
    assert_eq!(
        canonical,
        vec![
            CanonicalStreamEvent::ContentBlockDelta {
                index: 0,
                text: Some("final words".to_string()),
                partial_json: None,
            },
            CanonicalStreamEvent::MessageDelta {
                stop_reason: Some(StopReason::EndTurn),
                usage: Some(CanonicalUsage {
                    input_tokens: Some(5),
                    output_tokens: Some(7),
                }),
            },
        ]
    );

    // End-to-end into an Anthropic client stream: the trailing text rides
    // through as a text_delta ahead of the terminal, and stop_reason/usage land
    // on the message_delta.
    let mut with_terminal = canonical.clone();
    with_terminal.push(CanonicalStreamEvent::MessageStop);
    let (bytes, report) = render_stream("anthropic", &with_terminal);
    assert!(report.dropped.is_empty(), "{:?}", report.dropped);
    let rendered_frames = frames(&bytes);
    let text_delta = rendered_frames
        .iter()
        .find(|f| f.event.as_deref() == Some("content_block_delta"))
        .expect("a content_block_delta frame carrying the terminal text");
    assert_eq!(
        frame_json(text_delta)["delta"],
        serde_json::json!({"type": "text_delta", "text": "final words"}),
    );
    let message_delta = rendered_frames
        .iter()
        .find(|f| f.event.as_deref() == Some("message_delta"))
        .expect("a message_delta frame");
    let delta_json = frame_json(message_delta);
    assert_eq!(
        delta_json["delta"]["stop_reason"],
        serde_json::json!("end_turn")
    );
    assert_eq!(delta_json["usage"]["output_tokens"], serde_json::json!(7));
}

/// A realistic OpenAI stream that emits assistant text and THEN a tool call:
/// the text block (index 0) is followed by a tool_use block (index 1).
const OPENAI_TEXT_THEN_TOOL_CHUNKS: &[&str] = &[
    r#"{"object":"chat.completion.chunk","model":"gpt-4o","choices":[{"index":0,"delta":{"role":"assistant"},"finish_reason":null}]}"#,
    r#"{"object":"chat.completion.chunk","model":"gpt-4o","choices":[{"index":0,"delta":{"content":"Let me check."},"finish_reason":null}]}"#,
    r#"{"object":"chat.completion.chunk","model":"gpt-4o","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"get_weather","arguments":""}}]},"finish_reason":null}]}"#,
    r#"{"object":"chat.completion.chunk","model":"gpt-4o","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"location\":\"NYC\"}"}}]},"finish_reason":null}]}"#,
    r#"{"object":"chat.completion.chunk","model":"gpt-4o","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}"#,
    "[DONE]",
];

/// Two OpenAI tool calls streamed in parallel (slots 0 and 1), each a start
/// fragment (id + name) followed by an argument fragment.
const OPENAI_PARALLEL_TOOL_CHUNKS: &[&str] = &[
    r#"{"object":"chat.completion.chunk","model":"gpt-4o","choices":[{"index":0,"delta":{"role":"assistant"},"finish_reason":null}]}"#,
    r#"{"object":"chat.completion.chunk","model":"gpt-4o","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_a","type":"function","function":{"name":"get_weather","arguments":""}}]},"finish_reason":null}]}"#,
    r#"{"object":"chat.completion.chunk","model":"gpt-4o","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"location\":\"NYC\"}"}}]},"finish_reason":null}]}"#,
    r#"{"object":"chat.completion.chunk","model":"gpt-4o","choices":[{"index":0,"delta":{"tool_calls":[{"index":1,"id":"call_b","type":"function","function":{"name":"get_time","arguments":""}}]},"finish_reason":null}]}"#,
    r#"{"object":"chat.completion.chunk","model":"gpt-4o","choices":[{"index":0,"delta":{"tool_calls":[{"index":1,"function":{"arguments":"{\"tz\":\"ET\"}"}}]},"finish_reason":null}]}"#,
    r#"{"object":"chat.completion.chunk","model":"gpt-4o","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}"#,
    "[DONE]",
];

/// The rendered Anthropic frames' `event:` types, in order.
fn anthropic_event_types(bytes: &[u8]) -> Vec<String> {
    frames(bytes)
        .iter()
        .map(|f| {
            f.event
                .clone()
                .expect("Anthropic frames carry an event type")
        })
        .collect()
}

/// F1b regression (1): an OpenAI-origin stream of assistant text THEN a tool
/// call renders to Anthropic with block 0 (text) explicitly closed BEFORE block
/// 1 (tool) opens, and exactly one `content_block_stop` per started block. The
/// old renderer opened block 1 while block 0 was still open, leaving block 0
/// never finalized (malformed for a strict Anthropic SDK client).
#[test]
fn openai_text_then_tool_closes_text_block_before_tool_block() {
    let canonical = parse_stream("openai", OPENAI_TEXT_THEN_TOOL_CHUNKS);
    let (bytes, report) = render_stream("anthropic", &canonical);
    assert!(report.dropped.is_empty(), "{:?}", report.dropped);
    let types = anthropic_event_types(&bytes);

    let starts = types.iter().filter(|t| *t == "content_block_start").count();
    let stops = types.iter().filter(|t| *t == "content_block_stop").count();
    assert_eq!(starts, 2, "one start per block (text, tool): {types:?}");
    assert_eq!(stops, 2, "one stop per started block: {types:?}");

    // The text block's stop precedes the tool block's start.
    let rendered_frames = frames(&bytes);
    let stop0_pos = rendered_frames
        .iter()
        .position(|f| {
            f.event.as_deref() == Some("content_block_stop") && frame_json(f)["index"] == 0
        })
        .expect("a content_block_stop for index 0");
    let start1_pos = rendered_frames
        .iter()
        .position(|f| {
            f.event.as_deref() == Some("content_block_start") && frame_json(f)["index"] == 1
        })
        .expect("a content_block_start for index 1");
    assert!(
        stop0_pos < start1_pos,
        "block 0 must be stopped before block 1 starts: {types:?}"
    );
}

/// F1b regression (2): two parallel tool calls render with the first tool block
/// closed before the second opens, and start/stop counts balance.
#[test]
fn openai_parallel_tool_calls_close_each_block_before_the_next() {
    let canonical = parse_stream("openai", OPENAI_PARALLEL_TOOL_CHUNKS);
    let (bytes, report) = render_stream("anthropic", &canonical);
    assert!(report.dropped.is_empty(), "{:?}", report.dropped);
    let types = anthropic_event_types(&bytes);

    let starts = types.iter().filter(|t| *t == "content_block_start").count();
    let stops = types.iter().filter(|t| *t == "content_block_stop").count();
    assert_eq!(starts, 2, "one start per tool block: {types:?}");
    assert_eq!(stops, 2, "one stop per started tool block: {types:?}");

    let rendered_frames = frames(&bytes);
    let stop0_pos = rendered_frames
        .iter()
        .position(|f| {
            f.event.as_deref() == Some("content_block_stop") && frame_json(f)["index"] == 0
        })
        .expect("a content_block_stop for index 0");
    let start1_pos = rendered_frames
        .iter()
        .position(|f| {
            f.event.as_deref() == Some("content_block_start") && frame_json(f)["index"] == 1
        })
        .expect("a content_block_start for index 1");
    assert!(
        stop0_pos < start1_pos,
        "first tool block must be stopped before the second starts: {types:?}"
    );
}

/// Malformed chunk JSON on the data path must never panic and must not abort
/// the stream — each of the three parsers turns a garbage frame into
/// `Ok(None)` and keeps going.
#[test]
fn malformed_stream_chunk_is_dropped_not_fatal() {
    for provider in ["anthropic", "openai", "google"] {
        let events = parse_stream(provider, &["not json at all", ""]);
        assert!(
            events.is_empty(),
            "{provider} must drop malformed/empty frames"
        );
    }
}

/// M15 Task 7 obligation (a), focused at the render level (the streaming
/// translator's terminal-synthesis logic in miniature): a coarse Google source
/// stream parses to canonical events that carry NO
/// [`CanonicalStreamEvent::MessageStop`] — Gemini's stream has no terminal
/// event to lift. Rendered as-is to Anthropic there is therefore no
/// `message_stop`, so a client would be left truncated; appending a synthesized
/// `MessageStop` (exactly what `proxy::translate_stream` does at stream end)
/// makes the Anthropic terminal appear exactly once.
#[test]
fn coarse_source_without_terminal_needs_synthesized_message_stop() {
    let gemini = &[
        r#"{"candidates":[{"content":{"parts":[{"text":"Hello"}]}}]}"#,
        r#"{"candidates":[{"content":{"parts":[{"text":" world"}]}}]}"#,
    ];
    let canonical = parse_stream("google", gemini);
    assert!(
        !canonical.contains(&CanonicalStreamEvent::MessageStop),
        "a Gemini stream never yields MessageStop: {canonical:?}"
    );

    // Rendered as-is, no terminal reaches the Anthropic client.
    let (bytes, _) = render_stream("anthropic", &canonical);
    let no_terminal = String::from_utf8(bytes).unwrap();
    assert!(
        !no_terminal.contains("event: message_stop"),
        "got: {no_terminal}"
    );

    // With the synthesized terminal the Anthropic `message_stop` appears once.
    let mut with_terminal = canonical.clone();
    with_terminal.push(CanonicalStreamEvent::MessageStop);
    let (bytes, _) = render_stream("anthropic", &with_terminal);
    let text = String::from_utf8(bytes).unwrap();
    assert_eq!(
        text.matches("event: message_stop").count(),
        1,
        "got: {text}"
    );
}

/// M15 Task 7 obligation (c): `StreamRenderState::terminal_sent` guards against
/// a double terminal. Two `MessageStop`s in a row (as could happen if the proxy
/// synthesized one at stream end after a real one already flowed through) emit
/// the Anthropic `message_stop` frame — and the OpenAI `[DONE]` sentinel —
/// exactly once.
#[test]
fn double_message_stop_emits_single_terminal_per_dialect() {
    let events = vec![
        CanonicalStreamEvent::ContentBlockDelta {
            index: 0,
            text: Some("hi".to_string()),
            partial_json: None,
        },
        CanonicalStreamEvent::MessageStop,
        CanonicalStreamEvent::MessageStop,
    ];
    let (bytes, _) = render_stream("anthropic", &events);
    let anthropic = String::from_utf8(bytes).unwrap();
    assert_eq!(
        anthropic.matches("event: message_stop").count(),
        1,
        "got: {anthropic}"
    );

    let (bytes, _) = render_stream("openai", &events);
    let openai = String::from_utf8(bytes).unwrap();
    assert_eq!(openai.matches("[DONE]").count(), 1, "got: {openai}");
}

// -- (M15 Task 6) buffered cross-provider translation wired into the proxy ----
//
// End-to-end: a real gateway process in front of a wiremock upstream. A client
// speaking one provider's dialect (`from`) hits a `[route.translate]` route
// whose upstream speaks another (`to`); the request is re-rendered into `to`'s
// wire shape and endpoint on the way out, and the buffered response is
// re-rendered back into `from`'s shape on the way in.
mod buffered_proxy_e2e {
    use sluice::admin::Ready;
    use sluice::config::load::load_str;
    use sluice::observability::init_metrics;
    use sluice::server::serve_on;
    use tokio::net::TcpListener;
    use wiremock::matchers::{body_string_contains, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    async fn spawn_gateway(config_toml: String) -> (String, tokio::sync::oneshot::Sender<()>) {
        let cfg = load_str(&config_toml).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let ready = Ready::new();
        let metrics = init_metrics();
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        tokio::spawn(async move {
            let _ = serve_on(listener, cfg, ready, metrics, async {
                let _ = rx.await;
            })
            .await;
        });
        (format!("http://{addr}"), tx)
    }

    // A minimal Anthropic `/v1/messages` request. `system` is the distinguishing
    // marker: OpenAI has no top-level `system`, so a correct translation must
    // fold it into a leading `messages[0]` with role "system" — an unambiguous
    // "this is OpenAI-shaped" signal on the recorded upstream request.
    const ANTHROPIC_REQUEST: &str = r#"{
        "model": "claude-opus-4-1-20250805",
        "system": "You are helpful.",
        "messages": [{"role": "user", "content": "Hello"}],
        "max_tokens": 1024
    }"#;

    // A valid non-streaming OpenAI chat completion the upstream answers with.
    const OPENAI_RESPONSE: &str = r#"{
        "id": "chatcmpl-1",
        "object": "chat.completion",
        "model": "gpt-4o",
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": "Hello from OpenAI"},
            "finish_reason": "stop"
        }],
        "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}
    }"#;

    /// The headline e2e: client speaks Anthropic, upstream speaks OpenAI.
    /// Asserts BOTH directions of the buffered translation:
    /// (a) the client receives an Anthropic-shaped response (content blocks +
    ///     stop_reason), and
    /// (b) the wiremock upstream received an OpenAI-shaped request at the
    ///     OpenAI endpoint `/v1/chat/completions` (not the client's
    ///     `/v1/messages`).
    #[tokio::test]
    async fn anthropic_client_openai_upstream_round_trips_both_directions() {
        let upstream = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "application/json")
                    .set_body_string(OPENAI_RESPONSE),
            )
            .mount(&upstream)
            .await;

        let cfg = format!(
            r#"
            [[route]]
            id = "tr"
            upstream = "{up}"
              [route.translate]
              from = "anthropic"
              to = "openai"
        "#,
            up = upstream.uri(),
        );
        let (base, _guard) = spawn_gateway(cfg).await;

        let resp = reqwest::Client::new()
            .post(format!("{base}/tr/v1/messages"))
            .body(ANTHROPIC_REQUEST)
            .send()
            .await
            .unwrap();
        // (a) client sees an Anthropic-shaped response, upstream status verbatim.
        assert_eq!(resp.status(), 200);
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["content"][0]["type"], serde_json::json!("text"));
        assert_eq!(
            body["content"][0]["text"],
            serde_json::json!("Hello from OpenAI")
        );
        assert_eq!(body["stop_reason"], serde_json::json!("end_turn"));

        // (b) the upstream received an OpenAI-shaped request at the OpenAI path.
        let received = upstream.received_requests().await.unwrap();
        assert_eq!(received.len(), 1, "upstream should be hit exactly once");
        let req = &received[0];
        assert_eq!(req.url.path(), "/v1/chat/completions");
        let sent: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
        // OpenAI shape: system folded into a leading role="system" message,
        // and no top-level `system` field (which the Anthropic body carried).
        assert!(sent.get("system").is_none());
        assert_eq!(sent["messages"][0]["role"], serde_json::json!("system"));
        assert_eq!(
            sent["messages"][0]["content"],
            serde_json::json!("You are helpful.")
        );
        assert_eq!(sent["messages"][1]["role"], serde_json::json!("user"));
    }

    /// An upstream body that can't be parsed as the `to` dialect must surface a
    /// typed 502 to the client — never a partial, native-looking body.
    #[tokio::test]
    async fn unparseable_upstream_response_yields_502() {
        let upstream = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_string("this is not json"))
            .mount(&upstream)
            .await;

        let cfg = format!(
            r#"
            [[route]]
            id = "tr"
            upstream = "{up}"
              [route.translate]
              from = "anthropic"
              to = "openai"
        "#,
            up = upstream.uri(),
        );
        let (base, _guard) = spawn_gateway(cfg).await;

        let resp = reqwest::Client::new()
            .post(format!("{base}/tr/v1/messages"))
            .body(ANTHROPIC_REQUEST)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 502);
    }

    /// A `stream: true` translate request whose upstream answers with an SSE
    /// (`text/event-stream`) body is now handled by the STREAMING translation
    /// path (M15 Task 7), not the buffered 502 the Task 6 code returned. Even
    /// an upstream chunk that carries no recognizable content
    /// (`data: {"foo":1}`) must still yield a well-formed client stream: the
    /// translator synthesizes the Anthropic terminal at stream end, so the
    /// client gets a `message_start`/`message_stop` framed 200 rather than a
    /// 502 — and never a panic. (The rich happy-path assertions live in
    /// `streaming_proxy_e2e`.)
    #[tokio::test]
    async fn streaming_translate_request_is_now_streamed_not_502() {
        let upstream = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(
                // `set_body_string` always stamps "content-type: text/plain"
                // (see wiremock's `ResponseTemplate::generate_response`,
                // which unconditionally overwrites content-type from its own
                // `mime` field) — `set_body_raw` is the one builder method
                // that lets the mime type genuinely be "text/event-stream".
                ResponseTemplate::new(200)
                    .set_body_raw("data: {\"foo\":1}\n\n", "text/event-stream"),
            )
            .mount(&upstream)
            .await;

        let cfg = format!(
            r#"
            [[route]]
            id = "tr"
            upstream = "{up}"
              [route.translate]
              from = "anthropic"
              to = "openai"
        "#,
            up = upstream.uri(),
        );
        let (base, _guard) = spawn_gateway(cfg).await;

        let req_body = r#"{
            "model": "claude-opus-4-1-20250805",
            "messages": [{"role": "user", "content": "Hello"}],
            "max_tokens": 1024,
            "stream": true
        }"#;
        let resp = reqwest::Client::new()
            .post(format!("{base}/tr/v1/messages"))
            .body(req_body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let text = resp.text().await.unwrap();
        assert!(
            text.contains("event: message_stop"),
            "streaming translation must synthesize a terminal, got: {text}"
        );
    }

    /// A request body that isn't valid `from`-dialect JSON is a client error:
    /// the gateway answers 400 and never forwards the untranslatable body.
    #[tokio::test]
    async fn non_source_shaped_request_yields_400() {
        let upstream = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_string(OPENAI_RESPONSE))
            .mount(&upstream)
            .await;

        let cfg = format!(
            r#"
            [[route]]
            id = "tr"
            upstream = "{up}"
              [route.translate]
              from = "anthropic"
              to = "openai"
        "#,
            up = upstream.uri(),
        );
        let (base, _guard) = spawn_gateway(cfg).await;

        let resp = reqwest::Client::new()
            .post(format!("{base}/tr/v1/messages"))
            .body("this is not anthropic json")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 400);
        // The untranslatable body must never reach the upstream.
        let received = upstream.received_requests().await.unwrap();
        assert!(
            received.is_empty(),
            "upstream must not be hit for an untranslatable request"
        );
    }

    /// With `report_header = true` ALONE — no `expose_headers` entry for it —
    /// the client still gets an `x-sluice-translation` header summarizing
    /// dropped-field paths. `expose_headers` is deliberately left unset here
    /// (M15 Task 6 review Important 1): before the fix, `translate_response`
    /// inserted the header into the response but `reconstruct::sanitize_egress_headers`
    /// stripped every `x-sluice-*` header not in the (global) `expose_headers`
    /// allowlist, so `report_header = true` was a client-facing no-op unless
    /// the operator *also* listed the header in `expose_headers`. The design
    /// is that `report_header = true` surfaces the header on its own. The
    /// Anthropic request carries `top_k`, which OpenAI can't represent — so
    /// the summary reports at least that one drop, and never any values.
    #[tokio::test]
    async fn report_header_present_when_enabled_alone_without_expose_headers() {
        let upstream = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_string(OPENAI_RESPONSE))
            .mount(&upstream)
            .await;

        let cfg = format!(
            r#"
            [[route]]
            id = "tr"
            upstream = "{up}"
              [route.translate]
              from = "anthropic"
              to = "openai"
              report_header = true
        "#,
            up = upstream.uri(),
        );
        let (base, _guard) = spawn_gateway(cfg).await;

        let req_body = r#"{
            "model": "claude-opus-4-1-20250805",
            "messages": [{"role": "user", "content": "Hello"}],
            "max_tokens": 1024,
            "top_k": 40
        }"#;
        let resp = reqwest::Client::new()
            .post(format!("{base}/tr/v1/messages"))
            .body(req_body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let header = resp
            .headers()
            .get("x-sluice-translation")
            .expect("x-sluice-translation header must be present even with no expose_headers entry")
            .to_str()
            .unwrap()
            .to_string();
        assert!(header.contains("dropped="), "got: {header}");
        assert!(
            header.contains("sampling.top_k"),
            "top_k drop must be summarized, got: {header}"
        );
    }

    /// The default (`report_header` unset, i.e. `false`) never surfaces
    /// `x-sluice-translation` to the client, even without a config change:
    /// the header must genuinely be opt-in, not always-on now that
    /// `report_header = true` alone is enough to expose it.
    #[tokio::test]
    async fn report_header_absent_when_disabled() {
        let upstream = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_string(OPENAI_RESPONSE))
            .mount(&upstream)
            .await;

        let cfg = format!(
            r#"
            [[route]]
            id = "tr"
            upstream = "{up}"
              [route.translate]
              from = "anthropic"
              to = "openai"
        "#,
            up = upstream.uri(),
        );
        let (base, _guard) = spawn_gateway(cfg).await;

        let req_body = r#"{
            "model": "claude-opus-4-1-20250805",
            "messages": [{"role": "user", "content": "Hello"}],
            "max_tokens": 1024,
            "top_k": 40
        }"#;
        let resp = reqwest::Client::new()
            .post(format!("{base}/tr/v1/messages"))
            .body(req_body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        assert!(
            resp.headers().get("x-sluice-translation").is_none(),
            "x-sluice-translation must be absent when report_header is left at its default (false)"
        );
    }

    /// Regression guard: a NON-translate route is completely unaffected — the
    /// client body reaches the upstream and the upstream body reaches the
    /// client byte-for-byte, exactly as before this task.
    #[tokio::test]
    async fn non_translate_route_is_byte_identical_passthrough() {
        let upstream = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/messages"))
            .respond_with(ResponseTemplate::new(200).set_body_string("pong"))
            .mount(&upstream)
            .await;

        let cfg = format!(
            r#"
            [[route]]
            id = "plain"
            upstream = "{up}"
        "#,
            up = upstream.uri(),
        );
        let (base, _guard) = spawn_gateway(cfg).await;

        let resp = reqwest::Client::new()
            .post(format!("{base}/plain/v1/messages"))
            .body("ping")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        assert_eq!(resp.text().await.unwrap(), "pong");

        let received = upstream.received_requests().await.unwrap();
        assert_eq!(received.len(), 1);
        assert_eq!(received[0].body, b"ping");
    }

    /// The banned phrase a mock "guardrail" step keys its `abort` on, mirroring
    /// `tests/ai_native.rs`'s `guardrail_step_aborts_on_banned_phrase_in_messages`.
    const BANNED_PHRASE: &str = "the secret launch codes";

    /// M15 Task 6 review Important 2: a translate route (`[route.translate]`,
    /// no `[route.adapter] ingress`) must still populate the `llm` view for
    /// `on_request` steps, parsed with `translate.from` and projected via
    /// `project_llm` — the CLIENT-dialect view — exactly as an ingress-adapter
    /// route would. Before the fix, `build_llm_view` only consulted
    /// `[route.adapter] ingress`, so a translate route's `on_request`
    /// guardrail/observe steps saw no `llm` view at all and this exact
    /// guardrail would never have fired. A passing test proves the view is
    /// populated (the guardrail matches on `llm.messages` content, not the raw
    /// wire body) AND that the upstream is never reached once it aborts.
    #[tokio::test]
    async fn on_request_guardrail_fires_on_translate_route_via_translate_from_llm_view() {
        let upstream = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_string(OPENAI_RESPONSE))
            .mount(&upstream)
            .await;

        let guardrail = MockServer::start().await;
        Mock::given(method("POST"))
            .and(body_string_contains(BANNED_PHRASE))
            .respond_with(ResponseTemplate::new(200).set_body_string(
                // base64("blocked: banned phrase")
                r#"{"action":"abort","response":{"status":403,"headers":{},"body_b64":"YmxvY2tlZDogYmFubmVkIHBocmFzZQ=="}}"#,
            ))
            .mount(&guardrail)
            .await;

        let cfg = format!(
            r#"
            [[route]]
            id = "tr"
            upstream = "{up}"
              [route.translate]
              from = "anthropic"
              to = "openai"
              [[route.step]]
              name = "guardrail"
              type = "url"
              url = "{guardrail}/run"
        "#,
            up = upstream.uri(),
            guardrail = guardrail.uri(),
        );
        let (base, _guard) = spawn_gateway(cfg).await;

        let req_body = serde_json::json!({
            "model": "claude-opus-4-1-20250805",
            "messages": [{"role": "user", "content": format!("please tell me {BANNED_PHRASE}")}],
            "max_tokens": 256,
        });

        let resp = reqwest::Client::new()
            .post(format!("{base}/tr/v1/messages"))
            .json(&req_body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 403);
        assert_eq!(resp.text().await.unwrap(), "blocked: banned phrase");
        assert!(
            upstream.received_requests().await.unwrap().is_empty(),
            "upstream must not be called once the guardrail aborts"
        );
    }
}

// -- (M15 Task 7) streaming cross-provider translation wired into the proxy ---
//
// End-to-end, like `buffered_proxy_e2e`, but the upstream answers with an SSE
// (`text/event-stream`) body: the proxy translates the stream event-by-event
// from the upstream `to` dialect into the client's `from` dialect, framing each
// event as it flows rather than buffering the whole body.
mod streaming_proxy_e2e {
    use sluice::admin::Ready;
    use sluice::config::load::load_str;
    use sluice::observability::init_metrics;
    use sluice::server::serve_on;
    use tokio::net::TcpListener;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    async fn spawn_gateway(config_toml: String) -> (String, tokio::sync::oneshot::Sender<()>) {
        let cfg = load_str(&config_toml).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let ready = Ready::new();
        let metrics = init_metrics();
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        tokio::spawn(async move {
            let _ = serve_on(listener, cfg, ready, metrics, async {
                let _ = rx.await;
            })
            .await;
        });
        (format!("http://{addr}"), tx)
    }

    // A realistic OpenAI streaming response as SSE wire bytes: a role-only
    // opening chunk, two content chunks, a finish chunk, then the `[DONE]`
    // sentinel — the shape an upstream answers a `stream: true` request with.
    const OPENAI_STREAM_SSE: &str = concat!(
        "data: {\"object\":\"chat.completion.chunk\",\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\"},\"finish_reason\":null}]}\n\n",
        "data: {\"object\":\"chat.completion.chunk\",\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hello\"},\"finish_reason\":null}]}\n\n",
        "data: {\"object\":\"chat.completion.chunk\",\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\" world\"},\"finish_reason\":null}]}\n\n",
        "data: {\"object\":\"chat.completion.chunk\",\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
        "data: [DONE]\n\n",
    );

    /// The headline streaming e2e: client speaks Anthropic, upstream streams
    /// OpenAI `chat.completion.chunk`s. The client must receive ANTHROPIC SSE
    /// framing — a `message_start` (with a non-empty model, obligation b), the
    /// text carried on `content_block_delta` events, and a terminal
    /// `message_stop` — and OpenAI's own `[DONE]` terminal must never leak
    /// through (it renders to `message_stop` instead).
    #[tokio::test]
    async fn anthropic_client_openai_streaming_upstream_yields_anthropic_sse() {
        let upstream = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(200).set_body_raw(OPENAI_STREAM_SSE, "text/event-stream"),
            )
            .mount(&upstream)
            .await;

        let cfg = format!(
            r#"
            [[route]]
            id = "tr"
            upstream = "{up}"
              [route.translate]
              from = "anthropic"
              to = "openai"
        "#,
            up = upstream.uri(),
        );
        let (base, _guard) = spawn_gateway(cfg).await;

        let req_body = r#"{
            "model": "claude-opus-4-1-20250805",
            "messages": [{"role": "user", "content": "Hi"}],
            "max_tokens": 1024,
            "stream": true
        }"#;
        let resp = reqwest::Client::new()
            .post(format!("{base}/tr/v1/messages"))
            .body(req_body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        assert!(
            resp.headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .map(|v| v.starts_with("text/event-stream"))
                .unwrap_or(false),
            "client stream must keep an SSE content-type"
        );
        let body = resp.text().await.unwrap();

        assert!(body.contains("event: message_start"), "got: {body}");
        assert!(body.contains("event: content_block_delta"), "got: {body}");
        // M15.7 review Minor: assert the terminal is emitted EXACTLY ONCE,
        // not merely present — guards against a double-emitted terminal
        // (e.g. both a genuine upstream terminal and a synthesized one)
        // slipping past a looser `contains` check.
        let message_stop_count = body.matches("event: message_stop").count();
        assert_eq!(
            message_stop_count, 1,
            "expected exactly one message_stop terminal, got {message_stop_count} in: {body}"
        );
        assert!(body.contains("Hello"), "got: {body}");
        assert!(body.contains(" world"), "got: {body}");
        // Obligation (b): the synthesized message_start carries the effective
        // client model, not an empty string.
        assert!(
            body.contains("claude-opus-4-1-20250805"),
            "message_start must carry a real model, got: {body}"
        );
        // OpenAI's terminal must not leak into an Anthropic client stream.
        assert!(!body.contains("[DONE]"), "got: {body}");

        // The upstream saw an OpenAI-shaped request at the OpenAI endpoint.
        let received = upstream.received_requests().await.unwrap();
        assert_eq!(received.len(), 1);
        assert_eq!(received[0].url.path(), "/v1/chat/completions");
    }

    // A Gemini streaming response with NO terminal event: two text chunks and
    // nothing else (no `finishReason`, no `usageMetadata`, no `[DONE]`).
    const GEMINI_STREAM_NO_TERMINAL: &str = concat!(
        "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Hello\"}]}}]}\n\n",
        "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\" world\"}]}}]}\n\n",
    );

    /// Obligation (a), end-to-end: a Google (`to = "google"`) upstream streams a
    /// coarse Gemini response with NO terminal event. A Gemini-origin canonical
    /// stream therefore never yields a `MessageStop`, yet the Anthropic client
    /// must still receive a terminal `message_stop` — synthesized by the proxy's
    /// stream translator at upstream-stream-end.
    #[tokio::test]
    async fn google_streaming_upstream_with_no_terminal_still_yields_client_terminal() {
        let upstream = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_raw(GEMINI_STREAM_NO_TERMINAL, "text/event-stream"),
            )
            .mount(&upstream)
            .await;

        let cfg = format!(
            r#"
            [[route]]
            id = "tr"
            upstream = "{up}"
              [route.translate]
              from = "anthropic"
              to = "google"
        "#,
            up = upstream.uri(),
        );
        let (base, _guard) = spawn_gateway(cfg).await;

        let req_body = r#"{
            "model": "gemini-1.5-pro",
            "messages": [{"role": "user", "content": "Hi"}],
            "max_tokens": 1024,
            "stream": true
        }"#;
        let resp = reqwest::Client::new()
            .post(format!("{base}/tr/v1/messages"))
            .body(req_body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let body = resp.text().await.unwrap();
        assert!(body.contains("Hello"), "got: {body}");
        assert!(body.contains(" world"), "got: {body}");
        assert!(
            body.contains("event: message_stop"),
            "a coarse Google source with no terminal must still yield a client message_stop, got: {body}"
        );
    }

    /// Regression guard: a NON-translate streaming route forwards the upstream
    /// SSE body to the client byte-for-byte — the streaming-translation branch
    /// must not touch a route without `[route.translate]`.
    #[tokio::test]
    async fn non_translate_streaming_route_is_byte_identical_passthrough() {
        const SSE: &str = "data: {\"a\":1}\n\ndata: {\"b\":2}\n\n";
        let upstream = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/messages"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(SSE, "text/event-stream"))
            .mount(&upstream)
            .await;

        let cfg = format!(
            r#"
            [[route]]
            id = "plain"
            upstream = "{up}"
        "#,
            up = upstream.uri(),
        );
        let (base, _guard) = spawn_gateway(cfg).await;

        let resp = reqwest::Client::new()
            .post(format!("{base}/plain/v1/messages"))
            .body("ping")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        assert_eq!(resp.text().await.unwrap(), SSE);
    }
}
