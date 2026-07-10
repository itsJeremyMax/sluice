//! OpenAI `/v1/chat/completions` ingress adapter: parses the wire-format
//! request/response bodies into the gateway's lossless canonical LLM IR
//! ([`super::CanonicalRequest`]/[`super::CanonicalResponse`], design doc M15
//! Task 2), and serializes the canonical IR back out (design doc M15 Task
//! 3, the render direction). Content is parsed into structured
//! [`super::ContentBlock`]s rather than flattened to plain text —
//! flattening is [`super::project_llm`]'s job now, not this adapter's.

use serde_json::{json, Map, Value};

use super::adapter::{collect_extra, emit_or_drop_extra, Adapter, AdapterError};
use super::canonical::StreamToolRender;
use super::delta::{Delta, Finish, ToolCallDelta, Usage as DeltaUsage};
use super::{
    CanonicalMessage, CanonicalRequest, CanonicalResponse, CanonicalStreamEvent, CanonicalTool,
    CanonicalUsage, ContentBlock, RequestCtx, Role, Sampling, StopReason, StreamParseState,
    StreamRenderState, ToolChoice, TranslationReport,
};
use crate::sse::SseEvent;

/// Parses OpenAI Chat Completions API (`/v1/chat/completions`)
/// request/response bodies.
pub struct OpenAiAdapter;

impl Adapter for OpenAiAdapter {
    fn provider(&self) -> &str {
        "openai"
    }

    fn parse_request(
        &self,
        body: &[u8],
        _ctx: &RequestCtx,
    ) -> Result<CanonicalRequest, AdapterError> {
        parse_body(body)
    }

    fn parse_response(&self, body: &[u8]) -> Result<CanonicalResponse, AdapterError> {
        parse_response_body(body)
    }

    fn parse_delta(&self, event: &SseEvent) -> Option<Delta> {
        parse_delta(event)
    }

    fn render_request(
        &self,
        req: &CanonicalRequest,
        report: &mut TranslationReport,
    ) -> Result<Vec<u8>, AdapterError> {
        render_request_body(req, report)
    }

    fn render_response(
        &self,
        resp: &CanonicalResponse,
        report: &mut TranslationReport,
    ) -> Result<Vec<u8>, AdapterError> {
        render_response_body(resp, report)
    }

    fn parse_stream_event(
        &self,
        event: &SseEvent,
        st: &mut StreamParseState,
    ) -> Result<Option<CanonicalStreamEvent>, AdapterError> {
        parse_stream_event(event, st)
    }

    fn render_stream_event(
        &self,
        event: &CanonicalStreamEvent,
        st: &mut StreamRenderState,
        report: &mut TranslationReport,
    ) -> Result<Vec<u8>, AdapterError> {
        render_stream_event(event, st, report)
    }
}

/// Parse one OpenAI streaming SSE event's `data:` payload into a normalized
/// [`Delta`] (design doc M10 Task 2).
///
/// OpenAI's Chat Completions stream sends one JSON chunk per event, with
/// the sentinel literal `data: [DONE]` marking the end of the stream (not
/// JSON — handled before attempting to parse). Within a chunk: a top-level
/// `usage` object (present when the caller set
/// `stream_options.include_usage`, typically on a final chunk with an empty
/// `choices` array) takes priority and yields a `Usage` delta; otherwise
/// `choices[0].finish_reason`, if non-null, yields a `Finish` delta;
/// otherwise `choices[0].delta.content` yields a `Text` delta, or
/// `choices[0].delta.tool_calls[0].function` yields a `ToolCall` delta
/// fragment. Assumption: only the first entry of `tool_calls` is
/// considered — parallel tool calls stream with an `index` field this
/// normalized shape doesn't yet carry, a later refinement. Anything else
/// (malformed JSON, an empty `choices` array with no `usage`) yields `None`.
fn parse_delta(event: &SseEvent) -> Option<Delta> {
    if event.data.trim() == "[DONE]" {
        return None;
    }

    let value: Value = serde_json::from_str(&event.data).ok()?;

    if let Some(usage) = value.get("usage").filter(|u| !u.is_null()) {
        return Some(Delta::usage(DeltaUsage {
            input_tokens: usage.get("prompt_tokens").and_then(Value::as_u64),
            output_tokens: usage.get("completion_tokens").and_then(Value::as_u64),
        }));
    }

    let choice = value.get("choices").and_then(Value::as_array)?.first()?;

    if let Some(reason) = choice.get("finish_reason").and_then(Value::as_str) {
        return Some(Delta::finish(Finish {
            reason: Some(reason.to_string()),
        }));
    }

    let delta = choice.get("delta")?;

    if let Some(text) = delta.get("content").and_then(Value::as_str) {
        return Some(Delta::text(text));
    }

    if let Some(tool_call) = delta
        .get("tool_calls")
        .and_then(Value::as_array)
        .and_then(|calls| calls.first())
    {
        let id = tool_call
            .get("id")
            .and_then(Value::as_str)
            .map(str::to_string);
        let function = tool_call.get("function");
        let name = function
            .and_then(|f| f.get("name"))
            .and_then(Value::as_str)
            .map(str::to_string);
        let arguments_fragment = function
            .and_then(|f| f.get("arguments"))
            .and_then(Value::as_str)
            .map(str::to_string);
        return Some(Delta::tool_call(ToolCallDelta {
            id,
            name,
            arguments_fragment,
        }));
    }

    None
}

/// Flatten a `content` field (bare string or array of text parts) into
/// plain text. Only `{"type": "text", "text": "..."}` parts contribute. Used
/// for the fields the canonical IR still wants a plain string out of — a
/// folded-out system message's content, or a tool-role message's result —
/// not for general message content, which [`parse_content_blocks`] preserves
/// as structured blocks instead. A missing/malformed field yields an empty
/// string rather than an error, per this adapter's "tolerate missing
/// optional fields" contract.
fn flatten_content(content: &Value) -> String {
    match content {
        Value::String(s) => s.clone(),
        Value::Array(parts) => parts
            .iter()
            .filter_map(|part| {
                if part.get("type").and_then(Value::as_str) == Some("text") {
                    part.get("text").and_then(Value::as_str)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
            .join("\n"),
        _ => String::new(),
    }
}

/// Parse a data: URI (`data:<media-type>;base64,<data>`) into
/// `(media_type, data)`. Any URL that isn't a data URI is preserved
/// verbatim as `data` with an empty `media_type`, rather than dropped —
/// OpenAI's `image_url.url` can be an ordinary `https://` URL, which the
/// canonical [`ContentBlock::Image`] shape (media type + inline data) has no
/// better home for yet.
fn parse_image_url(url: &str) -> (String, String) {
    if let Some(rest) = url.strip_prefix("data:") {
        if let Some((meta, data)) = rest.split_once(',') {
            let media_type = meta.split(';').next().unwrap_or_default().to_string();
            return (media_type, data.to_string());
        }
    }
    (String::new(), url.to_string())
}

/// Parse a message `content` field (bare string or array of typed parts)
/// into canonical [`ContentBlock`]s. A bare string becomes a single `Text`
/// block; an array keeps every part type this adapter recognizes (`text`,
/// `image_url`) rather than only the text ones.
fn parse_content_blocks(content: &Value) -> Vec<ContentBlock> {
    match content {
        Value::String(s) => vec![ContentBlock::Text { text: s.clone() }],
        Value::Array(parts) => parts.iter().filter_map(parse_one_part).collect(),
        _ => Vec::new(),
    }
}

fn parse_one_part(part: &Value) -> Option<ContentBlock> {
    match part.get("type").and_then(Value::as_str)? {
        "text" => Some(ContentBlock::Text {
            text: part
                .get("text")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
        }),
        "image_url" => {
            let url = part.get("image_url")?.get("url").and_then(Value::as_str)?;
            let (media_type, data) = parse_image_url(url);
            Some(ContentBlock::Image { media_type, data })
        }
        _ => None,
    }
}

/// Parse an assistant message's `tool_calls` array into canonical
/// `ToolUse` blocks. Each entry's `function.arguments` is a *stringified*
/// JSON object on OpenAI's wire format (unlike Anthropic's `tool_use.input`,
/// which is already a JSON value) — parsed back into a [`Value`] here so the
/// canonical shape is uniform across providers; an arguments string that
/// isn't valid JSON falls back to `Value::Null` rather than failing the
/// whole parse.
fn tool_calls_to_blocks(tool_calls: &Value) -> Vec<ContentBlock> {
    tool_calls
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|tc| {
                    let function = tc.get("function")?;
                    let name = function
                        .get("name")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                    let input = function
                        .get("arguments")
                        .and_then(Value::as_str)
                        .and_then(|s| serde_json::from_str(s).ok())
                        .unwrap_or(Value::Null);
                    Some(ContentBlock::ToolUse {
                        id: tc
                            .get("id")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string(),
                        name,
                        input,
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Build one non-system message's canonical content: a `"tool"` (or the
/// legacy `"function"`) role message becomes a single `ToolResult` block;
/// every other role gets its `content` parsed into blocks, with any
/// `tool_calls` array appended as `ToolUse` blocks.
fn message_content(role: &str, message: &Value) -> Vec<ContentBlock> {
    if role == "tool" || role == "function" {
        let id = message
            .get("tool_call_id")
            .and_then(Value::as_str)
            .or_else(|| message.get("name").and_then(Value::as_str))
            .unwrap_or_default()
            .to_string();
        let content = message
            .get("content")
            .map(flatten_content)
            .unwrap_or_default();
        return vec![ContentBlock::ToolResult {
            id,
            content,
            is_error: false,
        }];
    }

    let mut blocks = message
        .get("content")
        .map(parse_content_blocks)
        .unwrap_or_default();
    if let Some(tool_calls) = message.get("tool_calls") {
        blocks.extend(tool_calls_to_blocks(tool_calls));
    }
    blocks
}

/// Map an OpenAI wire-format role string to the canonical [`Role`]. The
/// legacy `"function"` role (pre-`tool_calls` function-calling API) maps to
/// `Tool`, same as the modern `"tool"` role.
fn parse_role(role: &str) -> Role {
    match role {
        "system" => Role::System,
        "assistant" => Role::Assistant,
        "tool" | "function" => Role::Tool,
        _ => Role::User,
    }
}

/// Map an OpenAI `finish_reason` wire string to the canonical
/// [`StopReason`]. An unrecognized reason is preserved via `Other` rather
/// than dropped.
fn parse_stop_reason(reason: &str) -> StopReason {
    match reason {
        "stop" => StopReason::EndTurn,
        "length" => StopReason::MaxTokens,
        "tool_calls" | "function_call" => StopReason::ToolUse,
        other => StopReason::Other(other.to_string()),
    }
}

/// Parse OpenAI's `tool_choice` field: either the bare strings `"auto"`,
/// `"none"`, `"required"`, or an object `{"type": "function", "function":
/// {"name": "..."}}` pinning a specific tool. An unrecognized/malformed
/// shape yields `None` rather than an error.
fn parse_tool_choice(value: &Value) -> Option<ToolChoice> {
    match value {
        Value::String(s) => match s.as_str() {
            "auto" => Some(ToolChoice::Auto),
            "none" => Some(ToolChoice::None),
            "required" => Some(ToolChoice::Any),
            _ => None,
        },
        Value::Object(_) => {
            let name = value.get("function")?.get("name").and_then(Value::as_str)?;
            Some(ToolChoice::Tool(name.to_string()))
        }
        _ => None,
    }
}

/// Parse the `stop` field: either a bare string or an array of up to 4
/// strings.
fn parse_stop(value: &Value) -> Vec<String> {
    match value {
        Value::String(s) => vec![s.clone()],
        Value::Array(arr) => arr
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect(),
        _ => Vec::new(),
    }
}

/// Parse `tools` (modern) or, if absent, the deprecated top-level
/// `functions` array into canonical [`CanonicalTool`]s. OpenAI nests the
/// function name/description/schema one level deeper than Anthropic for
/// `tools` (`tools[].function.name`, `.description`, `.parameters`); the
/// legacy `functions` array has them at the top level of each entry
/// instead.
fn parse_tools(value: &Value) -> Vec<CanonicalTool> {
    if let Some(arr) = value.get("tools").and_then(Value::as_array) {
        return arr
            .iter()
            .filter_map(|t| {
                let function = t.get("function")?;
                let name = function.get("name").and_then(Value::as_str)?.to_string();
                Some(CanonicalTool {
                    name,
                    description: function
                        .get("description")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                    input_schema: function.get("parameters").cloned().unwrap_or(Value::Null),
                })
            })
            .collect();
    }

    value
        .get("functions")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(|f| {
                    let name = f.get("name").and_then(Value::as_str)?.to_string();
                    Some(CanonicalTool {
                        name,
                        description: f
                            .get("description")
                            .and_then(Value::as_str)
                            .map(str::to_string),
                        input_schema: f.get("parameters").cloned().unwrap_or(Value::Null),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Top-level request fields this adapter models by name — anything else
/// lands in [`CanonicalRequest::extra`] so round-tripping stays lossless.
const KNOWN_REQUEST_FIELDS: &[&str] = &[
    "model",
    "messages",
    "tools",
    "functions",
    "tool_choice",
    "max_tokens",
    "max_completion_tokens",
    "temperature",
    "top_p",
    "stop",
    "stream",
];

/// Top-level response fields this adapter models by name.
const KNOWN_RESPONSE_FIELDS: &[&str] = &["model", "choices", "usage"];

fn parse_body(body: &[u8]) -> Result<CanonicalRequest, AdapterError> {
    let value: Value = serde_json::from_slice(body)
        .map_err(|e| AdapterError::Malformed(format!("invalid JSON: {e}")))?;

    let model = value
        .get("model")
        .and_then(Value::as_str)
        .ok_or_else(|| AdapterError::Malformed("missing required field 'model'".to_string()))?
        .to_string();

    // Fold every `role: "system"` message out of `messages` into the
    // canonical top-level `system` field (joining more than one, though a
    // well-formed request will have at most one).
    let mut system_parts: Vec<String> = Vec::new();
    let messages = value
        .get("messages")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(|m| {
                    let role = m.get("role").and_then(Value::as_str).unwrap_or_default();
                    if role == "system" {
                        system_parts
                            .push(m.get("content").map(flatten_content).unwrap_or_default());
                        return None;
                    }
                    Some(CanonicalMessage {
                        role: parse_role(role),
                        content: message_content(role, m),
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    let system = if system_parts.is_empty() {
        None
    } else {
        Some(system_parts.join("\n"))
    };

    let tools = parse_tools(&value);
    let tool_choice = value.get("tool_choice").and_then(parse_tool_choice);

    // `max_completion_tokens` is the newer alias for `max_tokens`; accept
    // either, preferring `max_tokens` when both are present.
    let max_tokens = value
        .get("max_tokens")
        .and_then(Value::as_u64)
        .or_else(|| value.get("max_completion_tokens").and_then(Value::as_u64));

    let sampling = Sampling {
        temperature: value.get("temperature").and_then(Value::as_f64),
        top_p: value.get("top_p").and_then(Value::as_f64),
        top_k: None, // OpenAI's Chat Completions API has no top_k parameter.
        max_tokens,
        stop: value.get("stop").map(parse_stop).unwrap_or_default(),
    };

    let stream = value
        .get("stream")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    let extra = collect_extra(&value, KNOWN_REQUEST_FIELDS, "openai");

    Ok(CanonicalRequest {
        model,
        messages,
        system,
        tools,
        tool_choice,
        sampling,
        stream,
        extra,
    })
}

fn parse_response_body(body: &[u8]) -> Result<CanonicalResponse, AdapterError> {
    let value: Value = serde_json::from_slice(body)
        .map_err(|e| AdapterError::Malformed(format!("invalid JSON: {e}")))?;

    let model = value
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();

    let choice = value
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|c| c.first());

    let content = choice
        .and_then(|c| c.get("message"))
        .map(|m| message_content("assistant", m))
        .unwrap_or_default();

    let stop_reason = choice
        .and_then(|c| c.get("finish_reason"))
        .and_then(Value::as_str)
        .map(parse_stop_reason);

    let usage = value.get("usage").map(|u| CanonicalUsage {
        input_tokens: u.get("prompt_tokens").and_then(Value::as_u64),
        output_tokens: u.get("completion_tokens").and_then(Value::as_u64),
    });

    let extra = collect_extra(&value, KNOWN_RESPONSE_FIELDS, "openai");

    Ok(CanonicalResponse {
        model,
        content,
        stop_reason,
        usage,
        extra,
    })
}

// -- render (M15 Task 3) --------------------------------------------------

/// Render a data-URI or plain URL to reconstruct exactly what
/// [`parse_image_url`] would parse it back down to: a non-empty
/// `media_type` becomes a `data:<media_type>;base64,<data>` URI (the shape
/// `parse_image_url` strips apart); an empty `media_type` (this adapter's
/// marker for "the source was a plain, non-data URL") renders `data` back
/// out verbatim as the URL.
fn render_image_url(media_type: &str, data: &str) -> String {
    if media_type.is_empty() {
        data.to_string()
    } else {
        format!("data:{media_type};base64,{data}")
    }
}

/// Render a message's `Text`/`Image` blocks to OpenAI's `content` field
/// shape: a lone `Text` block becomes a bare string (mirroring
/// [`parse_content_blocks`]'s "a bare string is a single Text block"
/// convention exactly, so this is the tightest round trip); anything else
/// (more than one block, or any `Image`) becomes an array of typed parts.
/// An empty slice renders as JSON `null` rather than `""` — deliberately,
/// to round-trip exactly: [`message_content`]'s `parse_content_blocks(&Value::Null)`
/// yields zero blocks, matching an originally-empty slice, whereas
/// rendering `""` would reparse into a single `Text { text: "" }` block
/// that wasn't there before (`parse_content_blocks` treats a bare string,
/// empty or not, as one `Text` block). `null` also matches the real
/// OpenAI wire shape for a tool-call-only assistant message (`content:
/// null` alongside a populated `tool_calls`).
///
/// Every caller today pre-filters `blocks` to `Text`/`Image` only (see
/// [`render_message`]/[`render_response_body`], which split `ToolUse`
/// into `tool_calls` and `ToolResult` into separate `tool`-role messages
/// before ever building this slice) — so the `ToolUse`/`ToolResult` arm
/// below should be unreachable in practice. It still returns a real
/// [`AdapterError`] rather than `unreachable!()`, though: this module
/// commits to never panicking on a data path, and a `Result` costs nothing
/// a caller that's already threading `?` through render.
fn render_content_parts(blocks: &[&ContentBlock]) -> Result<Value, AdapterError> {
    if let [ContentBlock::Text { text }] = blocks {
        return Ok(json!(text));
    }
    if blocks.is_empty() {
        return Ok(Value::Null);
    }
    let mut parts: Vec<Value> = Vec::with_capacity(blocks.len());
    for block in blocks {
        let part = match block {
            ContentBlock::Text { text } => json!({"type": "text", "text": text}),
            ContentBlock::Image { media_type, data } => json!({
                "type": "image_url",
                "image_url": {"url": render_image_url(media_type, data)},
            }),
            ContentBlock::ToolUse { .. } | ContentBlock::ToolResult { .. } => {
                return Err(AdapterError::Malformed(
                    "render_content_parts received a ToolUse/ToolResult block; callers must \
                     filter content parts to Text/Image only before calling"
                        .to_string(),
                ));
            }
        };
        parts.push(part);
    }
    Ok(Value::Array(parts))
}

/// Render a [`StopReason`] to OpenAI's `finish_reason` wire string.
/// `EndTurn`/`MaxTokens`/`ToolUse` map onto OpenAI's own
/// `stop`/`length`/`tool_calls` reasons exactly; `StopSequence` has no
/// distinct OpenAI reason (a custom stop sequence also finishes with
/// `"stop"` on OpenAI's wire), so it downgrades to `"stop"` — recorded as a
/// note (the response still finishes cleanly, just without the
/// stop-sequence-specific detail) rather than a drop, since a `finish_reason`
/// is always written, just an imprecise one. `Other` re-emits its raw
/// string verbatim.
fn render_stop_reason(reason: &StopReason, report: &mut TranslationReport) -> String {
    match reason {
        StopReason::EndTurn => "stop".to_string(),
        StopReason::MaxTokens => "length".to_string(),
        StopReason::ToolUse => "tool_calls".to_string(),
        StopReason::StopSequence => {
            report.notes.push(
                "stop_reason: StopSequence has no distinct OpenAI finish_reason, mapped to \"stop\""
                    .to_string(),
            );
            "stop".to_string()
        }
        StopReason::Other(s) => s.clone(),
    }
}

/// Render a [`ToolChoice`] to OpenAI's `tool_choice` wire value. All four
/// canonical variants have an exact OpenAI equivalent
/// (`"auto"`/`"none"`/`"required"`/a pinned function object), matching
/// [`parse_tool_choice`]'s own mapping in reverse — lossless.
fn render_tool_choice(choice: &ToolChoice) -> Value {
    match choice {
        ToolChoice::Auto => json!("auto"),
        ToolChoice::None => json!("none"),
        ToolChoice::Any => json!("required"),
        ToolChoice::Tool(name) => json!({"type": "function", "function": {"name": name}}),
    }
}

/// Render one [`CanonicalMessage`] into zero or more OpenAI wire-format
/// message objects, pushed onto `out`. OpenAI's shape splits what
/// Anthropic/Google keep inline: a `ToolResult` block can't live inside
/// another message's `content` array — it must be its own `role: "tool"`
/// message carrying `tool_call_id` — so each `ToolResult` block in `msg`
/// becomes a separate pushed message (its `is_error` flag has no OpenAI
/// tool-message field, so a `true` value is recorded as dropped); a
/// `ToolUse` block becomes an entry in the *primary* message's `tool_calls`
/// array instead of a content part (matching [`tool_calls_to_blocks`]'s
/// parse side in reverse); remaining `Text`/`Image` blocks render via
/// [`render_content_parts`]. The primary message (role + content +
/// optional `tool_calls`) is skipped only when its own content would be
/// empty *and* the message's content was entirely `ToolResult` blocks
/// (already fully represented by the split-out `tool` messages) — an
/// originally empty message (no blocks at all) still renders as one
/// `content: null` message, preserving message count.
///
/// The primary message's `role` is derived from *what's in it*, not just
/// `msg.role`'s label, because OpenAI's wire format ties `role` to content
/// shape in ways the canonical `Role` doesn't guarantee line up: `tool_calls`
/// is only ever legal on `role: "assistant"`, so a message carrying a
/// `ToolUse` block is always rendered `role: "assistant"` regardless of what
/// `msg.role` says (reachable, e.g., via Google's `"function"`-role parsing,
/// which maps to canonical [`Role::Tool`] but doesn't police which part
/// types the message actually carries — a `Role::Tool` message could in
/// principle hold a `ToolUse` block rather than the usual `ToolResult`).
/// Symmetrically, `role: "tool"` is only ever legal paired with a
/// `tool_call_id`, which only the split-out `ToolResult` messages above
/// have — so if `msg.role` is [`Role::Tool`] but (after tool_calls/
/// tool_result handling) there's still non-empty `Text`/`Image` content left
/// with no id to pair it with, that leftover has no valid OpenAI
/// representation and is dropped with a report entry instead of emitting a
/// `role: "tool"` message with no `tool_call_id` (structurally invalid wire
/// JSON). This is what guarantees this renderer never emits `role: "tool"`
/// without a `tool_call_id`.
fn render_message(
    msg: &CanonicalMessage,
    index: usize,
    report: &mut TranslationReport,
    out: &mut Vec<Value>,
) -> Result<(), AdapterError> {
    let mut text_image_blocks: Vec<&ContentBlock> = Vec::new();
    let mut tool_calls: Vec<Value> = Vec::new();
    let mut saw_tool_result = false;

    for block in &msg.content {
        match block {
            ContentBlock::Text { .. } | ContentBlock::Image { .. } => {
                text_image_blocks.push(block);
            }
            ContentBlock::ToolUse { id, name, input } => {
                tool_calls.push(json!({
                    "id": id,
                    "type": "function",
                    "function": {
                        "name": name,
                        "arguments": serde_json::to_string(input).unwrap_or_default(),
                    },
                }));
            }
            ContentBlock::ToolResult {
                id,
                content,
                is_error,
            } => {
                saw_tool_result = true;
                if *is_error {
                    report.drop(
                        format!("messages[{index}].content.tool_result.is_error"),
                        "openai tool-role message has no is_error field",
                    );
                }
                out.push(json!({
                    "role": "tool",
                    "tool_call_id": id,
                    "content": content,
                }));
            }
        }
    }

    if text_image_blocks.is_empty() && tool_calls.is_empty() && saw_tool_result {
        return Ok(());
    }

    let role = if !tool_calls.is_empty() {
        if msg.role != Role::Assistant {
            report.notes.push(format!(
                "messages[{index}]: role \"{}\" has tool_use content, which openai only \
                 supports on an assistant message; rendered role forced to \"assistant\"",
                msg.role.as_str()
            ));
        }
        "assistant"
    } else if msg.role == Role::Tool {
        // No tool_calls and no tool_call_id available (any ToolResult
        // blocks were already split out above) — a "tool" role message
        // with neither is invalid OpenAI wire JSON, so this leftover
        // content is dropped rather than emitted unpaired.
        report.drop(
            format!("messages[{index}]"),
            "openai tool-role message has no tool_call_id to pair with non-tool_result \
             content; message dropped",
        );
        return Ok(());
    } else {
        msg.role.as_str()
    };

    let mut obj = Map::new();
    obj.insert("role".to_string(), json!(role));
    obj.insert(
        "content".to_string(),
        render_content_parts(&text_image_blocks)?,
    );
    if !tool_calls.is_empty() {
        obj.insert("tool_calls".to_string(), Value::Array(tool_calls));
    }
    out.push(Value::Object(obj));
    Ok(())
}

fn render_request_body(
    req: &CanonicalRequest,
    report: &mut TranslationReport,
) -> Result<Vec<u8>, AdapterError> {
    let mut messages = Vec::with_capacity(req.messages.len() + 1);
    if let Some(system) = &req.system {
        messages.push(json!({"role": "system", "content": system}));
    }
    for (index, msg) in req.messages.iter().enumerate() {
        render_message(msg, index, report, &mut messages)?;
    }

    let mut body = Map::new();
    body.insert("model".to_string(), json!(req.model));
    body.insert("messages".to_string(), Value::Array(messages));

    if !req.tools.is_empty() {
        let tools: Vec<Value> = req
            .tools
            .iter()
            .map(|t| {
                let mut function = Map::new();
                function.insert("name".to_string(), json!(t.name));
                if let Some(description) = &t.description {
                    function.insert("description".to_string(), json!(description));
                }
                function.insert("parameters".to_string(), t.input_schema.clone());
                json!({"type": "function", "function": Value::Object(function)})
            })
            .collect();
        body.insert("tools".to_string(), Value::Array(tools));
    }

    if let Some(choice) = &req.tool_choice {
        body.insert("tool_choice".to_string(), render_tool_choice(choice));
    }

    let Sampling {
        temperature,
        top_p,
        top_k,
        max_tokens,
        stop,
    } = &req.sampling;
    if let Some(temperature) = temperature {
        body.insert("temperature".to_string(), json!(temperature));
    }
    if let Some(top_p) = top_p {
        body.insert("top_p".to_string(), json!(top_p));
    }
    if top_k.is_some() {
        // Per the M15 Task 3 fidelity rules: OpenAI's Chat Completions API
        // has no top_k parameter at all, unlike Anthropic and Google.
        report.drop("sampling.top_k", "openai has no top_k");
    }
    if let Some(max_tokens) = max_tokens {
        // Render as `max_completion_tokens`, not `max_tokens`: OpenAI reasoning
        // models (o1/o3, gpt-5 reasoning) reject `max_tokens` with HTTP 400 and
        // require `max_completion_tokens`, which every current chat and
        // reasoning model accepts. `parse_body` already folds both inbound keys
        // into this one canonical field, so nothing downstream needs the old
        // key back.
        body.insert("max_completion_tokens".to_string(), json!(max_tokens));
    }
    if !stop.is_empty() {
        body.insert("stop".to_string(), json!(stop));
    }

    body.insert("stream".to_string(), json!(req.stream));

    emit_or_drop_extra("openai", &req.extra, &mut body, report);

    serde_json::to_vec(&Value::Object(body))
        .map_err(|e| AdapterError::Malformed(format!("failed to serialize request: {e}")))
}

fn render_response_body(
    resp: &CanonicalResponse,
    report: &mut TranslationReport,
) -> Result<Vec<u8>, AdapterError> {
    let mut text_image_blocks: Vec<&ContentBlock> = Vec::new();
    let mut tool_calls: Vec<Value> = Vec::new();

    for block in &resp.content {
        match block {
            ContentBlock::Text { .. } | ContentBlock::Image { .. } => {
                text_image_blocks.push(block);
            }
            ContentBlock::ToolUse { id, name, input } => {
                tool_calls.push(json!({
                    "id": id,
                    "type": "function",
                    "function": {
                        "name": name,
                        "arguments": serde_json::to_string(input).unwrap_or_default(),
                    },
                }));
            }
            ContentBlock::ToolResult { .. } => {
                // OpenAI's response shape is a single `message` object, not
                // an array of turns — there is nowhere for a standalone
                // tool_result block (that would be its own `role: "tool"`
                // message on the request side) to live in a response.
                report.drop(
                    "content",
                    "openai response message cannot carry a tool_result content block",
                );
            }
        }
    }

    let mut message = Map::new();
    message.insert("role".to_string(), json!("assistant"));
    message.insert(
        "content".to_string(),
        render_content_parts(&text_image_blocks)?,
    );
    if !tool_calls.is_empty() {
        message.insert("tool_calls".to_string(), Value::Array(tool_calls));
    }

    let mut choice = Map::new();
    choice.insert("index".to_string(), json!(0));
    choice.insert("message".to_string(), Value::Object(message));
    if let Some(stop_reason) = &resp.stop_reason {
        choice.insert(
            "finish_reason".to_string(),
            json!(render_stop_reason(stop_reason, report)),
        );
    }

    let mut body = Map::new();
    body.insert("model".to_string(), json!(resp.model));
    body.insert("choices".to_string(), json!([Value::Object(choice)]));

    if let Some(usage) = &resp.usage {
        let mut usage_obj = Map::new();
        if let Some(input_tokens) = usage.input_tokens {
            usage_obj.insert("prompt_tokens".to_string(), json!(input_tokens));
        }
        if let Some(output_tokens) = usage.output_tokens {
            usage_obj.insert("completion_tokens".to_string(), json!(output_tokens));
        }
        body.insert("usage".to_string(), Value::Object(usage_obj));
    }

    emit_or_drop_extra("openai", &resp.extra, &mut body, report);

    // A valid OpenAI chat completion requires `object: "chat.completion"`,
    // a string `id`, and an integer `created` at the top level; a buffered
    // cross-provider translation has none of these (the source dialect's
    // `extra` carries no `openai.*` tag), so a strict OpenAI SDK would
    // reject the rendered body without them. `emit_or_drop_extra` above
    // already promoted any genuine same-provider `openai.object` /
    // `openai.id` / `openai.created` extra into `body` (e.g. when the
    // source WAS openai), so insert only if still absent: a real
    // same-provider value always wins over the synthesized one. `object`
    // is a dialect constant; `id` and `created` have no canonical
    // equivalent to fall back to, so stable placeholders stand in —
    // render cannot use time/randomness, and only the fields'
    // presence/shape (a string id, an integer created) is guaranteed
    // here, not the values.
    if !body.contains_key("object") {
        body.insert("object".to_string(), json!("chat.completion"));
    }
    if !body.contains_key("id") {
        body.insert("id".to_string(), json!("chatcmpl-translated"));
    }
    if !body.contains_key("created") {
        body.insert("created".to_string(), json!(0));
    }

    serde_json::to_vec(&Value::Object(body))
        .map_err(|e| AdapterError::Malformed(format!("failed to serialize response: {e}")))
}

// -- streaming translation (M15 Task 4) -----------------------------------

/// Parse one OpenAI `chat.completion.chunk` SSE event up into at most one
/// canonical [`CanonicalStreamEvent`] (design doc M15 Task 4). OpenAI's
/// stream is *coarser* than the canonical (Anthropic-shaped) lifecycle: it
/// has no `message_start`/`content_block_start`/`stop` events, and streams a
/// tool call's name/id in its first `tool_calls[]` fragment then bare
/// argument fragments after. Since this method returns at most one canonical
/// event per chunk, it lifts each chunk to its single most salient canonical
/// event and remembers block indices in `st` (see [`StreamParseState`]); the
/// Anthropic renderer re-synthesizes the omitted `message_start`/text
/// `content_block_start`/`stop` from [`StreamRenderState`].
///
/// Mapping: the `[DONE]` sentinel -> [`CanonicalStreamEvent::MessageStop`]; a
/// final `usage` chunk (empty `choices`) ->
/// [`CanonicalStreamEvent::MessageDelta`] with `usage`; a non-null
/// `finish_reason` -> [`CanonicalStreamEvent::MessageDelta`] with
/// `stop_reason`; `delta.content` ->
/// [`CanonicalStreamEvent::ContentBlockDelta`] with `text` at the streamed
/// text block's index; the first `delta.tool_calls[]` fragment for a slot
/// (carrying id/name) -> [`CanonicalStreamEvent::ContentBlockStart`] with a
/// `ToolUse` block, and every later fragment for that slot ->
/// [`CanonicalStreamEvent::ContentBlockDelta`] with `partial_json`. A
/// role-only first delta, an empty keep-alive, or malformed JSON ->
/// `Ok(None)`.
///
/// Assumption: the tool-call-opening fragment carries `arguments: ""` (the
/// documented real-world shape), so opening it as a `ContentBlockStart`
/// discards no argument text; only the first `tool_calls[]` entry of a chunk
/// is considered (parallel calls arrive in separate chunks in practice).
fn parse_stream_event(
    event: &SseEvent,
    st: &mut StreamParseState,
) -> Result<Option<CanonicalStreamEvent>, AdapterError> {
    let data = event.data.trim();
    if data.is_empty() {
        return Ok(None);
    }
    if data == "[DONE]" {
        return Ok(Some(CanonicalStreamEvent::MessageStop));
    }
    let value: Value = match serde_json::from_str(data) {
        Ok(value) => value,
        Err(_) => return Ok(None),
    };

    // A final usage-only chunk (empty `choices`) carries the token accounting.
    if let Some(usage) = value.get("usage").filter(|u| !u.is_null()) {
        return Ok(Some(CanonicalStreamEvent::MessageDelta {
            stop_reason: None,
            usage: Some(CanonicalUsage {
                input_tokens: usage.get("prompt_tokens").and_then(Value::as_u64),
                output_tokens: usage.get("completion_tokens").and_then(Value::as_u64),
            }),
        }));
    }

    let choice = match value
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|choices| choices.first())
    {
        Some(choice) => choice,
        None => return Ok(None),
    };

    if let Some(reason) = choice.get("finish_reason").and_then(Value::as_str) {
        return Ok(Some(CanonicalStreamEvent::MessageDelta {
            stop_reason: Some(parse_stop_reason(reason)),
            usage: None,
        }));
    }

    let delta = match choice.get("delta") {
        Some(delta) => delta,
        None => return Ok(None),
    };

    if let Some(text) = delta.get("content").and_then(Value::as_str) {
        let index = st.text_index();
        return Ok(Some(CanonicalStreamEvent::ContentBlockDelta {
            index,
            text: Some(text.to_string()),
            partial_json: None,
        }));
    }

    if let Some(tool_call) = delta
        .get("tool_calls")
        .and_then(Value::as_array)
        .and_then(|calls| calls.first())
    {
        let slot = tool_call.get("index").and_then(Value::as_u64).unwrap_or(0);
        let (index, is_new) = st.tool_index(slot);
        let function = tool_call.get("function");
        if is_new {
            let id = tool_call
                .get("id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let name = function
                .and_then(|f| f.get("name"))
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            return Ok(Some(CanonicalStreamEvent::ContentBlockStart {
                index,
                block: ContentBlock::ToolUse {
                    id,
                    name,
                    input: Value::Null,
                },
            }));
        }
        let fragment = function
            .and_then(|f| f.get("arguments"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        return Ok(Some(CanonicalStreamEvent::ContentBlockDelta {
            index,
            text: None,
            partial_json: Some(fragment),
        }));
    }

    // Role-only first delta (or anything else) carries nothing canonical.
    Ok(None)
}

/// Append one fully-framed OpenAI SSE event (`data: <compact-json>\n\n`) to
/// `out`. OpenAI's stream has no `event:` type line (unlike Anthropic) — the
/// type is implicit in the `object: "chat.completion.chunk"` field.
fn push_data(out: &mut String, value: &Value) {
    out.push_str("data: ");
    out.push_str(&value.to_string());
    out.push_str("\n\n");
}

/// Emit one `chat.completion.chunk` carrying `delta` (the per-chunk
/// incremental payload) and an optional `finish_reason` on the single choice.
fn emit_chunk(
    out: &mut String,
    st: &StreamRenderState,
    delta: Value,
    finish_reason: Option<String>,
) {
    let mut choice = Map::new();
    choice.insert("index".to_string(), json!(0));
    choice.insert("delta".to_string(), delta);
    choice.insert(
        "finish_reason".to_string(),
        match finish_reason {
            Some(reason) => json!(reason),
            None => Value::Null,
        },
    );
    let chunk = json!({
        "object": "chat.completion.chunk",
        "model": st.model,
        "choices": [Value::Object(choice)],
    });
    push_data(out, &chunk);
}

/// Emit OpenAI's role-bearing first chunk (`delta: {"role": "assistant"}`)
/// and mark the stream started. This is the collapse mirror of the Anthropic
/// renderer's synthesis: OpenAI packs "the message is starting" into the
/// first chunk's `delta.role` rather than a standalone event.
fn emit_role_chunk(out: &mut String, st: &mut StreamRenderState) {
    let role = st.role_or_assistant();
    emit_chunk(out, st, json!({"role": role.as_str()}), None);
    st.message_started = true;
}

/// Emit the role-bearing first chunk if it hasn't been emitted yet — so a
/// canonical stream that never carried a [`CanonicalStreamEvent::MessageStart`]
/// (an OpenAI-origin stream) still opens with a valid role chunk before its
/// first content.
fn ensure_role_chunk(out: &mut String, st: &mut StreamRenderState) {
    if !st.message_started {
        emit_role_chunk(out, st);
    }
}

/// Render one canonical [`CanonicalStreamEvent`] down into fully-framed
/// OpenAI streaming bytes (design doc M15 Task 4). OpenAI is coarser than the
/// canonical lifecycle, so this renderer *collapses* the canonical start
/// events into OpenAI's chunk shape:
///
/// - [`CanonicalStreamEvent::MessageStart`] emits the role-bearing first
///   chunk (OpenAI carries the role there, not in a standalone event).
/// - A text [`CanonicalStreamEvent::ContentBlockStart`] emits nothing (OpenAI
///   has no content-start event); a `ToolUse` start emits a `tool_calls[]`
///   fragment carrying the assigned slot index, id, and name.
/// - A [`CanonicalStreamEvent::ContentBlockDelta`] with `text` emits a
///   `delta.content` chunk (opening the stream with a role chunk first if
///   needed); with `partial_json` emits a `tool_calls[]` argument fragment on
///   the block's slot.
/// - A [`CanonicalStreamEvent::ContentBlockStop`] emits nothing.
/// - A [`CanonicalStreamEvent::MessageDelta`] emits a `finish_reason` chunk
///   for its stop reason (via [`render_stop_reason`], which records any
///   downgrade in `report`) and, if it carries `usage`, a trailing usage-only
///   chunk.
/// - A [`CanonicalStreamEvent::MessageStop`] emits OpenAI's required
///   `data: [DONE]` terminal.
fn render_stream_event(
    event: &CanonicalStreamEvent,
    st: &mut StreamRenderState,
    report: &mut TranslationReport,
) -> Result<Vec<u8>, AdapterError> {
    let mut out = String::new();
    match event {
        CanonicalStreamEvent::MessageStart { model, role } => {
            st.model = model.clone();
            st.role = Some(*role);
            emit_role_chunk(&mut out, st);
        }
        CanonicalStreamEvent::ContentBlockStart { index, block } => {
            // Only a tool-use block has an OpenAI content-start representation
            // (a `tool_calls[]` opener); a text/image/tool_result start has no
            // OpenAI event and emits nothing.
            if let ContentBlock::ToolUse { id, name, .. } = block {
                ensure_role_chunk(&mut out, st);
                let slot = st.next_tool_slot;
                st.next_tool_slot += 1;
                st.tool_render.insert(
                    *index,
                    StreamToolRender {
                        slot,
                        name: name.clone(),
                        args: String::new(),
                    },
                );
                st.block_is_tool.insert(*index, true);
                emit_chunk(
                    &mut out,
                    st,
                    json!({
                        "tool_calls": [{
                            "index": slot,
                            "id": id,
                            "type": "function",
                            "function": {"name": name, "arguments": ""},
                        }],
                    }),
                    None,
                );
            }
        }
        CanonicalStreamEvent::ContentBlockDelta {
            index,
            text,
            partial_json,
        } => {
            if let Some(text) = text {
                ensure_role_chunk(&mut out, st);
                emit_chunk(&mut out, st, json!({"content": text}), None);
            } else if let Some(partial_json) = partial_json {
                let slot = match st.tool_render.get(index) {
                    Some(tool) => tool.slot,
                    None => {
                        // A `partial_json` arriving with no prior start (should
                        // not happen for a well-formed stream) still needs a
                        // slot; allocate one so the fragment isn't lost.
                        let slot = st.next_tool_slot;
                        st.next_tool_slot += 1;
                        st.tool_render.insert(
                            *index,
                            StreamToolRender {
                                slot,
                                ..StreamToolRender::default()
                            },
                        );
                        slot
                    }
                };
                emit_chunk(
                    &mut out,
                    st,
                    json!({
                        "tool_calls": [{
                            "index": slot,
                            "function": {"arguments": partial_json},
                        }],
                    }),
                    None,
                );
            }
        }
        CanonicalStreamEvent::ContentBlockStop { .. } => {}
        CanonicalStreamEvent::MessageDelta { stop_reason, usage } => {
            if let Some(stop_reason) = stop_reason {
                let reason = render_stop_reason(stop_reason, report);
                emit_chunk(&mut out, st, json!({}), Some(reason));
            }
            if let Some(usage) = usage {
                let mut usage_obj = Map::new();
                if let Some(input_tokens) = usage.input_tokens {
                    usage_obj.insert("prompt_tokens".to_string(), json!(input_tokens));
                }
                if let Some(output_tokens) = usage.output_tokens {
                    usage_obj.insert("completion_tokens".to_string(), json!(output_tokens));
                }
                let chunk = json!({
                    "object": "chat.completion.chunk",
                    "model": st.model,
                    "choices": [],
                    "usage": Value::Object(usage_obj),
                });
                push_data(&mut out, &chunk);
            }
        }
        CanonicalStreamEvent::MessageStop => {
            // Guard against a double terminal (M15 Task 7 obligation c): the
            // `data: [DONE]` sentinel is emitted at most once, even if a second
            // `MessageStop` arrives (e.g. the proxy synthesizing one at stream
            // end after a real one already flowed through).
            if st.terminal_sent {
                return Ok(out.into_bytes());
            }
            out.push_str("data: [DONE]\n\n");
            st.terminal_sent = true;
        }
    }
    Ok(out.into_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx() -> RequestCtx {
        RequestCtx {
            path: "/v1/chat/completions".to_string(),
            method: "POST".to_string(),
        }
    }

    #[test]
    fn parses_string_content() {
        let body = br#"{
            "model": "gpt-4o",
            "messages": [{"role": "user", "content": "hello there"}]
        }"#;
        let req = parse_body(body).unwrap();
        assert_eq!(req.model, "gpt-4o");
        assert_eq!(req.messages.len(), 1);
        assert_eq!(req.messages[0].role, Role::User);
        assert_eq!(
            req.messages[0].content,
            vec![ContentBlock::Text {
                text: "hello there".to_string()
            }]
        );
        assert_eq!(req.sampling.max_tokens, None);
        assert!(!req.stream);
        assert!(req.tools.is_empty());
    }

    #[test]
    fn parses_part_array_content_preserving_image_parts() {
        let body = br#"{
            "model": "gpt-4o",
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "text", "text": "first part"},
                    {"type": "image_url", "image_url": {"url": "data:image/png;base64,abc"}},
                    {"type": "text", "text": "second part"}
                ]
            }]
        }"#;
        let req = parse_body(body).unwrap();
        assert_eq!(req.messages.len(), 1);
        assert_eq!(
            req.messages[0].content,
            vec![
                ContentBlock::Text {
                    text: "first part".to_string()
                },
                ContentBlock::Image {
                    media_type: "image/png".to_string(),
                    data: "abc".to_string(),
                },
                ContentBlock::Text {
                    text: "second part".to_string()
                },
            ]
        );
    }

    #[test]
    fn plain_url_image_preserved_without_media_type() {
        let body = br#"{
            "model": "gpt-4o",
            "messages": [{
                "role": "user",
                "content": [{"type": "image_url", "image_url": {"url": "https://example.com/x.png"}}]
            }]
        }"#;
        let req = parse_body(body).unwrap();
        assert_eq!(
            req.messages[0].content,
            vec![ContentBlock::Image {
                media_type: String::new(),
                data: "https://example.com/x.png".to_string(),
            }]
        );
    }

    #[test]
    fn system_message_folds_out_into_system_field() {
        let body = br#"{
            "model": "gpt-4o",
            "messages": [
                {"role": "system", "content": "You are helpful."},
                {"role": "user", "content": "hi"}
            ]
        }"#;
        let req = parse_body(body).unwrap();
        assert_eq!(req.system.as_deref(), Some("You are helpful."));
        assert_eq!(req.messages.len(), 1);
        assert_eq!(req.messages[0].role, Role::User);
    }

    #[test]
    fn missing_system_message_is_none() {
        let body = br#"{"model": "gpt-4o", "messages": [{"role": "user", "content": "hi"}]}"#;
        let req = parse_body(body).unwrap();
        assert_eq!(req.system, None);
    }

    /// End-to-end regression test for the M15 Task 2 review fix: this
    /// adapter folds a leading `role: "system"` message out of `messages`
    /// into `CanonicalRequest.system` (see
    /// `system_message_folds_out_into_system_field` above), and
    /// `project_llm` must re-express it as `Llm.messages[0]` for
    /// `provider == "openai"` — matching this gateway's pre-M15 behavior,
    /// where the OpenAI adapter built `Llm.messages` straight off the wire
    /// array, system message included.
    #[test]
    fn project_llm_reproduces_leading_system_message_for_openai() {
        let body = br#"{
            "model": "gpt-4o",
            "messages": [
                {"role": "system", "content": "You are helpful."},
                {"role": "user", "content": "hi"}
            ]
        }"#;
        let req = parse_body(body).unwrap();

        let llm = crate::llm::project_llm(&req, "openai");

        assert_eq!(llm.messages.len(), 2);
        assert_eq!(llm.messages[0].role, "system");
        assert_eq!(llm.messages[0].content, "You are helpful.");
        assert_eq!(llm.messages[1].role, "user");
        assert_eq!(llm.messages[1].content, "hi");
    }

    #[test]
    fn assistant_tool_calls_become_tool_use_blocks() {
        let body = br#"{
            "model": "gpt-4o",
            "messages": [{
                "role": "assistant",
                "content": null,
                "tool_calls": [{"id": "call_1", "type": "function", "function": {"name": "get_weather", "arguments": "{\"location\":\"NYC\"}"}}]
            }]
        }"#;
        let req = parse_body(body).unwrap();
        assert_eq!(
            req.messages[0].content,
            vec![ContentBlock::ToolUse {
                id: "call_1".to_string(),
                name: "get_weather".to_string(),
                input: serde_json::json!({"location": "NYC"}),
            }]
        );
    }

    #[test]
    fn tool_role_message_becomes_tool_result_block() {
        let body = br#"{
            "model": "gpt-4o",
            "messages": [{"role": "tool", "tool_call_id": "call_1", "content": "72F and sunny"}]
        }"#;
        let req = parse_body(body).unwrap();
        assert_eq!(req.messages[0].role, Role::Tool);
        assert_eq!(
            req.messages[0].content,
            vec![ContentBlock::ToolResult {
                id: "call_1".to_string(),
                content: "72F and sunny".to_string(),
                is_error: false,
            }]
        );
    }

    #[test]
    fn parses_tool_function_names_and_schema() {
        let body = br#"{
            "model": "gpt-4o",
            "messages": [],
            "tools": [
                {"type": "function", "function": {"name": "get_weather", "description": "...", "parameters": {"type": "object"}}},
                {"type": "function", "function": {"name": "search"}}
            ]
        }"#;
        let req = parse_body(body).unwrap();
        assert_eq!(req.tools.len(), 2);
        assert_eq!(req.tools[0].name, "get_weather");
        assert_eq!(req.tools[0].description.as_deref(), Some("..."));
        assert_eq!(
            req.tools[0].input_schema,
            serde_json::json!({"type": "object"})
        );
        assert_eq!(req.tools[1].name, "search");
    }

    #[test]
    fn parses_legacy_functions_when_tools_absent() {
        let body = br#"{
            "model": "gpt-4o",
            "messages": [],
            "functions": [{"name": "get_weather", "parameters": {"type": "object"}}]
        }"#;
        let req = parse_body(body).unwrap();
        assert_eq!(req.tools.len(), 1);
        assert_eq!(req.tools[0].name, "get_weather");
        assert_eq!(
            req.tools[0].input_schema,
            serde_json::json!({"type": "object"})
        );
    }

    #[test]
    fn parses_tool_choice_variants() {
        for (json, expected) in [
            (r#""auto""#, ToolChoice::Auto),
            (r#""none""#, ToolChoice::None),
            (r#""required""#, ToolChoice::Any),
            (
                r#"{"type": "function", "function": {"name": "get_weather"}}"#,
                ToolChoice::Tool("get_weather".to_string()),
            ),
        ] {
            let body = format!(r#"{{"model": "m", "messages": [], "tool_choice": {json}}}"#);
            let req = parse_body(body.as_bytes()).unwrap();
            assert_eq!(req.tool_choice, Some(expected));
        }
    }

    #[test]
    fn parses_sampling_fields_and_stop_array() {
        let body = br#"{
            "model": "gpt-4o",
            "messages": [],
            "temperature": 0.7,
            "top_p": 0.9,
            "stop": ["STOP", "END"]
        }"#;
        let req = parse_body(body).unwrap();
        assert_eq!(req.sampling.temperature, Some(0.7));
        assert_eq!(req.sampling.top_p, Some(0.9));
        assert_eq!(
            req.sampling.stop,
            vec!["STOP".to_string(), "END".to_string()]
        );
    }

    #[test]
    fn parses_stop_as_bare_string() {
        let body = br#"{"model": "gpt-4o", "messages": [], "stop": "STOP"}"#;
        let req = parse_body(body).unwrap();
        assert_eq!(req.sampling.stop, vec!["STOP".to_string()]);
    }

    #[test]
    fn max_tokens_is_parsed() {
        let body = br#"{"model": "gpt-4o", "messages": [], "max_tokens": 512}"#;
        let req = parse_body(body).unwrap();
        assert_eq!(req.sampling.max_tokens, Some(512));
    }

    #[test]
    fn max_completion_tokens_alias_is_parsed() {
        let body = br#"{"model": "gpt-4o", "messages": [], "max_completion_tokens": 256}"#;
        let req = parse_body(body).unwrap();
        assert_eq!(req.sampling.max_tokens, Some(256));
    }

    #[test]
    fn max_tokens_takes_priority_over_alias_when_both_present() {
        let body = br#"{
            "model": "gpt-4o",
            "messages": [],
            "max_tokens": 512,
            "max_completion_tokens": 256
        }"#;
        let req = parse_body(body).unwrap();
        assert_eq!(req.sampling.max_tokens, Some(512));
    }

    #[test]
    fn missing_max_tokens_is_none() {
        let body = br#"{"model": "gpt-4o", "messages": []}"#;
        let req = parse_body(body).unwrap();
        assert_eq!(req.sampling.max_tokens, None);
    }

    #[test]
    fn parses_stream_flag_true() {
        let body = br#"{"model": "gpt-4o", "messages": [], "stream": true}"#;
        let req = parse_body(body).unwrap();
        assert!(req.stream);
    }

    #[test]
    fn missing_stream_defaults_to_false() {
        let body = br#"{"model": "gpt-4o", "messages": []}"#;
        let req = parse_body(body).unwrap();
        assert!(!req.stream);
    }

    #[test]
    fn unknown_top_level_fields_land_in_extra() {
        let body = br#"{"model": "gpt-4o", "messages": [], "user": "u_123"}"#;
        let req = parse_body(body).unwrap();
        assert_eq!(
            req.extra.get("openai.user"),
            Some(&serde_json::json!("u_123"))
        );
    }

    #[test]
    fn invalid_json_is_malformed_error() {
        let err = parse_body(b"not json at all").unwrap_err();
        assert!(matches!(err, AdapterError::Malformed(_)));
    }

    #[test]
    fn missing_model_is_malformed_error() {
        let body = br#"{"messages": []}"#;
        let err = parse_body(body).unwrap_err();
        assert!(matches!(err, AdapterError::Malformed(_)));
    }

    #[test]
    fn non_string_model_is_malformed_error() {
        let body = br#"{"model": 123, "messages": []}"#;
        let err = parse_body(body).unwrap_err();
        assert!(matches!(err, AdapterError::Malformed(_)));
    }

    #[test]
    fn missing_messages_defaults_to_empty() {
        let body = br#"{"model": "gpt-4o"}"#;
        let req = parse_body(body).unwrap();
        assert!(req.messages.is_empty());
    }

    #[test]
    fn adapter_trait_impl_reports_provider_and_parses() {
        let adapter = OpenAiAdapter;
        assert_eq!(adapter.provider(), "openai");
        let body = br#"{"model": "gpt-4o", "messages": []}"#;
        assert!(adapter.parse_request(body, &ctx()).is_ok());
    }

    // -- parse_response --------------------------------------------------

    #[test]
    fn parses_response_message_content_and_finish_reason_and_usage() {
        let body = br#"{
            "id": "chatcmpl-1",
            "model": "gpt-4o",
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": "hello there"},
                "finish_reason": "stop"
            }],
            "usage": {"prompt_tokens": 10, "completion_tokens": 20, "total_tokens": 30}
        }"#;
        let resp = parse_response_body(body).unwrap();
        assert_eq!(resp.model, "gpt-4o");
        assert_eq!(
            resp.content,
            vec![ContentBlock::Text {
                text: "hello there".to_string()
            }]
        );
        assert_eq!(resp.stop_reason, Some(StopReason::EndTurn));
        assert_eq!(
            resp.usage,
            Some(CanonicalUsage {
                input_tokens: Some(10),
                output_tokens: Some(20),
            })
        );
    }

    #[test]
    fn parses_response_tool_calls() {
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
            }]
        }"#;
        let resp = parse_response_body(body).unwrap();
        assert_eq!(
            resp.content,
            vec![ContentBlock::ToolUse {
                id: "call_1".to_string(),
                name: "get_weather".to_string(),
                input: serde_json::json!({"location": "NYC"}),
            }]
        );
        assert_eq!(resp.stop_reason, Some(StopReason::ToolUse));
    }

    #[test]
    fn unrecognized_finish_reason_becomes_other() {
        let body = br#"{
            "model": "m",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "x"}, "finish_reason": "content_filter"}]
        }"#;
        let resp = parse_response_body(body).unwrap();
        assert_eq!(
            resp.stop_reason,
            Some(StopReason::Other("content_filter".to_string()))
        );
    }

    #[test]
    fn missing_choices_yields_empty_content_and_no_stop_reason() {
        let body = br#"{"model": "m", "choices": []}"#;
        let resp = parse_response_body(body).unwrap();
        assert!(resp.content.is_empty());
        assert_eq!(resp.stop_reason, None);
    }

    #[test]
    fn response_unknown_fields_land_in_extra() {
        let body =
            br#"{"id": "chatcmpl-1", "object": "chat.completion", "model": "m", "choices": []}"#;
        let resp = parse_response_body(body).unwrap();
        assert_eq!(
            resp.extra.get("openai.id"),
            Some(&serde_json::json!("chatcmpl-1"))
        );
        assert_eq!(
            resp.extra.get("openai.object"),
            Some(&serde_json::json!("chat.completion"))
        );
    }

    #[test]
    fn response_invalid_json_is_malformed_error() {
        let err = parse_response_body(b"not json at all").unwrap_err();
        assert!(matches!(err, AdapterError::Malformed(_)));
    }

    #[test]
    fn adapter_trait_impl_parses_response() {
        let adapter = OpenAiAdapter;
        let body = br#"{"model": "m", "choices": []}"#;
        assert!(adapter.parse_response(body).is_ok());
    }

    use crate::llm::delta::DeltaKind;

    fn event(data: &str) -> SseEvent {
        SseEvent {
            data: data.to_string(),
        }
    }

    #[test]
    fn parses_text_delta() {
        let data =
            event(r#"{"choices":[{"index":0,"delta":{"content":"Hello"},"finish_reason":null}]}"#);
        let delta = parse_delta(&data).expect("must parse");
        assert_eq!(delta.kind, DeltaKind::Text);
        assert_eq!(delta.text.as_deref(), Some("Hello"));
    }

    #[test]
    fn parses_tool_call_argument_fragment() {
        let data = event(
            r#"{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","function":{"name":"get_weather","arguments":"{\"loc"}}]},"finish_reason":null}]}"#,
        );
        let delta = parse_delta(&data).expect("must parse");
        assert_eq!(delta.kind, DeltaKind::ToolCall);
        let tool_call = delta.tool_call.expect("tool_call must be set");
        assert_eq!(tool_call.id.as_deref(), Some("call_1"));
        assert_eq!(tool_call.name.as_deref(), Some("get_weather"));
        assert_eq!(tool_call.arguments_fragment.as_deref(), Some(r#"{"loc"#));
    }

    #[test]
    fn parses_top_level_usage() {
        let data = event(
            r#"{"choices":[],"usage":{"prompt_tokens":12,"completion_tokens":34,"total_tokens":46}}"#,
        );
        let delta = parse_delta(&data).expect("must parse");
        assert_eq!(delta.kind, DeltaKind::Usage);
        let usage = delta.usage.expect("usage must be set");
        assert_eq!(usage.input_tokens, Some(12));
        assert_eq!(usage.output_tokens, Some(34));
    }

    #[test]
    fn parses_finish_reason() {
        let data = event(r#"{"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}"#);
        let delta = parse_delta(&data).expect("must parse");
        assert_eq!(delta.kind, DeltaKind::Finish);
        assert_eq!(delta.finish.unwrap().reason.as_deref(), Some("stop"));
    }

    #[test]
    fn done_sentinel_is_none() {
        let data = event("[DONE]");
        assert!(parse_delta(&data).is_none());
    }

    #[test]
    fn malformed_json_is_none() {
        let data = event("not json");
        assert!(parse_delta(&data).is_none());
    }

    #[test]
    fn adapter_parse_delta_dispatches_to_free_function() {
        let adapter = OpenAiAdapter;
        let data =
            event(r#"{"choices":[{"index":0,"delta":{"content":"hi"},"finish_reason":null}]}"#);
        let delta = adapter.parse_delta(&data).expect("must parse");
        assert_eq!(delta.text.as_deref(), Some("hi"));
    }

    // -- render_request / render_response (M15 Task 3) --------------------

    /// No `top_k` here (unlike `anthropic::tests::sample_request`) —
    /// OpenAI's Chat Completions API has no such parameter, so a request
    /// carrying one can never round-trip losslessly through this adapter's
    /// render; that's covered separately by
    /// `render_request_drops_top_k_with_report_note`.
    fn sample_request() -> CanonicalRequest {
        CanonicalRequest {
            model: "gpt-4o".to_string(),
            messages: vec![CanonicalMessage {
                role: Role::User,
                content: vec![ContentBlock::Text {
                    text: "hi".to_string(),
                }],
            }],
            system: Some("You are helpful.".to_string()),
            tools: vec![CanonicalTool {
                name: "get_weather".to_string(),
                description: Some("Look up the weather".to_string()),
                input_schema: serde_json::json!({"type": "object"}),
            }],
            tool_choice: Some(ToolChoice::Auto),
            sampling: Sampling {
                temperature: Some(0.7),
                top_p: Some(0.9),
                top_k: None,
                max_tokens: Some(1024),
                stop: vec!["STOP".to_string()],
            },
            stream: false,
            extra: Default::default(),
        }
    }

    #[test]
    fn render_request_round_trips_through_parse() {
        let req = sample_request();
        let mut report = TranslationReport::default();
        let body = render_request_body(&req, &mut report).unwrap();
        assert!(report.dropped.is_empty());

        let reparsed = parse_body(&body).unwrap();
        assert_eq!(reparsed, req);
    }

    #[test]
    fn render_request_writes_leading_system_message_and_tools() {
        let req = sample_request();
        let mut report = TranslationReport::default();
        let body = render_request_body(&req, &mut report).unwrap();
        let value: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(value["messages"][0]["role"], json!("system"));
        assert_eq!(value["messages"][0]["content"], json!("You are helpful."));
        assert_eq!(value["messages"][1]["role"], json!("user"));
        assert_eq!(
            value["tools"][0],
            json!({
                "type": "function",
                "function": {
                    "name": "get_weather",
                    "description": "Look up the weather",
                    "parameters": {"type": "object"},
                },
            })
        );
        // OpenAI reasoning models require `max_completion_tokens`; the renderer
        // emits that key (never the o1/o3-rejected `max_tokens`).
        assert_eq!(value["max_completion_tokens"], json!(1024));
        assert!(value.get("max_tokens").is_none());
    }

    #[test]
    fn render_request_drops_top_k_with_report_note() {
        let mut req = sample_request();
        req.sampling.top_k = Some(40);
        let mut report = TranslationReport::default();
        let body = render_request_body(&req, &mut report).unwrap();
        let value: Value = serde_json::from_slice(&body).unwrap();

        assert!(value.get("top_k").is_none());
        assert_eq!(report.dropped.len(), 1);
        assert_eq!(report.dropped[0].path, "sampling.top_k");
    }

    #[test]
    fn render_request_splits_tool_result_into_separate_tool_message() {
        let mut req = sample_request();
        req.messages = vec![
            CanonicalMessage {
                role: Role::Assistant,
                content: vec![ContentBlock::ToolUse {
                    id: "call_1".to_string(),
                    name: "get_weather".to_string(),
                    input: serde_json::json!({"location": "NYC"}),
                }],
            },
            CanonicalMessage {
                role: Role::Tool,
                content: vec![ContentBlock::ToolResult {
                    id: "call_1".to_string(),
                    content: "72F and sunny".to_string(),
                    is_error: false,
                }],
            },
        ];
        let mut report = TranslationReport::default();
        let body = render_request_body(&req, &mut report).unwrap();
        let value: Value = serde_json::from_slice(&body).unwrap();

        // messages[0] is the leading system message; [1] is the assistant
        // tool_calls message; [2] is the split-out tool-result message.
        assert_eq!(
            value["messages"][1]["tool_calls"][0]["function"]["name"],
            json!("get_weather")
        );
        assert_eq!(value["messages"][2]["role"], json!("tool"));
        assert_eq!(value["messages"][2]["tool_call_id"], json!("call_1"));
        assert_eq!(value["messages"][2]["content"], json!("72F and sunny"));
    }

    /// M15 Task 3 review fix (Finding 2): a canonical `Role::Tool` message
    /// carrying a `ToolUse` block — reachable via Google's `"function"`-role
    /// parsing, which maps to `Role::Tool` without policing which part
    /// types the message actually holds (see `render_message`'s doc
    /// comment) — must never render as `{"role": "tool", "tool_calls":
    /// [...]}`, which has no `tool_call_id` and is structurally invalid
    /// OpenAI wire JSON. It must instead render as a valid `role:
    /// "assistant"` message carrying `tool_calls` (OpenAI's only legal home
    /// for a tool call).
    #[test]
    fn tool_role_message_with_tool_use_block_renders_as_assistant_with_tool_calls() {
        let mut req = sample_request();
        req.messages = vec![CanonicalMessage {
            role: Role::Tool,
            content: vec![ContentBlock::ToolUse {
                id: "call_1".to_string(),
                name: "get_weather".to_string(),
                input: serde_json::json!({"location": "NYC"}),
            }],
        }];
        let mut report = TranslationReport::default();
        let body = render_request_body(&req, &mut report).unwrap();
        let value: Value = serde_json::from_slice(&body).unwrap();

        // messages[0] is the leading system message; [1] is the message
        // built from the Role::Tool/ToolUse input above.
        let rendered = &value["messages"][1];
        assert_eq!(rendered["role"], json!("assistant"));
        assert_eq!(
            rendered["tool_calls"][0]["function"]["name"],
            json!("get_weather")
        );
        // Never a bare "tool" role with no tool_call_id anywhere in the
        // rendered body.
        for message in value["messages"].as_array().unwrap() {
            if message["role"] == json!("tool") {
                assert!(
                    message.get("tool_call_id").is_some(),
                    "a role:\"tool\" message must always carry tool_call_id: {message:?}"
                );
            }
        }
        assert!(!report.notes.is_empty());

        // The rendered body is valid, parseable OpenAI JSON, not garbage:
        // it reparses into an assistant message carrying the same tool use.
        let reparsed = parse_body(&body).unwrap();
        assert_eq!(
            reparsed.messages.iter().find(|m| m.role == Role::Assistant),
            Some(&CanonicalMessage {
                role: Role::Assistant,
                content: vec![ContentBlock::ToolUse {
                    id: "call_1".to_string(),
                    name: "get_weather".to_string(),
                    input: serde_json::json!({"location": "NYC"}),
                }],
            })
        );
    }

    /// M15 Task 3 review fix (Finding 3): `render_content_parts` used to
    /// `unreachable!()` on a `ToolUse`/`ToolResult` block; this module
    /// commits to no data-path panics, so it must return an `AdapterError`
    /// instead — exercised directly here since every real caller already
    /// pre-filters to `Text`/`Image` blocks, making this arm otherwise
    /// untestable through the public render entry points.
    #[test]
    fn render_content_parts_returns_error_instead_of_panicking_on_tool_blocks() {
        let block = ContentBlock::ToolUse {
            id: "call_1".to_string(),
            name: "get_weather".to_string(),
            input: serde_json::json!({}),
        };
        let blocks: Vec<&ContentBlock> = vec![&block];
        let err = render_content_parts(&blocks).unwrap_err();
        assert!(matches!(err, AdapterError::Malformed(_)));
    }

    /// A `user` field tagged as having come from a *different* provider's
    /// parse (`"anthropic.user"`) — this adapter's render can't verify it
    /// belongs to OpenAI's dialect, so it's dropped with a report entry.
    /// Contrast with
    /// `render_request_preserves_same_provider_extra_field_on_round_trip`
    /// below, where an `"openai."`-tagged entry survives instead.
    #[test]
    fn render_request_drops_cross_provider_extra_fields() {
        let mut req = sample_request();
        req.extra
            .insert("anthropic.user".to_string(), serde_json::json!("u_123"));
        let mut report = TranslationReport::default();
        render_request_body(&req, &mut report).unwrap();

        assert_eq!(report.dropped.len(), 1);
        assert_eq!(report.dropped[0].path, "extra.user");
        assert!(report.dropped[0].reason.contains("anthropic"));
    }

    /// M15 Task 3 review fix: an `extra` entry tagged with *this* adapter's
    /// own provider name must round-trip losslessly through render rather
    /// than being dropped.
    #[test]
    fn render_request_preserves_same_provider_extra_field_on_round_trip() {
        let mut req = sample_request();
        req.extra
            .insert("openai.user".to_string(), serde_json::json!("u_123"));
        let mut report = TranslationReport::default();
        let body = render_request_body(&req, &mut report).unwrap();
        assert!(
            report.dropped.is_empty(),
            "same-provider extra must survive: {:?}",
            report.dropped
        );

        let value: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["user"], json!("u_123"));

        let reparsed = parse_body(&body).unwrap();
        assert_eq!(reparsed, req);
    }

    /// `render_response_body` now synthesizes the target dialect's required
    /// `object`/`id`/`created` discriminator fields (FIX-6) whenever they
    /// are absent, so a bare `resp` with no `openai.*` extra tag still
    /// round trips, but the reparsed value gains those three fields back
    /// into `extra` (they aren't in `KNOWN_RESPONSE_FIELDS`, so parse
    /// collects them there, tagged as having come from openai, same as any
    /// other unmodeled openai field would).
    #[test]
    fn render_response_round_trips_through_parse() {
        let resp = CanonicalResponse {
            model: "gpt-4o".to_string(),
            content: vec![ContentBlock::Text {
                text: "hello there".to_string(),
            }],
            stop_reason: Some(StopReason::EndTurn),
            usage: Some(CanonicalUsage {
                input_tokens: Some(10),
                output_tokens: Some(20),
            }),
            extra: Default::default(),
        };
        let mut report = TranslationReport::default();
        let body = render_response_body(&resp, &mut report).unwrap();
        assert!(report.dropped.is_empty());

        let reparsed = parse_response_body(&body).unwrap();
        let mut expected = resp.clone();
        expected
            .extra
            .insert("openai.object".to_string(), json!("chat.completion"));
        expected
            .extra
            .insert("openai.id".to_string(), json!("chatcmpl-translated"));
        expected
            .extra
            .insert("openai.created".to_string(), json!(0));
        assert_eq!(reparsed, expected);
    }

    /// A representative real OpenAI response `extra` field (`object`, not
    /// modeled in `KNOWN_RESPONSE_FIELDS`) must survive a same-provider
    /// parse -> render. `id`/`created` are still synthesized (FIX-6) since
    /// this `extra` carries no same-provider tag for them, so they show up
    /// in the reparsed `extra` alongside the preserved `object`.
    #[test]
    fn render_response_preserves_same_provider_extra_field_on_round_trip() {
        let mut resp = CanonicalResponse {
            model: "gpt-4o".to_string(),
            content: vec![ContentBlock::Text {
                text: "hello there".to_string(),
            }],
            stop_reason: Some(StopReason::EndTurn),
            usage: None,
            extra: Default::default(),
        };
        resp.extra.insert(
            "openai.object".to_string(),
            serde_json::json!("chat.completion"),
        );

        let mut report = TranslationReport::default();
        let body = render_response_body(&resp, &mut report).unwrap();
        assert!(
            report.dropped.is_empty(),
            "same-provider extra must survive: {:?}",
            report.dropped
        );

        let value: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["object"], json!("chat.completion"));

        let reparsed = parse_response_body(&body).unwrap();
        let mut expected = resp.clone();
        expected
            .extra
            .insert("openai.id".to_string(), json!("chatcmpl-translated"));
        expected
            .extra
            .insert("openai.created".to_string(), json!(0));
        assert_eq!(reparsed, expected);
    }

    /// FIX-6: a buffered cross-provider translated response (no
    /// `openai.*` tag in `extra` at all, simulating a source dialect that
    /// isn't openai) must still carry the OpenAI chat.completion schema's
    /// required top-level discriminators, and those fields must NOT be
    /// reported as dropped (they are synthesized/emitted, not discarded).
    #[test]
    fn render_response_synthesizes_discriminators_for_cross_provider_translation() {
        let mut resp = CanonicalResponse {
            model: "gpt-4o".to_string(),
            content: vec![ContentBlock::Text {
                text: "hello there".to_string(),
            }],
            stop_reason: Some(StopReason::EndTurn),
            usage: None,
            extra: Default::default(),
        };
        // Tagged as having come from a different provider's parse, so
        // `emit_or_drop_extra` drops it rather than treating it as a
        // same-provider discriminator.
        resp.extra
            .insert("anthropic.id".to_string(), serde_json::json!("msg_1"));

        let mut report = TranslationReport::default();
        let body = render_response_body(&resp, &mut report).unwrap();

        let value: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["object"], json!("chat.completion"));
        assert!(value["id"].is_string());
        assert!(value["created"].is_i64() || value["created"].is_u64());

        for field in ["object", "id", "created"] {
            assert!(
                !report.dropped.iter().any(|d| d.path == field),
                "{field} must not be reported as dropped: {:?}",
                report.dropped
            );
        }
    }

    /// FIX-6: when the source WAS openai and its real `id`/`created`
    /// survived translation as same-provider extra, those real values must
    /// win over the synthesized placeholder/constant, not be clobbered by
    /// it.
    #[test]
    fn render_response_prefers_real_same_provider_id_and_created_over_synthesized() {
        let mut resp = CanonicalResponse {
            model: "gpt-4o".to_string(),
            content: vec![ContentBlock::Text {
                text: "hello there".to_string(),
            }],
            stop_reason: Some(StopReason::EndTurn),
            usage: None,
            extra: Default::default(),
        };
        resp.extra
            .insert("openai.id".to_string(), json!("chatcmpl-real0123"));
        resp.extra
            .insert("openai.created".to_string(), json!(1_700_000_000));

        let mut report = TranslationReport::default();
        let body = render_response_body(&resp, &mut report).unwrap();
        assert!(report.dropped.is_empty(), "{:?}", report.dropped);

        let value: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["id"], json!("chatcmpl-real0123"));
        assert_ne!(value["id"], json!("chatcmpl-translated"));
        assert_eq!(value["created"], json!(1_700_000_000));
        assert_eq!(value["object"], json!("chat.completion"));
    }

    #[test]
    fn render_response_maps_tool_use_to_tool_calls_and_finish_reason() {
        let resp = CanonicalResponse {
            model: "gpt-4o".to_string(),
            content: vec![ContentBlock::ToolUse {
                id: "call_1".to_string(),
                name: "get_weather".to_string(),
                input: serde_json::json!({"location": "NYC"}),
            }],
            stop_reason: Some(StopReason::ToolUse),
            usage: None,
            extra: Default::default(),
        };
        let mut report = TranslationReport::default();
        let body = render_response_body(&resp, &mut report).unwrap();
        let value: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(
            value["choices"][0]["message"]["tool_calls"][0]["function"]["name"],
            json!("get_weather")
        );
        assert_eq!(value["choices"][0]["finish_reason"], json!("tool_calls"));
    }

    #[test]
    fn render_response_downgrades_stop_sequence_with_note() {
        let resp = CanonicalResponse {
            model: "m".to_string(),
            content: vec![],
            stop_reason: Some(StopReason::StopSequence),
            usage: None,
            extra: Default::default(),
        };
        let mut report = TranslationReport::default();
        let body = render_response_body(&resp, &mut report).unwrap();
        let value: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(value["choices"][0]["finish_reason"], json!("stop"));
        assert!(!report.notes.is_empty());
    }

    #[test]
    fn adapter_trait_render_request_dispatches_to_free_function() {
        let adapter = OpenAiAdapter;
        let mut report = TranslationReport::default();
        let body = adapter
            .render_request(&sample_request(), &mut report)
            .unwrap();
        assert!(!body.is_empty());
    }
}
