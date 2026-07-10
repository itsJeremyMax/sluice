//! Anthropic `/v1/messages` ingress adapter: parses the wire-format request
//! and response bodies into the gateway's lossless canonical LLM IR
//! ([`super::CanonicalRequest`]/[`super::CanonicalResponse`], design doc M15
//! Task 2), and serializes the canonical IR back out (design doc M15 Task
//! 3, the render direction). Content blocks are parsed into structured
//! [`super::ContentBlock`] values (text, images, tool use/result) rather
//! than flattened to plain text — flattening to [`super::Message`]'s
//! single-string shape is [`super::project_llm`]'s job now, not this
//! adapter's.

use serde_json::{json, Map, Value};

use super::adapter::{collect_extra, emit_or_drop_extra, Adapter, AdapterError};
use super::delta::{Delta, Finish, ToolCallDelta, Usage as DeltaUsage};
use super::{
    CanonicalMessage, CanonicalRequest, CanonicalResponse, CanonicalStreamEvent, CanonicalTool,
    CanonicalUsage, ContentBlock, RequestCtx, Role, Sampling, StopReason, StreamParseState,
    StreamRenderState, ToolChoice, TranslationReport,
};
use crate::sse::SseEvent;

/// Parses Anthropic Messages API (`/v1/messages`) request/response bodies.
pub struct AnthropicAdapter;

impl Adapter for AnthropicAdapter {
    fn provider(&self) -> &str {
        "anthropic"
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
        _st: &mut StreamParseState,
    ) -> Result<Option<CanonicalStreamEvent>, AdapterError> {
        parse_stream_event(event)
    }

    fn render_stream_event(
        &self,
        event: &CanonicalStreamEvent,
        st: &mut StreamRenderState,
        _report: &mut TranslationReport,
    ) -> Result<Vec<u8>, AdapterError> {
        render_stream_event(event, st)
    }
}

/// Parse one Anthropic streaming SSE event's `data:` payload into a
/// normalized [`Delta`] (design doc M10 Task 2).
///
/// Anthropic's Messages API streams a sequence of typed events per
/// `type`: `content_block_delta` events carry either a `text_delta` (plain
/// text) or an `input_json_delta` (a fragment of a tool call's arguments,
/// streamed as raw partial JSON text in `partial_json`); `message_delta`
/// carries incremental `usage` (Anthropic reports `output_tokens` there,
/// cumulative across the response); `message_stop` signals the end of the
/// stream. Assumption: `message_stop` carries no fields in the documented
/// API (the terminal `stop_reason` actually arrives earlier, in a
/// `message_delta`'s `delta.stop_reason`) — this parser still emits a
/// `Finish` delta for `message_stop` per the task brief, with `reason` left
/// `None` since it isn't available on that event. Anything else
/// (`message_start`, `content_block_start`/`content_block_stop`, malformed
/// JSON) yields `None`.
fn parse_delta(event: &SseEvent) -> Option<Delta> {
    let value: Value = serde_json::from_str(&event.data).ok()?;
    let event_type = value.get("type").and_then(Value::as_str)?;

    match event_type {
        "content_block_delta" => {
            let delta = value.get("delta")?;
            match delta.get("type").and_then(Value::as_str)? {
                "text_delta" => {
                    let text = delta.get("text").and_then(Value::as_str)?;
                    Some(Delta::text(text))
                }
                "input_json_delta" => {
                    let arguments_fragment = delta
                        .get("partial_json")
                        .and_then(Value::as_str)
                        .map(str::to_string);
                    Some(Delta::tool_call(ToolCallDelta {
                        id: None,
                        name: None,
                        arguments_fragment,
                    }))
                }
                _ => None,
            }
        }
        "message_delta" => {
            let usage = value.get("usage")?;
            Some(Delta::usage(DeltaUsage {
                input_tokens: usage.get("input_tokens").and_then(Value::as_u64),
                output_tokens: usage.get("output_tokens").and_then(Value::as_u64),
            }))
        }
        "message_stop" => Some(Delta::finish(Finish { reason: None })),
        _ => None,
    }
}

/// Flatten a `content` field (bare string or array of content blocks) into
/// plain text. Only `{"type": "text", "text": "..."}` blocks contribute
/// (non-text blocks are skipped). Used for the few places the canonical IR
/// still wants a plain string out of a content field that can carry
/// structured blocks — a tool result's `content`, or a structured `system`
/// prompt — not for message content in general, which is parsed into
/// [`ContentBlock`]s by [`parse_content_blocks`] instead. A missing/malformed
/// field yields an empty string rather than an error, per this adapter's
/// "tolerate missing optional fields" contract.
fn flatten_content(content: &Value) -> String {
    match content {
        Value::String(s) => s.clone(),
        Value::Array(blocks) => blocks
            .iter()
            .filter_map(|block| {
                if block.get("type").and_then(Value::as_str) == Some("text") {
                    block.get("text").and_then(Value::as_str)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
            .join("\n"),
        _ => String::new(),
    }
}

/// Parse a message/response `content` field (bare string or array of typed
/// blocks) into canonical [`ContentBlock`]s, preserving structure instead of
/// flattening to text — this is the difference between this task's parse and
/// M9's: a bare string still becomes a single `Text` block, but an array of
/// blocks keeps every block Anthropic's wire format can carry (`text`,
/// `image`, `tool_use`, `tool_result`), not just the text ones.
fn parse_content_blocks(content: &Value) -> Vec<ContentBlock> {
    match content {
        Value::String(s) => vec![ContentBlock::Text { text: s.clone() }],
        Value::Array(blocks) => blocks.iter().filter_map(parse_one_block).collect(),
        _ => Vec::new(),
    }
}

/// Parse a single Anthropic content block object into a canonical
/// [`ContentBlock`]. An unrecognized `type` (or a block missing `type`
/// entirely) yields `None` and is dropped — the canonical IR has no
/// catch-all block variant yet to preserve it losslessly.
fn parse_one_block(block: &Value) -> Option<ContentBlock> {
    match block.get("type").and_then(Value::as_str)? {
        "text" => Some(ContentBlock::Text {
            text: block
                .get("text")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
        }),
        "image" => {
            let source = block.get("source")?;
            Some(ContentBlock::Image {
                media_type: source
                    .get("media_type")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                data: source
                    .get("data")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
            })
        }
        "tool_use" => Some(ContentBlock::ToolUse {
            id: block
                .get("id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            name: block
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            input: block.get("input").cloned().unwrap_or(Value::Null),
        }),
        "tool_result" => Some(ContentBlock::ToolResult {
            id: block
                .get("tool_use_id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            content: block
                .get("content")
                .map(flatten_content)
                .unwrap_or_default(),
            is_error: block
                .get("is_error")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        }),
        _ => None,
    }
}

/// Map an Anthropic wire-format role string to the canonical [`Role`].
/// Anthropic messages only ever use `"user"`/`"assistant"` on the wire (the
/// system prompt is a top-level field, not a message role) — this still
/// handles `"system"`/`"tool"` defensively for a caller-supplied body that
/// doesn't follow the documented shape, defaulting unrecognized roles to
/// `User` rather than failing the parse.
fn parse_role(role: &str) -> Role {
    match role {
        "assistant" => Role::Assistant,
        "system" => Role::System,
        "tool" => Role::Tool,
        _ => Role::User,
    }
}

/// Map an Anthropic `stop_reason` wire string to the canonical
/// [`StopReason`]. An unrecognized reason is preserved via `Other` rather
/// than dropped, keeping response parsing lossless.
fn parse_stop_reason(reason: &str) -> StopReason {
    match reason {
        "end_turn" => StopReason::EndTurn,
        "max_tokens" => StopReason::MaxTokens,
        "tool_use" => StopReason::ToolUse,
        "stop_sequence" => StopReason::StopSequence,
        other => StopReason::Other(other.to_string()),
    }
}

/// Parse Anthropic's `tool_choice` object (`{"type": "auto"}`,
/// `{"type": "any"}`, `{"type": "none"}`, `{"type": "tool", "name": "..."}`)
/// into the canonical [`ToolChoice`]. An unrecognized/malformed shape yields
/// `None` rather than an error.
fn parse_tool_choice(value: &Value) -> Option<ToolChoice> {
    match value.get("type").and_then(Value::as_str)? {
        "auto" => Some(ToolChoice::Auto),
        "any" => Some(ToolChoice::Any),
        "none" => Some(ToolChoice::None),
        "tool" => Some(ToolChoice::Tool(
            value.get("name").and_then(Value::as_str)?.to_string(),
        )),
        _ => None,
    }
}

/// Top-level request fields this adapter models by name — anything else
/// lands in [`CanonicalRequest::extra`] so round-tripping stays lossless.
const KNOWN_REQUEST_FIELDS: &[&str] = &[
    "model",
    "messages",
    "system",
    "tools",
    "tool_choice",
    "max_tokens",
    "temperature",
    "top_p",
    "top_k",
    "stop_sequences",
    "stream",
];

/// Top-level response fields this adapter models by name.
const KNOWN_RESPONSE_FIELDS: &[&str] = &["model", "content", "stop_reason", "usage"];

fn parse_body(body: &[u8]) -> Result<CanonicalRequest, AdapterError> {
    let value: Value = serde_json::from_slice(body)
        .map_err(|e| AdapterError::Malformed(format!("invalid JSON: {e}")))?;

    let model = value
        .get("model")
        .and_then(Value::as_str)
        .ok_or_else(|| AdapterError::Malformed("missing required field 'model'".to_string()))?
        .to_string();

    let messages = value
        .get("messages")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .map(|m| CanonicalMessage {
                    role: m
                        .get("role")
                        .and_then(Value::as_str)
                        .map(parse_role)
                        .unwrap_or(Role::User),
                    content: m
                        .get("content")
                        .map(parse_content_blocks)
                        .unwrap_or_default(),
                })
                .collect()
        })
        .unwrap_or_default();

    // Anthropic's system prompt is usually a bare string, but the API also
    // accepts an array of text blocks (a "structured" system prompt) — treat
    // it the same way tool_result content is flattened.
    let system = value.get("system").map(flatten_content);

    let tools = value
        .get("tools")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(|t| {
                    let name = t.get("name").and_then(Value::as_str)?.to_string();
                    Some(CanonicalTool {
                        name,
                        description: t
                            .get("description")
                            .and_then(Value::as_str)
                            .map(str::to_string),
                        input_schema: t.get("input_schema").cloned().unwrap_or(Value::Null),
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    let tool_choice = value.get("tool_choice").and_then(parse_tool_choice);

    let sampling = Sampling {
        temperature: value.get("temperature").and_then(Value::as_f64),
        top_p: value.get("top_p").and_then(Value::as_f64),
        top_k: value.get("top_k").and_then(Value::as_u64),
        max_tokens: value.get("max_tokens").and_then(Value::as_u64),
        stop: value
            .get("stop_sequences")
            .and_then(Value::as_array)
            .map(|arr| {
                arr.iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default(),
    };

    let stream = value
        .get("stream")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    let extra = collect_extra(&value, KNOWN_REQUEST_FIELDS, "anthropic");

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

    let content = value
        .get("content")
        .map(parse_content_blocks)
        .unwrap_or_default();

    let stop_reason = value
        .get("stop_reason")
        .and_then(Value::as_str)
        .map(parse_stop_reason);

    let usage = value.get("usage").map(|u| CanonicalUsage {
        input_tokens: u.get("input_tokens").and_then(Value::as_u64),
        output_tokens: u.get("output_tokens").and_then(Value::as_u64),
    });

    let extra = collect_extra(&value, KNOWN_RESPONSE_FIELDS, "anthropic");

    Ok(CanonicalResponse {
        model,
        content,
        stop_reason,
        usage,
        extra,
    })
}

// -- render (M15 Task 3) --------------------------------------------------

/// Render one canonical [`ContentBlock`] to Anthropic's wire-format content
/// block object. Anthropic's content-block array is the richest of the
/// three built-in dialects (design doc M15 Task 3's fidelity table) — all
/// four `ContentBlock` variants have a direct home here, so this never
/// drops anything.
fn render_block(block: &ContentBlock) -> Value {
    match block {
        ContentBlock::Text { text } => json!({"type": "text", "text": text}),
        ContentBlock::Image { media_type, data } => json!({
            "type": "image",
            "source": {"type": "base64", "media_type": media_type, "data": data},
        }),
        ContentBlock::ToolUse { id, name, input } => json!({
            "type": "tool_use",
            "id": id,
            "name": name,
            "input": input,
        }),
        ContentBlock::ToolResult {
            id,
            content,
            is_error,
        } => json!({
            "type": "tool_result",
            "tool_use_id": id,
            "content": content,
            "is_error": is_error,
        }),
    }
}

/// Render a [`StopReason`] to Anthropic's `stop_reason` wire string.
/// Anthropic's four named reasons round-trip exactly; `Other` re-emits its
/// raw string verbatim (Anthropic itself defines no closed enum on the wire,
/// so an arbitrary string is a legal `stop_reason` value) — lossless either
/// way, matching the fidelity rule that `Other` renders its payload where
/// possible.
fn render_stop_reason(reason: &StopReason) -> String {
    match reason {
        StopReason::EndTurn => "end_turn".to_string(),
        StopReason::MaxTokens => "max_tokens".to_string(),
        StopReason::ToolUse => "tool_use".to_string(),
        StopReason::StopSequence => "stop_sequence".to_string(),
        StopReason::Other(s) => s.clone(),
    }
}

/// Render a [`ToolChoice`] to Anthropic's `tool_choice` wire object.
/// Anthropic's four variants (`auto`/`any`/`none`/`tool`) are exactly the
/// canonical [`ToolChoice`] variants, so this is a lossless 1:1 mapping.
fn render_tool_choice(choice: &ToolChoice) -> Value {
    match choice {
        ToolChoice::Auto => json!({"type": "auto"}),
        ToolChoice::Any => json!({"type": "any"}),
        ToolChoice::None => json!({"type": "none"}),
        ToolChoice::Tool(name) => json!({"type": "tool", "name": name}),
    }
}

/// Render one [`CanonicalMessage`] to zero or one Anthropic wire-format
/// message objects, pushed onto `out`. Anthropic's `messages` array only
/// accepts `role: "user"`/`role: "assistant"` entries (system travels
/// out-of-band in the top-level `system` field, and Anthropic has no
/// separate "tool" message role — tool results are content blocks inside a
/// `user`-role message): a canonical [`Role::Tool`] message downgrades to
/// `role: "user"` (its `ToolResult` block content is already
/// self-describing via `tool_use_id`, so the downgrade loses no
/// information, just recorded as a note); a canonical [`Role::System`]
/// message is folded into `system_parts` by the caller instead of becoming
/// a message here (Anthropic can't represent a system-role message inline).
fn render_message(
    msg: &CanonicalMessage,
    index: usize,
    report: &mut TranslationReport,
    out: &mut Vec<Value>,
) {
    let role = match msg.role {
        Role::User => "user",
        Role::Assistant => "assistant",
        Role::Tool => {
            report.notes.push(format!(
                "messages[{index}]: role \"tool\" has no Anthropic equivalent, downgraded to \"user\""
            ));
            "user"
        }
        Role::System => unreachable!(
            "Role::System messages are folded into `system`, not rendered as a message"
        ),
    };
    out.push(json!({
        "role": role,
        "content": msg.content.iter().map(render_block).collect::<Vec<_>>(),
    }));
}

/// Split `req.messages` into the leading system text (folded together with
/// `req.system`, since Anthropic only has one place a system prompt can
/// live) and the rendered `user`/`assistant` message array. A
/// [`Role::System`] canonical message's non-`Text` blocks (e.g. an image)
/// have no representation in Anthropic's plain-string `system` field, so
/// those are dropped with a reason; its `Text` blocks are joined into the
/// system string, matching how this same adapter's parse side flattens a
/// structured Anthropic `system` array back down to one string.
fn render_system_and_messages(
    req: &CanonicalRequest,
    report: &mut TranslationReport,
) -> (Option<String>, Vec<Value>) {
    let mut system_parts: Vec<String> = req.system.iter().cloned().collect();
    let mut messages = Vec::with_capacity(req.messages.len());

    for (index, msg) in req.messages.iter().enumerate() {
        if msg.role == Role::System {
            for block in &msg.content {
                match block {
                    ContentBlock::Text { text } => system_parts.push(text.clone()),
                    _ => report.drop(
                        format!("messages[{index}].content"),
                        "anthropic system prompt is plain text; non-text content in a \
                         system-role message has no representation there",
                    ),
                }
            }
            continue;
        }
        render_message(msg, index, report, &mut messages);
    }

    let system = if system_parts.is_empty() {
        None
    } else {
        Some(system_parts.join("\n"))
    };
    (system, messages)
}

fn render_request_body(
    req: &CanonicalRequest,
    report: &mut TranslationReport,
) -> Result<Vec<u8>, AdapterError> {
    let (system, messages) = render_system_and_messages(req, report);

    let mut body = Map::new();
    body.insert("model".to_string(), json!(req.model));
    if let Some(system) = system {
        body.insert("system".to_string(), json!(system));
    }
    body.insert("messages".to_string(), Value::Array(messages));

    if !req.tools.is_empty() {
        let tools: Vec<Value> = req
            .tools
            .iter()
            .map(|t| {
                let mut tool = Map::new();
                tool.insert("name".to_string(), json!(t.name));
                if let Some(description) = &t.description {
                    tool.insert("description".to_string(), json!(description));
                }
                tool.insert("input_schema".to_string(), t.input_schema.clone());
                Value::Object(tool)
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
    if let Some(top_k) = top_k {
        body.insert("top_k".to_string(), json!(top_k));
    }
    if let Some(max_tokens) = max_tokens {
        body.insert("max_tokens".to_string(), json!(max_tokens));
    }
    if !stop.is_empty() {
        body.insert("stop_sequences".to_string(), json!(stop));
    }

    body.insert("stream".to_string(), json!(req.stream));

    emit_or_drop_extra("anthropic", &req.extra, &mut body, report);

    serde_json::to_vec(&Value::Object(body))
        .map_err(|e| AdapterError::Malformed(format!("failed to serialize request: {e}")))
}

fn render_response_body(
    resp: &CanonicalResponse,
    report: &mut TranslationReport,
) -> Result<Vec<u8>, AdapterError> {
    let mut body = Map::new();
    body.insert("model".to_string(), json!(resp.model));
    body.insert(
        "content".to_string(),
        Value::Array(resp.content.iter().map(render_block).collect()),
    );

    if let Some(stop_reason) = &resp.stop_reason {
        body.insert(
            "stop_reason".to_string(),
            json!(render_stop_reason(stop_reason)),
        );
    }

    if let Some(usage) = &resp.usage {
        let mut usage_obj = Map::new();
        if let Some(input_tokens) = usage.input_tokens {
            usage_obj.insert("input_tokens".to_string(), json!(input_tokens));
        }
        if let Some(output_tokens) = usage.output_tokens {
            usage_obj.insert("output_tokens".to_string(), json!(output_tokens));
        }
        body.insert("usage".to_string(), Value::Object(usage_obj));
    }

    emit_or_drop_extra("anthropic", &resp.extra, &mut body, report);

    // A valid Anthropic Messages response requires `type: "message"`,
    // `role: "assistant"`, and a string `id` at the top level; a buffered
    // cross-provider translation has none of these (the source dialect's
    // `extra` carries no `anthropic.*` tag), so a strict Anthropic SDK
    // would reject the rendered body without them. `emit_or_drop_extra`
    // above already promoted any genuine same-provider `anthropic.id` /
    // `anthropic.type` / `anthropic.role` extra into `body` (e.g. when the
    // source WAS anthropic), so insert only if still absent: a real
    // same-provider value always wins over the synthesized one. `type` and
    // `role` are dialect constants; `id` has no canonical equivalent to
    // fall back to, so a stable placeholder stands in — render cannot use
    // time/randomness, and only the field's presence/shape (a string) is
    // guaranteed here, not the value.
    if !body.contains_key("type") {
        body.insert("type".to_string(), json!("message"));
    }
    if !body.contains_key("role") {
        body.insert("role".to_string(), json!("assistant"));
    }
    if !body.contains_key("id") {
        body.insert("id".to_string(), json!("msg_translated"));
    }

    serde_json::to_vec(&Value::Object(body))
        .map_err(|e| AdapterError::Malformed(format!("failed to serialize response: {e}")))
}

// -- streaming translation (M15 Task 4) -----------------------------------

/// Parse one Anthropic streaming SSE event's `data:` payload up into a
/// canonical [`CanonicalStreamEvent`] (design doc M15 Task 4). Anthropic's
/// stream is already the granular lifecycle the canonical IR is modeled on,
/// so this is a near-direct structural mapping and needs no
/// [`StreamParseState`] (the coarser OpenAI/Google parsers do).
///
/// Dispatch is on the `type` field *inside* `event.data`, not the SSE
/// `event:` line — [`crate::sse::SseEvent`] only surfaces the `data:`
/// payload, and Anthropic redundantly carries the type in the JSON too.
/// `message_start` -> [`CanonicalStreamEvent::MessageStart`] (model+role from
/// the nested `message` object); `content_block_start` -> a
/// [`CanonicalStreamEvent::ContentBlockStart`] whose block is parsed from the
/// `content_block` object via the same [`parse_one_block`] the buffered
/// parser uses; a `content_block_delta` with a `text_delta` or
/// `input_json_delta` -> [`CanonicalStreamEvent::ContentBlockDelta`] carrying
/// `text` or `partial_json` respectively; `content_block_stop` ->
/// [`CanonicalStreamEvent::ContentBlockStop`]; `message_delta` ->
/// [`CanonicalStreamEvent::MessageDelta`] (`stop_reason` from `delta`,
/// incremental `usage`); `message_stop` ->
/// [`CanonicalStreamEvent::MessageStop`]. A `ping`, an empty keep-alive, an
/// unrecognized type, or malformed JSON all yield `Ok(None)` (never a panic,
/// never an error — one bad frame must not abort the stream).
fn parse_stream_event(event: &SseEvent) -> Result<Option<CanonicalStreamEvent>, AdapterError> {
    let data = event.data.trim();
    if data.is_empty() {
        return Ok(None);
    }
    let value: Value = match serde_json::from_str(data) {
        Ok(value) => value,
        Err(_) => return Ok(None),
    };
    let event_type = match value.get("type").and_then(Value::as_str) {
        Some(event_type) => event_type,
        None => return Ok(None),
    };

    let parsed = match event_type {
        "message_start" => {
            let message = value.get("message");
            let model = message
                .and_then(|m| m.get("model"))
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let role = message
                .and_then(|m| m.get("role"))
                .and_then(Value::as_str)
                .map(parse_role)
                .unwrap_or(Role::Assistant);
            CanonicalStreamEvent::MessageStart { model, role }
        }
        "content_block_start" => {
            let index = value.get("index").and_then(Value::as_u64).unwrap_or(0);
            let block = value
                .get("content_block")
                .and_then(parse_one_block)
                .unwrap_or(ContentBlock::Text {
                    text: String::new(),
                });
            CanonicalStreamEvent::ContentBlockStart { index, block }
        }
        "content_block_delta" => {
            let index = value.get("index").and_then(Value::as_u64).unwrap_or(0);
            let delta = value.get("delta");
            match delta.and_then(|d| d.get("type")).and_then(Value::as_str) {
                Some("text_delta") => {
                    let text = delta
                        .and_then(|d| d.get("text"))
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                    CanonicalStreamEvent::ContentBlockDelta {
                        index,
                        text: Some(text),
                        partial_json: None,
                    }
                }
                Some("input_json_delta") => {
                    let partial_json = delta
                        .and_then(|d| d.get("partial_json"))
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                    CanonicalStreamEvent::ContentBlockDelta {
                        index,
                        text: None,
                        partial_json: Some(partial_json),
                    }
                }
                _ => return Ok(None),
            }
        }
        "content_block_stop" => {
            let index = value.get("index").and_then(Value::as_u64).unwrap_or(0);
            CanonicalStreamEvent::ContentBlockStop { index }
        }
        "message_delta" => {
            let stop_reason = value
                .get("delta")
                .and_then(|d| d.get("stop_reason"))
                .and_then(Value::as_str)
                .map(parse_stop_reason);
            let usage = value.get("usage").map(|u| CanonicalUsage {
                input_tokens: u.get("input_tokens").and_then(Value::as_u64),
                output_tokens: u.get("output_tokens").and_then(Value::as_u64),
            });
            CanonicalStreamEvent::MessageDelta { stop_reason, usage }
        }
        "message_stop" => CanonicalStreamEvent::MessageStop,
        // `ping` and any other housekeeping/unknown event carry nothing
        // canonical.
        _ => return Ok(None),
    };

    Ok(Some(parsed))
}

/// Append one fully-framed Anthropic SSE event
/// (`event: <type>\ndata: <compact-json>\n\n`) to `out`. Anthropic is the
/// only one of the three dialects whose stream relies on the SSE `event:`
/// type line — which is exactly why [`Adapter::render_stream_event`] returns
/// raw wire bytes rather than [`SseEvent`]s ([`SseEvent`] can't carry the
/// `event:` line; see [`crate::sse`]).
fn frame(out: &mut String, event_type: &str, data: &Value) {
    out.push_str("event: ");
    out.push_str(event_type);
    out.push_str("\ndata: ");
    out.push_str(&data.to_string());
    out.push_str("\n\n");
}

/// Render the Anthropic `content_block` object for a
/// [`CanonicalStreamEvent::ContentBlockStart`]. A `tool_use` block's `input`
/// is always emitted as `{}` at *start* time — Anthropic streams the actual
/// arguments afterward as `input_json_delta` fragments, and the canonical
/// start block's `input` is likewise `Null`/empty for a stream lifted from a
/// dialect that streams args separately. A `ToolResult` never legitimately
/// begins a *streamed* block, so it degrades to an empty text block rather
/// than inventing a shape.
fn stream_block(block: &ContentBlock) -> Value {
    match block {
        ContentBlock::Text { text } => json!({"type": "text", "text": text}),
        ContentBlock::ToolUse { id, name, input } => {
            let input = if input.is_null() {
                json!({})
            } else {
                input.clone()
            };
            json!({"type": "tool_use", "id": id, "name": name, "input": input})
        }
        ContentBlock::Image { media_type, data } => json!({
            "type": "image",
            "source": {"type": "base64", "media_type": media_type, "data": data},
        }),
        ContentBlock::ToolResult { .. } => json!({"type": "text", "text": ""}),
    }
}

/// Emit Anthropic's `message_start` event, capturing nothing further (the
/// caller has already stashed model/role in `st`). The message object is
/// minimal but structurally valid: `content: []`, null stop reason, zeroed
/// usage — a streamed `message_start` only needs to open the message; the
/// real content and terminal usage arrive in later events.
fn emit_message_start(out: &mut String, st: &mut StreamRenderState) {
    let role = st.role_or_assistant();
    frame(
        out,
        "message_start",
        &json!({
            "type": "message_start",
            "message": {
                "type": "message",
                "role": role.as_str(),
                "model": st.model,
                "content": [],
                "stop_reason": null,
                "usage": {"input_tokens": 0, "output_tokens": 0},
            },
        }),
    );
    st.message_started = true;
}

/// Emit `message_start` if it hasn't been emitted yet. This is the
/// granularity bridge: a canonical stream lifted from OpenAI carries no
/// [`CanonicalStreamEvent::MessageStart`], but an Anthropic client requires a
/// `message_start` before anything else, so the first event that needs the
/// message open synthesizes it (with an empty `model`, the documented gap on
/// [`StreamRenderState::model`]).
fn ensure_message_start(out: &mut String, st: &mut StreamRenderState) {
    if !st.message_started {
        emit_message_start(out, st);
    }
}

/// Render one canonical [`CanonicalStreamEvent`] down into fully-framed
/// Anthropic SSE bytes (design doc M15 Task 4). Because Anthropic is the most
/// granular dialect, this renderer's job is mostly to *synthesize* the
/// lifecycle events a coarser source omitted:
///
/// - A [`CanonicalStreamEvent::MessageStart`] emits `message_start` directly.
/// - A [`CanonicalStreamEvent::ContentBlockStart`] emits `content_block_start`
///   (synthesizing `message_start` first if needed).
/// - A [`CanonicalStreamEvent::ContentBlockDelta`] emits `content_block_delta`
///   — but if its block was never explicitly started (the OpenAI-origin text
///   case), a synthetic `message_start` + text `content_block_start` are
///   emitted ahead of it.
/// - A [`CanonicalStreamEvent::ContentBlockStop`] emits `content_block_stop`.
/// - A [`CanonicalStreamEvent::MessageDelta`] first closes any still-open
///   block (an OpenAI-origin stream never sent a `content_block_stop`), then
///   emits `message_delta`.
/// - A [`CanonicalStreamEvent::MessageStop`] closes any open block, then emits
///   Anthropic's required `message_stop` terminal.
///
/// State on `st` (`message_started`, `open_block`, `started_blocks`) is what
/// makes the "synthesize exactly once, and only what's missing" logic correct
/// whether the source was Anthropic (already granular — nothing synthesized)
/// or OpenAI (coarse — full scaffolding synthesized).
fn render_stream_event(
    event: &CanonicalStreamEvent,
    st: &mut StreamRenderState,
) -> Result<Vec<u8>, AdapterError> {
    let mut out = String::new();
    match event {
        CanonicalStreamEvent::MessageStart { model, role } => {
            st.model = model.clone();
            st.role = Some(*role);
            emit_message_start(&mut out, st);
        }
        CanonicalStreamEvent::ContentBlockStart { index, block } => {
            ensure_message_start(&mut out, st);
            // A coarse source (OpenAI, Google) never sends `content_block_stop`,
            // so close whatever block is still open before opening this one.
            close_open_block_before(&mut out, st, *index);
            frame(
                &mut out,
                "content_block_start",
                &json!({
                    "type": "content_block_start",
                    "index": index,
                    "content_block": stream_block(block),
                }),
            );
            st.started_blocks.insert(*index);
            st.open_block = Some(*index);
        }
        CanonicalStreamEvent::ContentBlockDelta {
            index,
            text,
            partial_json,
        } => {
            ensure_message_start(&mut out, st);
            if !st.started_blocks.contains(index) {
                // Coarse-source (OpenAI) text: no explicit start ever arrived,
                // so synthesize a text `content_block_start` before the delta.
                // Close any block left open first (the same source never sent a
                // `content_block_stop`) so blocks never overlap.
                close_open_block_before(&mut out, st, *index);
                frame(
                    &mut out,
                    "content_block_start",
                    &json!({
                        "type": "content_block_start",
                        "index": index,
                        "content_block": {"type": "text", "text": ""},
                    }),
                );
                st.started_blocks.insert(*index);
                st.open_block = Some(*index);
            }
            let delta = if let Some(text) = text {
                json!({"type": "text_delta", "text": text})
            } else if let Some(partial_json) = partial_json {
                json!({"type": "input_json_delta", "partial_json": partial_json})
            } else {
                // A delta carrying neither text nor json has nothing to emit.
                return Ok(out.into_bytes());
            };
            frame(
                &mut out,
                "content_block_delta",
                &json!({
                    "type": "content_block_delta",
                    "index": index,
                    "delta": delta,
                }),
            );
        }
        CanonicalStreamEvent::ContentBlockStop { index } => {
            frame(
                &mut out,
                "content_block_stop",
                &json!({"type": "content_block_stop", "index": index}),
            );
            if st.open_block == Some(*index) {
                st.open_block = None;
            }
        }
        CanonicalStreamEvent::MessageDelta { stop_reason, usage } => {
            ensure_message_start(&mut out, st);
            if let Some(open) = st.open_block.take() {
                frame(
                    &mut out,
                    "content_block_stop",
                    &json!({"type": "content_block_stop", "index": open}),
                );
            }
            let mut delta = Map::new();
            if let Some(stop_reason) = stop_reason {
                delta.insert(
                    "stop_reason".to_string(),
                    json!(render_stop_reason(stop_reason)),
                );
            }
            let mut data = Map::new();
            data.insert("type".to_string(), json!("message_delta"));
            data.insert("delta".to_string(), Value::Object(delta));
            if let Some(usage) = usage {
                let mut usage_obj = Map::new();
                if let Some(input_tokens) = usage.input_tokens {
                    usage_obj.insert("input_tokens".to_string(), json!(input_tokens));
                }
                if let Some(output_tokens) = usage.output_tokens {
                    usage_obj.insert("output_tokens".to_string(), json!(output_tokens));
                }
                data.insert("usage".to_string(), Value::Object(usage_obj));
            }
            frame(&mut out, "message_delta", &Value::Object(data));
        }
        CanonicalStreamEvent::MessageStop => {
            // Guard against a double terminal (M15 Task 7 obligation c): once
            // the Anthropic `message_stop` has been emitted, a second
            // `MessageStop` (e.g. the proxy synthesizing one at stream end
            // after a real one already flowed through) must emit nothing.
            if st.terminal_sent {
                return Ok(out.into_bytes());
            }
            ensure_message_start(&mut out, st);
            if let Some(open) = st.open_block.take() {
                frame(
                    &mut out,
                    "content_block_stop",
                    &json!({"type": "content_block_stop", "index": open}),
                );
            }
            frame(&mut out, "message_stop", &json!({"type": "message_stop"}));
            st.terminal_sent = true;
        }
    }
    Ok(out.into_bytes())
}

/// Emit a `content_block_stop` for the block currently open on `st`, if any,
/// before a different block (`new_index`) is opened. Anthropic requires at most
/// one open content block at a time; coarse sources (OpenAI, Google) never send
/// an explicit [`CanonicalStreamEvent::ContentBlockStop`], so on a block switch
/// this synthesizes the stop the source omitted. A no-op when nothing is open
/// or the same index is reopened, and it clears `open_block` so the block is
/// never stopped twice (the later MessageDelta/MessageStop close only whatever
/// remains open).
fn close_open_block_before(out: &mut String, st: &mut StreamRenderState, new_index: u64) {
    if let Some(prev) = st.open_block {
        if prev != new_index {
            frame(
                out,
                "content_block_stop",
                &json!({"type": "content_block_stop", "index": prev}),
            );
            st.open_block = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::llm::delta::DeltaKind;

    fn ctx() -> RequestCtx {
        RequestCtx {
            path: "/v1/messages".to_string(),
            method: "POST".to_string(),
        }
    }

    #[test]
    fn parses_string_content() {
        let body = br#"{
            "model": "claude-opus-4-1-20250805",
            "messages": [{"role": "user", "content": "hello there"}]
        }"#;
        let req = parse_body(body).unwrap();
        assert_eq!(req.model, "claude-opus-4-1-20250805");
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
    fn parses_block_array_content_preserving_non_text_blocks() {
        let body = br#"{
            "model": "claude-opus-4-1-20250805",
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "text", "text": "first part"},
                    {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "abc"}},
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
    fn parses_tool_use_and_tool_result_blocks() {
        let body = br#"{
            "model": "claude-opus-4-1-20250805",
            "messages": [
                {"role": "assistant", "content": [
                    {"type": "tool_use", "id": "call_1", "name": "get_weather", "input": {"location": "NYC"}}
                ]},
                {"role": "user", "content": [
                    {"type": "tool_result", "tool_use_id": "call_1", "content": "72F and sunny", "is_error": false}
                ]}
            ]
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
        assert_eq!(
            req.messages[1].content,
            vec![ContentBlock::ToolResult {
                id: "call_1".to_string(),
                content: "72F and sunny".to_string(),
                is_error: false,
            }]
        );
    }

    #[test]
    fn parses_top_level_system_string() {
        let body = br#"{
            "model": "claude-opus-4-1-20250805",
            "system": "You are a helpful assistant.",
            "messages": []
        }"#;
        let req = parse_body(body).unwrap();
        assert_eq!(req.system.as_deref(), Some("You are a helpful assistant."));
    }

    #[test]
    fn parses_structured_system_blocks() {
        let body = br#"{
            "model": "claude-opus-4-1-20250805",
            "system": [{"type": "text", "text": "part one"}, {"type": "text", "text": "part two"}],
            "messages": []
        }"#;
        let req = parse_body(body).unwrap();
        assert_eq!(req.system.as_deref(), Some("part one\npart two"));
    }

    #[test]
    fn missing_system_is_none() {
        let body = br#"{"model": "claude-opus-4-1-20250805", "messages": []}"#;
        let req = parse_body(body).unwrap();
        assert_eq!(req.system, None);
    }

    #[test]
    fn parses_tools_with_input_schema_and_description() {
        let body = br#"{
            "model": "claude-opus-4-1-20250805",
            "messages": [],
            "tools": [
                {"name": "get_weather", "description": "...", "input_schema": {"type": "object"}},
                {"name": "search"}
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
        assert_eq!(req.tools[1].description, None);
        assert_eq!(req.tools[1].input_schema, Value::Null);
    }

    #[test]
    fn parses_tool_choice_variants() {
        for (json, expected) in [
            (r#"{"type": "auto"}"#, ToolChoice::Auto),
            (r#"{"type": "any"}"#, ToolChoice::Any),
            (r#"{"type": "none"}"#, ToolChoice::None),
            (
                r#"{"type": "tool", "name": "get_weather"}"#,
                ToolChoice::Tool("get_weather".to_string()),
            ),
        ] {
            let body = format!(r#"{{"model": "m", "messages": [], "tool_choice": {json}}}"#);
            let req = parse_body(body.as_bytes()).unwrap();
            assert_eq!(req.tool_choice, Some(expected));
        }
    }

    #[test]
    fn missing_tool_choice_is_none() {
        let body = br#"{"model": "claude-opus-4-1-20250805", "messages": []}"#;
        let req = parse_body(body).unwrap();
        assert_eq!(req.tool_choice, None);
    }

    #[test]
    fn parses_sampling_fields() {
        let body = br#"{
            "model": "claude-opus-4-1-20250805",
            "messages": [],
            "max_tokens": 1024,
            "temperature": 0.7,
            "top_p": 0.9,
            "top_k": 40,
            "stop_sequences": ["STOP", "END"]
        }"#;
        let req = parse_body(body).unwrap();
        assert_eq!(req.sampling.max_tokens, Some(1024));
        assert_eq!(req.sampling.temperature, Some(0.7));
        assert_eq!(req.sampling.top_p, Some(0.9));
        assert_eq!(req.sampling.top_k, Some(40));
        assert_eq!(
            req.sampling.stop,
            vec!["STOP".to_string(), "END".to_string()]
        );
    }

    #[test]
    fn parses_stream_flag_true() {
        let body = br#"{
            "model": "claude-opus-4-1-20250805",
            "messages": [],
            "stream": true
        }"#;
        let req = parse_body(body).unwrap();
        assert!(req.stream);
    }

    #[test]
    fn missing_stream_defaults_to_false() {
        let body = br#"{"model": "claude-opus-4-1-20250805", "messages": []}"#;
        let req = parse_body(body).unwrap();
        assert!(!req.stream);
    }

    #[test]
    fn missing_max_tokens_is_none() {
        let body = br#"{"model": "claude-opus-4-1-20250805", "messages": []}"#;
        let req = parse_body(body).unwrap();
        assert_eq!(req.sampling.max_tokens, None);
    }

    #[test]
    fn present_max_tokens_is_parsed() {
        let body = br#"{"model": "claude-opus-4-1-20250805", "messages": [], "max_tokens": 1024}"#;
        let req = parse_body(body).unwrap();
        assert_eq!(req.sampling.max_tokens, Some(1024));
    }

    #[test]
    fn unknown_top_level_fields_land_in_extra() {
        let body = br#"{
            "model": "claude-opus-4-1-20250805",
            "messages": [],
            "metadata": {"user_id": "u_123"}
        }"#;
        let req = parse_body(body).unwrap();
        assert_eq!(
            req.extra.get("anthropic.metadata"),
            Some(&serde_json::json!({"user_id": "u_123"}))
        );
    }

    #[test]
    fn no_unknown_fields_leaves_extra_empty() {
        let body = br#"{"model": "claude-opus-4-1-20250805", "messages": []}"#;
        let req = parse_body(body).unwrap();
        assert!(req.extra.is_empty());
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
    fn missing_messages_defaults_to_empty() {
        let body = br#"{"model": "claude-opus-4-1-20250805"}"#;
        let req = parse_body(body).unwrap();
        assert!(req.messages.is_empty());
    }

    #[test]
    fn adapter_trait_impl_reports_provider_and_parses() {
        let adapter = AnthropicAdapter;
        assert_eq!(adapter.provider(), "anthropic");
        let body = br#"{"model": "claude-opus-4-1-20250805", "messages": []}"#;
        assert!(adapter.parse_request(body, &ctx()).is_ok());
    }

    // -- parse_response --------------------------------------------------

    #[test]
    fn parses_response_content_and_stop_reason_and_usage() {
        let body = br#"{
            "id": "msg_1",
            "type": "message",
            "role": "assistant",
            "model": "claude-opus-4-1-20250805",
            "content": [{"type": "text", "text": "hello there"}],
            "stop_reason": "end_turn",
            "usage": {"input_tokens": 10, "output_tokens": 20}
        }"#;
        let resp = parse_response_body(body).unwrap();
        assert_eq!(resp.model, "claude-opus-4-1-20250805");
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
    fn parses_response_tool_use_content() {
        let body = br#"{
            "model": "claude-opus-4-1-20250805",
            "content": [{"type": "tool_use", "id": "call_1", "name": "get_weather", "input": {"location": "NYC"}}],
            "stop_reason": "tool_use"
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
    fn unrecognized_stop_reason_becomes_other() {
        let body = br#"{"model": "m", "content": [], "stop_reason": "refusal"}"#;
        let resp = parse_response_body(body).unwrap();
        assert_eq!(
            resp.stop_reason,
            Some(StopReason::Other("refusal".to_string()))
        );
    }

    #[test]
    fn missing_stop_reason_is_none() {
        let body = br#"{"model": "m", "content": []}"#;
        let resp = parse_response_body(body).unwrap();
        assert_eq!(resp.stop_reason, None);
    }

    #[test]
    fn response_unknown_fields_land_in_extra() {
        let body = br#"{"id": "msg_1", "type": "message", "model": "m", "content": []}"#;
        let resp = parse_response_body(body).unwrap();
        assert_eq!(
            resp.extra.get("anthropic.id"),
            Some(&serde_json::json!("msg_1"))
        );
        assert_eq!(
            resp.extra.get("anthropic.type"),
            Some(&serde_json::json!("message"))
        );
    }

    #[test]
    fn response_invalid_json_is_malformed_error() {
        let err = parse_response_body(b"not json at all").unwrap_err();
        assert!(matches!(err, AdapterError::Malformed(_)));
    }

    #[test]
    fn adapter_trait_impl_parses_response() {
        let adapter = AnthropicAdapter;
        let body = br#"{"model": "m", "content": []}"#;
        assert!(adapter.parse_response(body).is_ok());
    }

    fn event(data: &str) -> SseEvent {
        SseEvent {
            data: data.to_string(),
        }
    }

    #[test]
    fn parses_text_delta() {
        let data = event(
            r#"{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hello"}}"#,
        );
        let delta = parse_delta(&data).expect("must parse");
        assert_eq!(delta.kind, DeltaKind::Text);
        assert_eq!(delta.text.as_deref(), Some("Hello"));
    }

    #[test]
    fn parses_tool_call_input_json_delta() {
        let data = event(
            r#"{"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"loc"}}"#,
        );
        let delta = parse_delta(&data).expect("must parse");
        assert_eq!(delta.kind, DeltaKind::ToolCall);
        let tool_call = delta.tool_call.expect("tool_call must be set");
        assert_eq!(tool_call.arguments_fragment.as_deref(), Some(r#"{"loc"#));
    }

    #[test]
    fn parses_message_delta_usage() {
        let data = event(
            r#"{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":42}}"#,
        );
        let delta = parse_delta(&data).expect("must parse");
        assert_eq!(delta.kind, DeltaKind::Usage);
        let usage = delta.usage.expect("usage must be set");
        assert_eq!(usage.output_tokens, Some(42));
    }

    #[test]
    fn parses_message_stop_as_finish() {
        let data = event(r#"{"type":"message_stop"}"#);
        let delta = parse_delta(&data).expect("must parse");
        assert_eq!(delta.kind, DeltaKind::Finish);
    }

    #[test]
    fn unrecognized_event_type_is_none() {
        let data = event(r#"{"type":"content_block_start","index":0}"#);
        assert!(parse_delta(&data).is_none());
    }

    #[test]
    fn malformed_json_is_none() {
        let data = event("not json");
        assert!(parse_delta(&data).is_none());
    }

    #[test]
    fn adapter_parse_delta_dispatches_to_free_function() {
        let adapter = AnthropicAdapter;
        let data = event(
            r#"{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hi"}}"#,
        );
        let delta = adapter.parse_delta(&data).expect("must parse");
        assert_eq!(delta.text.as_deref(), Some("hi"));
    }

    // -- render_request / render_response (M15 Task 3) --------------------

    fn sample_request() -> CanonicalRequest {
        CanonicalRequest {
            model: "claude-opus-4-1-20250805".to_string(),
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
                top_k: Some(40),
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
    fn render_request_writes_system_and_tools_and_sampling() {
        let req = sample_request();
        let mut report = TranslationReport::default();
        let body = render_request_body(&req, &mut report).unwrap();
        let value: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(value["system"], json!("You are helpful."));
        assert_eq!(value["tools"][0]["name"], json!("get_weather"));
        assert_eq!(value["max_tokens"], json!(1024));
        assert_eq!(value["top_k"], json!(40));
        assert_eq!(value["tool_choice"], json!({"type": "auto"}));
    }

    #[test]
    fn render_request_downgrades_tool_role_message_with_note() {
        let mut req = sample_request();
        req.messages = vec![CanonicalMessage {
            role: Role::Tool,
            content: vec![ContentBlock::ToolResult {
                id: "call_1".to_string(),
                content: "72F and sunny".to_string(),
                is_error: false,
            }],
        }];
        let mut report = TranslationReport::default();
        let body = render_request_body(&req, &mut report).unwrap();
        let value: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(value["messages"][0]["role"], json!("user"));
        assert!(!report.notes.is_empty());
    }

    /// A `metadata` field tagged as having come from a *different*
    /// provider's parse (`"openai.metadata"`) — this adapter's render can't
    /// verify it belongs to Anthropic's dialect, so it's dropped with a
    /// report entry rather than guessed at. Contrast with
    /// `render_request_preserves_same_provider_extra_field_on_round_trip`
    /// below, where an `"anthropic."`-tagged entry survives instead.
    #[test]
    fn render_request_drops_cross_provider_extra_fields() {
        let mut req = sample_request();
        req.extra.insert(
            "openai.metadata".to_string(),
            serde_json::json!({"user_id": "u_1"}),
        );
        let mut report = TranslationReport::default();
        render_request_body(&req, &mut report).unwrap();

        assert_eq!(report.dropped.len(), 1);
        assert_eq!(report.dropped[0].path, "extra.metadata");
        assert!(report.dropped[0].reason.contains("openai"));
    }

    /// M15 Task 3 review fix: an `extra` entry tagged with *this* adapter's
    /// own provider name must round-trip losslessly through render rather
    /// than being dropped — the whole point of source-tagging `extra` in
    /// `collect_extra`/`emit_or_drop_extra` (see `adapter`'s module doc
    /// comment).
    #[test]
    fn render_request_preserves_same_provider_extra_field_on_round_trip() {
        let mut req = sample_request();
        req.extra.insert(
            "anthropic.metadata".to_string(),
            serde_json::json!({"user_id": "u_1"}),
        );
        let mut report = TranslationReport::default();
        let body = render_request_body(&req, &mut report).unwrap();
        assert!(
            report.dropped.is_empty(),
            "same-provider extra must survive: {:?}",
            report.dropped
        );

        let value: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["metadata"], json!({"user_id": "u_1"}));

        let reparsed = parse_body(&body).unwrap();
        assert_eq!(reparsed, req);
    }

    /// `render_response_body` now synthesizes the target dialect's required
    /// `type`/`role`/`id` discriminator fields (FIX-6) whenever they are
    /// absent, so a bare `resp` with no `anthropic.*` extra tag still round
    /// trips, but the reparsed value gains those three fields back into
    /// `extra` (they aren't in `KNOWN_RESPONSE_FIELDS`, so parse collects
    /// them there, tagged as having come from anthropic, same as any other
    /// unmodeled anthropic field would).
    #[test]
    fn render_response_round_trips_through_parse() {
        let resp = CanonicalResponse {
            model: "claude-opus-4-1-20250805".to_string(),
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
            .insert("anthropic.type".to_string(), json!("message"));
        expected
            .extra
            .insert("anthropic.role".to_string(), json!("assistant"));
        expected
            .extra
            .insert("anthropic.id".to_string(), json!("msg_translated"));
        assert_eq!(reparsed, expected);
    }

    /// A representative real Anthropic response `extra` field (`id`, not
    /// modeled in `KNOWN_RESPONSE_FIELDS`) must survive a same-provider
    /// parse -> render, matching the request-side
    /// `render_request_preserves_same_provider_extra_field_on_round_trip`
    /// above. `type`/`role` are still synthesized (FIX-6) since this
    /// `extra` carries no same-provider tag for them, so they show up in
    /// the reparsed `extra` alongside the preserved `id`.
    #[test]
    fn render_response_preserves_same_provider_extra_field_on_round_trip() {
        let mut resp = CanonicalResponse {
            model: "claude-opus-4-1-20250805".to_string(),
            content: vec![ContentBlock::Text {
                text: "hello there".to_string(),
            }],
            stop_reason: Some(StopReason::EndTurn),
            usage: None,
            extra: Default::default(),
        };
        resp.extra
            .insert("anthropic.id".to_string(), serde_json::json!("msg_1"));

        let mut report = TranslationReport::default();
        let body = render_response_body(&resp, &mut report).unwrap();
        assert!(
            report.dropped.is_empty(),
            "same-provider extra must survive: {:?}",
            report.dropped
        );

        let value: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["id"], json!("msg_1"));

        let reparsed = parse_response_body(&body).unwrap();
        let mut expected = resp.clone();
        expected
            .extra
            .insert("anthropic.type".to_string(), json!("message"));
        expected
            .extra
            .insert("anthropic.role".to_string(), json!("assistant"));
        assert_eq!(reparsed, expected);
    }

    /// FIX-6: a buffered cross-provider translated response (no
    /// `anthropic.*` tag in `extra` at all, simulating a source dialect
    /// that isn't anthropic) must still carry the Anthropic Messages
    /// schema's required top-level discriminators, and those fields must
    /// NOT be reported as dropped (they are synthesized/emitted, not
    /// discarded).
    #[test]
    fn render_response_synthesizes_discriminators_for_cross_provider_translation() {
        let mut resp = CanonicalResponse {
            model: "claude-opus-4-1-20250805".to_string(),
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
            .insert("openai.id".to_string(), serde_json::json!("chatcmpl_1"));

        let mut report = TranslationReport::default();
        let body = render_response_body(&resp, &mut report).unwrap();

        let value: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["type"], json!("message"));
        assert_eq!(value["role"], json!("assistant"));
        assert!(value["id"].is_string());

        for field in ["type", "role", "id"] {
            assert!(
                !report.dropped.iter().any(|d| d.path == field),
                "{field} must not be reported as dropped: {:?}",
                report.dropped
            );
        }
    }

    /// FIX-6: when the source WAS anthropic and its real `type`/`id`
    /// survived translation as same-provider extra, those real values must
    /// win over the synthesized placeholder/constant, not be clobbered by
    /// it.
    #[test]
    fn render_response_prefers_real_same_provider_type_and_id_over_synthesized() {
        let mut resp = CanonicalResponse {
            model: "claude-opus-4-1-20250805".to_string(),
            content: vec![ContentBlock::Text {
                text: "hello there".to_string(),
            }],
            stop_reason: Some(StopReason::EndTurn),
            usage: None,
            extra: Default::default(),
        };
        resp.extra
            .insert("anthropic.type".to_string(), json!("message"));
        resp.extra
            .insert("anthropic.id".to_string(), json!("msg_real_0123"));

        let mut report = TranslationReport::default();
        let body = render_response_body(&resp, &mut report).unwrap();
        assert!(report.dropped.is_empty(), "{:?}", report.dropped);

        let value: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["id"], json!("msg_real_0123"));
        assert_ne!(value["id"], json!("msg_translated"));
        assert_eq!(value["type"], json!("message"));
        assert_eq!(value["role"], json!("assistant"));
    }

    #[test]
    fn render_response_preserves_other_stop_reason_verbatim() {
        let resp = CanonicalResponse {
            model: "m".to_string(),
            content: vec![],
            stop_reason: Some(StopReason::Other("refusal".to_string())),
            usage: None,
            extra: Default::default(),
        };
        let mut report = TranslationReport::default();
        let body = render_response_body(&resp, &mut report).unwrap();
        let value: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["stop_reason"], json!("refusal"));
    }

    #[test]
    fn adapter_trait_render_request_dispatches_to_free_function() {
        let adapter = AnthropicAdapter;
        let mut report = TranslationReport::default();
        let body = adapter
            .render_request(&sample_request(), &mut report)
            .unwrap();
        assert!(!body.is_empty());
    }
}
