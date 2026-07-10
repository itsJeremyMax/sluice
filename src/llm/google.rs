//! Google Gemini `generateContent` ingress adapter: parses the wire-format
//! request/response bodies into the gateway's lossless canonical LLM IR
//! ([`super::CanonicalRequest`]/[`super::CanonicalResponse`], design doc M15
//! Task 2), and serializes the canonical IR back out (design doc M15 Task 3,
//! the render direction).
//!
//! Google's request shape differs from Anthropic/OpenAI in one notable way:
//! the model id is normally carried in the URL path (e.g.
//! `/v1beta/models/gemini-1.5-pro:generateContent`), not the JSON body. As of
//! this task the [`super::Adapter::parse_request`] trait carries a
//! [`super::RequestCtx`] with that path, so [`model_from_path`] reads the
//! segment between `models/` and `:` and only falls back to a body `model`
//! field (then an empty string) when the path doesn't have that shape.
//! Streaming detection stays a known gap: Google signals it via the
//! `streamGenerateContent` path segment rather than a body field, and this
//! adapter still defaults `stream` to `false` regardless — extracting it
//! from `ctx.path` too is left to a later refinement, matching the scope of
//! this task (model-from-path only). [`render_request_body`] mirrors this
//! gap: `Adapter::render_request` takes no `RequestCtx` (a render call has
//! no URL to pick a path segment for), so it always writes to the
//! `generateContent` body shape and, if `req.stream` is `true`, records a
//! drop noting Google has no body-level way to ask for streaming.

use serde_json::{json, Map, Value};

use super::adapter::{collect_extra, emit_or_drop_extra, Adapter, AdapterError};
use super::canonical::StreamToolRender;
use super::delta::{Delta, Finish, Usage as DeltaUsage};
use super::{
    CanonicalMessage, CanonicalRequest, CanonicalResponse, CanonicalStreamEvent, CanonicalTool,
    CanonicalUsage, ContentBlock, RequestCtx, Role, Sampling, StopReason, StreamParseState,
    StreamRenderState, TranslationReport,
};
use crate::sse::SseEvent;

/// Parses Google Gemini `generateContent` request/response bodies.
pub struct GoogleAdapter;

impl Adapter for GoogleAdapter {
    fn provider(&self) -> &str {
        "google"
    }

    fn parse_request(
        &self,
        body: &[u8],
        ctx: &RequestCtx,
    ) -> Result<CanonicalRequest, AdapterError> {
        parse_body(body, ctx)
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

/// Parse one Google Gemini streaming SSE event's `data:` payload into a
/// normalized [`Delta`] (design doc M10 Task 2). Best-effort: Google's
/// `streamGenerateContent` shape is less uniformly documented than
/// Anthropic's/OpenAI's event-typed streams, so this is implemented against
/// the commonly-observed chunk shape rather than a formal spec.
///
/// Each chunk is a `GenerateContentResponse`-shaped JSON object.
/// `usageMetadata` (token counts) is checked first and, when present,
/// yields a `Usage` delta — assumption: on Gemini's final chunk,
/// `usageMetadata` and `candidates[0].finishReason` commonly arrive
/// together, and this parser prioritizes surfacing the usage accounting
/// over the finish reason for that chunk, matching the same
/// usage-takes-priority convention as the OpenAI adapter in this module set.
/// Otherwise `candidates[0].finishReason`, if present, yields a `Finish`
/// delta; otherwise `candidates[0].content.parts[0].text` yields a `Text`
/// delta. Anything else (malformed JSON, no candidates, a non-text first
/// part) yields `None`.
fn parse_delta(event: &SseEvent) -> Option<Delta> {
    let value: Value = serde_json::from_str(&event.data).ok()?;

    if let Some(usage) = value.get("usageMetadata") {
        return Some(Delta::usage(DeltaUsage {
            input_tokens: usage.get("promptTokenCount").and_then(Value::as_u64),
            output_tokens: usage.get("candidatesTokenCount").and_then(Value::as_u64),
        }));
    }

    let candidate = value.get("candidates").and_then(Value::as_array)?.first()?;

    if let Some(reason) = candidate.get("finishReason").and_then(Value::as_str) {
        return Some(Delta::finish(Finish {
            reason: Some(reason.to_string()),
        }));
    }

    let text = candidate
        .get("content")?
        .get("parts")?
        .as_array()?
        .first()?
        .get("text")
        .and_then(Value::as_str)?;
    Some(Delta::text(text))
}

/// Join a `parts[]` array's text fields into one string. Only
/// `{"text": "..."}` parts contribute (non-text parts — e.g. inline data —
/// are skipped). Used for `systemInstruction`, which carries its text the
/// same `parts[]`-shaped way `contents[]` entries do. A missing/malformed
/// `parts` field yields an empty string rather than an error.
fn flatten_parts(parts: &Value) -> String {
    match parts.as_array() {
        Some(parts) => parts
            .iter()
            .filter_map(|p| p.get("text").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join("\n"),
        None => String::new(),
    }
}

/// Parse a `parts[]` array into canonical [`ContentBlock`]s, preserving
/// every part type this adapter recognizes: `text`, `inlineData` (a base64
/// image), `functionCall` (a tool invocation), and `functionResponse` (a
/// tool result). Google's part shapes have no `id` field the way
/// Anthropic's/OpenAI's tool blocks do, so the function *name* doubles as
/// the canonical `id` for `ToolUse`/`ToolResult` blocks — documented
/// assumption, not a wire-format guarantee of uniqueness.
fn parts_to_blocks(parts: &Value) -> Vec<ContentBlock> {
    parts
        .as_array()
        .map(|arr| arr.iter().filter_map(parse_one_part).collect())
        .unwrap_or_default()
}

fn parse_one_part(part: &Value) -> Option<ContentBlock> {
    if let Some(text) = part.get("text").and_then(Value::as_str) {
        return Some(ContentBlock::Text {
            text: text.to_string(),
        });
    }
    if let Some(inline) = part.get("inlineData") {
        return Some(ContentBlock::Image {
            media_type: inline
                .get("mimeType")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            data: inline
                .get("data")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
        });
    }
    if let Some(call) = part.get("functionCall") {
        let name = call
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        return Some(ContentBlock::ToolUse {
            id: name.clone(),
            name,
            input: call.get("args").cloned().unwrap_or(Value::Null),
        });
    }
    if let Some(response) = part.get("functionResponse") {
        let name = response
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let content = response
            .get("response")
            .map(|r| serde_json::to_string(r).unwrap_or_default())
            .unwrap_or_default();
        return Some(ContentBlock::ToolResult {
            id: name,
            content,
            is_error: false,
        });
    }
    None
}

/// Map a Google wire-format role string to the canonical [`Role`]. Google's
/// `contents[].role` is documented as `"user"` or `"model"`; the legacy
/// `"function"` role (a tool result turn) maps to `Tool`. Anything else
/// (including a missing role) defaults to `User`.
fn parse_role(role: &str) -> Role {
    match role {
        "model" => Role::Assistant,
        "function" => Role::Tool,
        "system" => Role::System,
        _ => Role::User,
    }
}

/// Map a Google `finishReason` wire string to the canonical [`StopReason`].
/// An unrecognized reason (e.g. `"SAFETY"`, `"RECITATION"`) is preserved via
/// `Other` rather than dropped.
fn parse_stop_reason(reason: &str) -> StopReason {
    match reason {
        "STOP" => StopReason::EndTurn,
        "MAX_TOKENS" => StopReason::MaxTokens,
        other => StopReason::Other(other.to_string()),
    }
}

/// Extract the model id from a Google request path
/// (`/v1beta/models/<model>:<method>`) — the segment between `models/` and
/// the next `:`. Returns `None` for a path that doesn't contain a `models/`
/// segment (or where that segment is empty), so callers can fall back to a
/// body `model` field.
fn model_from_path(path: &str) -> Option<String> {
    let after_models = path.split("models/").nth(1)?;
    let model = after_models.split(':').next().unwrap_or(after_models);
    if model.is_empty() {
        None
    } else {
        Some(model.to_string())
    }
}

/// Top-level request fields this adapter models by name — anything else
/// lands in [`CanonicalRequest::extra`] so round-tripping stays lossless.
const KNOWN_REQUEST_FIELDS: &[&str] = &[
    "model",
    "contents",
    "systemInstruction",
    "tools",
    "generationConfig",
];

/// Top-level response fields this adapter models by name.
const KNOWN_RESPONSE_FIELDS: &[&str] = &["candidates", "usageMetadata", "modelVersion", "model"];

fn parse_body(body: &[u8], ctx: &RequestCtx) -> Result<CanonicalRequest, AdapterError> {
    let value: Value = serde_json::from_slice(body)
        .map_err(|e| AdapterError::Malformed(format!("invalid JSON: {e}")))?;

    // Google normally carries the model in the URL path — see the module
    // doc comment. Fall back to a body `model` field, then an empty string,
    // for a caller that supplied neither.
    let model = model_from_path(&ctx.path).unwrap_or_else(|| {
        value
            .get("model")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string()
    });

    let messages = value
        .get("contents")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .map(|c| CanonicalMessage {
                    role: c
                        .get("role")
                        .and_then(Value::as_str)
                        .map(parse_role)
                        .unwrap_or(Role::User),
                    content: c.get("parts").map(parts_to_blocks).unwrap_or_default(),
                })
                .collect()
        })
        .unwrap_or_default();

    let system = value
        .get("systemInstruction")
        .and_then(|si| si.get("parts"))
        .map(flatten_parts);

    // Google nests function declarations under `tools[].functionDeclarations`
    // rather than a flat `tools[].name` — collect across every tools entry.
    // The schema lives under `parameters`, Google's name for what
    // Anthropic/OpenAI call `input_schema`/`parameters`.
    let tools = value
        .get("tools")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(|t| t.get("functionDeclarations")?.as_array())
                .flatten()
                .filter_map(|fd| {
                    let name = fd.get("name").and_then(Value::as_str)?.to_string();
                    Some(CanonicalTool {
                        name,
                        description: fd
                            .get("description")
                            .and_then(Value::as_str)
                            .map(str::to_string),
                        input_schema: fd.get("parameters").cloned().unwrap_or(Value::Null),
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    let generation_config = value.get("generationConfig");
    let sampling = Sampling {
        temperature: generation_config
            .and_then(|gc| gc.get("temperature"))
            .and_then(Value::as_f64),
        top_p: generation_config
            .and_then(|gc| gc.get("topP"))
            .and_then(Value::as_f64),
        top_k: generation_config
            .and_then(|gc| gc.get("topK"))
            .and_then(Value::as_u64),
        max_tokens: generation_config
            .and_then(|gc| gc.get("maxOutputTokens"))
            .and_then(Value::as_u64),
        stop: generation_config
            .and_then(|gc| gc.get("stopSequences"))
            .and_then(Value::as_array)
            .map(|arr| {
                arr.iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default(),
    };

    // No body field (nor, for this task, path segment) signals streaming for
    // Google — see the module doc comment's "known gap" note.
    let stream = false;

    let extra = collect_extra(&value, KNOWN_REQUEST_FIELDS, "google");

    Ok(CanonicalRequest {
        model,
        messages,
        system,
        tools,
        tool_choice: None,
        sampling,
        stream,
        extra,
    })
}

fn parse_response_body(body: &[u8]) -> Result<CanonicalResponse, AdapterError> {
    let value: Value = serde_json::from_slice(body)
        .map_err(|e| AdapterError::Malformed(format!("invalid JSON: {e}")))?;

    // Gemini responses don't echo the request's model id the way
    // Anthropic's/OpenAI's do; `modelVersion` is the closest analog when
    // present. Absent either, model defaults to empty string rather than
    // failing the parse.
    let model = value
        .get("modelVersion")
        .and_then(Value::as_str)
        .or_else(|| value.get("model").and_then(Value::as_str))
        .unwrap_or_default()
        .to_string();

    let candidate = value
        .get("candidates")
        .and_then(Value::as_array)
        .and_then(|c| c.first());

    let content = candidate
        .and_then(|c| c.get("content"))
        .and_then(|c| c.get("parts"))
        .map(parts_to_blocks)
        .unwrap_or_default();

    let stop_reason = candidate
        .and_then(|c| c.get("finishReason"))
        .and_then(Value::as_str)
        .map(parse_stop_reason);

    let usage = value.get("usageMetadata").map(|u| CanonicalUsage {
        input_tokens: u.get("promptTokenCount").and_then(Value::as_u64),
        output_tokens: u.get("candidatesTokenCount").and_then(Value::as_u64),
    });

    let extra = collect_extra(&value, KNOWN_RESPONSE_FIELDS, "google");

    Ok(CanonicalResponse {
        model,
        content,
        stop_reason,
        usage,
        extra,
    })
}

// -- render (M15 Task 3) --------------------------------------------------

/// Render one canonical [`ContentBlock`] to a Google `parts[]` entry.
/// `path` identifies the block for `report` (e.g. `"messages[0].content"`
/// or `"content"` for a response). `ToolUse`/`ToolResult`'s canonical `id`
/// only has a home in Google's wire format when it equals `name` — Google's
/// `functionCall`/`functionResponse` objects carry a function *name*, not a
/// separate call id (see [`parse_one_part`], which doubles `name` as `id`
/// on the way in) — so an `id` that diverges from `name` (only possible for
/// a `ToolUse`/`ToolResult` block that arrived from a provider that *does*
/// distinguish them, e.g. Anthropic's `call_1` vs. `get_weather`) is
/// recorded as dropped rather than silently discarded. `ToolResult.content`
/// is a plain string (this adapter's own uniform shape for a tool result
/// across all three providers) but Google's `functionResponse.response`
/// wants a JSON object; if `content` happens to parse as JSON it's used
/// as-is (round-tripping a Google-authored `ToolResult` exactly, since its
/// `content` is itself `serde_json::to_string`'d from that object — see
/// `parse_one_part`), otherwise it's wrapped as `{"result": content}` with
/// a note. A `ToolResult.is_error` of `true` has no `functionResponse`
/// field to carry it, so it's dropped when set.
fn render_block(block: &ContentBlock, path: &str, report: &mut TranslationReport) -> Value {
    match block {
        ContentBlock::Text { text } => json!({"text": text}),
        ContentBlock::Image { media_type, data } => json!({
            "inlineData": {"mimeType": media_type, "data": data},
        }),
        ContentBlock::ToolUse { id, name, input } => {
            if id != name {
                report.drop(
                    format!("{path}.tool_use.id"),
                    "google functionCall has no separate id field; only name is preserved",
                );
            }
            json!({"functionCall": {"name": name, "args": input}})
        }
        ContentBlock::ToolResult {
            id,
            content,
            is_error,
        } => {
            if *is_error {
                report.drop(
                    format!("{path}.tool_result.is_error"),
                    "google functionResponse has no is_error field",
                );
            }
            let response = serde_json::from_str::<Value>(content).unwrap_or_else(|_| {
                report.notes.push(format!(
                    "{path}.tool_result.content: not a JSON object, wrapped as {{\"result\": ...}} for google functionResponse"
                ));
                json!({"result": content})
            });
            json!({"functionResponse": {"name": id, "response": response}})
        }
    }
}

/// Render one [`CanonicalMessage`] to a Google `contents[]` entry, or
/// `None` for a [`Role::System`] message (Google has no system-role content
/// entry — that's [`render_system_instruction`]'s job, folding it into
/// `systemInstruction` instead, matching how this adapter's parse side
/// never produces a `Role::System` canonical message from `contents[]`
/// either).
fn render_message(
    msg: &CanonicalMessage,
    index: usize,
    report: &mut TranslationReport,
) -> Option<Value> {
    let role = match msg.role {
        Role::User => "user",
        Role::Assistant => "model",
        Role::Tool => "function",
        Role::System => return None,
    };
    let parts: Vec<Value> = msg
        .content
        .iter()
        .map(|b| render_block(b, &format!("messages[{index}].content"), report))
        .collect();
    Some(json!({"role": role, "parts": parts}))
}

/// Build `systemInstruction` from `req.system` plus any [`Role::System`]
/// canonical messages, folding them together the same way
/// [`super::anthropic::render_system_and_messages`] does for Anthropic's
/// top-level `system` string — Google's `systemInstruction` is likewise a
/// single out-of-band slot, not a per-message thing. A system message's
/// non-`Text` blocks (e.g. an image) have no representation in
/// `systemInstruction`, which this adapter only ever populates with text
/// parts, so those are dropped with a reason.
fn render_system_instruction(
    req: &CanonicalRequest,
    report: &mut TranslationReport,
) -> Option<Value> {
    let mut parts: Vec<String> = req.system.iter().cloned().collect();
    for (index, msg) in req.messages.iter().enumerate() {
        if msg.role != Role::System {
            continue;
        }
        for block in &msg.content {
            match block {
                ContentBlock::Text { text } => parts.push(text.clone()),
                _ => report.drop(
                    format!("messages[{index}].content"),
                    "google systemInstruction is text-only; non-text content in a \
                     system-role message has no representation there",
                ),
            }
        }
    }
    if parts.is_empty() {
        None
    } else {
        Some(json!({"parts": parts.iter().map(|p| json!({"text": p})).collect::<Vec<_>>()}))
    }
}

fn render_request_body(
    req: &CanonicalRequest,
    report: &mut TranslationReport,
) -> Result<Vec<u8>, AdapterError> {
    let system_instruction = render_system_instruction(req, report);

    let contents: Vec<Value> = req
        .messages
        .iter()
        .enumerate()
        .filter_map(|(index, msg)| render_message(msg, index, report))
        .collect();

    let mut body = Map::new();
    // Google normally reads the model id off the URL path (see the module
    // doc comment); render_request has no RequestCtx to write a path, so
    // this is written as the same body `model` fallback field this
    // adapter's own parse_body already recognizes.
    body.insert("model".to_string(), json!(req.model));
    body.insert("contents".to_string(), Value::Array(contents));
    if let Some(system_instruction) = system_instruction {
        body.insert("systemInstruction".to_string(), system_instruction);
    }

    if !req.tools.is_empty() {
        let declarations: Vec<Value> = req
            .tools
            .iter()
            .map(|t| {
                let mut fd = Map::new();
                fd.insert("name".to_string(), json!(t.name));
                if let Some(description) = &t.description {
                    fd.insert("description".to_string(), json!(description));
                }
                fd.insert("parameters".to_string(), t.input_schema.clone());
                Value::Object(fd)
            })
            .collect();
        body.insert(
            "tools".to_string(),
            json!([{"functionDeclarations": declarations}]),
        );
    }

    if req.tool_choice.is_some() {
        // This adapter's parse side never populates tool_choice for Google
        // (it hardcodes `None` — see parse_body); rendering one back out
        // would mean inventing a toolConfig shape this codebase has no
        // parse-side counterpart for, so it's recorded as dropped instead.
        report.drop(
            "tool_choice",
            "google adapter does not model tool_choice/toolConfig yet",
        );
    }

    let Sampling {
        temperature,
        top_p,
        top_k,
        max_tokens,
        stop,
    } = &req.sampling;
    let mut generation_config = Map::new();
    if let Some(temperature) = temperature {
        generation_config.insert("temperature".to_string(), json!(temperature));
    }
    if let Some(top_p) = top_p {
        generation_config.insert("topP".to_string(), json!(top_p));
    }
    if let Some(top_k) = top_k {
        generation_config.insert("topK".to_string(), json!(top_k));
    }
    if let Some(max_tokens) = max_tokens {
        generation_config.insert("maxOutputTokens".to_string(), json!(max_tokens));
    }
    if !stop.is_empty() {
        generation_config.insert("stopSequences".to_string(), json!(stop));
    }
    if !generation_config.is_empty() {
        body.insert(
            "generationConfig".to_string(),
            Value::Object(generation_config),
        );
    }

    if req.stream {
        // Google signals streaming via the `streamGenerateContent` URL path
        // segment, not a body field (see the module doc comment) — render
        // has no ctx/path to place that on, so a `true` stream flag can't
        // be expressed here.
        report.drop(
            "stream",
            "google signals streaming via URL path segment, not body; render_request has \
             no ctx to select streamGenerateContent",
        );
    }

    emit_or_drop_extra("google", &req.extra, &mut body, report);

    serde_json::to_vec(&Value::Object(body))
        .map_err(|e| AdapterError::Malformed(format!("failed to serialize request: {e}")))
}

/// Render a [`StopReason`] to Google's `finishReason` wire string.
/// `EndTurn`/`MaxTokens` map onto Google's own `STOP`/`MAX_TOKENS` reasons
/// exactly; `ToolUse`/`StopSequence` have no distinct Google reason (Gemini
/// finishes a function-call turn with `STOP` same as any other turn, and
/// has no stop-sequence-specific reason either), so both downgrade to
/// `STOP` with a note. `Other` re-emits its raw string verbatim.
fn render_stop_reason(reason: &StopReason, report: &mut TranslationReport) -> String {
    match reason {
        StopReason::EndTurn => "STOP".to_string(),
        StopReason::MaxTokens => "MAX_TOKENS".to_string(),
        StopReason::ToolUse => {
            report.notes.push(
                "stop_reason: ToolUse has no distinct google finishReason, mapped to \"STOP\""
                    .to_string(),
            );
            "STOP".to_string()
        }
        StopReason::StopSequence => {
            report.notes.push(
                "stop_reason: StopSequence has no distinct google finishReason, mapped to \"STOP\""
                    .to_string(),
            );
            "STOP".to_string()
        }
        StopReason::Other(s) => s.clone(),
    }
}

fn render_response_body(
    resp: &CanonicalResponse,
    report: &mut TranslationReport,
) -> Result<Vec<u8>, AdapterError> {
    let parts: Vec<Value> = resp
        .content
        .iter()
        .map(|b| render_block(b, "content", report))
        .collect();

    let mut candidate = Map::new();
    candidate.insert(
        "content".to_string(),
        json!({"parts": parts, "role": "model"}),
    );
    candidate.insert("index".to_string(), json!(0));
    if let Some(stop_reason) = &resp.stop_reason {
        candidate.insert(
            "finishReason".to_string(),
            json!(render_stop_reason(stop_reason, report)),
        );
    }

    let mut body = Map::new();
    body.insert("candidates".to_string(), json!([Value::Object(candidate)]));
    body.insert("modelVersion".to_string(), json!(resp.model));

    if let Some(usage) = &resp.usage {
        let mut usage_obj = Map::new();
        if let Some(input_tokens) = usage.input_tokens {
            usage_obj.insert("promptTokenCount".to_string(), json!(input_tokens));
        }
        if let Some(output_tokens) = usage.output_tokens {
            usage_obj.insert("candidatesTokenCount".to_string(), json!(output_tokens));
        }
        body.insert("usageMetadata".to_string(), Value::Object(usage_obj));
    }

    emit_or_drop_extra("google", &resp.extra, &mut body, report);

    serde_json::to_vec(&Value::Object(body))
        .map_err(|e| AdapterError::Malformed(format!("failed to serialize response: {e}")))
}

// -- streaming translation (M15 Task 4) -----------------------------------

/// Parse one Google `streamGenerateContent` SSE event up into at most one
/// canonical [`CanonicalStreamEvent`] (design doc M15 Task 4). Like OpenAI,
/// Google's stream is coarser than the canonical (Anthropic-shaped)
/// lifecycle: there is no `message_start`/`content_block_start`/`stop` and no
/// terminal sentinel (the HTTP stream simply ends). Each chunk is a
/// `GenerateContentResponse`-shaped object.
///
/// Mapping: a `usageMetadata` chunk -> [`CanonicalStreamEvent::MessageDelta`]
/// carrying `usage` (and `stop_reason` too when the same chunk also has a
/// `finishReason`, which Gemini commonly co-locates on its final chunk); a
/// text part -> [`CanonicalStreamEvent::ContentBlockDelta`] with `text` at the
/// streamed text block's index; a `functionCall` part (which arrives *whole*,
/// not fragmented) -> [`CanonicalStreamEvent::ContentBlockStart`] with a fully
/// populated `ToolUse` block at a freshly allocated index; a bare
/// `finishReason` -> [`CanonicalStreamEvent::MessageDelta`] with `stop_reason`.
/// No candidates, a non-text/non-call part, or malformed JSON -> `Ok(None)`.
/// Google emits no [`CanonicalStreamEvent::MessageStop`] (it has no terminal
/// event); the target renderer supplies its own terminal if it needs one.
fn parse_stream_event(
    event: &SseEvent,
    st: &mut StreamParseState,
) -> Result<Option<CanonicalStreamEvent>, AdapterError> {
    let data = event.data.trim();
    if data.is_empty() {
        return Ok(None);
    }
    let value: Value = match serde_json::from_str(data) {
        Ok(value) => value,
        Err(_) => return Ok(None),
    };

    let candidate = value
        .get("candidates")
        .and_then(Value::as_array)
        .and_then(|candidates| candidates.first());

    // Gemini commonly co-locates the final text part with `finishReason` and
    // `usageMetadata` in ONE terminal chunk, so a single chunk can lift to two
    // canonical events: the content delta (or tool-call start) AND the
    // terminal `MessageDelta`. Extract the content event first so it is emitted
    // before the terminal (order matters), then build the message_delta from
    // any `finishReason`/`usageMetadata`. Returning the content event early
    // (the old behavior) would silently drop the tail of the assistant
    // message on every terminal chunk that carries content.
    let content_event = candidate.and_then(|candidate| parse_content_part(candidate, st));

    let usage = value.get("usageMetadata");
    let stop_reason = candidate
        .and_then(|candidate| candidate.get("finishReason"))
        .and_then(Value::as_str)
        .map(parse_stop_reason);

    let message_delta = if usage.is_some() || stop_reason.is_some() {
        Some(CanonicalStreamEvent::MessageDelta {
            stop_reason,
            usage: usage.map(|usage| CanonicalUsage {
                input_tokens: usage.get("promptTokenCount").and_then(Value::as_u64),
                output_tokens: usage.get("candidatesTokenCount").and_then(Value::as_u64),
            }),
        })
    } else {
        None
    };

    // Return the first event directly; queue any second (the terminal
    // message_delta after a content delta) for the stream driver to drain in
    // order. A chunk with only content, only usage/finishReason, or neither
    // still behaves exactly as before.
    match (content_event, message_delta) {
        (Some(content), Some(delta)) => {
            st.pending.push_back(delta);
            Ok(Some(content))
        }
        (Some(content), None) => Ok(Some(content)),
        (None, Some(delta)) => Ok(Some(delta)),
        (None, None) => Ok(None),
    }
}

/// Lift a Gemini candidate's first content part into a canonical stream event:
/// a `text` part becomes a [`CanonicalStreamEvent::ContentBlockDelta`] on the
/// single streamed text block's index; a `functionCall` part (which arrives
/// whole, not fragmented) becomes a [`CanonicalStreamEvent::ContentBlockStart`]
/// with a fully populated `ToolUse` block at a fresh index. Any other part, or
/// no parts, yields `None`.
fn parse_content_part(
    candidate: &Value,
    st: &mut StreamParseState,
) -> Option<CanonicalStreamEvent> {
    let part = candidate
        .get("content")
        .and_then(|content| content.get("parts"))
        .and_then(Value::as_array)
        .and_then(|parts| parts.first())?;

    if let Some(text) = part.get("text").and_then(Value::as_str) {
        let index = st.text_index();
        return Some(CanonicalStreamEvent::ContentBlockDelta {
            index,
            text: Some(text.to_string()),
            partial_json: None,
        });
    }
    if let Some(call) = part.get("functionCall") {
        let name = call
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        // Gemini has no separate call id; the function name doubles as the
        // canonical id (matching this adapter's buffered `parse_one_part`).
        return Some(CanonicalStreamEvent::ContentBlockStart {
            index: st.fresh_index(),
            block: ContentBlock::ToolUse {
                id: name.clone(),
                name,
                input: call.get("args").cloned().unwrap_or(Value::Null),
            },
        });
    }
    None
}

/// Append one fully-framed Google SSE event (`data: <compact-json>\n\n`) to
/// `out`. Google's stream, like OpenAI's, has no `event:` type line.
fn push_data(out: &mut String, value: &Value) {
    out.push_str("data: ");
    out.push_str(&value.to_string());
    out.push_str("\n\n");
}

/// Build one Gemini stream chunk from `parts`, an optional `finishReason`,
/// and an optional `usageMetadata` object.
fn google_chunk(parts: Vec<Value>, finish_reason: Option<String>, usage: Option<Value>) -> Value {
    let mut candidate = Map::new();
    candidate.insert(
        "content".to_string(),
        json!({"parts": parts, "role": "model"}),
    );
    candidate.insert("index".to_string(), json!(0));
    if let Some(finish_reason) = finish_reason {
        candidate.insert("finishReason".to_string(), json!(finish_reason));
    }
    let mut chunk = Map::new();
    chunk.insert("candidates".to_string(), json!([Value::Object(candidate)]));
    if let Some(usage) = usage {
        chunk.insert("usageMetadata".to_string(), usage);
    }
    Value::Object(chunk)
}

/// Render one canonical [`CanonicalStreamEvent`] down into fully-framed Google
/// `streamGenerateContent` bytes (design doc M15 Task 4). Google is coarse
/// like OpenAI, so:
///
/// - [`CanonicalStreamEvent::MessageStart`] emits nothing (Gemini has no
///   per-message start event); the model/role are captured for bookkeeping.
/// - A `ToolUse` [`CanonicalStreamEvent::ContentBlockStart`] whose `input` is
///   already whole (a Gemini/Anthropic-origin call) emits the `functionCall`
///   part immediately; one whose `input` is `Null` (an OpenAI-origin call
///   whose arguments stream as `partial_json`) is buffered and emitted when
///   its block stops — Gemini can only express a tool call's arguments as one
///   whole JSON object. A text start emits nothing.
/// - A [`CanonicalStreamEvent::ContentBlockDelta`] with `text` emits a text
///   chunk; with `partial_json` accumulates into the pending tool call.
/// - A [`CanonicalStreamEvent::ContentBlockStop`] flushes a buffered tool call
///   as a `functionCall` chunk (parsing the accumulated argument text as JSON,
///   falling back to `{}` if it isn't valid).
/// - A [`CanonicalStreamEvent::MessageDelta`] emits a chunk carrying its
///   `finishReason` (via [`render_stop_reason`], recording downgrades in
///   `report`) and/or `usageMetadata`.
/// - A [`CanonicalStreamEvent::MessageStop`] emits nothing — Gemini's stream
///   has no terminal sentinel.
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
            st.message_started = true;
        }
        CanonicalStreamEvent::ContentBlockStart { index, block } => {
            // Only a tool-use block maps to a Gemini part at start; a
            // text/image start streams (or emits) nothing here.
            if let ContentBlock::ToolUse { name, input, .. } = block {
                st.block_is_tool.insert(*index, true);
                if input.is_null() {
                    // Arguments will stream as `partial_json`; buffer until the
                    // block stops.
                    st.tool_render.insert(
                        *index,
                        StreamToolRender {
                            slot: 0,
                            name: name.clone(),
                            args: String::new(),
                        },
                    );
                } else {
                    // Whole call already known — emit the `functionCall` now.
                    push_data(
                        &mut out,
                        &google_chunk(
                            vec![json!({"functionCall": {"name": name, "args": input}})],
                            None,
                            None,
                        ),
                    );
                }
            }
        }
        CanonicalStreamEvent::ContentBlockDelta {
            index,
            text,
            partial_json,
        } => {
            if let Some(text) = text {
                push_data(
                    &mut out,
                    &google_chunk(vec![json!({"text": text})], None, None),
                );
            } else if let Some(partial_json) = partial_json {
                st.tool_render
                    .entry(*index)
                    .or_default()
                    .args
                    .push_str(partial_json);
            }
        }
        CanonicalStreamEvent::ContentBlockStop { index } => {
            if let Some(tool) = st.tool_render.remove(index) {
                let args = serde_json::from_str::<Value>(&tool.args).unwrap_or_else(|_| json!({}));
                push_data(
                    &mut out,
                    &google_chunk(
                        vec![json!({"functionCall": {"name": tool.name, "args": args}})],
                        None,
                        None,
                    ),
                );
            }
        }
        CanonicalStreamEvent::MessageDelta { stop_reason, usage } => {
            let finish_reason = stop_reason
                .as_ref()
                .map(|reason| render_stop_reason(reason, report));
            let usage = usage.map(|usage| {
                let mut usage_obj = Map::new();
                if let Some(input_tokens) = usage.input_tokens {
                    usage_obj.insert("promptTokenCount".to_string(), json!(input_tokens));
                }
                if let Some(output_tokens) = usage.output_tokens {
                    usage_obj.insert("candidatesTokenCount".to_string(), json!(output_tokens));
                }
                Value::Object(usage_obj)
            });
            push_data(&mut out, &google_chunk(vec![], finish_reason, usage));
        }
        CanonicalStreamEvent::MessageStop => {
            // Gemini's stream has no terminal event; nothing to emit. Still
            // record it (and short-circuit a repeat) so the terminal_sent flag
            // is authoritative for the proxy's double-terminal guard (M15 Task
            // 7 obligation c).
            if st.terminal_sent {
                return Ok(out.into_bytes());
            }
            st.terminal_sent = true;
        }
    }
    Ok(out.into_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx(path: &str) -> RequestCtx {
        RequestCtx {
            path: path.to_string(),
            method: "POST".to_string(),
        }
    }

    #[test]
    fn parses_contents_into_messages() {
        let body = br#"{
            "contents": [
                {"role": "user", "parts": [{"text": "hello there"}]}
            ]
        }"#;
        let req = parse_body(body, &ctx("/v1beta/models/gemini-1.5-pro:generateContent")).unwrap();
        assert_eq!(req.messages.len(), 1);
        assert_eq!(req.messages[0].role, Role::User);
        assert_eq!(
            req.messages[0].content,
            vec![ContentBlock::Text {
                text: "hello there".to_string()
            }]
        );
    }

    #[test]
    fn maps_model_role_to_assistant() {
        let body = br#"{"contents": [{"role": "model", "parts": [{"text": "hi"}]}]}"#;
        let req = parse_body(body, &ctx("/v1beta/models/gemini-1.5-pro:generateContent")).unwrap();
        assert_eq!(req.messages[0].role, Role::Assistant);
    }

    #[test]
    fn joins_multiple_text_parts() {
        let body = br#"{
            "contents": [
                {"role": "user", "parts": [{"text": "first part"}, {"text": "second part"}]}
            ]
        }"#;
        let req = parse_body(body, &ctx("/v1beta/models/gemini-1.5-pro:generateContent")).unwrap();
        assert_eq!(
            req.messages[0].content,
            vec![
                ContentBlock::Text {
                    text: "first part".to_string()
                },
                ContentBlock::Text {
                    text: "second part".to_string()
                },
            ]
        );
    }

    #[test]
    fn parses_inline_data_as_image_block() {
        let body = br#"{
            "contents": [
                {"role": "user", "parts": [{"inlineData": {"mimeType": "image/png", "data": "abc"}}]}
            ]
        }"#;
        let req = parse_body(body, &ctx("/v1beta/models/gemini-1.5-pro:generateContent")).unwrap();
        assert_eq!(
            req.messages[0].content,
            vec![ContentBlock::Image {
                media_type: "image/png".to_string(),
                data: "abc".to_string(),
            }]
        );
    }

    #[test]
    fn parses_function_call_as_tool_use_block() {
        let body = br#"{
            "contents": [
                {"role": "model", "parts": [{"functionCall": {"name": "get_weather", "args": {"location": "NYC"}}}]}
            ]
        }"#;
        let req = parse_body(body, &ctx("/v1beta/models/gemini-1.5-pro:generateContent")).unwrap();
        assert_eq!(
            req.messages[0].content,
            vec![ContentBlock::ToolUse {
                id: "get_weather".to_string(),
                name: "get_weather".to_string(),
                input: serde_json::json!({"location": "NYC"}),
            }]
        );
    }

    #[test]
    fn parses_function_response_as_tool_result_block() {
        let body = br#"{
            "contents": [
                {"role": "function", "parts": [{"functionResponse": {"name": "get_weather", "response": {"result": "sunny"}}}]}
            ]
        }"#;
        let req = parse_body(body, &ctx("/v1beta/models/gemini-1.5-pro:generateContent")).unwrap();
        assert_eq!(req.messages[0].role, Role::Tool);
        assert_eq!(
            req.messages[0].content,
            vec![ContentBlock::ToolResult {
                id: "get_weather".to_string(),
                content: serde_json::to_string(&serde_json::json!({"result": "sunny"})).unwrap(),
                is_error: false,
            }]
        );
    }

    #[test]
    fn model_comes_from_path_segment() {
        let body = br#"{"contents": []}"#;
        let req = parse_body(body, &ctx("/v1beta/models/gemini-1.5-pro:generateContent")).unwrap();
        assert_eq!(req.model, "gemini-1.5-pro");
    }

    #[test]
    fn model_from_path_wins_over_body_model_field() {
        let body = br#"{"model": "body-model", "contents": []}"#;
        let req = parse_body(body, &ctx("/v1beta/models/gemini-1.5-pro:generateContent")).unwrap();
        assert_eq!(req.model, "gemini-1.5-pro");
    }

    #[test]
    fn falls_back_to_body_model_field_when_path_has_no_model_segment() {
        let body = br#"{"model": "gemini-1.5-pro", "contents": []}"#;
        let req = parse_body(body, &ctx("/some/other/path")).unwrap();
        assert_eq!(req.model, "gemini-1.5-pro");
    }

    #[test]
    fn missing_model_in_both_path_and_body_defaults_to_empty_string() {
        let body = br#"{"contents": []}"#;
        let req = parse_body(body, &ctx("/some/other/path")).unwrap();
        assert_eq!(req.model, "");
    }

    #[test]
    fn model_from_path_handles_streaming_method_segment() {
        assert_eq!(
            model_from_path("/v1beta/models/gemini-1.5-pro:streamGenerateContent"),
            Some("gemini-1.5-pro".to_string())
        );
    }

    #[test]
    fn model_from_path_none_when_no_models_segment() {
        assert_eq!(model_from_path("/v1beta/foo/bar"), None);
    }

    /// A caller-supplied API key query string (`?key=...`) trails after the
    /// `:<method>` segment, not before it — `model_from_path` splits on the
    /// first `:` after `models/`, so the query string never reaches the
    /// extracted model id in the first place.
    #[test]
    fn model_from_path_handles_trailing_query_string() {
        assert_eq!(
            model_from_path("/v1beta/models/gemini-2.5-pro:generateContent?key=abc"),
            Some("gemini-2.5-pro".to_string())
        );
    }

    #[test]
    fn parses_system_instruction() {
        let body = br#"{
            "contents": [],
            "systemInstruction": {"role": "system", "parts": [{"text": "You are helpful."}]}
        }"#;
        let req = parse_body(body, &ctx("/v1beta/models/gemini-1.5-pro:generateContent")).unwrap();
        assert_eq!(req.system.as_deref(), Some("You are helpful."));
    }

    #[test]
    fn missing_system_instruction_is_none() {
        let body = br#"{"contents": []}"#;
        let req = parse_body(body, &ctx("/v1beta/models/gemini-1.5-pro:generateContent")).unwrap();
        assert_eq!(req.system, None);
    }

    #[test]
    fn parses_function_declaration_names_and_schema() {
        let body = br#"{
            "contents": [],
            "tools": [
                {"functionDeclarations": [
                    {"name": "get_weather", "description": "...", "parameters": {"type": "object"}},
                    {"name": "search"}
                ]}
            ]
        }"#;
        let req = parse_body(body, &ctx("/v1beta/models/gemini-1.5-pro:generateContent")).unwrap();
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
    fn parses_generation_config_sampling() {
        let body = br#"{
            "contents": [],
            "generationConfig": {
                "temperature": 0.7,
                "topP": 0.9,
                "topK": 40,
                "maxOutputTokens": 2048,
                "stopSequences": ["STOP"]
            }
        }"#;
        let req = parse_body(body, &ctx("/v1beta/models/gemini-1.5-pro:generateContent")).unwrap();
        assert_eq!(req.sampling.temperature, Some(0.7));
        assert_eq!(req.sampling.top_p, Some(0.9));
        assert_eq!(req.sampling.top_k, Some(40));
        assert_eq!(req.sampling.max_tokens, Some(2048));
        assert_eq!(req.sampling.stop, vec!["STOP".to_string()]);
    }

    #[test]
    fn missing_generation_config_is_default_sampling() {
        let body = br#"{"contents": []}"#;
        let req = parse_body(body, &ctx("/v1beta/models/gemini-1.5-pro:generateContent")).unwrap();
        assert_eq!(req.sampling, Sampling::default());
    }

    #[test]
    fn stream_defaults_to_false() {
        let body = br#"{"contents": []}"#;
        let req = parse_body(body, &ctx("/v1beta/models/gemini-1.5-pro:generateContent")).unwrap();
        assert!(!req.stream);
    }

    #[test]
    fn stream_defaults_to_false_even_on_stream_generate_content_path() {
        let body = br#"{"contents": []}"#;
        let req = parse_body(
            body,
            &ctx("/v1beta/models/gemini-1.5-pro:streamGenerateContent"),
        )
        .unwrap();
        assert!(!req.stream);
    }

    #[test]
    fn missing_contents_defaults_to_empty_messages() {
        let body = br#"{}"#;
        let req = parse_body(body, &ctx("/v1beta/models/gemini-1.5-pro:generateContent")).unwrap();
        assert!(req.messages.is_empty());
    }

    #[test]
    fn unknown_top_level_fields_land_in_extra() {
        let body = br#"{"contents": [], "safetySettings": ["x"]}"#;
        let req = parse_body(body, &ctx("/v1beta/models/gemini-1.5-pro:generateContent")).unwrap();
        assert_eq!(
            req.extra.get("google.safetySettings"),
            Some(&serde_json::json!(["x"]))
        );
    }

    #[test]
    fn invalid_json_is_malformed_error() {
        let err =
            parse_body(b"not json at all", &ctx("/v1beta/models/x:generateContent")).unwrap_err();
        assert!(matches!(err, AdapterError::Malformed(_)));
    }

    #[test]
    fn adapter_trait_impl_reports_provider_and_parses() {
        let adapter = GoogleAdapter;
        assert_eq!(adapter.provider(), "google");
        let body = br#"{"contents": []}"#;
        assert!(adapter
            .parse_request(body, &ctx("/v1beta/models/gemini-1.5-pro:generateContent"))
            .is_ok());
    }

    // -- parse_response --------------------------------------------------

    #[test]
    fn parses_response_content_finish_reason_and_usage() {
        let body = br#"{
            "candidates": [{
                "content": {"parts": [{"text": "hello there"}], "role": "model"},
                "finishReason": "STOP",
                "index": 0
            }],
            "usageMetadata": {"promptTokenCount": 10, "candidatesTokenCount": 20, "totalTokenCount": 30},
            "modelVersion": "gemini-1.5-pro"
        }"#;
        let resp = parse_response_body(body).unwrap();
        assert_eq!(resp.model, "gemini-1.5-pro");
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
    fn missing_model_version_defaults_to_empty_string() {
        let body = br#"{"candidates": []}"#;
        let resp = parse_response_body(body).unwrap();
        assert_eq!(resp.model, "");
    }

    #[test]
    fn unrecognized_finish_reason_becomes_other() {
        let body = br#"{"candidates": [{"content": {"parts": [], "role": "model"}, "finishReason": "SAFETY", "index": 0}]}"#;
        let resp = parse_response_body(body).unwrap();
        assert_eq!(
            resp.stop_reason,
            Some(StopReason::Other("SAFETY".to_string()))
        );
    }

    #[test]
    fn no_candidates_yields_empty_content_and_no_stop_reason() {
        let body = br#"{"candidates": []}"#;
        let resp = parse_response_body(body).unwrap();
        assert!(resp.content.is_empty());
        assert_eq!(resp.stop_reason, None);
    }

    #[test]
    fn response_unknown_fields_land_in_extra() {
        let body = br#"{"candidates": [], "promptFeedback": {"blockReason": "SAFETY"}}"#;
        let resp = parse_response_body(body).unwrap();
        assert_eq!(
            resp.extra.get("google.promptFeedback"),
            Some(&serde_json::json!({"blockReason": "SAFETY"}))
        );
    }

    #[test]
    fn response_invalid_json_is_malformed_error() {
        let err = parse_response_body(b"not json at all").unwrap_err();
        assert!(matches!(err, AdapterError::Malformed(_)));
    }

    #[test]
    fn adapter_trait_impl_parses_response() {
        let adapter = GoogleAdapter;
        let body = br#"{"candidates": []}"#;
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
        let data = event(
            r#"{"candidates":[{"content":{"parts":[{"text":"Hello"}],"role":"model"},"index":0}]}"#,
        );
        let delta = parse_delta(&data).expect("must parse");
        assert_eq!(delta.kind, DeltaKind::Text);
        assert_eq!(delta.text.as_deref(), Some("Hello"));
    }

    #[test]
    fn parses_usage_metadata() {
        let data = event(
            r#"{"candidates":[],"usageMetadata":{"promptTokenCount":10,"candidatesTokenCount":20,"totalTokenCount":30}}"#,
        );
        let delta = parse_delta(&data).expect("must parse");
        assert_eq!(delta.kind, DeltaKind::Usage);
        let usage = delta.usage.expect("usage must be set");
        assert_eq!(usage.input_tokens, Some(10));
        assert_eq!(usage.output_tokens, Some(20));
    }

    #[test]
    fn parses_finish_reason() {
        let data = event(
            r#"{"candidates":[{"content":{"parts":[],"role":"model"},"finishReason":"STOP","index":0}]}"#,
        );
        let delta = parse_delta(&data).expect("must parse");
        assert_eq!(delta.kind, DeltaKind::Finish);
        assert_eq!(delta.finish.unwrap().reason.as_deref(), Some("STOP"));
    }

    #[test]
    fn usage_metadata_takes_priority_over_finish_reason_when_both_present() {
        let data = event(
            r#"{"candidates":[{"finishReason":"STOP","index":0}],"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":2}}"#,
        );
        let delta = parse_delta(&data).expect("must parse");
        assert_eq!(delta.kind, DeltaKind::Usage);
    }

    #[test]
    fn no_candidates_is_none() {
        let data = event(r#"{"candidates":[]}"#);
        assert!(parse_delta(&data).is_none());
    }

    #[test]
    fn malformed_json_is_none() {
        let data = event("not json");
        assert!(parse_delta(&data).is_none());
    }

    #[test]
    fn adapter_parse_delta_dispatches_to_free_function() {
        let adapter = GoogleAdapter;
        let data = event(
            r#"{"candidates":[{"content":{"parts":[{"text":"hi"}],"role":"model"},"index":0}]}"#,
        );
        let delta = adapter.parse_delta(&data).expect("must parse");
        assert_eq!(delta.text.as_deref(), Some("hi"));
    }

    // -- render_request / render_response (M15 Task 3) --------------------

    /// `tool_choice: None` — this adapter's parse side never populates it
    /// (see `parse_body`), and render drops any `Some(_)` (covered by
    /// `render_request_drops_tool_choice`), so a request carrying one could
    /// never round-trip losslessly through this adapter.
    fn sample_request() -> CanonicalRequest {
        CanonicalRequest {
            model: "gemini-1.5-pro".to_string(),
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
            tool_choice: None,
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

        let reparsed =
            parse_body(&body, &ctx("/v1beta/models/gemini-1.5-pro:generateContent")).unwrap();
        assert_eq!(reparsed, req);
    }

    #[test]
    fn render_request_writes_system_instruction_and_function_declarations() {
        let req = sample_request();
        let mut report = TranslationReport::default();
        let body = render_request_body(&req, &mut report).unwrap();
        let value: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(
            value["systemInstruction"]["parts"][0]["text"],
            json!("You are helpful.")
        );
        assert_eq!(
            value["tools"][0]["functionDeclarations"][0]["name"],
            json!("get_weather")
        );
        assert_eq!(value["generationConfig"]["maxOutputTokens"], json!(1024));
        assert_eq!(value["generationConfig"]["topK"], json!(40));
    }

    #[test]
    fn render_request_drops_tool_choice() {
        let mut req = sample_request();
        req.tool_choice = Some(crate::llm::ToolChoice::Auto);
        let mut report = TranslationReport::default();
        render_request_body(&req, &mut report).unwrap();

        assert_eq!(report.dropped.len(), 1);
        assert_eq!(report.dropped[0].path, "tool_choice");
    }

    #[test]
    fn render_request_drops_stream_true_with_note() {
        let mut req = sample_request();
        req.stream = true;
        let mut report = TranslationReport::default();
        let body = render_request_body(&req, &mut report).unwrap();
        let value: Value = serde_json::from_slice(&body).unwrap();

        assert!(value.get("stream").is_none());
        assert_eq!(report.dropped.len(), 1);
        assert_eq!(report.dropped[0].path, "stream");
    }

    /// A `safetySettings` field tagged as having come from a *different*
    /// provider's parse (`"anthropic.safetySettings"`) — this adapter's
    /// render can't verify it belongs to Google's dialect, so it's dropped
    /// with a report entry. Contrast with
    /// `render_request_preserves_same_provider_extra_field_on_round_trip`
    /// below, where a `"google."`-tagged entry survives instead.
    #[test]
    fn render_request_drops_cross_provider_extra_fields() {
        let mut req = sample_request();
        req.extra.insert(
            "anthropic.safetySettings".to_string(),
            serde_json::json!(["x"]),
        );
        let mut report = TranslationReport::default();
        render_request_body(&req, &mut report).unwrap();

        assert_eq!(report.dropped.len(), 1);
        assert_eq!(report.dropped[0].path, "extra.safetySettings");
        assert!(report.dropped[0].reason.contains("anthropic"));
    }

    /// M15 Task 3 review fix: an `extra` entry tagged with *this* adapter's
    /// own provider name must round-trip losslessly through render rather
    /// than being dropped.
    #[test]
    fn render_request_preserves_same_provider_extra_field_on_round_trip() {
        let mut req = sample_request();
        req.extra.insert(
            "google.safetySettings".to_string(),
            serde_json::json!(["x"]),
        );
        let mut report = TranslationReport::default();
        let body = render_request_body(&req, &mut report).unwrap();
        assert!(
            report.dropped.is_empty(),
            "same-provider extra must survive: {:?}",
            report.dropped
        );

        let value: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["safetySettings"], json!(["x"]));

        let reparsed =
            parse_body(&body, &ctx("/v1beta/models/gemini-1.5-pro:generateContent")).unwrap();
        assert_eq!(reparsed, req);
    }

    #[test]
    fn render_request_drops_tool_use_id_when_it_diverges_from_name() {
        let mut req = sample_request();
        req.messages = vec![CanonicalMessage {
            role: Role::Assistant,
            content: vec![ContentBlock::ToolUse {
                id: "call_1".to_string(),
                name: "get_weather".to_string(),
                input: serde_json::json!({"location": "NYC"}),
            }],
        }];
        let mut report = TranslationReport::default();
        let body = render_request_body(&req, &mut report).unwrap();
        let value: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(
            value["contents"][0]["parts"][0]["functionCall"]["name"],
            json!("get_weather")
        );
        assert!(report
            .dropped
            .iter()
            .any(|d| d.path == "messages[0].content.tool_use.id"));
    }

    #[test]
    fn render_response_round_trips_through_parse() {
        let resp = CanonicalResponse {
            model: "gemini-1.5-pro".to_string(),
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
        assert_eq!(reparsed, resp);
    }

    /// A representative real Gemini response `extra` field
    /// (`promptFeedback`, not modeled in `KNOWN_RESPONSE_FIELDS`) must
    /// survive a same-provider parse -> render.
    #[test]
    fn render_response_preserves_same_provider_extra_field_on_round_trip() {
        let mut resp = CanonicalResponse {
            model: "gemini-1.5-pro".to_string(),
            content: vec![ContentBlock::Text {
                text: "hello there".to_string(),
            }],
            stop_reason: Some(StopReason::EndTurn),
            usage: None,
            extra: Default::default(),
        };
        resp.extra.insert(
            "google.promptFeedback".to_string(),
            serde_json::json!({"blockReason": "SAFETY"}),
        );

        let mut report = TranslationReport::default();
        let body = render_response_body(&resp, &mut report).unwrap();
        assert!(
            report.dropped.is_empty(),
            "same-provider extra must survive: {:?}",
            report.dropped
        );

        let value: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["promptFeedback"], json!({"blockReason": "SAFETY"}));

        let reparsed = parse_response_body(&body).unwrap();
        assert_eq!(reparsed, resp);
    }

    #[test]
    fn render_response_downgrades_tool_use_stop_reason_with_note() {
        let resp = CanonicalResponse {
            model: "m".to_string(),
            content: vec![],
            stop_reason: Some(StopReason::ToolUse),
            usage: None,
            extra: Default::default(),
        };
        let mut report = TranslationReport::default();
        let body = render_response_body(&resp, &mut report).unwrap();
        let value: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(value["candidates"][0]["finishReason"], json!("STOP"));
        assert!(!report.notes.is_empty());
    }

    #[test]
    fn adapter_trait_render_request_dispatches_to_free_function() {
        let adapter = GoogleAdapter;
        let mut report = TranslationReport::default();
        let body = adapter
            .render_request(&sample_request(), &mut report)
            .unwrap();
        assert!(!body.is_empty());
    }
}
