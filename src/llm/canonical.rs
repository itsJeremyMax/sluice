//! Canonical LLM IR: a lossless, provider-agnostic representation of an LLM
//! request/response/stream event (design doc M15 Task 1). Where [`super::Llm`]
//! is a deliberately lossy read-side *view* (content flattened to plain
//! text, tools reduced to a bare name) meant for steps that only need to
//! glance at a request, the types in this module are meant to be the
//! gateway's internal spine for *translating* between provider dialects
//! (Anthropic `/v1/messages`, OpenAI `/v1/chat/completions`, Google
//! `generateContent`): every field a provider's wire format can carry has
//! somewhere to live here, so parsing one dialect in and serializing another
//! dialect out doesn't have to lose information the destination dialect
//! could have represented.
//!
//! This module defines the shapes; [`super::anthropic`], [`super::openai`],
//! and [`super::google`] parse a provider's wire format into them
//! ([`super::adapter::Adapter::parse_request`]/`parse_response`, M15 Task 2)
//! and serialize them back out
//! ([`super::adapter::Adapter::render_request`]/`render_response`, M15 Task
//! 3). [`project_llm`] is a third, one-way thing: a projection from the
//! lossless [`CanonicalRequest`] down to the lossy [`super::Llm`] view, so
//! existing steps that only know about `Llm` keep working on top of the
//! canonical IR.
//!
//! # Render fidelity by provider pair (M15 Task 3)
//!
//! Rendering (`CanonicalRequest`/`CanonicalResponse` -> a provider's wire
//! bytes) is lossless exactly when every canonical field the request/response
//! actually uses has a home in the target dialect; where it doesn't, the
//! adapter calls [`TranslationReport::drop`] instead of guessing or silently
//! discarding. The gaps are provider-specific, not symmetric:
//!
//! - **Sampling.** Anthropic and Google both accept `temperature`, `top_p`,
//!   `top_k`, `max_tokens`/`maxOutputTokens`, and stop sequences — fully
//!   lossless. OpenAI's Chat Completions API has no `top_k` parameter at
//!   all: rendering a `Sampling.top_k: Some(_)` to OpenAI always drops it
//!   (`"sampling.top_k"`, `"openai has no top_k"`).
//! - **`tool_choice`.** Anthropic (`auto`/`any`/`none`/`tool`) and OpenAI
//!   (`"auto"`/`"none"`/`"required"`/a pinned function object) each map onto
//!   the four canonical [`ToolChoice`] variants 1:1 — lossless both ways.
//!   Google is the outlier: this codebase's Google adapter has never parsed
//!   `tool_choice`/`toolConfig` (parse always yields `None`), so rendering a
//!   `Some(_)` to Google always drops it rather than inventing a
//!   `toolConfig` shape with no parse-side counterpart to round-trip through.
//! - **`stop_reason`/`finish_reason`.** Anthropic's four named reasons plus
//!   `Other`'s raw-string passthrough are exactly the canonical
//!   [`StopReason`] variants — lossless. OpenAI has no reason distinct from
//!   `"stop"` for a custom stop sequence, so `StopReason::StopSequence`
//!   downgrades to `"stop"` (recorded as a report *note*, not a drop, since
//!   a `finish_reason` is still written — just a less specific one). Google
//!   has no reason distinct from `"STOP"` for either a stop sequence or a
//!   tool call, so both `StopReason::StopSequence` and `StopReason::ToolUse`
//!   downgrade to `"STOP"` with a note. `StopReason::Other`'s raw string
//!   renders verbatim to all three (each dialect accepts an arbitrary string
//!   there on the wire).
//! - **Content blocks.** Anthropic's and Google's content-block/`parts[]`
//!   arrays are structurally rich enough to carry all four
//!   [`ContentBlock`] variants inline within any message. OpenAI splits a
//!   `ToolResult` block out into its own `role: "tool"` message (it can't
//!   live inside another message's `content`), and drops `ToolResult`
//!   blocks entirely from a *response* (a Chat Completions response is a
//!   single `message` object, with nowhere to put a standalone tool
//!   result). `ToolResult.is_error` has no field on OpenAI's tool message at
//!   all, and is dropped whenever `true`. Google's `functionCall`/
//!   `functionResponse` objects key on a function *name*, not a separate
//!   call id — a canonical `ToolUse`/`ToolResult.id` that differs from
//!   `name` (only possible for a block that arrived from a dialect that
//!   *does* distinguish them, e.g. Anthropic's `call_1` vs. `get_weather`)
//!   drops the `id`; `ToolResult.is_error` has no Google field either and is
//!   dropped when `true`.
//! - **System prompt.** Fully lossless to all three: Anthropic's top-level
//!   `system` string, OpenAI's leading `role: "system"` message, and
//!   Google's `systemInstruction` object are all exactly the single
//!   canonical `system: Option<String>` field, re-expressed per dialect.
//! - **Streaming.** Anthropic and OpenAI both have a top-level `stream`
//!   boolean — lossless. Google signals streaming via a URL path segment
//!   (`streamGenerateContent`) rather than a body field, and
//!   `Adapter::render_request` has no `RequestCtx` to place a path on, so a
//!   `stream: true` canonical request always drops when rendered to Google.
//! - **`extra`.** Each entry is tagged with its source provider at parse
//!   time (`"<provider>.<key>"`, via [`super::adapter::collect_extra`]).
//!   Render re-emits an entry losslessly when its source provider matches
//!   the render target (a same-provider round trip), and drops it with a
//!   report entry otherwise — see [`super::adapter::emit_or_drop_extra`]'s
//!   doc comment.

use std::collections::{HashMap, HashSet, VecDeque};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::{Llm, Message, Tool};

/// Who a [`CanonicalMessage`] is attributed to. Unlike [`super::Message`]'s
/// bare `String` role, this is a closed set — the union of roles all three
/// built-in providers recognize — so translation code can match
/// exhaustively instead of string-comparing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    System,
    User,
    Assistant,
    Tool,
}

impl Role {
    /// The lowercase wire string for this role, matching
    /// [`super::Message::role`]'s existing convention
    /// (`"system"`/`"user"`/`"assistant"`/`"tool"`).
    pub fn as_str(&self) -> &'static str {
        match self {
            Role::System => "system",
            Role::User => "user",
            Role::Assistant => "assistant",
            Role::Tool => "tool",
        }
    }
}

/// One block of a [`CanonicalMessage`]'s content. A message carries a `Vec`
/// of these rather than a single string because provider wire formats do:
/// Anthropic and OpenAI both accept an array of typed content parts per
/// message (interleaving text, tool calls/results, images), which today's
/// [`super::Message::content`] flattens away. Serialized with an internal
/// `type` tag (`"text"`, `"tool_use"`, `"tool_result"`, `"image"`) mirroring
/// the shape provider wire formats already use for content blocks.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentBlock {
    Text {
        text: String,
    },
    ToolUse {
        id: String,
        name: String,
        input: Value,
    },
    ToolResult {
        id: String,
        content: String,
        is_error: bool,
    },
    Image {
        media_type: String,
        data: String,
    },
}

/// A single canonical message: a [`Role`] and its content blocks.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CanonicalMessage {
    pub role: Role,
    pub content: Vec<ContentBlock>,
}

/// A tool definition, lossless version of [`super::Tool`]. `input_schema`
/// keeps the raw JSON Schema `Value` rather than a typed shape since
/// providers accept arbitrary caller-supplied schemas.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CanonicalTool {
    pub name: String,
    pub description: Option<String>,
    pub input_schema: Value,
}

/// How the model should choose (or not choose) among the request's tools.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolChoice {
    Auto,
    Any,
    None,
    Tool(String),
}

/// Why the model stopped generating, normalized across providers'
/// differently-named terminal reasons (e.g. Anthropic's `end_turn` vs.
/// OpenAI's `stop`). `Other` preserves a provider-specific reason this set
/// doesn't have a slot for, keeping the mapping lossless.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopReason {
    EndTurn,
    MaxTokens,
    ToolUse,
    StopSequence,
    Other(String),
}

/// Sampling/generation parameters for a [`CanonicalRequest`], gathered from
/// wherever each provider puts them on the wire (e.g. Anthropic and OpenAI
/// both accept `temperature` and `top_p` at the top level of the request
/// body; `max_tokens` is Anthropic's name, OpenAI's newer alias is
/// `max_completion_tokens`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct Sampling {
    pub temperature: Option<f64>,
    pub top_p: Option<f64>,
    pub top_k: Option<u64>,
    pub max_tokens: Option<u64>,
    pub stop: Vec<String>,
}

/// Token accounting for a completed (non-streaming) response.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Usage {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
}

/// One field a translation (canonical -> wire, or wire -> canonical) could
/// not carry across losslessly — e.g. a provider-specific request field with
/// no home in the canonical shape yet, or a canonical field the destination
/// dialect has no wire representation for. Recorded rather than silently
/// discarded so callers (logging, `X-Sluice-*` response headers, tests) can
/// surface what was lost.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DroppedField {
    pub path: String,
    pub reason: String,
}

/// Accumulates what a translation lost. `notes` is a looser channel for
/// observations that aren't a specific dropped field (e.g. "provider X has
/// no equivalent of tool_choice: any, downgraded to auto").
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct TranslationReport {
    pub dropped: Vec<DroppedField>,
    pub notes: Vec<String>,
}

impl TranslationReport {
    /// Record that the field at `path` was dropped during translation, with
    /// a human-readable `reason`.
    pub fn drop(&mut self, path: impl Into<String>, reason: impl Into<String>) {
        self.dropped.push(DroppedField {
            path: path.into(),
            reason: reason.into(),
        });
    }
}

/// Metadata about the original HTTP request a [`CanonicalRequest`] was
/// parsed from — which endpoint and method it arrived on. Translation code
/// needs this to know which wire dialect to serialize back out to when the
/// canonical request is replayed against a (possibly different) provider.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RequestCtx {
    pub path: String,
    pub method: String,
}

/// The canonical, lossless representation of an LLM request. Parsed from a
/// provider's wire format (a later M15 task); [`project_llm`] projects this
/// down to the lossy [`super::Llm`] view existing steps already know about.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CanonicalRequest {
    pub model: String,
    pub messages: Vec<CanonicalMessage>,
    /// The system prompt, canonicalized to a single top-level field
    /// regardless of how the source dialect carries it (Anthropic's
    /// top-level `system`, Google's `systemInstruction`, or a leading
    /// `Role::System` message in OpenAI's `messages` array). This is the
    /// primary carrier for translation; `Role::System` remains a valid
    /// [`CanonicalMessage`] role for dialects that need it re-expressed
    /// as a message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system: Option<String>,
    pub tools: Vec<CanonicalTool>,
    pub tool_choice: Option<ToolChoice>,
    pub sampling: Sampling,
    pub stream: bool,
    /// Provider-specific or unrecognized top-level request fields this
    /// version of the canonical IR has no typed slot for, kept verbatim so
    /// round-tripping through the canonical shape doesn't silently drop
    /// them.
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

/// The canonical, lossless representation of a completed (non-streaming) LLM
/// response. A response is always assistant-authored, so unlike
/// [`CanonicalMessage`] there is no `role` field.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CanonicalResponse {
    pub model: String,
    pub content: Vec<ContentBlock>,
    pub stop_reason: Option<StopReason>,
    pub usage: Option<Usage>,
    /// Provider-specific or unrecognized top-level response fields this
    /// version of the canonical IR has no typed slot for, kept verbatim so
    /// round-tripping through the canonical shape doesn't silently drop
    /// them.
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

/// The canonical, lossless representation of one streaming response event.
/// Modeled on the most granular of the three built-in providers' streaming
/// lifecycles (Anthropic's) since a lossless IR must be able to hold
/// whatever the richest dialect can express; translating a coarser
/// dialect's events (e.g. OpenAI's single per-chunk delta) up to this shape
/// or down from it is later M15 work.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CanonicalStreamEvent {
    MessageStart {
        model: String,
        role: Role,
    },
    ContentBlockStart {
        index: u64,
        block: ContentBlock,
    },
    ContentBlockDelta {
        index: u64,
        text: Option<String>,
        partial_json: Option<String>,
    },
    ContentBlockStop {
        index: u64,
    },
    MessageDelta {
        stop_reason: Option<StopReason>,
        usage: Option<Usage>,
    },
    MessageStop,
}

/// Per-stream bookkeeping carried across successive calls to
/// [`super::adapter::Adapter::parse_stream_event`] (design doc M15 Task 4).
///
/// # Why streaming parse needs state at all
///
/// The three provider streaming dialects differ in *granularity*: Anthropic's
/// stream is already the granular lifecycle the canonical IR
/// ([`CanonicalStreamEvent`]) is modeled on (an explicit `message_start`,
/// per-block `content_block_start`/`stop`, etc.), so its parser is
/// stateless. OpenAI and Google are coarser — a single
/// `chat.completion.chunk` (or Gemini chunk) *packs together* what Anthropic
/// splits across several events, and streams tool calls as a name/id in the
/// first fragment followed by bare argument fragments with no per-fragment
/// identity. Since [`super::adapter::Adapter::parse_stream_event`] returns at
/// most one canonical event per wire event, the parser can't re-emit the
/// whole missing lifecycle inline; instead it lifts each wire event to the
/// single most salient canonical event and remembers just enough here to (a)
/// give the one streamed text block a stable canonical index, and (b)
/// correlate a tool call's later argument fragments back to the
/// [`CanonicalStreamEvent::ContentBlockStart`] that first announced its
/// id/name. The remaining lifecycle scaffolding (a synthetic
/// `message_start`, a text block's `content_block_start`/`stop`) is
/// re-synthesized on the *render* side, keyed off [`StreamRenderState`] —
/// which is where the target dialect's required granularity is known.
///
/// `Default`-constructs to "nothing seen yet"; construct one per stream.
#[derive(Debug, Default)]
pub struct StreamParseState {
    /// The canonical content-block index allocated to the single streamed
    /// text block, if text has been seen. OpenAI/Google surface streamed
    /// assistant text as an unindexed run of content fragments; the canonical
    /// IR wants them all under one indexed block, so the first text fragment
    /// allocates an index here that every later text fragment reuses.
    pub(crate) text_block: Option<u64>,
    /// The next canonical content-block index to hand out. Bumped whenever a
    /// new streamed block (the text block, or a tool call) is first seen, so
    /// indices are dense and collision-free across text and tool blocks.
    pub(crate) next_index: u64,
    /// Maps a source dialect's tool-call slot index (OpenAI's
    /// `tool_calls[].index`) to the canonical content-block index allocated
    /// for it. Presence of a key means that tool call's
    /// [`CanonicalStreamEvent::ContentBlockStart`] has already been emitted,
    /// so subsequent argument fragments for the same slot become
    /// [`CanonicalStreamEvent::ContentBlockDelta`]s instead.
    pub(crate) tool_indices: HashMap<u64, u64>,
    /// Extra canonical events a single wire event expanded into, beyond the one
    /// [`super::adapter::Adapter::parse_stream_event`] returns directly. The
    /// method's contract is one returned event per call, but a coarse source
    /// can pack several canonical events into one chunk: a Gemini terminal
    /// chunk carries a final text part AND the `finishReason`/`usageMetadata`
    /// that become a `MessageDelta`. The parser returns the first (the content
    /// delta) and queues the rest here, in emission order; the stream driver
    /// drains them with [`StreamParseState::drain_pending`] after each wire
    /// event, before reading the next one.
    pub(crate) pending: VecDeque<CanonicalStreamEvent>,
}

impl StreamParseState {
    /// Take any canonical events a single wire event expanded into beyond the
    /// one returned by [`super::adapter::Adapter::parse_stream_event`], in
    /// emission order. Empty in the common case. The stream driver calls this
    /// once after every wire event so a chunk that lifts to more than one
    /// canonical event (e.g. a Gemini terminal chunk carrying both final text
    /// and stop_reason/usage) is fully surfaced.
    pub fn drain_pending(&mut self) -> Vec<CanonicalStreamEvent> {
        self.pending.drain(..).collect()
    }

    /// Return the canonical index for the streamed text block, allocating one
    /// on first use. Idempotent for the rest of the stream.
    pub(crate) fn text_index(&mut self) -> u64 {
        match self.text_block {
            Some(index) => index,
            None => {
                let index = self.next_index;
                self.next_index += 1;
                self.text_block = Some(index);
                index
            }
        }
    }

    /// Return the canonical content-block index for a source tool-call `slot`
    /// and whether this call newly allocated it (`true` the first time a slot
    /// is seen — the caller should emit a
    /// [`CanonicalStreamEvent::ContentBlockStart`]; `false` afterwards — the
    /// caller should emit a [`CanonicalStreamEvent::ContentBlockDelta`] for an
    /// argument fragment).
    pub(crate) fn tool_index(&mut self, slot: u64) -> (u64, bool) {
        if let Some(index) = self.tool_indices.get(&slot) {
            (*index, false)
        } else {
            let index = self.next_index;
            self.next_index += 1;
            self.tool_indices.insert(slot, index);
            (index, true)
        }
    }

    /// Allocate a fresh canonical content-block index unconditionally (used
    /// for a Gemini `functionCall` part, which arrives whole in one chunk and
    /// therefore needs no slot-to-index correlation across later fragments).
    pub(crate) fn fresh_index(&mut self) -> u64 {
        let index = self.next_index;
        self.next_index += 1;
        index
    }
}

/// A tool call under construction on the *render* side of a stream
/// translation — see [`StreamRenderState::tool_render`].
#[derive(Debug, Default, Clone)]
pub(crate) struct StreamToolRender {
    /// The target dialect's tool-call slot index (OpenAI's
    /// `tool_calls[].index`), assigned when the block's start is rendered.
    pub(crate) slot: u64,
    /// The tool name carried on the canonical
    /// [`CanonicalStreamEvent::ContentBlockStart`] — needed when a target
    /// dialect (Google) emits the whole tool call only at block stop.
    pub(crate) name: String,
    /// Accumulated argument-fragment text. Some target dialects (Google) can
    /// only emit a tool call's arguments as one whole JSON object, so the
    /// streamed `partial_json` fragments are buffered here and flushed when
    /// the block stops.
    pub(crate) args: String,
}

/// Per-stream bookkeeping carried across successive calls to
/// [`super::adapter::Adapter::render_stream_event`] (design doc M15 Task 4).
///
/// # Why streaming render needs state at all
///
/// Render is where the coarser dialects' missing lifecycle is *synthesized*
/// (the mirror of [`StreamParseState`]'s note). A canonical stream produced
/// from OpenAI carries no [`CanonicalStreamEvent::MessageStart`] and no
/// `content_block_start`/`stop` around its text (OpenAI's wire format has no
/// equivalent to lift), yet an Anthropic client *requires* a `message_start`
/// then a `content_block_start` before any delta and a `content_block_stop`
/// before `message_delta`. So the Anthropic renderer, seeing the first
/// [`CanonicalStreamEvent::ContentBlockDelta`] with nothing started yet,
/// emits a synthetic `message_start` + `content_block_start` ahead of it, and
/// closes the open block when `message_delta`/end arrives — tracking exactly
/// that here (what's been started, which block is currently open). The OpenAI
/// renderer does the opposite, *collapsing* the canonical start events into
/// the role-bearing first chunk it holds until content flows, and correlating
/// each tool call's id/name/args into one `tool_calls[]` slot. Emitting the
/// target's required terminal (Anthropic `message_stop`, OpenAI `[DONE]`) on
/// [`CanonicalStreamEvent::MessageStop`] is likewise a render-side concern.
///
/// `Default`-constructs to "nothing emitted yet"; construct one per stream.
#[derive(Debug, Default)]
pub struct StreamRenderState {
    /// Whether the target dialect's stream preamble (Anthropic's
    /// `message_start`, OpenAI's role-bearing first chunk) has been emitted.
    /// Guards the one-time synthesis of that preamble.
    pub(crate) message_started: bool,
    /// The model id captured from a [`CanonicalStreamEvent::MessageStart`],
    /// re-expressed in the target's preamble (Anthropic's `message.model`,
    /// OpenAI's per-chunk `model`). Empty when the source dialect never
    /// surfaced a per-stream model (e.g. an OpenAI-origin canonical stream,
    /// whose model isn't lifted to canonical) — a documented granularity gap,
    /// not an error.
    pub(crate) model: String,
    /// The role captured from a [`CanonicalStreamEvent::MessageStart`];
    /// defaults to [`Role::Assistant`] when absent (a streamed response is
    /// always assistant-authored).
    pub(crate) role: Option<Role>,
    /// The canonical index of the content block currently open in the target
    /// stream, if any — so a coarser source that never sent an explicit
    /// [`CanonicalStreamEvent::ContentBlockStop`] can have one synthesized
    /// before the target's `message_delta`/terminal.
    pub(crate) open_block: Option<u64>,
    /// Which canonical block indices have had their target-side start
    /// emitted, so the renderer synthesizes a start exactly once per block.
    pub(crate) started_blocks: HashSet<u64>,
    /// Whether a given canonical block index is a tool-call block (vs. text),
    /// consulted when closing/flushing it.
    pub(crate) block_is_tool: HashMap<u64, bool>,
    /// Per-tool-call render state keyed by canonical block index — the slot
    /// index assigned in the target dialect plus accumulating id/name/args.
    pub(crate) tool_render: HashMap<u64, StreamToolRender>,
    /// The next target-dialect tool-call slot index to assign.
    pub(crate) next_tool_slot: u64,
    /// Whether the target's terminal (Anthropic `message_stop`, OpenAI
    /// `[DONE]`) has been emitted, so it's emitted at most once.
    pub(crate) terminal_sent: bool,
}

impl StreamRenderState {
    /// The captured role, or [`Role::Assistant`] if a
    /// [`CanonicalStreamEvent::MessageStart`] never set one.
    pub(crate) fn role_or_assistant(&self) -> Role {
        self.role.unwrap_or(Role::Assistant)
    }
}

/// Flatten a canonical message's content blocks to plain text, exactly
/// matching today's provider adapters' `flatten_content`
/// ([`super::anthropic::flatten_content`], [`super::openai::flatten_content`]):
/// only `Text` blocks contribute, joined with `"\n"`; non-text blocks
/// (`ToolUse`, `ToolResult`, `Image`) are skipped.
fn flatten_content(blocks: &[ContentBlock]) -> String {
    blocks
        .iter()
        .filter_map(|block| match block {
            ContentBlock::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Project a [`CanonicalRequest`] down to the lossy [`super::Llm`] view.
/// `provider` is supplied by the caller rather than read off `req` — unlike
/// `model`, the provider isn't part of the canonical request shape; it's
/// known from which adapter parsed the wire body in the first place, the
/// same way [`super::Llm::provider`] is populated today. `facts` is always
/// `None` here for the same reason it's `None` right after adapter parsing
/// today: resolving them requires a registry lookup a projection function
/// doesn't have access to (see [`super::resolve_facts`], called separately
/// once the model is known).
///
/// `messages` is provider-specific in one respect: for `provider ==
/// "openai"`, a present `req.system` is re-expressed as a leading `role:
/// "system"` [`Message`]. This reproduces the pre-M15 behavior, where the
/// OpenAI adapter built `Llm.messages` directly off the wire `messages`
/// array (system message included, since OpenAI carries system in-band as
/// `role: "system"`), before [`super::openai`] started folding it out into
/// the canonical `system` field. Anthropic and Google never carried system
/// in their message list on the wire (Anthropic's `system` is a top-level
/// field, Google's `systemInstruction` a top-level object), so their
/// projections never had a system message here either, and this function
/// doesn't inject one for them. The canonical model itself stays clean —
/// system lives only in `req.system`; this is purely a read-side
/// reconstruction for the legacy per-provider `Llm` shape. Known edge case:
/// if a caller ever produces a `CanonicalRequest` with more than one
/// (historically only possible pre-fold, e.g. hand-built for a test) or a
/// non-leading OpenAI system message, they've already been folded/joined
/// into the single `req.system` string by the adapter, so they collapse to
/// one leading message here — acceptable, matching the adapter's own
/// "join more than one system message" behavior.
pub fn project_llm(req: &CanonicalRequest, provider: &str) -> Llm {
    let mut messages: Vec<Message> = req
        .messages
        .iter()
        .map(|m| Message {
            role: m.role.as_str().to_string(),
            content: flatten_content(&m.content),
        })
        .collect();

    if provider == "openai" {
        if let Some(system) = &req.system {
            messages.insert(
                0,
                Message {
                    role: Role::System.as_str().to_string(),
                    content: system.clone(),
                },
            );
        }
    }

    Llm {
        provider: provider.to_string(),
        model: req.model.clone(),
        messages,
        tools: req
            .tools
            .iter()
            .map(|t| Tool {
                name: t.name.clone(),
            })
            .collect(),
        max_tokens: req.sampling.max_tokens,
        stream: req.stream,
        facts: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_ctx() -> RequestCtx {
        RequestCtx {
            path: "/v1/messages".to_string(),
            method: "POST".to_string(),
        }
    }

    // -- serde round trips -------------------------------------------------

    #[test]
    fn role_round_trips_and_matches_message_role_strings() {
        for (role, expected) in [
            (Role::System, "system"),
            (Role::User, "user"),
            (Role::Assistant, "assistant"),
            (Role::Tool, "tool"),
        ] {
            assert_eq!(role.as_str(), expected);
            let json = serde_json::to_string(&role).unwrap();
            assert_eq!(json, format!("\"{expected}\""));
            let back: Role = serde_json::from_str(&json).unwrap();
            assert_eq!(role, back);
        }
    }

    #[test]
    fn content_block_text_round_trips() {
        let block = ContentBlock::Text {
            text: "hello".to_string(),
        };
        let json = serde_json::to_string(&block).unwrap();
        let back: ContentBlock = serde_json::from_str(&json).unwrap();
        assert_eq!(block, back);
    }

    #[test]
    fn content_block_tool_use_round_trips() {
        let block = ContentBlock::ToolUse {
            id: "call_1".to_string(),
            name: "get_weather".to_string(),
            input: serde_json::json!({"location": "NYC"}),
        };
        let json = serde_json::to_string(&block).unwrap();
        let back: ContentBlock = serde_json::from_str(&json).unwrap();
        assert_eq!(block, back);
    }

    #[test]
    fn content_block_tool_result_round_trips() {
        let block = ContentBlock::ToolResult {
            id: "call_1".to_string(),
            content: "72F and sunny".to_string(),
            is_error: false,
        };
        let json = serde_json::to_string(&block).unwrap();
        let back: ContentBlock = serde_json::from_str(&json).unwrap();
        assert_eq!(block, back);
    }

    #[test]
    fn content_block_image_round_trips() {
        let block = ContentBlock::Image {
            media_type: "image/png".to_string(),
            data: "base64data".to_string(),
        };
        let json = serde_json::to_string(&block).unwrap();
        let back: ContentBlock = serde_json::from_str(&json).unwrap();
        assert_eq!(block, back);
    }

    #[test]
    fn canonical_message_round_trips() {
        let msg = CanonicalMessage {
            role: Role::User,
            content: vec![ContentBlock::Text {
                text: "hi".to_string(),
            }],
        };
        let json = serde_json::to_string(&msg).unwrap();
        let back: CanonicalMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(msg, back);
    }

    #[test]
    fn canonical_tool_round_trips() {
        let tool = CanonicalTool {
            name: "get_weather".to_string(),
            description: Some("Look up the weather".to_string()),
            input_schema: serde_json::json!({"type": "object"}),
        };
        let json = serde_json::to_string(&tool).unwrap();
        let back: CanonicalTool = serde_json::from_str(&json).unwrap();
        assert_eq!(tool, back);
    }

    #[test]
    fn tool_choice_variants_round_trip() {
        for choice in [
            ToolChoice::Auto,
            ToolChoice::Any,
            ToolChoice::None,
            ToolChoice::Tool("get_weather".to_string()),
        ] {
            let json = serde_json::to_string(&choice).unwrap();
            let back: ToolChoice = serde_json::from_str(&json).unwrap();
            assert_eq!(choice, back);
        }
    }

    #[test]
    fn stop_reason_variants_round_trip() {
        for reason in [
            StopReason::EndTurn,
            StopReason::MaxTokens,
            StopReason::ToolUse,
            StopReason::StopSequence,
            StopReason::Other("safety".to_string()),
        ] {
            let json = serde_json::to_string(&reason).unwrap();
            let back: StopReason = serde_json::from_str(&json).unwrap();
            assert_eq!(reason, back);
        }
    }

    #[test]
    fn sampling_round_trips() {
        let sampling = Sampling {
            temperature: Some(0.7),
            top_p: Some(0.9),
            top_k: Some(40),
            max_tokens: Some(1024),
            stop: vec!["STOP".to_string()],
        };
        let json = serde_json::to_string(&sampling).unwrap();
        let back: Sampling = serde_json::from_str(&json).unwrap();
        assert_eq!(sampling, back);
    }

    #[test]
    fn sampling_default_is_all_empty() {
        let sampling = Sampling::default();
        assert_eq!(sampling.temperature, None);
        assert_eq!(sampling.top_p, None);
        assert_eq!(sampling.top_k, None);
        assert_eq!(sampling.max_tokens, None);
        assert!(sampling.stop.is_empty());
    }

    #[test]
    fn usage_round_trips() {
        let usage = Usage {
            input_tokens: Some(10),
            output_tokens: Some(20),
        };
        let json = serde_json::to_string(&usage).unwrap();
        let back: Usage = serde_json::from_str(&json).unwrap();
        assert_eq!(usage, back);
    }

    #[test]
    fn dropped_field_round_trips() {
        let field = DroppedField {
            path: "tool_choice".to_string(),
            reason: "provider has no equivalent".to_string(),
        };
        let json = serde_json::to_string(&field).unwrap();
        let back: DroppedField = serde_json::from_str(&json).unwrap();
        assert_eq!(field, back);
    }

    #[test]
    fn translation_report_round_trips() {
        let mut report = TranslationReport::default();
        report.drop("tool_choice", "provider has no equivalent");
        report
            .notes
            .push("downgraded tool_choice: any to auto".to_string());
        let json = serde_json::to_string(&report).unwrap();
        let back: TranslationReport = serde_json::from_str(&json).unwrap();
        assert_eq!(report, back);
    }

    #[test]
    fn request_ctx_round_trips() {
        let ctx = sample_ctx();
        let json = serde_json::to_string(&ctx).unwrap();
        let back: RequestCtx = serde_json::from_str(&json).unwrap();
        assert_eq!(ctx, back);
    }

    #[test]
    fn canonical_request_round_trips() {
        let req = CanonicalRequest {
            model: "claude-opus-4-1-20250805".to_string(),
            messages: vec![CanonicalMessage {
                role: Role::User,
                content: vec![ContentBlock::Text {
                    text: "hi".to_string(),
                }],
            }],
            tools: vec![CanonicalTool {
                name: "get_weather".to_string(),
                description: None,
                input_schema: Value::Null,
            }],
            tool_choice: Some(ToolChoice::Auto),
            sampling: Sampling::default(),
            stream: false,
            system: None,
            extra: Default::default(),
        };
        let json = serde_json::to_string(&req).unwrap();
        let back: CanonicalRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(req, back);
    }

    /// `system` and `extra` are the two fields this task adds to
    /// [`CanonicalRequest`]/[`CanonicalResponse`]: `system` is the canonical
    /// home for a system prompt regardless of which wire shape it came in
    /// on, and `extra` is the losslessness escape valve for
    /// provider-specific top-level fields with no other home. Both should
    /// round-trip when populated, and an empty `extra` (the common case,
    /// since most requests won't carry unknown fields) should serialize
    /// cleanly rather than littering the JSON with an empty object — same
    /// for an absent `system`.
    #[test]
    fn canonical_request_system_and_extra_round_trip() {
        let mut extra = Map::new();
        extra.insert("safety_settings".to_string(), serde_json::json!(["x"]));
        let req = CanonicalRequest {
            model: "claude-opus-4-1-20250805".to_string(),
            messages: vec![],
            tools: vec![],
            tool_choice: None,
            sampling: Sampling::default(),
            stream: false,
            system: Some("You are a helpful assistant.".to_string()),
            extra,
        };
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("\"system\":\"You are a helpful assistant.\""));
        assert!(json.contains("\"safety_settings\""));
        let back: CanonicalRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(req, back);
    }

    #[test]
    fn canonical_request_empty_system_and_extra_serialize_cleanly() {
        let req = CanonicalRequest {
            model: "m".to_string(),
            messages: vec![],
            tools: vec![],
            tool_choice: None,
            sampling: Sampling::default(),
            stream: false,
            system: None,
            extra: Default::default(),
        };
        let json = serde_json::to_string(&req).unwrap();
        assert!(!json.contains("\"system\""));
        assert!(!json.contains("\"extra\""));
        let back: CanonicalRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(req, back);
    }

    #[test]
    fn canonical_response_empty_extra_serializes_cleanly() {
        let resp = CanonicalResponse {
            model: "m".to_string(),
            content: vec![],
            stop_reason: None,
            usage: None,
            extra: Default::default(),
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(!json.contains("\"extra\""));
        let back: CanonicalResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(resp, back);
    }

    #[test]
    fn canonical_response_round_trips() {
        let resp = CanonicalResponse {
            model: "claude-opus-4-1-20250805".to_string(),
            content: vec![ContentBlock::Text {
                text: "hello there".to_string(),
            }],
            stop_reason: Some(StopReason::EndTurn),
            usage: Some(Usage {
                input_tokens: Some(10),
                output_tokens: Some(20),
            }),
            extra: Default::default(),
        };
        let json = serde_json::to_string(&resp).unwrap();
        let back: CanonicalResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(resp, back);
    }

    #[test]
    fn canonical_stream_event_variants_round_trip() {
        let events = vec![
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
            CanonicalStreamEvent::ContentBlockStop { index: 0 },
            CanonicalStreamEvent::MessageDelta {
                stop_reason: Some(StopReason::EndTurn),
                usage: Some(Usage {
                    input_tokens: None,
                    output_tokens: Some(20),
                }),
            },
            CanonicalStreamEvent::MessageStop,
        ];
        for event in events {
            let json = serde_json::to_string(&event).unwrap();
            let back: CanonicalStreamEvent = serde_json::from_str(&json).unwrap();
            assert_eq!(event, back);
        }
    }

    // -- project_llm ---------------------------------------------------------

    /// Oracle: mirrors `anthropic::tests::parses_block_array_content_flattening_text_blocks`
    /// — a user message with `[Text, Image, Text]` content flattens to
    /// `"first part\nsecond part"`, matching `anthropic::flatten_content` /
    /// `openai::flatten_content` exactly (non-text blocks skipped, `Text`
    /// blocks joined with `"\n"`).
    #[test]
    fn project_llm_flattens_mixed_content_blocks_like_existing_adapters() {
        let req = CanonicalRequest {
            model: "claude-opus-4-1-20250805".to_string(),
            messages: vec![CanonicalMessage {
                role: Role::User,
                content: vec![
                    ContentBlock::Text {
                        text: "first part".to_string(),
                    },
                    ContentBlock::Image {
                        media_type: "image/png".to_string(),
                        data: "...".to_string(),
                    },
                    ContentBlock::Text {
                        text: "second part".to_string(),
                    },
                ],
            }],
            tools: vec![CanonicalTool {
                name: "get_weather".to_string(),
                description: Some("...".to_string()),
                input_schema: Value::Null,
            }],
            tool_choice: None,
            sampling: Sampling {
                max_tokens: Some(1024),
                ..Sampling::default()
            },
            stream: true,
            system: None,
            extra: Default::default(),
        };

        let llm = project_llm(&req, "anthropic");

        assert_eq!(
            llm,
            Llm {
                provider: "anthropic".to_string(),
                model: "claude-opus-4-1-20250805".to_string(),
                messages: vec![Message {
                    role: "user".to_string(),
                    content: "first part\nsecond part".to_string(),
                }],
                tools: vec![Tool {
                    name: "get_weather".to_string(),
                }],
                max_tokens: Some(1024),
                stream: true,
                facts: None,
            }
        );
    }

    #[test]
    fn project_llm_maps_all_roles_to_lowercase_strings() {
        let req = CanonicalRequest {
            model: "m".to_string(),
            messages: vec![
                CanonicalMessage {
                    role: Role::System,
                    content: vec![],
                },
                CanonicalMessage {
                    role: Role::User,
                    content: vec![],
                },
                CanonicalMessage {
                    role: Role::Assistant,
                    content: vec![],
                },
                CanonicalMessage {
                    role: Role::Tool,
                    content: vec![],
                },
            ],
            tools: vec![],
            tool_choice: None,
            sampling: Sampling::default(),
            stream: false,
            system: None,
            extra: Default::default(),
        };

        let llm = project_llm(&req, "openai");
        let roles: Vec<&str> = llm.messages.iter().map(|m| m.role.as_str()).collect();
        assert_eq!(roles, vec!["system", "user", "assistant", "tool"]);
    }

    #[test]
    fn project_llm_empty_content_flattens_to_empty_string() {
        let req = CanonicalRequest {
            model: "m".to_string(),
            messages: vec![CanonicalMessage {
                role: Role::User,
                content: vec![],
            }],
            tools: vec![],
            tool_choice: None,
            sampling: Sampling::default(),
            stream: false,
            system: None,
            extra: Default::default(),
        };

        let llm = project_llm(&req, "google");
        assert_eq!(llm.messages[0].content, "");
    }

    #[test]
    fn project_llm_sets_provider_from_argument_not_request() {
        let req = CanonicalRequest {
            model: "m".to_string(),
            messages: vec![],
            tools: vec![],
            tool_choice: None,
            sampling: Sampling::default(),
            stream: false,
            system: None,
            extra: Default::default(),
        };
        assert_eq!(project_llm(&req, "anthropic").provider, "anthropic");
        assert_eq!(project_llm(&req, "openai").provider, "openai");
    }

    #[test]
    fn project_llm_facts_always_none() {
        let req = CanonicalRequest {
            model: "m".to_string(),
            messages: vec![],
            tools: vec![],
            tool_choice: None,
            sampling: Sampling::default(),
            stream: false,
            system: None,
            extra: Default::default(),
        };
        assert!(project_llm(&req, "anthropic").facts.is_none());
    }

    /// Pre-M15, the OpenAI adapter built `Llm.messages` straight from the
    /// wire `messages` array, including a leading `role: "system"` entry
    /// (Anthropic/Google never had this, since their system prompt travels
    /// out-of-band on the wire too). Task 2 moved OpenAI's system message
    /// out of `CanonicalRequest.messages` into `.system`, which silently
    /// dropped it from the projected `Llm` view — this test locks in the
    /// fix: for `provider == "openai"`, `project_llm` re-expresses
    /// `req.system` as a leading system message, reproducing the historical
    /// byte-identical shape.
    #[test]
    fn project_llm_openai_prepends_system_message_from_system_field() {
        let req = CanonicalRequest {
            model: "gpt-4o".to_string(),
            messages: vec![CanonicalMessage {
                role: Role::User,
                content: vec![ContentBlock::Text {
                    text: "hi".to_string(),
                }],
            }],
            tools: vec![],
            tool_choice: None,
            sampling: Sampling::default(),
            stream: false,
            system: Some("You are helpful.".to_string()),
            extra: Default::default(),
        };

        let llm = project_llm(&req, "openai");

        assert_eq!(llm.messages.len(), 2);
        assert_eq!(llm.messages[0].role, "system");
        assert_eq!(llm.messages[0].content, "You are helpful.");
        assert_eq!(llm.messages[1].role, "user");
        assert_eq!(llm.messages[1].content, "hi");
    }

    /// Same `system`-populated request shape, but for `provider ==
    /// "anthropic"` — historically Anthropic never carried system in
    /// `messages` (it's a separate top-level wire field), so `project_llm`
    /// must not inject it there either.
    #[test]
    fn project_llm_anthropic_does_not_prepend_system_message() {
        let req = CanonicalRequest {
            model: "claude-opus-4-1-20250805".to_string(),
            messages: vec![CanonicalMessage {
                role: Role::User,
                content: vec![ContentBlock::Text {
                    text: "hi".to_string(),
                }],
            }],
            tools: vec![],
            tool_choice: None,
            sampling: Sampling::default(),
            stream: false,
            system: Some("You are helpful.".to_string()),
            extra: Default::default(),
        };

        let llm = project_llm(&req, "anthropic");

        assert_eq!(llm.messages.len(), 1);
        assert_eq!(llm.messages[0].role, "user");
    }

    /// Same for `provider == "google"` — Gemini's `systemInstruction` is
    /// also out-of-band, never folded into `contents`/`messages`.
    #[test]
    fn project_llm_google_does_not_prepend_system_message() {
        let req = CanonicalRequest {
            model: "gemini-1.5-pro".to_string(),
            messages: vec![],
            tools: vec![],
            tool_choice: None,
            sampling: Sampling::default(),
            stream: false,
            system: Some("You are helpful.".to_string()),
            extra: Default::default(),
        };

        let llm = project_llm(&req, "google");

        assert!(llm.messages.is_empty());
    }

    /// No `system` field set at all: OpenAI projection must not inject a
    /// phantom system message.
    #[test]
    fn project_llm_openai_no_system_field_no_prepend() {
        let req = CanonicalRequest {
            model: "gpt-4o".to_string(),
            messages: vec![CanonicalMessage {
                role: Role::User,
                content: vec![],
            }],
            tools: vec![],
            tool_choice: None,
            sampling: Sampling::default(),
            stream: false,
            system: None,
            extra: Default::default(),
        };

        let llm = project_llm(&req, "openai");

        assert_eq!(llm.messages.len(), 1);
        assert_eq!(llm.messages[0].role, "user");
    }

    #[test]
    fn project_llm_max_tokens_comes_from_sampling() {
        let mut req = CanonicalRequest {
            model: "m".to_string(),
            messages: vec![],
            tools: vec![],
            tool_choice: None,
            sampling: Sampling::default(),
            stream: false,
            system: None,
            extra: Default::default(),
        };
        assert_eq!(project_llm(&req, "anthropic").max_tokens, None);
        req.sampling.max_tokens = Some(512);
        assert_eq!(project_llm(&req, "anthropic").max_tokens, Some(512));
    }
}
