//! The `llm` view: a provider-agnostic representation of the LLM-shaped
//! portion of a request, threaded through the envelope alongside the raw
//! `HttpMsg` (design doc M9). As of M15 Task 2, an [`adapter::Adapter`]
//! parses the wire body into the lossless [`canonical::CanonicalRequest`]
//! IR, and [`canonical::project_llm`] projects that down to this lossy `Llm`
//! shape, which is then enriched with [`registry::ModelFacts`] via
//! [`resolve_facts`] before being attached to the envelope — the gateway's
//! internal spine for translating between provider dialects
//! (Anthropic `/v1/messages`, OpenAI `/v1/chat/completions`, Google
//! `generateContent`, see [`anthropic::AnthropicAdapter`],
//! [`openai::OpenAiAdapter`], [`google::GoogleAdapter`]) is the canonical IR
//! itself, not this view. Approximate token counting ([`tokenize`]) and cost
//! estimation from resolved facts ([`cost`]) are library helpers a step can
//! use against this view (Task 4). Normalized streaming-delta parsing
//! ([`delta`]) of [`crate::sse::SseEvent`]s lands in `Adapter::parse_delta`
//! (Task 2 of M10).

pub mod adapter;
pub mod anthropic;
pub mod canonical;
pub mod cost;
pub mod delta;
pub mod google;
pub mod openai;
pub mod tokenize;

pub use canonical::{
    project_llm, CanonicalMessage, CanonicalRequest, CanonicalResponse, CanonicalStreamEvent,
    CanonicalTool, ContentBlock, DroppedField, RequestCtx, Role, Sampling, StopReason,
    StreamParseState, StreamRenderState, ToolChoice, TranslationReport, Usage as CanonicalUsage,
};

use serde::{Deserialize, Serialize};

use crate::registry::{self, ModelFacts};

/// A single chat message. `content` is flattened to plain text for M9;
/// structured (multi-block) content is a later refinement.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Message {
    pub role: String,
    pub content: String,
}

/// A tool available to the model. Minimal for M9 — just the name.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Tool {
    pub name: String,
}

/// Provider-agnostic view of an LLM request, attached to the envelope
/// alongside the raw `HttpMsg` so steps can reason about the request
/// without re-parsing provider-specific wire formats themselves.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Llm {
    pub provider: String,
    pub model: String,
    pub messages: Vec<Message>,
    pub tools: Vec<Tool>,
    pub max_tokens: Option<u64>,
    pub stream: bool,
    pub facts: Option<ModelFacts>,
}

/// Look up a model's resolved facts by `(provider, model)`, cloning them out
/// of the registry. Thin wrapper over [`registry::Registry::get`]; unknown
/// model → `None`, never an error (same contract as `Registry::get`).
pub fn resolve_facts(reg: &registry::Registry, provider: &str, model: &str) -> Option<ModelFacts> {
    reg.get(provider, model).cloned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_facts_returns_seeded_models_facts() {
        let reg = registry::Registry::embedded();
        let facts = resolve_facts(&reg, "anthropic", "claude-opus-4-1-20250805")
            .expect("seeded anthropic model must resolve");
        assert_eq!(facts.provider, "anthropic");
        assert_eq!(facts.id, "claude-opus-4-1-20250805");
    }

    #[test]
    fn resolve_facts_returns_none_for_unknown_model() {
        let reg = registry::Registry::embedded();
        assert!(resolve_facts(&reg, "anthropic", "no-such-model").is_none());
    }
}
