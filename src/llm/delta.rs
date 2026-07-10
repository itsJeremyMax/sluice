//! Normalized streaming delta: the provider-agnostic shape one SSE event
//! parses into (design doc M10 Task 2). Each of the three built-in
//! [`super::adapter::Adapter`] impls translates its own provider's
//! streaming wire format into this shape via `parse_delta`, so downstream
//! steps (buffering, mutation, cost accounting) can reason about a stream
//! without knowing which provider produced it.

use serde::{Deserialize, Serialize};

/// What kind of information a [`Delta`] carries. Exactly one of `text`,
/// `tool_call`, `usage`, `finish` is expected to be populated to match
/// `kind`; `Other` carries none of them (an event this gateway recognized
/// as belonging to the stream but doesn't need to normalize further, e.g.
/// an Anthropic `content_block_start`/`content_block_stop` housekeeping
/// event).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeltaKind {
    Text,
    ToolCall,
    Usage,
    Finish,
    Other,
}

/// A single normalized streaming delta.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Delta {
    pub kind: DeltaKind,
    pub text: Option<String>,
    pub tool_call: Option<ToolCallDelta>,
    pub usage: Option<Usage>,
    pub finish: Option<Finish>,
}

impl Delta {
    /// A `Text`-kind delta carrying `text`.
    pub fn text(text: impl Into<String>) -> Self {
        Self {
            kind: DeltaKind::Text,
            text: Some(text.into()),
            tool_call: None,
            usage: None,
            finish: None,
        }
    }

    /// A `ToolCall`-kind delta carrying `tool_call`.
    pub fn tool_call(tool_call: ToolCallDelta) -> Self {
        Self {
            kind: DeltaKind::ToolCall,
            text: None,
            tool_call: Some(tool_call),
            usage: None,
            finish: None,
        }
    }

    /// A `Usage`-kind delta carrying `usage`.
    pub fn usage(usage: Usage) -> Self {
        Self {
            kind: DeltaKind::Usage,
            text: None,
            tool_call: None,
            usage: Some(usage),
            finish: None,
        }
    }

    /// A `Finish`-kind delta carrying `finish`.
    pub fn finish(finish: Finish) -> Self {
        Self {
            kind: DeltaKind::Finish,
            text: None,
            tool_call: None,
            usage: None,
            finish: Some(finish),
        }
    }
}

/// A fragment of a streaming tool call. Providers stream tool call
/// arguments incrementally as raw JSON text fragments (`arguments_fragment`)
/// that the consumer concatenates and parses once the call completes; `id`
/// and `name` are typically only present on the first fragment of a given
/// call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolCallDelta {
    pub id: Option<String>,
    pub name: Option<String>,
    pub arguments_fragment: Option<String>,
}

/// Token accounting reported mid- or end-of-stream. Fields are optional
/// because providers report them at different points (e.g. OpenAI's
/// `prompt_tokens` only appears on the final usage-bearing chunk).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Usage {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
}

/// The stream's terminal reason, if the provider supplied one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Finish {
    pub reason: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delta_round_trips_through_json() {
        let delta = Delta::text("hello");
        let json = serde_json::to_string(&delta).unwrap();
        let back: Delta = serde_json::from_str(&json).unwrap();
        assert_eq!(delta, back);
    }

    #[test]
    fn usage_delta_round_trips_through_json() {
        let delta = Delta::usage(Usage {
            input_tokens: Some(10),
            output_tokens: Some(20),
        });
        let json = serde_json::to_string(&delta).unwrap();
        let back: Delta = serde_json::from_str(&json).unwrap();
        assert_eq!(delta, back);
    }

    #[test]
    fn tool_call_delta_round_trips_through_json() {
        let delta = Delta::tool_call(ToolCallDelta {
            id: Some("call_1".to_string()),
            name: Some("get_weather".to_string()),
            arguments_fragment: Some(r#"{"loc":"#.to_string()),
        });
        let json = serde_json::to_string(&delta).unwrap();
        let back: Delta = serde_json::from_str(&json).unwrap();
        assert_eq!(delta, back);
    }

    #[test]
    fn finish_delta_round_trips_through_json() {
        let delta = Delta::finish(Finish {
            reason: Some("stop".to_string()),
        });
        let json = serde_json::to_string(&delta).unwrap();
        let back: Delta = serde_json::from_str(&json).unwrap();
        assert_eq!(delta, back);
    }
}
