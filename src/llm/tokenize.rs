//! Approximate token counting for the `llm` view (design doc M9 Task 4).
//!
//! This is a heuristic, not a real tokenizer: it exists so a step (e.g. a
//! budget check) can get an order-of-magnitude token estimate from the
//! provider-agnostic `llm` view without the gateway shipping (or the step
//! having to vendor) a real BPE tokenizer. Every count this module returns
//! is tagged [`Confidence::Approx`] — M9 has no exact counting path for any
//! provider. Exact counting is a later refinement: OpenAI models can be
//! counted exactly with `tiktoken`, and Anthropic exposes a network
//! `count_tokens` endpoint; both would report [`Confidence::Exact`] once
//! wired up. Until then, callers must not treat these counts as billing-grade.

use super::Message;

/// How trustworthy a token count is. M9 only ever produces [`Approx`](Confidence::Approx) —
/// see the module doc comment for what an exact count would require.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Confidence {
    /// A provider-exact count (e.g. `tiktoken`, Anthropic's network
    /// `count_tokens`). Not produced by anything in M9.
    Exact,
    /// A heuristic estimate. What every M9 count carries.
    Approx,
}

/// Estimate the token count of a single piece of text for a `(provider,
/// model)` pair. `provider`/`model` are accepted so a future exact tokenizer
/// can dispatch on them (e.g. `tiktoken` encoding choice); the M9 heuristic
/// ignores them and applies the same ~4-characters-per-token estimate to
/// every provider and model.
///
/// Always returns [`Confidence::Approx`]. The estimate is
/// `ceil(chars / 4)`, rounding up so any non-empty text counts as at least
/// one token; monotonically non-decreasing in the input's character count.
pub fn count_tokens(_provider: &str, _model: &str, text: &str) -> (u64, Confidence) {
    let chars = text.chars().count() as u64;
    let tokens = chars.div_ceil(4);
    (tokens, Confidence::Approx)
}

/// Estimate the token count of a whole conversation for a `(provider,
/// model)` pair, summing [`count_tokens`] over every message's content.
/// Role names and message structure/overhead are not counted — only
/// `content` text, per the same heuristic as `count_tokens`. Always returns
/// [`Confidence::Approx`] (an empty slice sums to `(0, Approx)`).
pub fn count_messages(provider: &str, model: &str, messages: &[Message]) -> (u64, Confidence) {
    let total = messages
        .iter()
        .map(|m| count_tokens(provider, model, &m.content).0)
        .sum();
    (total, Confidence::Approx)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn count_tokens_returns_approx() {
        let (_, confidence) = count_tokens("anthropic", "claude-opus-4-1-20250805", "hello world");
        assert_eq!(confidence, Confidence::Approx);
    }

    #[test]
    fn count_tokens_empty_text_is_zero() {
        let (tokens, _) = count_tokens("anthropic", "claude-opus-4-1-20250805", "");
        assert_eq!(tokens, 0);
    }

    #[test]
    fn count_tokens_is_monotonic_in_text_length() {
        let short = "hi";
        let medium = "hi there friend";
        let long = "hi there friend, this is a much longer message with many more words in it";

        let (short_count, _) = count_tokens("openai", "gpt-5", short);
        let (medium_count, _) = count_tokens("openai", "gpt-5", medium);
        let (long_count, _) = count_tokens("openai", "gpt-5", long);

        assert!(short_count <= medium_count);
        assert!(medium_count <= long_count);
        assert!(short_count < long_count);
    }

    #[test]
    fn count_tokens_grows_with_every_added_character() {
        // A stronger monotonicity check than the coarse short/medium/long
        // buckets above: appending characters one at a time must never
        // decrease the estimate.
        let mut text = String::new();
        let mut prev = 0;
        for _ in 0..40 {
            text.push('x');
            let (count, _) = count_tokens("google", "gemini-2.5-pro", &text);
            assert!(count >= prev, "token count decreased as text grew");
            prev = count;
        }
    }

    #[test]
    fn count_messages_sums_over_all_message_contents() {
        let messages = vec![
            Message {
                role: "user".to_string(),
                content: "hello".to_string(), // 5 chars -> ceil(5/4) = 2
            },
            Message {
                role: "assistant".to_string(),
                content: "hi there".to_string(), // 8 chars -> ceil(8/4) = 2
            },
        ];
        let (total, confidence) =
            count_messages("anthropic", "claude-opus-4-1-20250805", &messages);
        assert_eq!(total, 4);
        assert_eq!(confidence, Confidence::Approx);
    }

    #[test]
    fn count_messages_on_empty_slice_is_zero() {
        let (total, confidence) = count_messages("anthropic", "claude-opus-4-1-20250805", &[]);
        assert_eq!(total, 0);
        assert_eq!(confidence, Confidence::Approx);
    }
}
