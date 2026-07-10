//! Cost estimation from resolved model facts (design doc M9 Task 4).
//!
//! `registry::ModelFacts::cost_input`/`cost_output` are seeded straight from
//! models.dev, whose `cost` block is priced **per 1,000,000 tokens** (see
//! `registry/seed/providers/anthropic/claude-opus-4-1-20250805.toml`, whose
//! `cost.input = 15.0` / `cost.output = 75.0` are asserted verbatim as
//! dollars-per-million-tokens by `registry::tests::
//! embedded_contains_seeded_anthropic_model_with_resolved_facts`). This
//! module's math matches that unit: multiply a token count by
//! `price / 1_000_000`, not `price` directly.
//!
//! This is a library helper, not something the gateway runs on the request
//! path — a step (e.g. a budget check) calls it with the token counts it
//! computed (see [`super::tokenize`]) and the `llm.facts` the gateway
//! already attached to its envelope.

use crate::registry::ModelFacts;

/// Estimate the dollar cost of a request from resolved model facts and
/// token counts: `input_tokens/1e6 * cost_input + output_tokens/1e6 *
/// cost_output`, per the per-1M-token pricing convention documented at the
/// module level. Returns `None` if either price is absent from `facts` —
/// callers (e.g. a fail-closed budget step) get an unambiguous "can't be
/// priced" signal rather than a cost estimate that silently ignores one
/// side's price.
pub fn estimate_cost(facts: &ModelFacts, input_tokens: u64, output_tokens: u64) -> Option<f64> {
    let cost_input = facts.cost_input?;
    let cost_output = facts.cost_output?;
    let input_cost = (input_tokens as f64 / 1_000_000.0) * cost_input;
    let output_cost = (output_tokens as f64 / 1_000_000.0) * cost_output;
    Some(input_cost + output_cost)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn facts(cost_input: Option<f64>, cost_output: Option<f64>) -> ModelFacts {
        ModelFacts {
            id: "test-model".to_string(),
            provider: "test-provider".to_string(),
            context: Some(100_000),
            max_output: Some(4096),
            cost_input,
            cost_output,
            modalities: vec!["text".to_string()],
            tool_call: true,
            status: Some("stable".to_string()),
        }
    }

    #[test]
    fn estimate_cost_with_known_prices() {
        // $15/M input, $75/M output (Claude Opus seed prices) — 1M input
        // tokens plus 1M output tokens should cost exactly $15 + $75 = $90.
        let f = facts(Some(15.0), Some(75.0));
        let cost = estimate_cost(&f, 1_000_000, 1_000_000).expect("both prices present");
        assert!((cost - 90.0).abs() < 1e-9, "got {cost}");
    }

    #[test]
    fn estimate_cost_scales_with_partial_million() {
        let f = facts(Some(15.0), Some(75.0));
        // 500k input tokens -> half of $15 = $7.50; 200k output tokens ->
        // a fifth of $75 = $15.00; total $22.50.
        let cost = estimate_cost(&f, 500_000, 200_000).expect("both prices present");
        assert!((cost - 22.5).abs() < 1e-9, "got {cost}");
    }

    #[test]
    fn estimate_cost_zero_tokens_is_zero() {
        let f = facts(Some(15.0), Some(75.0));
        let cost = estimate_cost(&f, 0, 0).expect("both prices present");
        assert_eq!(cost, 0.0);
    }

    #[test]
    fn estimate_cost_none_when_input_price_absent() {
        let f = facts(None, Some(75.0));
        assert!(estimate_cost(&f, 100, 100).is_none());
    }

    #[test]
    fn estimate_cost_none_when_output_price_absent() {
        let f = facts(Some(15.0), None);
        assert!(estimate_cost(&f, 100, 100).is_none());
    }

    #[test]
    fn estimate_cost_none_when_both_prices_absent() {
        let f = facts(None, None);
        assert!(estimate_cost(&f, 100, 100).is_none());
    }
}
