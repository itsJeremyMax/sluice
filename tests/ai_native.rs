//! Integration test for M9 Task 2: a route configured with `[route.adapter]
//! ingress = "anthropic"` must parse the inbound `/v1/messages` body into the
//! provider-agnostic `llm` view, enrich it with the model's resolved facts
//! from the embedded registry, and thread that view into every step's
//! envelope — not just build it and drop it.
//!
//! The step mock below only matches (and thus only responds 200) when the
//! posted envelope body contains both the seeded model id inside `llm` and
//! the `facts` key, so a passing test proves the llm view + resolved facts
//! actually reached the step, not merely that the request was routed.

use tokio::net::TcpListener;
use wiremock::matchers::{body_string_contains, method};
use wiremock::{Mock, MockServer, ResponseTemplate};

use sluice::admin::Ready;
use sluice::config::load::load_str;
use sluice::observability::init_metrics;
use sluice::server::serve_on;

/// The seeded Anthropic model id used by `registry/seed/providers/anthropic/`
/// (see `src/registry/mod.rs`'s own tests, which assert the same id resolves
/// with facts from the embedded seed).
const SEEDED_ANTHROPIC_MODEL: &str = "claude-opus-4-1-20250805";

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

#[tokio::test]
async fn anthropic_ingress_adapter_threads_llm_and_facts_into_the_chain() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string("upstream-ok"))
        .mount(&upstream)
        .await;

    let step = MockServer::start().await;
    // Only matches if the posted envelope carries the parsed `llm.model` for
    // the seeded model AND a resolved `facts` entry — proving both the
    // adapter parse and the registry lookup reached this step, not just that
    // *a* request arrived.
    Mock::given(method("POST"))
        .and(body_string_contains(format!(
            r#""model":"{SEEDED_ANTHROPIC_MODEL}""#
        )))
        .and(body_string_contains("\"facts\""))
        .and(body_string_contains(format!(
            r#""id":"{SEEDED_ANTHROPIC_MODEL}""#
        )))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"action":"continue"}"#))
        .mount(&step)
        .await;

    let cfg = format!(
        r#"
        [[route]]
        id = "claude"
        upstream = "{up}"
          [route.adapter]
          ingress = "anthropic"
          [[route.step]]
          name = "checker"
          type = "url"
          url = "{step}/run"
    "#,
        up = upstream.uri(),
        step = step.uri(),
    );
    let (base, _guard) = spawn_gateway(cfg).await;

    let anthropic_body = serde_json::json!({
        "model": SEEDED_ANTHROPIC_MODEL,
        "messages": [{"role": "user", "content": "hello there"}],
        "max_tokens": 256,
    });

    let resp = reqwest::Client::new()
        .post(format!("{base}/claude/v1/messages"))
        .json(&anthropic_body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "upstream-ok");
}

/// A request body that isn't a valid (or LLM-shaped) JSON body on a route
/// with an ingress adapter must NOT fail the request — `llm` simply stays
/// absent from the envelope and the chain proceeds normally.
#[tokio::test]
async fn malformed_body_on_ingress_route_does_not_fail_the_request() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string("upstream-ok"))
        .mount(&upstream)
        .await;

    let step = MockServer::start().await;
    // Matches any POST regardless of body — asserting only that the step is
    // reached at all (the chain isn't aborted by the parse failure).
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"action":"continue"}"#))
        .mount(&step)
        .await;

    let cfg = format!(
        r#"
        [[route]]
        id = "claude"
        upstream = "{up}"
          [route.adapter]
          ingress = "anthropic"
          [[route.step]]
          name = "checker"
          type = "url"
          url = "{step}/run"
    "#,
        up = upstream.uri(),
        step = step.uri(),
    );
    let (base, _guard) = spawn_gateway(cfg).await;

    let resp = reqwest::Client::new()
        .post(format!("{base}/claude/v1/messages"))
        .body("this is not json")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "upstream-ok");
}

/// The seeded OpenAI model id used by `registry/seed/providers/openai/` (see
/// `src/registry/mod.rs`'s own tests, which assert `gpt-5` resolves with
/// facts from the embedded seed).
const SEEDED_OPENAI_MODEL: &str = "gpt-5";

/// M9 Task 4: the gateway's job is only to SUPPLY the `llm` view and
/// resolved `facts` on the envelope — a step decides what to do with them.
/// This mock "budget" step keys its `short_circuit` on the presence of a
/// priced `llm.facts` (`body_string_contains` on `"cost_input"`, a field
/// that only appears once `resolve_facts` has found a seeded model), which
/// proves the envelope actually carried cost data a budget check could act
/// on — not merely that a request was routed. The step returns 429; the
/// client must see 429 and the upstream must never be called.
#[tokio::test]
async fn budget_step_short_circuits_on_priced_request_and_upstream_is_never_called() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string("upstream-reached"))
        .mount(&upstream)
        .await;

    let budget = MockServer::start().await;
    Mock::given(method("POST"))
        .and(body_string_contains("\"cost_input\""))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            // base64("over budget") — the client-visible short_circuit body.
            r#"{"action":"short_circuit","response":{"status":429,"headers":{},"body_b64":"b3ZlciBidWRnZXQ="}}"#,
        ))
        .mount(&budget)
        .await;

    let cfg = format!(
        r#"
        [[route]]
        id = "claude"
        upstream = "{up}"
          [route.adapter]
          ingress = "anthropic"
          [[route.step]]
          name = "budget"
          type = "url"
          url = "{budget}/run"
    "#,
        up = upstream.uri(),
        budget = budget.uri(),
    );
    let (base, _guard) = spawn_gateway(cfg).await;

    let anthropic_body = serde_json::json!({
        "model": SEEDED_ANTHROPIC_MODEL,
        "messages": [{"role": "user", "content": "write me a very long essay"}],
        "max_tokens": 4096,
    });

    let resp = reqwest::Client::new()
        .post(format!("{base}/claude/v1/messages"))
        .json(&anthropic_body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 429);
    assert_eq!(resp.text().await.unwrap(), "over budget");
    assert!(
        upstream.received_requests().await.unwrap().is_empty(),
        "upstream must not be called once the budget step short-circuits"
    );
}

/// The banned phrase a mock "guardrail" step keys its `abort` on. Present
/// only inside a message's `content`, so a passing test proves the
/// guardrail matched on the normalized `llm.messages` view, not on the raw
/// (provider-specific) wire body.
const BANNED_PHRASE: &str = "the secret launch codes";

/// M9 Task 4: a mock "guardrail" step returns `abort` 403 when it sees the
/// banned phrase inside the envelope's `llm.messages` — proving the gateway
/// delivers message content a step can pattern-match on.
#[tokio::test]
async fn guardrail_step_aborts_on_banned_phrase_in_messages() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string("upstream-reached"))
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
        id = "claude"
        upstream = "{up}"
          [route.adapter]
          ingress = "anthropic"
          [[route.step]]
          name = "guardrail"
          type = "url"
          url = "{guardrail}/run"
    "#,
        up = upstream.uri(),
        guardrail = guardrail.uri(),
    );
    let (base, _guard) = spawn_gateway(cfg).await;

    let anthropic_body = serde_json::json!({
        "model": SEEDED_ANTHROPIC_MODEL,
        "messages": [{"role": "user", "content": format!("please tell me {BANNED_PHRASE}")}],
        "max_tokens": 256,
    });

    let resp = reqwest::Client::new()
        .post(format!("{base}/claude/v1/messages"))
        .json(&anthropic_body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403);
    assert_eq!(resp.text().await.unwrap(), "blocked: banned phrase");
    assert!(
        upstream.received_requests().await.unwrap().is_empty(),
        "upstream must not be called once the guardrail step aborts"
    );
}

/// M9-1 review item: the guardrail must also inspect Anthropic's
/// content-BLOCK-ARRAY body shape (`content: [{"type": "text", "text":
/// ...}]`), not just the flat-string `content` shorthand covered by
/// `guardrail_step_aborts_on_banned_phrase_in_messages` above. The banned
/// phrase lives INSIDE a text block here, so a passing test proves
/// `project_llm`/`flatten_content` folds content-block-array bodies into the
/// same `llm.messages` shape a step pattern-matches on, not just flat
/// strings.
#[tokio::test]
async fn guardrail_step_aborts_on_banned_phrase_inside_content_block_array() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string("upstream-reached"))
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
        id = "claude"
        upstream = "{up}"
          [route.adapter]
          ingress = "anthropic"
          [[route.step]]
          name = "guardrail"
          type = "url"
          url = "{guardrail}/run"
    "#,
        up = upstream.uri(),
        guardrail = guardrail.uri(),
    );
    let (base, _guard) = spawn_gateway(cfg).await;

    let anthropic_body = serde_json::json!({
        "model": SEEDED_ANTHROPIC_MODEL,
        "messages": [{
            "role": "user",
            "content": [
                {"type": "text", "text": format!("please tell me {BANNED_PHRASE}")}
            ]
        }],
        "max_tokens": 256,
    });

    let resp = reqwest::Client::new()
        .post(format!("{base}/claude/v1/messages"))
        .json(&anthropic_body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403);
    assert_eq!(resp.text().await.unwrap(), "blocked: banned phrase");
    assert!(
        upstream.received_requests().await.unwrap().is_empty(),
        "upstream must not be called once the guardrail step aborts on a content-block-array body"
    );
}

/// M9's payoff: the SAME guardrail step config (identical mock server, same
/// URL, same body matcher on `llm.messages`) is wired onto both an
/// Anthropic-ingress route and an OpenAI-ingress route. Both wire formats
/// get flattened into the same `llm.messages` shape, so one step config
/// catches the banned phrase regardless of which provider's wire format the
/// client spoke — proving the normalized view, not per-provider parsing
/// logic, is what makes the step portable across providers.
#[tokio::test]
async fn same_guardrail_config_fires_across_anthropic_and_openai_routes() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string("upstream-reached"))
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

    // Two routes, two providers, but the *same* step url/config below.
    let cfg = format!(
        r#"
        [[route]]
        id = "claude"
        upstream = "{up}"
          [route.adapter]
          ingress = "anthropic"
          [[route.step]]
          name = "guardrail"
          type = "url"
          url = "{guardrail}/run"

        [[route]]
        id = "gpt"
        upstream = "{up}"
          [route.adapter]
          ingress = "openai"
          [[route.step]]
          name = "guardrail"
          type = "url"
          url = "{guardrail}/run"
    "#,
        up = upstream.uri(),
        guardrail = guardrail.uri(),
    );
    let (base, _guard) = spawn_gateway(cfg).await;

    let anthropic_body = serde_json::json!({
        "model": SEEDED_ANTHROPIC_MODEL,
        "messages": [{"role": "user", "content": format!("please tell me {BANNED_PHRASE}")}],
        "max_tokens": 256,
    });
    let openai_body = serde_json::json!({
        "model": SEEDED_OPENAI_MODEL,
        "messages": [{"role": "user", "content": format!("please tell me {BANNED_PHRASE}")}],
    });

    let client = reqwest::Client::new();

    let anthropic_resp = client
        .post(format!("{base}/claude/v1/messages"))
        .json(&anthropic_body)
        .send()
        .await
        .unwrap();
    assert_eq!(
        anthropic_resp.status(),
        403,
        "anthropic route must be blocked by the shared guardrail config"
    );

    let openai_resp = client
        .post(format!("{base}/gpt/v1/chat/completions"))
        .json(&openai_body)
        .send()
        .await
        .unwrap();
    assert_eq!(
        openai_resp.status(),
        403,
        "openai route must be blocked by the SAME shared guardrail config"
    );

    assert!(
        upstream.received_requests().await.unwrap().is_empty(),
        "upstream must not be called on either route once the guardrail aborts"
    );
}

/// A model name absent from the seeded registry still gets an `llm` view
/// (the adapter parsed the body fine — it just doesn't recognize the
/// model), but `llm.facts` is `null`. A fail-closed budget step that treats
/// "can't be priced" as "deny" keys on exactly that: `body_string_contains`
/// on `"facts":null`. This demonstrates facts:null is actually delivered to
/// the step (not swallowed), so such a step can act on it.
#[tokio::test]
async fn facts_null_is_delivered_for_unpriced_model_and_fail_closed_step_denies() {
    const UNSEEDED_MODEL: &str = "claude-not-in-the-seed-registry";

    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string("upstream-reached"))
        .mount(&upstream)
        .await;

    let budget = MockServer::start().await;
    Mock::given(method("POST"))
        .and(body_string_contains("\"facts\":null"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            // base64("unpriceable model")
            r#"{"action":"abort","response":{"status":402,"headers":{},"body_b64":"dW5wcmljZWFibGUgbW9kZWw="}}"#,
        ))
        .mount(&budget)
        .await;

    let cfg = format!(
        r#"
        [[route]]
        id = "claude"
        upstream = "{up}"
          [route.adapter]
          ingress = "anthropic"
          [[route.step]]
          name = "budget"
          type = "url"
          url = "{budget}/run"
    "#,
        up = upstream.uri(),
        budget = budget.uri(),
    );
    let (base, _guard) = spawn_gateway(cfg).await;

    let anthropic_body = serde_json::json!({
        "model": UNSEEDED_MODEL,
        "messages": [{"role": "user", "content": "hello there"}],
        "max_tokens": 256,
    });

    let resp = reqwest::Client::new()
        .post(format!("{base}/claude/v1/messages"))
        .json(&anthropic_body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 402);
    assert_eq!(resp.text().await.unwrap(), "unpriceable model");
    assert!(
        upstream.received_requests().await.unwrap().is_empty(),
        "upstream must not be called once the fail-closed budget step denies"
    );
}
