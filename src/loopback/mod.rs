//! Continuation registry (design doc §4.5, M12 Task 2): tracks in-flight
//! loopback park/resume chains.
//!
//! When `proxy::run_from` reaches a `mode = "loopback"` `url` step, it does
//! not run the step like an ordinary transform — it signs a [`token::ChainToken`]
//! naming where the chain should resume, registers a [`Continuation`] here
//! under a fresh `cid`, fires a POST carrying that token to the step's tool
//! (without waiting for the tool's own response), and parks the current
//! handler on the continuation's `oneshot` receiver. The callback (M12 Task
//! 3) verifies the token the tool hands back, looks the continuation up by
//! `cid` via [`Registry::take`], resumes the chain from `resume_index`, and
//! feeds the resulting client response back through `tx` — which is what
//! actually unparks the original handler.
//!
//! A continuation whose tool never calls back is cleaned up one of two
//! ways: the parked handler's own park timeout (see `proxy::dispatch_loopback`)
//! gives up and removes its own entry, or — for a park that itself never got
//! the chance to run that cleanup (e.g. the whole process is under memory
//! pressure from many abandoned chains) — a periodic [`Registry::sweep`]
//! drops anything older than its TTL. Either path drops the continuation's
//! `tx`, which fails the parked `oneshot::Receiver` on the other end.

pub mod token;

use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use hyper::Response;
use tokio::sync::oneshot;

use crate::config::Config;
use crate::reconstruct::ResponseBody;

/// Everything a parked `run_from` invocation needs preserved while its
/// chain is suspended waiting on a loopback tool's callback.
pub struct Continuation {
    /// Fed the eventual client response once the callback resumes the
    /// chain. Dropping this (without ever calling `send`) is itself a
    /// meaningful outcome: it fails the parked receiver, which is exactly
    /// what should happen to a continuation that expires or is discarded
    /// unresumed.
    pub tx: oneshot::Sender<Response<ResponseBody>>,
    /// When this continuation was registered. `sweep` measures age from
    /// this monotonic instant, never from any wall-clock/token field.
    pub created: Instant,
    pub route_id: String,
    /// Step index the resumed chain should run FROM — the loopback step's
    /// own index + 1 (see `proxy::run_from`, `proxy::dispatch_loopback`).
    /// This is the value `proxy::handle_callback` actually drives control
    /// flow from. The signed token handed back by the callback carries the
    /// same number as a `ChainToken::resume_index` claim, but that claim
    /// exists only so the token's MAC can authenticate the callback (prove
    /// it names a chain this gateway actually parked) — server-held state
    /// here, not client-supplied claims, is what decides where the chain
    /// resumes, in case a bug (or, defense in depth, some future signing
    /// mistake) ever let the two diverge.
    pub resume_index: usize,
    /// Hop count the resumed chain should carry forward if it dispatches
    /// another `mode = "loopback"` step — the loopback step's own `next_hop`
    /// at dispatch time. Same server-held-over-token-claim rationale as
    /// [`resume_index`](Self::resume_index): the signed token's `hop` claim
    /// authenticates the callback, but this field is what actually drives
    /// the `max_hops` guard on any subsequent hop.
    pub hop: u32,
    /// The `context` map accumulated by this chain's own `on_request` steps
    /// up through (and not including) the loopback step itself, captured at
    /// dispatch time. `proxy::handle_callback` resumes `run_from` with a
    /// clone of this, not a fresh empty map — otherwise every `set_context`
    /// write made before the park would be silently lost to the steps (and
    /// any `on_response` step) that run after it.
    pub context: serde_json::Map<String, serde_json::Value>,
    /// The parked request's own `HttpMsg::path` at the moment it dispatched
    /// (post any earlier steps' `set_path` mutations) — NOT the same thing
    /// as the callback HTTP request's own path (which is always just
    /// `config.gateway.callback_path`, a transport detail of how the tool
    /// happens to call back into the gateway). The callback resumes the
    /// logical request at this path — used to recompute the upstream URL and
    /// shown to any later steps' envelopes — while method/headers/body come
    /// from whatever the callback request itself actually carried (the
    /// tool-mutated values). See `proxy::handle_callback`.
    pub path: String,
    pub correlation_id: String,
    /// The config snapshot the parked request itself observed. Resuming
    /// must never pick up a config swap that landed while the chain was
    /// parked — see `proxy::ProxyState::config`'s own doc comment for why
    /// every request pins a single snapshot for its whole lifetime.
    pub config: Arc<Config>,
}

/// Registry of in-flight continuations, keyed by the `cid` embedded in the
/// signed `x-chain-token`. A thin wrapper over `DashMap` so the registry can
/// be shared (`Arc<Registry>`, see `proxy::ProxyState`) across every
/// concurrent request with no external locking.
#[derive(Default)]
pub struct Registry {
    map: DashMap<String, Continuation>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a continuation under `cid`, unless the registry already
    /// holds `max_parked` or more entries — in which case nothing is
    /// inserted and this returns `false`, so the caller
    /// (`proxy::dispatch_loopback`) can refuse to park (answering 503)
    /// instead of growing this map without bound. Without a cap, a client
    /// that keeps opening new chains against a `mode = "loopback"` step
    /// whose tool never calls back could park an unbounded number of
    /// continuations — each holding an `oneshot::Sender`, a cloned `Config`,
    /// and an accumulated `context` map — for as long as `sweep`'s TTL
    /// allows one to live.
    ///
    /// If `cid` already names an entry (not expected in practice — `cid` is
    /// a fresh UUID per dispatch), the previous entry is replaced (and hence
    /// dropped, failing whatever was parked on it) — the cap check only
    /// gates genuinely NEW entries, not this practically-never-hit replace
    /// case, so it never rejects a re-registration of an existing `cid`.
    ///
    /// This check-then-insert is not atomic against concurrent `register`
    /// calls (`DashMap` gives per-shard locking, not a single global one) —
    /// under a race, a handful of registrations beyond `max_parked` can
    /// land before the length is next observed as over cap. That's an
    /// accepted, benign soft cap (same "extra work rather than a
    /// correctness issue" tradeoff `proxy::get_or_compile_wasm`'s own doc
    /// comment makes for its cache-insert race), not a hard limit.
    pub fn register(&self, cid: String, continuation: Continuation, max_parked: usize) -> bool {
        // The cap only ever gates a genuinely NEW `cid`: an existing `cid`
        // being replaced doesn't grow the map, so it must never be blocked
        // by a cap that's already at its limit (see this method's doc
        // comment).
        if !self.map.contains_key(&cid) && self.map.len() >= max_parked {
            return false;
        }
        self.map.insert(cid, continuation);
        true
    }

    /// Remove and return the continuation registered under `cid`, if any.
    /// One-shot by construction: a second `take` for the same `cid` (a
    /// replayed or duplicated callback) finds nothing.
    pub fn take(&self, cid: &str) -> Option<Continuation> {
        self.map.remove(cid).map(|(_, continuation)| continuation)
    }

    /// Drop every continuation registered longer than `ttl` ago. Dropping a
    /// `Continuation` drops its `tx`, which fails the parked receiver on the
    /// other end — see this module's doc comment.
    pub fn sweep(&self, ttl: Duration) {
        let now = Instant::now();
        self.map
            .retain(|_, continuation| now.duration_since(continuation.created) < ttl);
    }

    /// Number of continuations currently registered.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A cap high enough to never actually bind in any test below that
    /// isn't specifically exercising the cap itself — lets most tests call
    /// `register` without having to think about `max_parked` at all.
    const UNBOUNDED: usize = usize::MAX;

    fn continuation(route_id: &str) -> (Continuation, oneshot::Receiver<Response<ResponseBody>>) {
        let (tx, rx) = oneshot::channel();
        let mut context = serde_json::Map::new();
        context.insert("writer".to_string(), serde_json::json!({"seen": true}));
        let cont = Continuation {
            tx,
            created: Instant::now(),
            route_id: route_id.to_string(),
            resume_index: 2,
            hop: 1,
            context,
            path: "/claude/v1/messages".to_string(),
            correlation_id: "corr-1".to_string(),
            config: Arc::new(Config {
                gateway: crate::config::Gateway::default(),
                routes: Vec::new(),
                gateways: Vec::new(),
            }),
        };
        (cont, rx)
    }

    #[test]
    fn new_registry_is_empty() {
        let registry = Registry::new();
        assert!(registry.is_empty());
        assert_eq!(registry.len(), 0);
    }

    #[test]
    fn register_then_take_returns_the_same_continuation_and_removes_it() {
        let registry = Registry::new();
        let (cont, _rx) = continuation("claude");
        assert!(registry.register("cid-1".to_string(), cont, UNBOUNDED));
        assert_eq!(registry.len(), 1);

        let taken = registry
            .take("cid-1")
            .expect("just-registered continuation");
        assert_eq!(taken.route_id, "claude");
        assert_eq!(taken.resume_index, 2);
        assert_eq!(taken.hop, 1);
        assert_eq!(
            taken.context.get("writer"),
            Some(&serde_json::json!({"seen": true}))
        );
        assert_eq!(taken.correlation_id, "corr-1");
        assert!(registry.is_empty());
    }

    #[test]
    fn take_on_unknown_cid_returns_none() {
        let registry = Registry::new();
        assert!(registry.take("does-not-exist").is_none());
    }

    #[test]
    fn take_is_one_shot_a_second_take_returns_none() {
        let registry = Registry::new();
        let (cont, _rx) = continuation("claude");
        registry.register("cid-1".to_string(), cont, UNBOUNDED);

        assert!(registry.take("cid-1").is_some());
        assert!(
            registry.take("cid-1").is_none(),
            "a second take for the same cid must find nothing"
        );
    }

    #[tokio::test]
    async fn sweep_drops_expired_entries_and_fails_the_parked_receiver() {
        let registry = Registry::new();
        let (cont, rx) = continuation("claude");
        registry.register("cid-1".to_string(), cont, UNBOUNDED);

        tokio::time::sleep(Duration::from_millis(20)).await;
        registry.sweep(Duration::from_millis(5));

        assert!(
            registry.is_empty(),
            "an entry older than the ttl must be swept"
        );
        assert!(
            rx.await.is_err(),
            "dropping the continuation's tx must fail the parked receiver"
        );
    }

    #[tokio::test]
    async fn sweep_keeps_entries_younger_than_the_ttl() {
        let registry = Registry::new();
        let (cont, _rx) = continuation("claude");
        registry.register("cid-1".to_string(), cont, UNBOUNDED);

        registry.sweep(Duration::from_secs(60));

        assert_eq!(
            registry.len(),
            1,
            "a fresh continuation must survive a sweep with a generous ttl"
        );
    }

    #[test]
    fn register_replacing_an_existing_cid_drops_the_previous_continuation() {
        let registry = Registry::new();
        let (cont_a, mut rx_a) = continuation("claude");
        let (cont_b, _rx_b) = continuation("openai");
        registry.register("cid-1".to_string(), cont_a, UNBOUNDED);
        registry.register("cid-1".to_string(), cont_b, UNBOUNDED);

        assert_eq!(registry.len(), 1);
        let taken = registry.take("cid-1").unwrap();
        assert_eq!(taken.route_id, "openai");
        assert!(
            rx_a.try_recv().is_err(),
            "the replaced continuation's tx must have been dropped"
        );
    }

    /// Fix D (M14 final review, loopback DoS): once the registry already
    /// holds `max_parked` entries, a further `register` for a genuinely NEW
    /// `cid` must be refused (returns `false`, inserts nothing) rather than
    /// growing the map past the configured cap.
    #[test]
    fn register_refuses_beyond_max_parked_cap() {
        let registry = Registry::new();
        let (cont_a, _rx_a) = continuation("claude");
        let (cont_b, _rx_b) = continuation("claude");
        let (cont_c, _rx_c) = continuation("claude");

        assert!(registry.register("cid-1".to_string(), cont_a, 2));
        assert!(registry.register("cid-2".to_string(), cont_b, 2));
        assert_eq!(registry.len(), 2);

        assert!(
            !registry.register("cid-3".to_string(), cont_c, 2),
            "a third registration must be refused once max_parked (2) is already held"
        );
        assert_eq!(
            registry.len(),
            2,
            "a refused registration must not have inserted anything"
        );
        assert!(
            registry.take("cid-3").is_none(),
            "the refused cid must not be findable at all"
        );
    }

    /// A `register` call that REPLACES an existing `cid` (see this struct's
    /// own doc comment: not expected in practice, `cid` is a fresh UUID per
    /// dispatch, but supported) must not be blocked by the cap — the
    /// registry's size doesn't grow in that case, so there's nothing for the
    /// cap to protect against.
    #[test]
    fn register_replacing_existing_cid_is_not_blocked_by_cap_already_at_limit() {
        let registry = Registry::new();
        let (cont_a, _rx_a) = continuation("claude");
        let (cont_b, _rx_b) = continuation("openai");

        assert!(registry.register("cid-1".to_string(), cont_a, 1));
        assert_eq!(registry.len(), 1);

        assert!(
            registry.register("cid-1".to_string(), cont_b, 1),
            "replacing the same cid at an already-at-cap registry must still succeed"
        );
        assert_eq!(registry.len(), 1);
        assert_eq!(registry.take("cid-1").unwrap().route_id, "openai");
    }
}
