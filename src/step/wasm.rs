use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use wasmtime::{Config, Engine, Instance, Module, Store, StoreLimitsBuilder, Trap};

use crate::directive::Directive;
use crate::envelope::Envelope;
use crate::step::StepError;

/// How often the shared per-engine epoch ticker (see [`WasmStep::new`])
/// increments the `Engine`'s epoch. Each call's timeout is translated into a
/// number of ticks (`ceil(timeout / EPOCH_TICK)`, at least 1) via
/// `Store::set_epoch_deadline`, so this constant is the granularity of every
/// call's timeout, not a timeout itself: a 10ms tick bounds a 5s configured
/// timeout to within ~10ms, which is plenty tight for gateway-scale step
/// timeouts while keeping the ticker thread's wakeup rate low.
const EPOCH_TICK: Duration = Duration::from_millis(10);

/// Cap on a single guest linear memory's growth (wired via `Store::limiter`
/// with a `wasmtime::StoreLimits` in `invoke`): a guest calling `memory.grow`
/// in a loop (accidentally or maliciously) can grow at most this far before
/// `memory.grow` starts returning failure to the guest, instead of being
/// able to run the host out of memory.
const MAX_GUEST_MEMORY_BYTES: usize = 64 * 1024 * 1024;

/// A `type = "wasm"` step: a compiled wasmtime [`Module`] the gateway calls
/// per invocation over a small ptr/len ABI (see [`WasmStep::run`]), the wasm
/// equivalent of [`crate::step::url::UrlTransform`] /
/// [`crate::step::script::ScriptOneshot`].
///
/// The guest module is expected to export:
/// - `memory`: the linear memory the host writes the envelope into and
///   reads the directive back out of.
/// - `alloc(size: i32) -> i32`: returns a guest pointer to at least `size`
///   bytes of scratch space the host may write into.
/// - `run(ptr: i32, len: i32) -> i64`: given the pointer/length of the
///   envelope JSON the host just wrote, returns a packed
///   `(out_ptr as i64) << 32 | (out_len as i64)` describing where the
///   guest wrote its directive JSON response.
///
/// `Engine`/`Module` are compiled once in [`WasmStep::from_path`] (module
/// compilation is the expensive part) and are cheap to clone (both are
/// `Arc`-backed and `Send + Sync`) — [`WasmStep::run`] clones them into a
/// `spawn_blocking` closure so the actually-sync wasmtime call doesn't block
/// the async runtime.
///
/// Deliberately holds NO baked-in timeout: the module cache in
/// `proxy::get_or_compile_wasm` is keyed by path only and shared across every
/// step config that happens to reference that same `.wasm` file, so a
/// timeout can't be baked in at compile time without silently applying
/// whichever step's config first compiled that path to every other step
/// sharing it. Instead each [`WasmStep::run`] call takes its own `timeout`,
/// supplied per call by `proxy::run_step` from that step's own
/// `timeout_ms`.
pub struct WasmStep {
    pub engine: Engine,
    pub module: Module,
    /// Set by `Drop` to signal the ticker thread (spawned in `new`) to stop;
    /// checked by that thread once per tick.
    ticker_stop: Arc<AtomicBool>,
    /// `Some` until `Drop` runs; joined there so a dropped `WasmStep` never
    /// leaves its ticker thread running.
    ticker_handle: Option<std::thread::JoinHandle<()>>,
}

impl WasmStep {
    /// Compile the wasm module at `path` once, with epoch interruption
    /// enabled so a runaway guest can be bounded (see `run`), and start this
    /// step's dedicated epoch ticker thread.
    pub fn from_path(path: &Path) -> Result<Self, StepError> {
        let mut config = Config::new();
        config.epoch_interruption(true);
        // Use POSIX signal handlers rather than a process-wide Mach exception
        // port for trap handling on macOS. The Mach-port handler is a single
        // process-wide resource; constructing engines concurrently (many
        // parallel test threads, or concurrent wasm-cache misses under load)
        // races its install/teardown and can abort the process (SIGABRT). The
        // signal-based path is per-thread safe and behaves identically for our
        // epoch-interruption use. No effect off macOS.
        config.macos_use_mach_ports(false);
        let engine = Engine::new(&config)
            .map_err(|e| StepError::WasmAbi(format!("failed to create wasm engine: {e}")))?;
        let module = Module::from_file(&engine, path).map_err(|e| {
            StepError::WasmAbi(format!(
                "failed to compile wasm module '{}': {e}",
                path.display()
            ))
        })?;
        Ok(Self::new(engine, module))
    }

    /// Test-only constructor for an already-compiled module (avoids every
    /// test having to round-trip a module through a temp file). `engine`
    /// must have been built with `Config::epoch_interruption(true)` for the
    /// timeout test to actually bound a runaway guest.
    #[cfg(test)]
    pub fn from_module(engine: Engine, module: Module) -> Self {
        Self::new(engine, module)
    }

    /// Spawn the ONE dedicated background thread this `WasmStep`/`Engine`
    /// lives with: it does nothing but call `engine.increment_epoch()` every
    /// [`EPOCH_TICK`] for as long as this `WasmStep` is alive, then exits
    /// once `Drop` flips `ticker_stop`.
    ///
    /// This is the fix for the cross-contamination bug the old design had:
    /// previously, every call shared one `Engine` but ALSO drove
    /// `increment_epoch()` itself, from its own `tokio::time::timeout`
    /// branch, on ITS OWN timeout firing. Since every concurrent `Store` on
    /// that engine has its deadline set to the SAME "one tick past
    /// creation", any one call's timeout incrementing the shared epoch
    /// tripped every other concurrent call's deadline too, trapping
    /// unrelated in-flight requests that hadn't timed out at all.
    ///
    /// With a single continuous ticker instead, the engine's epoch just
    /// advances at a constant rate, independent of any individual call. Each
    /// call sets its OWN deadline as a tick count relative to the engine's
    /// epoch *at the moment that call's `Store` is created*
    /// (`Store::set_epoch_deadline` is documented as relative-to-current,
    /// not absolute), so two concurrent calls with different — or the same —
    /// timeouts each trap purely based on how many ticks elapse during THEIR
    /// OWN execution, never because of what any other call did.
    fn new(engine: Engine, module: Module) -> Self {
        let ticker_stop = Arc::new(AtomicBool::new(false));
        let thread_stop = ticker_stop.clone();
        let thread_engine = engine.clone();
        let ticker_handle = std::thread::Builder::new()
            .name("wasm-epoch-ticker".to_string())
            .spawn(move || loop {
                std::thread::sleep(EPOCH_TICK);
                if thread_stop.load(Ordering::Relaxed) {
                    break;
                }
                thread_engine.increment_epoch();
            })
            .expect("failed to spawn wasm epoch ticker thread");
        Self {
            engine,
            module,
            ticker_stop,
            ticker_handle: Some(ticker_handle),
        }
    }

    /// Serialize `env` to JSON, hand it to the guest over the alloc/run ABI,
    /// and parse the guest's response bytes as a [`Directive`].
    ///
    /// Bounded by `timeout`, supplied by the caller (`proxy::run_step`) from
    /// the step's OWN configured `timeout_ms` — NOT baked into this
    /// `WasmStep`, since the module cache this is stored in is shared by
    /// path across every step config referencing that path (see this
    /// struct's doc comment).
    ///
    /// The actual bound comes from wasmtime's epoch-interruption trap, not
    /// from the `spawn_blocking` join itself: `invoke` sets this call's
    /// `Store` epoch deadline to `ceil(timeout / EPOCH_TICK)` ticks beyond
    /// the engine's epoch at `Store` creation, and the shared ticker thread
    /// (started once in `new`, ticking for this `WasmStep`'s whole lifetime)
    /// advances that epoch at a constant rate regardless of what any other
    /// concurrent call is doing. Once enough ticks elapse, wasmtime traps
    /// the guest call from the inside with `Trap::Interrupt`, which `invoke`
    /// maps to `StepError::WasmTimeout` — so the `spawn_blocking` future
    /// resolves on its own; there is no separate task racing a timer against
    /// it and no manual `increment_epoch()` call on any "this looks timed
    /// out" branch here, which is exactly what let one call's timeout trap
    /// every other concurrent call in the old design.
    pub async fn run(&self, env: &Envelope, timeout: Duration) -> Result<Directive, StepError> {
        let bytes = serde_json::to_vec(env)?;
        let engine = self.engine.clone();
        let module = self.module.clone();
        let deadline_ticks = epoch_ticks_for(timeout);

        tokio::task::spawn_blocking(move || Self::invoke(&engine, &module, &bytes, deadline_ticks))
            .await
            .map_err(|join_err| {
                StepError::WasmAbi(format!("wasm task did not complete: {join_err}"))
            })?
    }

    /// The synchronous guest call: instantiate a fresh `Store` (with a
    /// per-call epoch deadline and a memory-growth limiter), write the input
    /// bytes into guest-allocated memory, call `run`, and read the directive
    /// bytes back out. Runs entirely inside `spawn_blocking` (see `run`
    /// above) since every wasmtime call here is blocking.
    fn invoke(
        engine: &Engine,
        module: &Module,
        input: &[u8],
        deadline_ticks: u64,
    ) -> Result<Directive, StepError> {
        let limits = StoreLimitsBuilder::new()
            .memory_size(MAX_GUEST_MEMORY_BYTES)
            .build();
        let mut store = Store::new(engine, limits);
        store.limiter(|limits| limits);
        // Relative to the engine's epoch AT THIS MOMENT (Store::set_epoch_deadline
        // is documented as "ticks beyond current", not an absolute epoch) —
        // this call traps once the shared ticker (running independently,
        // see `WasmStep::new`) has advanced the engine's epoch this many
        // times since NOW, regardless of what any other concurrent call on
        // this same engine is doing.
        store.set_epoch_deadline(deadline_ticks);

        let instance = Instance::new(&mut store, module, &[])
            .map_err(|e| classify_trap(e, "instantiation"))?;

        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| StepError::WasmAbi("guest module does not export 'memory'".into()))?;

        let alloc = instance
            .get_typed_func::<i32, i32>(&mut store, "alloc")
            .map_err(|e| {
                StepError::WasmAbi(format!("guest module missing 'alloc(i32) -> i32': {e}"))
            })?;

        let run_fn = instance
            .get_typed_func::<(i32, i32), i64>(&mut store, "run")
            .map_err(|e| {
                StepError::WasmAbi(format!("guest module missing 'run(i32, i32) -> i64': {e}"))
            })?;

        let input_len = i32::try_from(input.len())
            .map_err(|_| StepError::WasmAbi("envelope too large for wasm i32 ABI".into()))?;

        let in_ptr = alloc
            .call(&mut store, input_len)
            .map_err(|e| classify_trap(e, "alloc"))?;

        // `Memory::write` is itself bounds-checked against the guest's
        // current `data_size` and returns an `Err` rather than writing (or
        // allocating anything unbounded) on an out-of-bounds `in_ptr` — no
        // separate clamp needed here the way `out_len` below needs one,
        // since `input.len()` is host-controlled (bounded by
        // `max_body_bytes`), not guest-controlled.
        memory
            .write(&mut store, in_ptr as usize, input)
            .map_err(|e| {
                StepError::WasmAbi(format!("failed writing envelope into guest memory: {e}"))
            })?;

        let packed = run_fn
            .call(&mut store, (in_ptr, input_len))
            .map_err(|e| classify_trap(e, "run"))?;

        let out_ptr = (packed >> 32) as u32 as usize;
        let out_len = (packed & 0xFFFF_FFFF) as u32 as usize;

        // `out_ptr`/`out_len` are entirely guest-controlled (unpacked from
        // whatever `run` returned) — `out_len` alone can be up to ~4GiB.
        // Validate BOTH fit within the guest's actual current memory before
        // allocating `out_buf`, so a malicious/buggy guest returning a huge
        // or out-of-bounds (ptr, len) gets a clean `WasmAbi` error instead of
        // the host attempting a multi-gigabyte allocation (or panicking on
        // an out-of-bounds `memory.read`).
        let mem_size = memory.data_size(&store);
        let out_end = out_ptr.checked_add(out_len);
        if !matches!(out_end, Some(end) if end <= mem_size) {
            return Err(StepError::WasmAbi(format!(
                "guest 'run' returned out-of-bounds output (ptr={out_ptr}, len={out_len}, memory size={mem_size} bytes)"
            )));
        }

        let mut out_buf = vec![0u8; out_len];
        memory.read(&store, out_ptr, &mut out_buf).map_err(|e| {
            StepError::WasmAbi(format!("failed reading directive from guest memory: {e}"))
        })?;

        let directive: Directive = serde_json::from_slice(&out_buf)?;
        Ok(directive)
    }
}

impl Drop for WasmStep {
    /// Stop this `WasmStep`'s dedicated ticker thread. The thread checks
    /// `ticker_stop` once per `EPOCH_TICK`, so this join blocks for at most
    /// one tick — a small, bounded cost paid once per `WasmStep` (i.e. once
    /// per distinct wasm module path, since the proxy's module cache holds
    /// one long-lived `Arc<WasmStep>` per path) rather than per call.
    fn drop(&mut self) {
        self.ticker_stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.ticker_handle.take() {
            let _ = handle.join();
        }
    }
}

/// Translate a wall-clock `timeout` into a tick count for
/// `Store::set_epoch_deadline`: at least 1 (a deadline of 0 traps
/// immediately — see wasmtime's own doc comment on `set_epoch_deadline`),
/// otherwise the number of `EPOCH_TICK`-sized ticks needed to cover
/// `timeout`, rounded up so a call's actual bound is never shorter than the
/// timeout it was given (only ever up to one tick longer).
fn epoch_ticks_for(timeout: Duration) -> u64 {
    let tick_ms = EPOCH_TICK.as_millis().max(1);
    let timeout_ms = timeout.as_millis();
    (timeout_ms.div_ceil(tick_ms)).max(1) as u64
}

/// Map a wasmtime call error to the right [`StepError`]: an epoch-deadline
/// trap (`Trap::Interrupt`) means this call's own timeout elapsed, so it
/// becomes `WasmTimeout`; anything else is a genuine guest trap/fault.
fn classify_trap(err: wasmtime::Error, what: &str) -> StepError {
    if matches!(err.downcast_ref::<Trap>(), Some(Trap::Interrupt)) {
        StepError::WasmTimeout
    } else {
        StepError::WasmTrap(format!("'{what}' trapped: {err}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope::build_request_envelope;
    use crate::http_msg::HttpMsg;
    use std::collections::BTreeMap;
    use std::time::Instant;

    fn msg() -> HttpMsg {
        HttpMsg {
            method: "POST".into(),
            path: "/v1/messages".into(),
            headers: BTreeMap::new(),
            body_b64: String::new(),
        }
    }

    fn env() -> Envelope {
        build_request_envelope(
            "claude",
            "wasm-step",
            &msg(),
            &serde_json::Map::new(),
            "corr-test",
            None,
        )
    }

    fn epoch_engine() -> Engine {
        let mut config = Config::new();
        config.epoch_interruption(true);
        // Match `from_path`: signal-based traps, not the process-wide macOS
        // Mach exception port, so parallel test engines cannot race it into a
        // SIGABRT at construction/teardown.
        config.macos_use_mach_ports(false);
        Engine::new(&config).expect("engine construction")
    }

    /// Escape raw bytes as a WAT string-literal body using `\XX` hex
    /// escapes for every byte, so we never have to think about which
    /// characters WAT's text-format string syntax requires escaping (`"`,
    /// `\`, control bytes, ...) — every byte is spelled unambiguously.
    fn wat_escape(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("\\{b:02x}")).collect()
    }

    /// A module implementing the alloc/run ABI: `alloc` is a trivial bump
    /// allocator over a global starting past the data segment holding the
    /// canned directive JSON, and `run` ignores its input entirely and
    /// always returns the packed ptr/len of that canned JSON.
    fn continue_module_wat() -> String {
        let payload =
            br#"{"action":"continue","ops":[{"op":"set_header","name":"x-wasm","value":"seen"}]}"#;
        let data_offset: i64 = 8;
        let bump_start = data_offset + payload.len() as i64;
        let packed: i64 = (data_offset << 32) | (payload.len() as i64);
        format!(
            r#"(module
  (memory (export "memory") 1)
  (data (i32.const {data_offset}) "{escaped}")
  (global $bump (mut i32) (i32.const {bump_start}))
  (func (export "alloc") (param $size i32) (result i32)
    (local $ret i32)
    global.get $bump
    local.set $ret
    global.get $bump
    local.get $size
    i32.add
    global.set $bump
    local.get $ret)
  (func (export "run") (param $ptr i32) (param $len i32) (result i64)
    i64.const {packed}))
"#,
            escaped = wat_escape(payload),
        )
    }

    /// A module whose `run` never returns — used to exercise the epoch
    /// timeout path. `alloc` is a stub (fixed offset) since `run` ignores
    /// its input anyway.
    fn infinite_loop_module_wat() -> &'static str {
        r#"(module
  (memory (export "memory") 1)
  (func (export "alloc") (param $size i32) (result i32)
    i32.const 0)
  (func (export "run") (param $ptr i32) (param $len i32) (result i64)
    (loop $forever
      br $forever)
    i64.const 0))
"#
    }

    /// A module whose `run` returns a packed (ptr, len) describing an
    /// out-of-bounds region well past its one-page (64KiB) memory — used to
    /// exercise the `out_len`/`out_ptr` clamp. `alloc` is a stub since `run`
    /// ignores its input.
    fn out_of_bounds_module_wat() -> String {
        // ptr = 0, len = 0x1000_0000 (256MiB) — comfortably past the
        // module's single 64KiB page, and comfortably short of overflowing
        // the packed i64 or the u32 truncation `invoke` applies.
        let len: i64 = 0x1000_0000;
        let packed: i64 = len; // (0i64 << 32) | len
        format!(
            r#"(module
  (memory (export "memory") 1)
  (func (export "alloc") (param $size i32) (result i32)
    i32.const 0)
  (func (export "run") (param $ptr i32) (param $len i32) (result i64)
    i64.const {packed}))
"#
        )
    }

    /// A module whose `run` behavior depends on the LENGTH of its input:
    /// below `threshold` bytes it returns a canned "continue" directive
    /// immediately; at or above `threshold` it loops forever. Used by
    /// `concurrent_calls_have_independent_timeouts` to make one SINGLE
    /// `WasmStep` (one `Module`, one `Engine`) behave differently for two
    /// concurrently in-flight calls, purely based on which envelope each
    /// call happens to pass — exactly the shape of two different concurrent
    /// requests hitting the same path-keyed cache entry in
    /// `proxy::get_or_compile_wasm`.
    fn variable_length_module_wat(threshold: i32) -> String {
        let payload = br#"{"action":"continue","ops":[]}"#;
        let data_offset: i64 = 8;
        let bump_start = data_offset + payload.len() as i64;
        let packed: i64 = (data_offset << 32) | (payload.len() as i64);
        format!(
            r#"(module
  (memory (export "memory") 1)
  (data (i32.const {data_offset}) "{escaped}")
  (global $bump (mut i32) (i32.const {bump_start}))
  (func (export "alloc") (param $size i32) (result i32)
    (local $ret i32)
    global.get $bump
    local.set $ret
    global.get $bump
    local.get $size
    i32.add
    global.set $bump
    local.get $ret)
  (func (export "run") (param $ptr i32) (param $len i32) (result i64)
    (if (i32.ge_u (local.get $len) (i32.const {threshold}))
      (then
        (loop $forever
          br $forever)))
    i64.const {packed}))
"#,
            escaped = wat_escape(payload),
        )
    }

    /// Builds an envelope whose serialized JSON is at least `min_bytes`
    /// long, by giving it a padded correlation id — used to drive
    /// `variable_length_module_wat`'s input-length-dependent branch from the
    /// test side without needing any other ABI changes.
    fn env_with_min_len(min_bytes: usize) -> Envelope {
        let correlation_id = "x".repeat(min_bytes);
        build_request_envelope(
            "claude",
            "wasm-step",
            &msg(),
            &serde_json::Map::new(),
            &correlation_id,
            None,
        )
    }

    #[tokio::test]
    async fn run_returns_continue_directive_with_set_header_op() {
        let engine = epoch_engine();
        let module = Module::new(&engine, continue_module_wat()).expect("module compiles");
        let step = WasmStep::from_module(engine, module);

        let directive = step.run(&env(), Duration::from_millis(1000)).await.unwrap();
        match directive {
            Directive::Continue { ops } => {
                assert_eq!(ops.len(), 1);
                match &ops[0] {
                    crate::directive::Op::SetHeader { name, value } => {
                        assert_eq!(name, "x-wasm");
                        assert_eq!(value, "seen");
                    }
                    other => panic!("expected set_header op, got {other:?}"),
                }
            }
            other => panic!("expected continue, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn infinite_loop_guest_times_out_promptly() {
        let engine = epoch_engine();
        let module = Module::new(&engine, infinite_loop_module_wat()).expect("module compiles");
        let step = WasmStep::from_module(engine, module);

        let start = Instant::now();
        let err = step
            .run(&env(), Duration::from_millis(200))
            .await
            .unwrap_err();
        assert!(matches!(err, StepError::WasmTimeout), "{err:?}");
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "run() must return promptly after the epoch timeout, took {:?}",
            start.elapsed()
        );
    }

    /// The concurrency-isolation regression test for the cross-contamination
    /// bug this fix addresses: two concurrent `run` calls on the SAME
    /// `WasmStep` (same `Engine`, same `Module`, same shared epoch ticker) —
    /// one a finite/fast guest invocation with a generous timeout, one an
    /// infinite-loop guest invocation with a short timeout — must each be
    /// bounded purely by their OWN timeout.
    ///
    /// `variable_length_module_wat` makes the ONE module's `run` behavior
    /// depend on input length, so both "finite" and "infinite loop" calls
    /// are genuinely the same compiled module/engine, exactly mirroring two
    /// different concurrent requests hitting the same path-keyed entry in
    /// `proxy::get_or_compile_wasm`'s cache.
    ///
    /// Before this fix (manual `engine.increment_epoch()` on the async-level
    /// `tokio::time::timeout` branch), the short-timeout infinite-loop
    /// call's timeout firing would bump the engine's epoch once, which —
    /// because every concurrent store's deadline was "one tick past its own
    /// creation epoch" — would ALSO trip the generous-timeout finite call's
    /// deadline, spuriously trapping a call that hadn't timed out at all.
    /// With the fix (one continuous ticker plus per-call relative
    /// deadlines), the finite call must succeed regardless of the other
    /// call timing out concurrently on the same engine.
    #[tokio::test]
    async fn concurrent_calls_have_independent_timeouts() {
        const LOOP_THRESHOLD: i32 = 2_000;
        let engine = epoch_engine();
        let module = Module::new(&engine, variable_length_module_wat(LOOP_THRESHOLD))
            .expect("module compiles");
        let step = WasmStep::from_module(engine, module);

        let finite_env = env_with_min_len(16); // well under the threshold
        let loop_env = env_with_min_len(4_096); // well over the threshold

        let finite_task = step.run(&finite_env, Duration::from_secs(30));
        let loop_task = step.run(&loop_env, Duration::from_millis(50));

        let (finite_result, loop_result) = tokio::join!(finite_task, loop_task);

        let loop_err = loop_result.unwrap_err();
        assert!(
            matches!(loop_err, StepError::WasmTimeout),
            "infinite-loop call should time out, got {loop_err:?}"
        );

        let finite_directive = finite_result
            .expect("finite call must NOT be spuriously trapped by the other call's timeout");
        assert!(
            matches!(finite_directive, Directive::Continue { .. }),
            "expected the finite call to succeed with a Continue directive, got {finite_directive:?}"
        );
    }

    /// Best-effort no-leak check: the ticker thread must stop once its
    /// `WasmStep` drops (see `Drop for WasmStep`), rather than accumulating
    /// one live thread per compiled module for the life of the process.
    /// Repeatedly creating and dropping `WasmStep`s and asserting the
    /// process's thread count stays flat is a reasonable proxy for "the
    /// thread actually exited", since `Drop` joins the ticker thread
    /// synchronously before returning.
    #[test]
    fn ticker_thread_stops_on_drop() {
        // `std::thread` has no public "list all threads"/"is this handle's
        // thread alive" API, so this asserts on a proxy instead: `Drop`
        // joins the ticker thread before returning (see `impl Drop for
        // WasmStep`), so repeatedly creating and dropping `WasmStep`s below
        // completing promptly (rather than hanging) demonstrates each one's
        // ticker thread actually stopped — a leaked/never-stopped thread
        // would make that `join()` block forever, hanging this test.
        let engine = epoch_engine();
        let module = Module::new(&engine, continue_module_wat()).expect("module compiles");

        for _ in 0..20 {
            let step = WasmStep::new(engine.clone(), module.clone());
            // Dropping here must join the ticker thread (bounded by
            // `EPOCH_TICK`) rather than leaving it running detached.
            drop(step);
        }
        // Reaching this point without hanging demonstrates every one of the
        // 20 ticker threads spawned above actually stopped and was joined.
    }

    #[test]
    fn from_path_rejects_nonexistent_module() {
        // `WasmStep` isn't `Debug` (wasmtime's `Engine`/`Module` aren't), so
        // this matches manually rather than via `unwrap_err()`.
        match WasmStep::from_path(Path::new("/no/such/module.wasm")) {
            Ok(_) => panic!("expected a nonexistent module path to fail to compile"),
            Err(err) => assert!(matches!(err, StepError::WasmAbi(_)), "{err}"),
        }
    }

    #[tokio::test]
    async fn out_of_bounds_out_len_is_rejected_without_panicking() {
        let engine = epoch_engine();
        let module = Module::new(&engine, out_of_bounds_module_wat()).expect("module compiles");
        let step = WasmStep::from_module(engine, module);

        let err = step
            .run(&env(), Duration::from_millis(1000))
            .await
            .unwrap_err();
        assert!(matches!(err, StepError::WasmAbi(_)), "{err:?}");
    }
}
