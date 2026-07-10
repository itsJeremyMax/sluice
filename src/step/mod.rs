pub mod script;
pub mod url;
pub mod wasm;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum StepError {
    #[error("step transport error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("could not decode directive: {0}")]
    Decode(#[from] serde_json::Error),
    #[error("script step process error: {0}")]
    Spawn(#[from] std::io::Error),
    #[error("script step timed out")]
    Timeout,
    /// A `type = "wasm"` step's guest call did not return within its
    /// configured timeout (bounded via wasmtime epoch interruption — see
    /// `step::wasm::WasmStep::run`).
    #[error("wasm step timed out")]
    WasmTimeout,
    /// A `type = "wasm"` step's guest code trapped (panicked/faulted) or
    /// its module failed to instantiate for a reason other than a missing
    /// ABI export.
    #[error("wasm step trapped: {0}")]
    WasmTrap(String),
    /// A `type = "wasm"` step's guest module does not satisfy the expected
    /// host/guest ABI (missing/mis-signatured `memory`/`alloc`/`run`
    /// exports, a memory access out of bounds, or a module/engine
    /// construction failure).
    #[error("wasm step ABI error: {0}")]
    WasmAbi(String),
}
