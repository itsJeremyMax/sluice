pub mod admin;
pub mod cli;
pub mod config;
pub mod directive;
pub mod envelope;
pub mod http_msg;
pub mod llm;
pub mod loopback;
pub mod observability;
pub mod proxy;
pub mod reconstruct;
pub mod registry;
pub mod router;
pub mod server;
pub mod sse;
pub mod step;
pub mod update;

/// Crate version, re-exported so out-of-crate tooling (sluice-bench) can
/// label results with the sluice version it measured.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
