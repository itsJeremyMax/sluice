//! sluice-bench: end-to-end overhead benchmarks. Spec:
//! docs/superpowers/specs/2026-07-10-benchmarks-design.md.
//! Run with `cargo run --release -p sluice-bench` (add `--quick` for a
//! fast smoke run). Everything is offline: a simulated LLM upstream is
//! started in-process and sluice is served in-process around it.

mod chart;
mod load;
mod scenarios;
mod stats;
mod upstream;

/// CLI flags. Deliberately trivial — no clap dependency for two flags.
pub struct Args {
    pub quick: bool,
}

impl Args {
    fn parse() -> Self {
        Self {
            quick: std::env::args().any(|a| a == "--quick"),
        }
    }
}

fn main() {
    let args = Args::parse();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");
    rt.block_on(run(args));
}

async fn run(args: Args) {
    println!("sluice-bench (quick: {})", args.quick);
    // Filled in by later tasks.
}
