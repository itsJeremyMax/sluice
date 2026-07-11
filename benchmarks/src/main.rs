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

use std::path::PathBuf;
use std::time::Duration;

use load::{Cell, Results};
use scenarios::Scenario;
use upstream::Sim;

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

/// Per-run timing knobs: how long to run each concurrency level before
/// recording (`warmup`), how long to record (`window`), and the
/// concurrency levels to sweep.
struct Plan {
    warmup: Duration,
    window: Duration,
    concurrency: Vec<usize>,
}

impl Plan {
    fn for_args(args: &Args) -> Self {
        if args.quick {
            Self {
                warmup: Duration::from_millis(200),
                window: Duration::from_secs(1),
                concurrency: vec![1, 8],
            }
        } else {
            Self {
                warmup: Duration::from_secs(2),
                window: Duration::from_secs(5),
                concurrency: vec![1, 8, 64],
            }
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
    let plan = Plan::for_args(&args);
    let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..");

    let mut cells = Vec::new();
    for scenario in Scenario::all() {
        if !scenario.available() {
            continue;
        }
        let upstream_addr = upstream::start(Sim::default_bench());
        let sluice_addr = scenarios::start_sluice(&scenario, upstream_addr, &workspace_root).await;

        let baseline_url = format!("http://{upstream_addr}{}", scenario.baseline_path());
        let sluice_url = format!("http://{sluice_addr}{}", scenario.request_path());
        let body = scenario.request_body();
        let streaming = scenario.is_streaming();

        for &concurrency in &plan.concurrency {
            let baseline_run = load::run_cell(
                baseline_url.clone(),
                body,
                concurrency,
                plan.warmup,
                plan.window,
                false,
            )
            .await;
            let sluice_run = load::run_cell(
                sluice_url.clone(),
                body,
                concurrency,
                plan.warmup,
                plan.window,
                streaming,
            )
            .await;

            let name = scenario.name();
            if sluice_run.summary.count == 0 || baseline_run.summary.count == 0 {
                panic!(
                    "benchmark cell produced zero samples (scenario {name}, concurrency {concurrency}): the target is not serving; results would be meaningless"
                );
            }

            let sluice = sluice_run.summary;
            let baseline = baseline_run.summary;
            cells.push(Cell {
                scenario: name.to_string(),
                concurrency,
                added_p50_ms: sluice.p50_ms - baseline.p50_ms,
                added_p99_ms: sluice.p99_ms - baseline.p99_ms,
                ttfb_p50_ms: sluice_run.ttfb_p50_ms,
                sluice_errors: sluice_run.errors,
                baseline_errors: baseline_run.errors,
                sluice,
                baseline,
            });
        }
    }

    print!("{}", render_table(&cells));

    let results = Results::new(cells);
    write_results(&results);

    // chart rendering added in the next task
}

/// Render `cells` as a fixed-width text table, mirroring the root crate's
/// `render_models_table` style (`src/main.rs`).
fn render_table(cells: &[Cell]) -> String {
    let mut out = format!(
        "{:<14} {:>4} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}\n",
        "SCENARIO", "CONC", "RPS", "P50", "P95", "P99", "+P50", "+P99"
    );
    for c in cells {
        out.push_str(&format!(
            "{:<14} {:>4} {:>9.1} {:>9.2} {:>9.2} {:>9.2} {:>9.2} {:>9.2}\n",
            c.scenario,
            c.concurrency,
            c.sluice.rps,
            c.sluice.p50_ms,
            c.sluice.p95_ms,
            c.sluice.p99_ms,
            c.added_p50_ms,
            c.added_p99_ms,
        ));
        if c.sluice_errors > 0 {
            out.push_str(&format!(
                "  warning: {} error responses excluded from sluice samples\n",
                c.sluice_errors
            ));
        }
        if c.baseline_errors > 0 {
            out.push_str(&format!(
                "  warning: {} error responses excluded from baseline samples\n",
                c.baseline_errors
            ));
        }
    }
    out
}

/// Write `results` pretty-printed to `benchmarks/results/latest.json`,
/// resolved via `CARGO_MANIFEST_DIR` so the path is correct regardless of
/// the caller's working directory.
fn write_results(results: &Results) {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("results/latest.json");
    let json = serde_json::to_string_pretty(results).expect("serialize results");
    std::fs::write(&path, json).expect("write benchmarks/results/latest.json");
    println!("\nwrote {}", path.display());
}
