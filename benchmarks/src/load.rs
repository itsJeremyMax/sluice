//! Concurrent load driver: spins up N tokio worker tasks that hammer a
//! target URL for a fixed warmup + measurement window, and the result
//! structs (`Cell`, `Results`, `Machine`) that `main.rs` serializes to
//! `benchmarks/results/latest.json`.

use std::time::Duration;

/// One row of the results table: a single scenario at a single concurrency
/// level, with the sluice-fronted and bare-upstream (baseline) summaries
/// side by side and the deltas `main.rs` prints as the `+P50`/`+P99`
/// columns. The four `*ttfb*` fields (sluice-side and baseline
/// time-to-first-byte p50/p99) are only `Some` for the streaming scenario;
/// the chart plots streaming overhead as added TTFB because total stream
/// duration is dominated by SSE pacing sleeps and its percentile deltas
/// are pacing jitter, not gateway overhead.
#[derive(Clone, serde::Serialize)]
pub struct Cell {
    pub scenario: String,
    pub concurrency: usize,
    pub sluice: crate::stats::Summary,
    pub baseline: crate::stats::Summary,
    pub added_p50_ms: f64,
    pub added_p99_ms: f64,
    pub ttfb_p50_ms: Option<f64>,
    pub ttfb_p99_ms: Option<f64>,
    pub baseline_ttfb_p50_ms: Option<f64>,
    pub baseline_ttfb_p99_ms: Option<f64>,
    pub sluice_errors: usize,
    pub baseline_errors: usize,
}

/// Machine facts recorded alongside a run so `latest.json` is
/// self-describing when compared across machines.
#[derive(serde::Serialize)]
pub struct Machine {
    pub os: String,
    pub arch: String,
    pub cpus: usize,
}

impl Machine {
    /// Read the current machine's OS/arch (compile-time constants) and
    /// logical CPU count (`std::thread::available_parallelism`, falling
    /// back to 1 if the platform can't report it).
    pub fn current() -> Self {
        Self {
            os: std::env::consts::OS.to_string(),
            arch: std::env::consts::ARCH.to_string(),
            cpus: std::thread::available_parallelism().map_or(1, |n| n.get()),
        }
    }
}

/// The full report written to `benchmarks/results/latest.json`.
#[derive(serde::Serialize)]
pub struct Results {
    pub sluice_version: String,
    pub date: String,
    pub machine: Machine,
    pub cells: Vec<Cell>,
}

impl Results {
    /// Build a report for `cells`, stamping the current sluice version,
    /// today's date (UTC), and this machine's facts.
    pub fn new(cells: Vec<Cell>) -> Self {
        Self {
            sluice_version: sluice::VERSION.to_string(),
            date: today_utc(),
            machine: Machine::current(),
            cells,
        }
    }
}

/// Convert a day count since the Unix epoch (1970-01-01) into a
/// `(year, month, day)` civil date. Howard Hinnant's `civil_from_days`
/// algorithm (proleptic Gregorian, valid for all `i64` inputs) — used here
/// instead of a chrono dependency since this is the only date computation
/// the whole benchmark harness needs.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

/// Today's date (UTC, from the system clock) as `YYYY-MM-DD`, computed
/// purely from `SystemTime` arithmetic (see `civil_from_days`).
fn today_utc() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before 1970")
        .as_secs() as i64;
    let days = secs.div_euclid(86_400);
    let (y, m, d) = civil_from_days(days);
    format!("{y:04}-{m:02}-{d:02}")
}

/// Result of [`run_cell`]: the latency `summary` (built only from
/// successful, recorded samples), the streaming time-to-first-chunk p50
/// and p99 (when `streaming` was requested), and the count of
/// non-2xx/transport errors observed during the recorded window.
///
/// `Clone` so a single shared baseline measurement (see `main.rs`) can be
/// reused as the baseline for multiple scenario cells at the same
/// concurrency level without re-running the load driver.
#[derive(Clone)]
pub struct CellRun {
    pub summary: crate::stats::Summary,
    pub ttfb_p50_ms: Option<f64>,
    pub ttfb_p99_ms: Option<f64>,
    pub errors: usize,
}

/// Run `concurrency` worker tasks against `url` for `warmup` (discarded)
/// followed by `window` (recorded), each looping POST -> read full body.
/// When `streaming` is set, also records time-to-first-chunk per request
/// and returns its p50/p99 in `CellRun::ttfb_p50_ms`/`ttfb_p99_ms`;
/// otherwise both are `None`.
///
/// Transport errors and non-2xx responses are never timed as latency
/// samples — that would silently record a fast-failing route as
/// artificially *low* latency. Instead, while recording is active, they
/// increment `CellRun::errors` so callers can detect a broken target
/// instead of trusting a corrupted summary.
pub async fn run_cell(
    url: String,
    body: &'static str,
    concurrency: usize,
    warmup: Duration,
    window: Duration,
    streaming: bool,
) -> CellRun {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;

    let recording = Arc::new(AtomicBool::new(false));
    let stop = Arc::new(AtomicBool::new(false));
    let latencies = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let ttfbs = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let errors = Arc::new(AtomicUsize::new(0));

    let mut workers = Vec::new();
    for _ in 0..concurrency {
        let (url, recording, stop, latencies, ttfbs, errors) = (
            url.clone(),
            recording.clone(),
            stop.clone(),
            latencies.clone(),
            ttfbs.clone(),
            errors.clone(),
        );
        workers.push(tokio::spawn(async move {
            let client = reqwest::Client::new();
            while !stop.load(Ordering::Relaxed) {
                let start = std::time::Instant::now();
                let resp = match client.post(&url).body(body).send().await {
                    Ok(resp) => resp,
                    Err(_) => {
                        if recording.load(Ordering::Relaxed) {
                            errors.fetch_add(1, Ordering::Relaxed);
                        }
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        continue;
                    }
                };
                if !resp.status().is_success() {
                    if recording.load(Ordering::Relaxed) {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                    // Drain the body so the connection can be reused.
                    let _ = resp.bytes().await;
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                }
                let first_chunk_at = if streaming {
                    use futures_util::StreamExt;
                    let mut stream = resp.bytes_stream();
                    let first = stream.next().await.map(|_| start.elapsed());
                    while stream.next().await.is_some() {}
                    first
                } else {
                    let _ = resp.bytes().await;
                    None
                };
                if recording.load(Ordering::Relaxed) {
                    latencies.lock().await.push(start.elapsed());
                    if let Some(t) = first_chunk_at {
                        ttfbs.lock().await.push(t);
                    }
                }
            }
        }));
    }

    tokio::time::sleep(warmup).await;
    recording.store(true, Ordering::Relaxed);
    tokio::time::sleep(window).await;
    recording.store(false, Ordering::Relaxed);
    stop.store(true, Ordering::Relaxed);
    for w in workers {
        let _ = w.await;
    }

    let mut lat = std::mem::take(&mut *latencies.lock().await);
    let summary = crate::stats::summarize(&mut lat, window);
    let (ttfb_p50_ms, ttfb_p99_ms) = if streaming {
        let mut t = std::mem::take(&mut *ttfbs.lock().await);
        t.sort_unstable();
        (
            Some(crate::stats::percentile_ms(&t, 0.50)),
            Some(crate::stats::percentile_ms(&t, 0.99)),
        )
    } else {
        (None, None)
    };
    CellRun {
        summary,
        ttfb_p50_ms,
        ttfb_p99_ms,
        errors: errors.load(Ordering::Relaxed),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::upstream::{self, Sim};
    use std::time::Duration;

    #[tokio::test(flavor = "multi_thread")]
    async fn run_cell_collects_samples_and_positive_rps() {
        let up = upstream::start(Sim::default_bench());
        let run = run_cell(
            format!("http://{up}/v1/messages"),
            "{}",
            2,
            Duration::from_millis(50),
            Duration::from_millis(300),
            false,
        )
        .await;
        assert!(run.summary.count > 0, "no samples collected");
        assert!(run.summary.rps > 0.0);
        assert!(
            run.summary.p50_ms >= 2.0,
            "p50 below simulated ttft: {}",
            run.summary.p50_ms
        );
        assert!(run.ttfb_p50_ms.is_none());
        assert!(run.ttfb_p99_ms.is_none());
        assert_eq!(run.errors, 0, "healthy upstream should produce no errors");
    }

    /// A dead target (nothing listening) should never produce a latency
    /// sample: `run_cell` must not time connection failures. Whether
    /// `errors` ends up `0` or `>0` depends on timing, not on gate
    /// correctness — errors are only counted while `recording` is true
    /// (mirroring how samples are only recorded then), and workers spend
    /// most of a short warmup/window retrying a `Duration::from_millis(10)`
    /// backoff after each failed connect. So the load-bearing assertion
    /// here is `summary.count == 0`; we don't assert an exact `errors`
    /// count, only that it's consistent with the gated semantics (it can
    /// legitimately be 0 if every failed attempt landed during warmup, or
    /// positive if any landed during the recording window).
    #[tokio::test(flavor = "multi_thread")]
    async fn run_cell_against_dead_target_records_no_samples() {
        // Bind then immediately drop: reserves a port nothing is listening
        // on, so connects to it fail fast with a transport error.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
        let addr = listener.local_addr().expect("local addr");
        drop(listener);

        let run = run_cell(
            format!("http://{addr}/v1/messages"),
            "{}",
            2,
            Duration::from_millis(20),
            Duration::from_millis(100),
            false,
        )
        .await;

        assert_eq!(
            run.summary.count, 0,
            "a dead target must never produce a timed latency sample"
        );
        // errors is gated the same way samples are: only counted once
        // `recording` flips true. With a 100ms window and 10ms backoff per
        // failed attempt, we expect at least one counted error per worker.
        assert!(
            run.errors > 0,
            "expected at least one gated error to be counted during the recording window"
        );
    }

    #[test]
    fn civil_from_days_matches_known_epoch_dates() {
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(19_914), (2024, 7, 10));
        // The day this task brief is dated.
        assert_eq!(civil_from_days(20_644), (2026, 7, 10));
    }
}
