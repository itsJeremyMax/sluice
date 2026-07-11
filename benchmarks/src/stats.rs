//! Latency aggregation: sort-once nearest-rank percentiles over recorded
//! request durations. Small sample counts (a few thousand per cell) make
//! exact sorting cheaper and simpler than a streaming sketch.

use std::time::Duration;

#[derive(Clone, serde::Serialize)]
pub struct Summary {
    pub count: usize,
    pub rps: f64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
}

/// Nearest-rank percentile (q in 0..=1) over an already-sorted slice, in
/// fractional milliseconds. Empty input reports 0.
pub fn percentile_ms(sorted: &[Duration], q: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((sorted.len() - 1) as f64 * q).round() as usize;
    sorted[idx].as_secs_f64() * 1000.0
}

/// Sort `latencies` in place and summarize them against the measure
/// `window` that produced them (rps = count / window seconds).
pub fn summarize(latencies: &mut [Duration], window: Duration) -> Summary {
    latencies.sort_unstable();
    let count = latencies.len();
    let secs = window.as_secs_f64();
    Summary {
        count,
        rps: if secs > 0.0 { count as f64 / secs } else { 0.0 },
        p50_ms: percentile_ms(latencies, 0.50),
        p95_ms: percentile_ms(latencies, 0.95),
        p99_ms: percentile_ms(latencies, 0.99),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentiles_of_a_known_distribution() {
        // 1..=100 ms: p50 = 50 or 51, p99 = 99 or 100 depending on rounding;
        // pin the nearest-rank convention exactly.
        let mut v: Vec<Duration> = (1..=100).map(Duration::from_millis).collect();
        let s = summarize(&mut v, Duration::from_secs(1));
        assert_eq!(s.count, 100);
        assert!((s.rps - 100.0).abs() < f64::EPSILON);
        assert!((s.p50_ms - 50.0).abs() <= 1.0, "p50 was {}", s.p50_ms);
        assert!((s.p99_ms - 99.0).abs() <= 1.0, "p99 was {}", s.p99_ms);
    }

    #[test]
    fn single_sample_is_every_percentile() {
        let mut v = vec![Duration::from_millis(7)];
        let s = summarize(&mut v, Duration::from_secs(1));
        assert_eq!(s.p50_ms, 7.0);
        assert_eq!(s.p95_ms, 7.0);
        assert_eq!(s.p99_ms, 7.0);
    }

    #[test]
    fn empty_samples_summarize_to_zeroes() {
        let mut v = Vec::new();
        let s = summarize(&mut v, Duration::from_secs(1));
        assert_eq!(s.count, 0);
        assert_eq!(s.p50_ms, 0.0);
        assert_eq!(s.rps, 0.0);
    }
}
