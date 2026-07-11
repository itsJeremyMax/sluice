# Benchmarks

End-to-end overhead of putting sluice between a client and an LLM
upstream, measured against a simulated upstream (2ms time-to-first-token,
1ms inter-chunk, 20 SSE chunks) so runs are deterministic and offline.
Each scenario is measured twice — direct to the upstream (baseline) and
through sluice — and the numbers reported are the *added* latency.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="assets/chart-dark.svg">
  <img alt="Added latency per scenario" src="assets/chart.svg">
</picture>

## Running

```
cargo run --release -p sluice-bench            # full run (~3 min)
cargo run --release -p sluice-bench -- --quick # smoke run (~20 s)
```

Results land in `results/latest.json` (machine, sluice version, date, and
the full matrix) and the charts above are re-rendered on every run. The
committed numbers come from the machine and version labeled inside the
chart and JSON — treat cross-machine comparisons as directional only.

The four buffered scenarios (passthrough, translation, script-step,
wasm-step) all hit the same simulated JSON upstream with functionally
identical requests, so they share a single direct-to-upstream baseline
measurement per concurrency level rather than each measuring its own.
Streaming hits a different endpoint through a different read path, so it
keeps its own dedicated baseline. Added-latency values, especially p99,
still carry roughly ±1ms of tail noise on a laptop-class machine — a tiny
or slightly negative added p99 should be read as "no measurable overhead,"
not as sluice being faster than the bare upstream.

The streaming chart row plots added *time-to-first-byte*, not added total
stream duration: each streamed request lasts ~48ms dominated by the
upstream's paced SSE sleeps, so comparing two independently measured
total-duration percentiles yields multi-ms pacing jitter rather than
gateway overhead. Total stream duration still appears in
`results/latest.json` for transparency, but its p99 delta is
jitter-dominated and not charted.

## Scenarios

- **passthrough** — plain proxy route, no steps
- **translation** — openai→anthropic wire translation
- **script-step** — a oneshot subprocess step in the request chain (unix only)
- **wasm-step** — a wasmtime guest step in the request chain
- **streaming** — SSE passthrough; charted as added time-to-first-byte

Concurrency levels 1 / 8 / 64; nearest-rank percentiles over a 5s window
after a 2s warmup. Not run in CI — shared-runner numbers are noise.
