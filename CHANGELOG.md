# Changelog

## [0.1.0](https://github.com/itsJeremyMax/sluice/compare/v0.1.0...v0.1.0) (2026-07-11)


### Features

* **bench:** load driver, results json, stdout table ([e95ff84](https://github.com/itsJeremyMax/sluice/commit/e95ff84604095e543ddc3fe8d2950ed48708f803))
* **bench:** percentile stats ([c5a359a](https://github.com/itsJeremyMax/sluice/commit/c5a359a429cf961a5ed571e336380fa5d45febab))
* **bench:** scenario configs + in-process sluice launcher ([679f285](https://github.com/itsJeremyMax/sluice/commit/679f285c942ba936fc616c567b16178282591c06))
* **bench:** simulated LLM upstream (json + sse pacing) ([5ffeaf5](https://github.com/itsJeremyMax/sluice/commit/5ffeaf59f798f8b5e3bcbf0d6d42fb3a9dd5be82))
* **bench:** svg chart, benchmarks README, committed baseline results ([8ef98a8](https://github.com/itsJeremyMax/sluice/commit/8ef98a804d8f38ff3e9e767d22edcff1846c0246))
* release resolution, verified download, atomic self-replace ([0036646](https://github.com/itsJeremyMax/sluice/commit/003664637725971b000588c6d65c40f001d5b517))
* sluice update --check / self-update with verified binaries ([3c7e163](https://github.com/itsJeremyMax/sluice/commit/3c7e1639aa9df38e9c12b500cbc99024590193db))
* sluice-bench workspace crate skeleton ([7b0ad10](https://github.com/itsJeremyMax/sluice/commit/7b0ad103f358234499ced16fa42f2417382db5ad))
* sluice, a self-hosted AI gateway ([8bd1484](https://github.com/itsJeremyMax/sluice/commit/8bd1484d35961a624af48518d0f5918bd89b6e39))
* update helpers — version parse, target triple, asset names ([beb533c](https://github.com/itsJeremyMax/sluice/commit/beb533cee4ec87219ec7b0d5d31553d940df33b3))
* versioned release asset names + bare-binary assets for self-update ([b9e1d5b](https://github.com/itsJeremyMax/sluice/commit/b9e1d5b6cc33a61126d37c2749cccbdaded70e16))


### Bug Fixes

* **bench:** async listener wait so single-worker runtimes cannot deadlock ([126a428](https://github.com/itsJeremyMax/sluice/commit/126a428b4c6d5cef8abf78f7b52cb1718d341dee))
* **bench:** chart streaming overhead as added time-to-first-byte ([2368447](https://github.com/itsJeremyMax/sluice/commit/236844731199cdfe83142080c2b09576654a7ca9))
* **bench:** count error responses instead of timing them; abort on zero-sample cells ([ff3c348](https://github.com/itsJeremyMax/sluice/commit/ff3c348b143623e6598010b0e9ac94d788237d63))
* **bench:** shared per-concurrency baseline to kill tail-noise negatives ([c93b7fb](https://github.com/itsJeremyMax/sluice/commit/c93b7fb36fa49fffcad0347b903307f795b67ebf))
* noop-guardrail.wasm now returns a valid continue directive ([3b6b4fd](https://github.com/itsJeremyMax/sluice/commit/3b6b4fda1aa2a5e3353539e379bd94a7b3f3a961))
* workspace-wide clippy gate + lint fix + doc pointer cleanup ([c06987a](https://github.com/itsJeremyMax/sluice/commit/c06987ad9b203ca0816e15409a81168676f9c478))


### Miscellaneous Chores

* release 0.1.0 ([5ef1224](https://github.com/itsJeremyMax/sluice/commit/5ef12245af1f6170afa7f654670ac9fcc10c93c9))
