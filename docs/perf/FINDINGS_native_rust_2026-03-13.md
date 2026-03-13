# Findings: native Rust checkpoint (`native_check_2026-03-13_rust.json`)

Context:
- Baseline artifact: `docs/perf/baseline_2026-03-12.json`
- New artifact: `docs/perf/native_check_2026-03-13_rust.json`
- Native extension was built with `maturin develop` (dev profile, not `--release`)
- `ATHAR_PARALLEL` was not enabled in the benchmark (`context.seed_text_fingerprints.parallel = 0.0`)

Top results:
- Suite total elapsed: `501.7s -> 379.3s` (`-24.4%`)
- `house_v1_v2` end-to-end (parse + diff): `441.3s -> 318.8s` (`-27.8%`)
- `house_v1_v2` diff phase only (`time_ms`): `363.4s -> 229.2s` (`-36.9%`)

Largest `house_v1_v2` wins:
- `context.seed_text_fingerprints`: `161.4s -> 48.8s` (`-69.7%`)
- `context.precompute_old_identity`: `64.4s -> 49.8s` (`-22.7%`)
- `context.precompute_new_identity`: `64.3s -> 48.3s` (`-24.8%`)
- `context.assign_old_ids.wl_total_ms`: `40.8s -> 23.7s` (`-41.9%`)
- `context.assign_new_ids.wl_total_ms`: `40.0s -> 24.9s` (`-37.8%`)
- `prepare_context`: `350.8s -> 215.8s` (`-38.5%`)

Current post-Rust bottlenecks (`house_v1_v2`):
- Parse wall time remains high: `89.6s` (`old_graph 34.3s`, `new_graph 55.3s`)
- `prepare_context`: `215.8s`
- Within `prepare_context`, the biggest remaining timed stages are:
  - `context.precompute_old_identity`: `49.8s`
  - `context.seed_text_fingerprints`: `48.8s`
  - `context.precompute_new_identity`: `48.3s`
  - `context.assign_new_ids.wl_total_ms`: `24.9s`
  - `context.assign_old_ids.wl_total_ms`: `23.7s`

Interpretation:
- The Rust fingerprint + WL work is a real, material win.
- Same-file cases barely changed because they short-circuit diff work; parse now dominates those runs.
- The next obvious engine-side opportunity is still side-parallel context preparation, because old/new seeded precompute is still serial in this artifact.
- The benchmark was run against an unoptimized Rust build, so there is likely still free performance available from `maturin develop --release` before more code changes.

Recommended next steps:
1. Rebuild native in release mode and rerun the same benchmark:
   - `maturin develop --release --manifest-path athar/_native/Cargo.toml`
2. Benchmark with `ATHAR_PARALLEL=1` on the same case set to measure side-overlap gains on:
   - `context.seed_text_fingerprints`
   - `context.precompute_old_identity`
   - `context.precompute_new_identity`
3. If more wall-clock reduction is needed after that, switch focus to parsing/caching rather than more matcher work, because same-file runs are now mostly parse-bound and `prepare_context` is already the dominant diff-stage target.
