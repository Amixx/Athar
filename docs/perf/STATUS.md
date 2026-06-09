# Performance Work Status

Date: 2026-04-02

## Current Focus (Rewrite Re-baseline)

- Re-baseline the current rewrite runtime before additional Phase 2 feature work.
- Keep perf docs anchored to observed 2026-04-02 behavior.

## In Progress

- Fresh rewrite baseline run with stage heartbeats:
  - `benchmark_diff_engine` on default case set (interrupted after hotspot detection)
  - intended artifact path: `docs/perf/baseline_rewrite_2026-04-02.json`
- Targeted prepare-context profiling:
  - intended artifact path: `docs/perf/profile_prepare_context_house_v1_v2_scrambled_parallel0_2026-04-02.json`

## New Concrete Finding

- `docs/perf/FINDINGS_prepare_context_parallel_seed_wait_2026-04-02.md`
  - During `house_v1_v2_scrambled`, `diff_graphs` remained in `prepare_context` (`items=2/22`, ~5% progress) for extended heartbeats.
  - Interrupt traceback shows wait in `_prepare_seeded_sides_parallel` on multiprocessing pipe `recv()`.

## Next Perf Steps

- Finish targeted serial/parallel `prepare_context` profiling runs and compare wall-time.
- Run holy-grail pair probes (`real-world-test/real-world-spanish-180mb.ifc` vs `real-world-test/uni-project-house-50mb.ifc`) using:
  - `make perf-holy-grail-serial`
  - `make perf-holy-grail-parallel`
- Publish refreshed `docs/perf/SUMMARY.md` once baseline artifacts complete.
