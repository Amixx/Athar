# Performance Work Status (Phase 4 In Progress)

Date: 2026-03-12

Phase status:
- Phase 0-3: implemented
- Phase 4: in progress (benchmarking + hardening)

## Completed Artifacts

- `docs/perf/matcher_quality_2026-03-12.json`
  - Deterministic stage-scenario quality metrics (`precision/recall/F1`) for:
    - rooted remap
    - typed-path propagation
    - secondary matching
- `docs/perf/determinism_stress_2026-03-12.json`
  - Repeated-run hash stability checks for:
    - `diff_graphs`
    - `stream_diff_graphs` (`ndjson`, `chunked_json`)

## In Progress

- `docs/perf/batch11_baseline_2026-03-12.json`
  - Runtime + peak memory baseline for:
    - `diff_graphs`
    - `stream_diff_graphs` (`ndjson`, `chunked_json`)
  - Cases:
    - `data/BasicHouse.ifc` (same-file comparison)
    - `data/AdvancedProject.ifc` (same-file comparison)
- `docs/perf/wl_backend_benchmark_2026-03-12-night.json`
  - WL backend perf comparisons across `auto|sha256|xxh3_64|blake3|blake2b_64`.
- `docs/perf/wl_backend_consistency_2026-03-12.json`
  - Backend partition consistency vs `sha256` baseline.
- `docs/perf/perf_suite_run_2026-03-12-night.json`
  - Overnight sequential suite run (WL benchmark/consistency + matcher quality + determinism + summary).

## Ready-To-Run Harnesses

- `scripts/explore/benchmark_diff_engine.py`
- `scripts/explore/evaluate_matcher_quality.py`
- `scripts/explore/stress_determinism.py`
- `scripts/explore/benchmark_wl_backends.py`
- `scripts/explore/check_wl_backend_consistency.py`
- `scripts/explore/benchmark_owner_projection.py`
- `scripts/explore/run_perf_suite.py`
