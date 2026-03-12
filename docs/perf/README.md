# Performance Baselines

This folder stores reproducible benchmark outputs for the low-level diff engine.

## Harness

Use:

```bash
python -m scripts.explore.benchmark_diff_engine --warmup 1 --iterations 2 --out docs/perf/batch11_baseline_YYYY-MM-DD.json
```

Optional bottleneck breakdown for `diff_graphs` stage timings:

```bash
python -m scripts.explore.benchmark_diff_engine --warmup 0 --iterations 1 --engine-timings --out docs/perf/batch11_baseline_YYYY-MM-DD.json
```

By default, the harness benchmarks same-file comparisons for:

- `data/BasicHouse.ifc`
- `data/AdvancedProject.ifc`

Each case records:

- `diff_graphs`
- `stream_diff_graphs_ndjson`
- `stream_diff_graphs_chunked_json`

Additional harnesses:

- `python -m scripts.explore.benchmark_wl_backends --out docs/perf/wl_backend_benchmark_YYYY-MM-DD.json`
- `python -m scripts.explore.check_wl_backend_consistency --out docs/perf/wl_backend_consistency_YYYY-MM-DD.json`
- `python -m scripts.explore.benchmark_owner_projection --out docs/perf/owner_projection_benchmark_YYYY-MM-DD.json`
- `python -m scripts.explore.run_perf_suite --tag YYYY-MM-DD`

Operational notes:

- Long-running harnesses print progress to stderr (case/graph/backend and per-iteration steps).
- `benchmark_diff_engine --engine-timings` records per-stage `diff_graphs` timing breakdowns from `stats.timings_ms` under `metrics.diff_graphs.engine_timings_ms`.
- `benchmark_diff_engine` stores parser timings per case under `parse_ms` (`old_graph`, `new_graph`, `total`).
- `stress_determinism` prints per-round progress (`--progress-every`) and completion timing to stderr.
- `evaluate_matcher_quality` prints per-scenario start/done progress and total completion timing to stderr.
- `check_wl_backend_consistency` outputs compact partition fingerprints (`sha256` + size stats) to avoid oversized JSON artifacts.
- `run_perf_suite` supports bounded execution via `--step-timeout-s` and scoped WL inputs via `--wl-graph` / `--wl-consistency-graph`.
- `run_perf_suite` writes its manifest incrementally after each step and supports `--resume` to skip previously successful steps whose artifacts still exist.
- `run_perf_suite --baseline-engine-timings` forwards `--engine-timings` to the baseline benchmark step.
- `render_perf_summary` includes a `Diff Stage Timings (diff_graphs)` section when baseline artifacts contain `engine_timings_ms`.

For every metric, it captures:

- `time_ms` samples + summary (`min`, `max`, `mean`, `median`, `p95`)
- `peak_mem_bytes` samples + summary (Python `tracemalloc` peak)
- output-signature stability across runs

## Notes

- These baselines are for trend tracking within this repository/environment.
- `peak_mem_bytes` reflects Python allocation peak observed by `tracemalloc`, not full process RSS.
- Keep benchmark config (`warmup`, `iterations`, `profile`, `guid_policy`, `chunk_size`) consistent when comparing files.
- Script modules are intended to run via `python -m scripts.explore.<name>` from repo root, so no `PYTHONPATH` override is required.
