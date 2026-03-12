# Performance Baselines

This folder stores reproducible benchmark outputs for the low-level diff engine.

Most recent concrete hotspot analysis:

- `docs/perf/FINDINGS_ifchouse_same_input_2026-03-12.md`

## Harness

Use:

```bash
python -m scripts.explore.benchmark_diff_engine --warmup 1 --iterations 2 --out docs/perf/batch11_baseline_YYYY-MM-DD.json
```

During dev loops, run only one metric to avoid triple full-engine runs:

```bash
python -m scripts.explore.benchmark_diff_engine --metric diff_graphs --warmup 0 --iterations 1 --engine-timings --heartbeat-s 15 --out /tmp/diff-only.json
```

Optional bottleneck breakdown for `diff_graphs` stage timings:

```bash
python -m scripts.explore.benchmark_diff_engine --warmup 0 --iterations 1 --engine-timings --out docs/perf/batch11_baseline_YYYY-MM-DD.json
```

Force iteration heartbeats every 15s while long metrics are running:

```bash
python -m scripts.explore.benchmark_diff_engine --warmup 0 --iterations 1 --heartbeat-s 15 --out docs/perf/batch11_baseline_YYYY-MM-DD.json
```

Write live progress snapshots to a sidecar JSON file:

```bash
python -m scripts.explore.benchmark_diff_engine --warmup 0 --iterations 1 --progress-file /tmp/benchmark-progress.json --out docs/perf/batch11_baseline_YYYY-MM-DD.json
```

Watch sidecar progress in a separate terminal:

```bash
python -m scripts.explore.watch_progress --file /tmp/benchmark-progress.json --interval-s 2
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
- `benchmark_diff_engine --heartbeat-s N` prints heartbeat logs during each metric iteration (`0` disables), including coarse `progress~...` / `eta~...` estimates using stage-aware progress where available. ETA is rendered in human duration form (`h m s` when needed).
- If a heuristic ETA model is exceeded, heartbeat reports `eta~overrun ...` instead of `0s`.
- Stream metrics (`stream_ndjson`, `stream_chunked_json`) now expose count-based heartbeat progress from emitted stream record counts, with ETA derived from observed throughput when expected record counts are known.
- `benchmark_diff_engine --progress-file PATH` writes live sidecar snapshots (`state`, `current_case`, metric/stage progress, ETA hints) for external monitoring.
- Sidecar `current_case` snapshots include stream counters (`completed`, `total`) and emitted `bytes` where available.
- For `diff_graphs`, heartbeat progress includes context-step granularity (`root_remap`, ID assignment/matching stages, indexing/stats), then base-change scan progress, then derived-marker completion.
- `stress_determinism` prints per-round progress (`--progress-every`) and completion timing to stderr.
- `evaluate_matcher_quality` prints per-scenario start/done progress and total completion timing to stderr.
- `check_wl_backend_consistency` outputs compact partition fingerprints (`sha256` + size stats) to avoid oversized JSON artifacts.
- `run_perf_suite` supports bounded execution via `--step-timeout-s` and scoped WL inputs via `--wl-graph` / `--wl-consistency-graph`.
- `run_perf_suite` writes its manifest incrementally after each step and supports `--resume` to skip previously successful steps whose artifacts still exist.
- `run_perf_suite --baseline-engine-timings` forwards `--engine-timings` to the baseline benchmark step.
- `run_perf_suite --heartbeat-s N` prints suite-level heartbeat logs while a step is running (set `0` to disable).
- `run_perf_suite --baseline-progress-file PATH` forwards baseline benchmark sidecar progress output (`benchmark_diff_engine --progress-file`).
- When both `--heartbeat-s` and `--baseline-progress-file` are set, suite heartbeat lines include nested baseline detail (`case/metric/stage/progress/eta`) from the sidecar.
- Suite manifest `current_step` now includes latest heartbeat snapshot (including nested baseline probe summary when available) while running.
- The suite manifest includes `state` (`running|failed|completed`) and transient `current_step` metadata while execution is in progress.
- `render_perf_summary` includes a `Diff Stage Timings (diff_graphs)` section when baseline artifacts contain `engine_timings_ms`.
- `render_perf_summary` includes a `Perf Suite Run` section when a suite manifest is provided (`--suite-manifest`), showing step status/elapsed and current in-progress step.
- If suite manifest `current_step.heartbeat.probe` is present, `render_perf_summary` includes live baseline probe details (case/metric/stage/progress/eta) in `Perf Suite Run`.

For every metric, it captures:

- `time_ms` samples + summary (`min`, `max`, `mean`, `median`, `p95`)
- `peak_mem_bytes` samples + summary (Python `tracemalloc` peak)
- output-signature stability across runs

## Notes

- These baselines are for trend tracking within this repository/environment.
- `peak_mem_bytes` reflects Python allocation peak observed by `tracemalloc`, not full process RSS.
- Keep benchmark config (`warmup`, `iterations`, `profile`, `guid_policy`, `chunk_size`) consistent when comparing files.
- Script modules are intended to run via `python -m scripts.explore.<name>` from repo root, so no `PYTHONPATH` override is required.
