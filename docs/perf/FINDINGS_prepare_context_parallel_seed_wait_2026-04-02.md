# Findings: `prepare_context` Parallel Seed Wait

Date: 2026-04-02

## Command / Context

```bash
.venv/bin/python -m scripts.explore.benchmark_diff_engine \
  --warmup 0 \
  --iterations 1 \
  --heartbeat-s 20 \
  --engine-timings \
  --progress-file /tmp/athar_benchmark_progress_rewrite.json \
  --out docs/perf/baseline_rewrite_2026-04-02.json
```

Case in progress when interrupted:
- `house_v1_v2_scrambled`
- `tests/fixtures/house_v1.ifc` vs `tests/fixtures/house_v2_scrambled.ifc`

## Key Measurements Observed

- Parse timings before metric start:
  - old graph: `0m 53s 571.8ms`
  - new graph: `2m 46s 224.3ms`
- `diff_graphs` heartbeat repeatedly reported:
  - `stage=prepare_context`
  - `items=2/22`
  - `progress~5.0%`
  - elapsed reaching `~9m 15s` with no stage-step advancement

## Hotspot Signal

- The run remained in early `prepare_context` step progression for multiple heartbeat windows.
- On manual interrupt, traceback was blocked in:
  - `athar.diff.context.prepare_diff_context`
  - `_prepare_seeded_sides_parallel`
  - waiting on `multiprocessing` pipe `recv()` (`recv_conn.recv()`)

## Follow-up Actions Chosen

- Add targeted profiling run(s) for `prepare_diff_context` with explicit parallel mode control (`ATHAR_PARALLEL=0` / `ATHAR_PARALLEL=1`) to isolate whether the stall is specific to side-parallel seed preparation.
- Keep this as a rewrite re-baseline blocker for perf docs: classify `prepare_context` parallel seed wait as the first hotspot to resolve or bound.

Additional observation during `profile_prepare_context` (serial mode attempt, interrupted):
- old graph parse reported: `5m 0s 765.5ms` before entering new-graph parse.
