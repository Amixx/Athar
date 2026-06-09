# Perf Summary (Rewrite Re-baseline)

Date: 2026-04-02

Current summary is intentionally provisional while the fresh baseline artifacts complete.

## Early Hotspot Signal

- On `house_v1_v2_scrambled`, `diff_graphs` remained in `prepare_context` at `items=2/22` (~5% progress) for multiple heartbeat windows up to ~9m elapsed.
- Interrupt traceback showed waiting in `_prepare_seeded_sides_parallel` (`multiprocessing` pipe `recv()`).

See:
- `docs/perf/FINDINGS_prepare_context_parallel_seed_wait_2026-04-02.md`

## Re-baseline Status

- Fresh baseline artifact target: `docs/perf/baseline_rewrite_2026-04-02.json` (in progress / interrupted for focused hotspot probing).
- Targeted profile artifact target: `docs/perf/profile_prepare_context_house_v1_v2_scrambled_parallel0_2026-04-02.json` (in progress).

## Last Known Stable Checks (prior run set)

- matcher quality (2026-03-12): precision/recall/F1 = `1.0/1.0/1.0`
- determinism stress (2026-03-12):
  - `diff_graphs` stable: `True`
  - `stream_diff_graphs_ndjson` stable: `True`
  - `stream_diff_graphs_chunked_json` stable: `True`
