# Findings: prepare_context hotspot profile (house_v1 -> house_v2)

## Context
- Command:
  - `python -m scripts.explore.profile_prepare_context --old tests/fixtures/house_v1.ifc --new tests/fixtures/house_v2.ifc --warmup 0 --iterations 1 --heartbeat-s 15 --cprofile --cprofile-sort cumulative --cprofile-top 60 --out /tmp/house-v1-v2-prepare-context.json`
- Artifact:
  - `/tmp/house-v1-v2-prepare-context.json`

## Measured facts
- Parse total: `90.1s`
- `prepare_context` wall: `1210.9s` (cProfile-enabled run)
- Peak Python memory during prepare: `~2.28 GB`
- Biggest prepare stage timings (`timing_collector`):
  - `assign_new_ids.wl_total_ms`: `216.3s`
  - `assign_old_ids.wl_total_ms`: `214.1s`
  - `index_old_by_identity`: `37.7s`
  - `index_new_by_identity`: `26.8s`
  - `build_compare_entities`: `24.7s`

## Hotspot interpretation
- Structural hashing was duplicated during identity precompute:
  - once for `profile_hashes`
  - again as WL initial colors
- Owner projection full closure materialization remained expensive in diff runs when triggered (`O(roots * reachable graph)` traversal pattern).
- cProfile run included heartbeat thread wait overhead; profiler now suppresses heartbeat during profiled iteration.

## Actions implemented
- WL init now accepts/reuses precomputed initial colors (`profile_hashes`) instead of recomputing structural hashes.
- `prepare_diff_context` now exposes explicit precompute timing steps:
  - `precompute_old_identity`
  - `precompute_new_identity`
- Rooted-owner projection default changed to demand-driven reverse reachability with memoized step results.
  - Eager full owner-index mode remains available when `ATHAR_OWNER_INDEX_DISK_THRESHOLD > 0`.
- Minor allocation reduction in `_index_by_identity` by reusing a shared default identity object.

## Next verification step
- Re-run isolated prepare profile without cProfile first:
  - `python -m scripts.explore.profile_prepare_context --old tests/fixtures/house_v1.ifc --new tests/fixtures/house_v2.ifc --warmup 0 --iterations 1 --heartbeat-s 15 --out /tmp/house-v1-v2-prepare-context-after.json`
- Then run one cProfile capture for updated hotspot ranking.
