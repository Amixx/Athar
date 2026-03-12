# Findings: house_v1_vs_v2 diff-only benchmark (2026-03-12)

## Command context

- Artifact analyzed: `/tmp/house-v1-v2-diff-only.json`
- Case: `tests/fixtures/house_v1.ifc` -> `tests/fixtures/house_v2.ifc`
- Metric set: `diff_graphs` only (`--metric diff_graphs`)
- Profile/policy: `semantic_stable`, `guid_policy=fail_fast`

## Measured results (single iteration)

- Parse total: `76,290.240 ms` (`old=34,101.607 ms`, `new=42,188.632 ms`)
- `diff_graphs` mean: `1,040,054.173 ms` (~17m 20s)
- Peak Python memory (`p95`): `6,328,698,980 bytes` (~5.89 GiB)

## Top engine timings (`stats.timings_ms`, mean)

1. `prepare_context`: `664,066.972 ms` (~63.8% of diff)
2. `emit_base_changes`: `361,498.022 ms` (~34.8%)
3. `emit_derived_markers`: `2,318.290 ms` (~0.2%)

Context breakdown:

1. `context.assign_new_ids`: `235,679.201 ms`
2. `context.assign_old_ids`: `226,022.382 ms`
3. `context.secondary_match`: `120,074.725 ms`
4. `context.match_unique_ids`: `30,410.813 ms`
5. `context.index_new_by_identity`: `23,805.470 ms`

## Concrete next optimization targets

1. Base-change scan fast path:
   - Avoid deep equality + field-op generation for pairs already proven identical by stable identity/equality tokens.
   - `emit_base_changes` is currently ~35% of wall time.
2. Identity assignment hot path:
   - Reduce repeated work in `assign_old_ids`/`assign_new_ids` (largest combined block).
   - Focus on WL/hash precompute reuse and avoiding expensive normalization work for already-resolved categories.
3. Secondary matcher cost control:
   - Tighten candidate blocking and unresolved gate behavior for large unmatched populations.
   - `secondary_match` is ~11.5% of end-to-end diff time.
4. Matching/indexing overhead:
   - Optimize `match_unique_ids` + identity indexing structures (`index_*_by_identity`).
5. Memory pressure reduction:
   - With ~5.9 GiB peak, prioritize reducing temporary allocations in identity prep and base-change emission.

## Immediate instrumentation status

- `benchmark_diff_engine.py` now logs human-readable durations (`Xm Ys Zms`) for parse/heartbeat/mean/completion.
- Benchmark JSON now includes `run_summary.total_elapsed_ms` and `run_summary.total_elapsed_text`.
- Progress sidecar completion payload now includes `total_elapsed_ms` and `total_elapsed_text`.

## Implemented follow-up optimizations

- `entity_for_profile()` now has a no-copy fast path when semantic-stable volatility fields are absent.
- Unique-ID matching now avoids per-ID list allocations (`_match_steps_by_unique_id` counter-based pass).
- Identity indexing now iterates sorted STEP IDs directly and avoids per-bucket resorting.
- Secondary matcher now exits immediately when unresolved sets are empty and builds blocks/features from unresolved sets only.
- Base-change equality now uses prebuilt compare-entity caches (for non-`H:` shared IDs) to avoid repeated deep ref-target normalization in the hot loop.
- Ref-target normalization now uses copy-on-write recursion and scalar fast paths to reduce temporary allocations.
