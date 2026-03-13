# Findings: post-Rust stage breakdown and next targets (2026-03-13)

## Command context

- Artifact: `docs/perf/native_check_2026-03-13_rust.json`
- Command: `benchmark_diff_engine.py --engine-timings --metric diff_graphs --iterations 1 --warmup 0 --heartbeat-s 10`
- Build: `maturin develop` (dev profile, not `--release`)
- `ATHAR_PARALLEL` not set (`seed_text_fingerprints.parallel = 0.0`)
- Profile: `semantic_stable`, guid_policy: `fail_fast`

## Time budget (house_v1_v2, 1 iteration)

Total `diff_graphs`: `229.2s`. `prepare_context`: `215.8s` (94.2%). Parse: `89.6s` (overlapped).

| Stage | Time | % of prepare_context |
|---|---|---|
| `context.seed_text_fingerprints` | 48.8s | 22.6% |
| `context.precompute_old_identity` | 49.8s | 23.1% |
| `context.precompute_new_identity` | 48.3s | 22.4% |
| `context.assign_old/new_ids` (incl. WL) | ~7.0s | 3.2% |
| WL total (old + new, 3 rounds each) | 23.7s + 24.9s = 48.6s | 22.5% |
| `context.index_old/new_by_identity` | 4.1s + 3.8s = 7.9s | 3.7% |
| `context.emit_base_changes` | 7.5s | 3.5% |
| `context.build_stats` | 4.3s | 2.0% |
| Matching stages (remap / path / secondary) | ~7.6s | 3.5% |
| `context.seed_guid_pairs` | 3.6s | 1.7% |

Note: WL total (`assign_*_ids.wl_total_ms`) is a sub-timer *inside* `precompute_*_identity`,
not a separate stage. The ~26s residual per precompute side (after subtracting WL) is adjacency
building + SCC.

## What the remaining time inside `precompute_*_identity` actually is

`precompute_old_identity = 49.8s`:
- WL refinement = 23.7s (3 rounds, native xxh3_64)
- Remaining ≈ 26s: `build_adjacency` + `build_reverse_adjacency` + Tarjan SCC
- Profile entity prep is already reused from `seed_text_fingerprints` (passed as
  `precomputed_profile_entities`)
- `structural_hash` (SHA256) is already skipped: `old_seed_colors = old_all_fingerprints`
  covers all entities, so the `seeded_colors` fast path triggers

`precompute_new_identity = 48.3s`: same breakdown, ~19s residual.

Both `build_adjacency` and `build_reverse_adjacency` are pure Python. For each entity they
iterate refs, filter by graph membership, and sort by (path, target_type, target_id). With
200k+ entities and multiple refs each, this is 1M+ Python iterations with per-entity sort.

## Key observation: GUID coverage = 0.003 (pathological case)

`seed_guid_pairs.coverage = 0.003` means only 0.3% of entities have matching GUIDs across the
two files. This is a worst-case scenario — almost all matching load falls on text fingerprinting
(114,508 matched) and WL. A typical real-world BIM diff (same model, minor property edits) has
80-100% GUID overlap and completes in seconds via the `exact_guid` fast path. The house_v1_v2
case is useful for stress-testing but is not representative of median usage.

## Next native acceleration targets (in priority order)

### 1. Multi-round native WL — save ~15s per side (medium effort)

Currently `native_wl_round` is called in a Python loop:
```python
for round_idx in range(1, rounds + 1):
    next_colors = _NATIVE_WL_ROUND(colors, adjacency)  # full Python→Rust each call
    colors = next_colors                               # full Rust→Python each call
```
For 3 rounds × 2 sides = 6 full PyDict↔Rust HashMap conversions of 200k+ entries. The
adjacency dict is also re-extracted from Python on every call.

Add `native_wl_refine(initial_colors, adjacency, max_rounds) -> (final_colors, rounds_done)`
that converts adjacency once and runs all rounds in Rust with early-exit on no-change. Expected
to remove ~2/3 of the conversion overhead (~15s savings per side). The existing
`native_wl_round` can stay as a fallback / testing hook.

### 2. Native adjacency building — save ~20-26s per side (medium effort)

`build_adjacency` and `build_reverse_adjacency` in `athar/graph/graph_utils.py` are pure Python.
After WL (23.7s), the remaining ~26s in `precompute_old_identity` is dominated by these two
functions plus Tarjan SCC. Porting adjacency building to Rust (iterate entity refs, filter
membership, return sorted tuples) would be the single largest remaining Python-only bottleneck.

Tarjan SCC is also pure Python and could be bundled in the same native module; it shares the
graph structure with adjacency so co-location would avoid an extra Python↔Rust trip.

### 3. `build_stats` pure-Python refactor — save ~4s (low effort, no Rust needed)

`_matched_summary` in `athar/diff/stats.py` iterates `old_by_id` and `new_by_id` (200k+
entries, each a list of identity dicts) to count match methods. This second pass is redundant —
the match method is already known at index-build time in `_index_by_identity`. Pre-accumulating
method counts there and passing them directly to `build_stats` would eliminate the ~4s pass
entirely with no native code.

## Test coverage gaps

### Missing: high-GUID-overlap case (most important)

house_v1_v2 (0.3% GUID) exercises the worst-case path exclusively. The common real-world path
— same model, a few properties changed, 95%+ GUID stability — exercises `exact_guid` +
path_propagation and completes in seconds. The benchmark gives no visibility into that regime.

Fix: use `scripts/make_modified_ifc.py` to produce `BasicHouse_modified.ifc` and add a
`basichouse_v1_v2` case. This gives a medium-size (manageable parse time) high-GUID-overlap
case.

### Missing: medium-size cross-version diff

Only one real diff case (house_v1_v2) which is very large. Adding a 10k–50k entity case
(BasicHouse-derived) would allow scaling analysis and faster iteration on benchmarks.

### Missing: `raw_exact` profile

All benchmark cases use `semantic_stable`. The `raw_exact` profile skips OwnerHistory
filtering, which changes adjacency and profile entity prep costs. Currently not measured.

### Missing: streaming metrics for the large case

The benchmark was run with `--metric diff_graphs` only. `emit_base_changes` (7.5s) is visible
here, but ndjson/chunked_json streaming overhead is not measured for this case.

## Recommended next steps

1. Rebuild in release mode and rerun before any code changes:
   `maturin develop --release --manifest-path athar/_native/Cargo.toml`
   This alone likely yields free 10-30% over the dev build numbers above.

2. Add `native_wl_refine` (multi-round, convert adjacency once).

3. Add native `build_adjacency` + `build_reverse_adjacency` (and optionally Tarjan SCC).

4. Refactor `build_stats` to avoid the second identity pass (pure Python, minimal risk).

5. Add `basichouse_v1_v2` benchmark case using `make_modified_ifc.py` output.
