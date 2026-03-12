# Analysis: Path to 60s for house_v1_v2 (2026-03-12)

## Current state after fingerprint-as-seed unification

**Total: 490s** (79s parse + 411s diff_graphs)
Down from 784s before any fixes (37% reduction).

### Where the time goes (411s diff_graphs)

| Stage | Time | % | Notes |
|-------|------|---|-------|
| seed_text_fingerprints | 160s | 39% | Single-pass fingerprint + profile entity computation for 2M entities |
| WL refinement (2 sides) | 86s | 21% | 43s × 2, xxh3_64, 3-round adaptive cap |
| Non-WL precompute (2 sides) | 68s | 17% | Adjacency builds + Tarjan SCC, ~34s per side |
| Unaccounted overhead | 37s | 9% | Allocations, dict overhead, un-instrumented gaps |
| emit_base_changes | 25s | 6% | Scans ~1M entity IDs for 111 actual changes |
| index_by_identity (2 sides) | 7s | 2% | Down from 74s after gc.disable |
| build_stats | 6s | 1.5% | Full traversal for match-method counters |
| match_unique_ids + match_after_path | 6s | 1.5% | Two O(n) passes over entity IDs |
| Everything else | 16s | 4% | root_remap, secondary_match, emit_derived_markers, etc. |

### Cost of 79s parse (not diff_graphs)

| Phase | Time |
|-------|------|
| old_graph parse (ifcopenshell.open + extract) | 33s |
| new_graph parse (ifcopenshell.open + extract) | 46s |

Parse is ifcopenshell C++ — mostly opaque. Already overlapping open/extract across files.

---

## Why 60s is hard with pure Python

The top 3 costs (fingerprinting, WL, adjacency) share a root cause: **recursive Python dict/list traversal with hash calls for ~2M entities**.

- **Fingerprinting**: `entity_text_fingerprint()` walks nested attribute dicts recursively, calling `hasher.update()` per node. ~80µs/entity in pure Python. This is the interpreter overhead ceiling — xxhash itself is fast but the Python call overhead per `update()` dominates.

- **WL rounds**: Per round, each of ~1M entities builds a payload from sorted neighbor colors and hashes it. The payload construction is Python list sorting + concatenation. 3 rounds × 2 sides × ~5s/round ≈ 30s in just round execution, plus per-round overhead.

- **Adjacency**: `build_adjacency` iterates all entities' ref lists, allocates sorted edge tuples. Built 4× per graph when profile ≠ raw (graph + profile-filtered copy, forward + reverse). ~6s per build.

**Theoretical pure-Python floor**: ~150-200s for 2M entities at this complexity. We're at 2.1× that floor, meaning only ~50% of the time is overhead we could cut in Python.

---

## Optimization paths (ranked by impact × feasibility)

### Path 1: Multiprocessing for old/new independence (411s → ~280s)

Old and new graphs are processed independently through fingerprinting, identity precompute, and WL. Currently sequential. With `fork()`-based multiprocessing:

- **Fingerprint old + new in parallel** then merge for matching (~160s → ~90s wall clock, overlap + merge cost)
- **WL old + new in parallel** (~86s → ~50s wall clock)
- **Adjacency old + new in parallel**

**Challenges**:
- fork() shares memory copy-on-write (good for read-only graph dicts)
- Results must be sent back to parent: pickling ~1M-entry dicts is ~5-10s overhead per transfer
- Alternative: `multiprocessing.shared_memory` or memory-mapped arrays for color vectors

**Estimated savings: ~130s** (411→280s). Medium effort, medium risk.

### Path 2: C/Rust extension for hash inner loops (160s+86s → ~30s)

The fingerprinting and WL inner loops are prime targets for a native extension:

- **Entity fingerprinting**: A Rust function that accepts a Python dict (entity) and returns a hash string. Walks the nested dict/list structure via PyO3 without creating intermediate Python objects. Expected: ~2-5µs/entity instead of ~80µs. **160s → ~5-10s**.

- **WL round payload + hash**: A Rust function that takes a color vector + adjacency arrays and returns a new color vector. No per-node Python calls. Expected: ~1-2s/round instead of ~5s. **86s → ~20-30s**.

- **Adjacency build**: Build forward + reverse adjacency from entity ref lists in one native pass. **68s → ~5-10s**.

**Estimated savings: ~300s** (411→~80-110s diff_graphs). High effort (1-2 weeks), low risk (Rust as pure computation, Python for orchestration).

### Path 3: Graph cache for repeat files (490s → ~100s second run)

Already architected (`graph_cache.py`). Cache both parsed GraphIR and identity precompute state keyed by file content hash.

- First run: full cost (~490s)
- Repeat file hit: skip 79s parse + ~313s identity precompute = **skip ~390s**
- Remaining: matching, emit, stats ≈ ~100s

**Real-world impact**: Users typically diff against files they've seen before. The common workflow (edit → re-export → diff) hits cache on the baseline file.

**Challenge**: Identity state is currently pair-dependent (fingerprint seeds depend on BOTH files). Need to either:
- Cache unseeded identity state and replay seed application (~10s)
- Cache fingerprints + profile entities separately from WL state

**Estimated savings: ~390s on repeat files**. Medium effort, low risk.

### Path 4: Reduce adjacency builds (68s → ~20s)

Currently `_precompute_identity_state` builds 4 data structures per graph:
1. `build_adjacency(entities)` — full graph forward
2. `build_reverse_adjacency(entities)` — full graph reverse
3. `build_adjacency(id_entities)` — profile-filtered forward (when profile ≠ raw)
4. `build_reverse_adjacency(id_entities)` — profile-filtered reverse

For `semantic_stable`, only OwnerHistory-related edges differ. Instead of rebuilding, filter the full adjacency lazily.

Also: adjacency from identity precompute is already threaded to WL and secondary matching (verified — no redundant builds there). The win is eliminating builds 3+4.

**Estimated savings: ~12-25s**. Low effort, low risk.

### Path 5: Cheaper emit for unchanged entities (25s → ~5s)

The emit loop iterates ALL ~1M entity IDs via `sorted(set(old_by_id) | set(new_by_id))`. For 111 actual changes, 99.99% of iterations are skipped at lines 416-426 (H: hash match checks). The 25s is:

- Sorting ~1M 70-char hash strings: ~3s
- Iterating 1M items with dict lookups + string checks: ~5-10s
- Processing 111 actual changes with owner projection + field_ops: ~5-10s
- Python loop overhead: ~5s

Optimizations:
- **Pre-intersect old_by_id and new_by_id**: Instead of iterating ALL entity IDs, compute the symmetric difference first to find only ADD/REMOVE/potential-MODIFY candidates. Changed entities = IDs with different item counts or different profile hashes. This turns O(all_entities) into O(changed_entities) for the expensive path.
- **Skip sort for identical graphs**: If old_by_id.keys() == new_by_id.keys() (common for near-identical files), skip the set union + sort.
- **Accumulate changes during index_by_identity**: Tag entities whose profile_hash differs between old and new during indexing, then only scan those.

**Estimated savings: ~15-20s**. Low effort, low risk.

### Path 6: Accumulate stats during identity (6s → ~0s)

`build_stats` does a post-hoc O(n) traversal to count match methods. Instead, accumulate counters during `_assign_ids` and `_apply_step_matches`.

**Estimated savings: ~6s**. Low effort, low risk.

---

## Projected outcomes

| Scenario | Estimated | Path |
|----------|-----------|------|
| Current | **490s** | Baseline |
| + Paths 4+5+6 (Python quick wins) | **~370s** | Low risk, ~1 day |
| + Path 1 (multiprocessing) | **~250s** | Medium risk, ~2 days |
| + Path 3 (graph cache, repeat files) | **~100s** (second run) | Medium effort, ~1 day |
| + Path 2 (Rust extension) | **~80-100s** (first run) | High effort, ~2 weeks |
| Paths 2+3+4+5+6 combined (repeat files) | **~30-50s** | Best target |
| Paths 2+3+4+5+6 combined (first run) | **~80-100s** | Realistic first-run floor |

### The 60s question

60s for first-run cold-start requires a native extension (Path 2). Without it, the Python interpreter overhead on 2M entities puts a ~150s floor on fingerprint+WL+adjacency alone.

60s for repeat files (cache hit on at least one file) is achievable with Paths 3+4+5+6, no native code needed.

---

## Recommended priority order

1. **Path 5** (cheaper emit) — highest ROI, lowest risk, ~15-20s savings
2. **Path 4** (reduce adjacency builds) — straightforward, ~12-25s savings
3. **Path 6** (accumulate stats) — trivial, ~6s savings
4. **Path 3** (graph cache) — transformative for real-world UX, ~390s savings on repeat
5. **Path 1** (multiprocessing) — medium effort, helps first-run significantly
6. **Path 2** (Rust extension) — highest absolute savings, highest effort

Paths 1-6 combined (repeat files): **~30-50s**. Paths 1-6 combined (first run): **~80-100s**.
