# Analysis: 60s Target for house_v1_v2 (2026-03-12)

## Context

- Case: `house_v1.ifc` vs `house_v2.ifc`, ~1,026,311 entities/side, ~1.7M edges/side
- Baseline: 784s (13 min) before any fixes
- Hardware: Apple M1 Mac
- After 4 implemented fixes (structural_hash dedup, xxhash dep, lazy owner projection, instrumentation): estimated ~200s

## Fixes already implemented

1. **Structural hash dedup**: pass `profile_hashes` as `initial_colors` to WL — eliminates ~18s of duplicate hashing
2. **xxhash dependency**: added to requirements, WL backend resolves to xxh3_64
3. **Lazy owner projection**: `RootedOwnerProjector` defaults to on-demand reverse traversal with caching instead of eager O(R×N) full materialization — eliminates ~255s catastrophe
4. **Instrumentation**: `_precompute_identity_state` now has per-stage timing in `prepare_diff_context`

## Remaining cost breakdown (~200s estimated)

| Stage | Cost | Notes |
|-------|------|-------|
| IFC parse (2×39s, sequential) | 78s | ifcopenshell C++ floor |
| Adjacency/reverse (built 4-6×) | ~20s | `build_adjacency` in wl_refine_colors, wl_refine_with_scc_fallback, matcher; `_build_reverse_sources` in owner projector |
| WL rounds (3×1M×2 sides) | ~10s | xxhash enabled BUT double-hashed: line 86 does sha256 on top of xxhash output |
| structural_hash (json.dumps+sha256, 1M×2) | ~18s | json.dumps dominates at ~4.5µs/call, sha256 only ~0.6µs |
| `_graph_for_profile` (×2) | ~6s | Full dict copy for semantic_stable profile |
| `_index_by_identity` (×2) | ~8s | Allocates per-entity wrapper dicts for all 1M entities |
| `_build_compare_entities` | ~6s | Eagerly normalizes ref targets for all non-H: entities |
| Tarjan SCC (×2) | ~4s | |
| Owner projection (lazy) | ~5-30s | Depends on graph connectivity, cache warming |
| Matching stages | ~2s | Already fast |
| Scan + emit | ~5s | Already fast |
| Sort/alloc overhead | ~10s | |

## Optimization plan (ranked by impact/effort)

### Tier 1: Low-risk pure-Python fixes (~200s → ~80-95s)

**A. Kill WL double-hashing (~6-8s savings)**
- `wl_refinement.py` line 86: with xxhash, does `xxh3_64_hexdigest(blob)` then `sha256(digest)` on top
- 1M extra sha256 calls per round × 3 rounds × 2 sides = 6M wasted sha256
- Fix: use xxhash output directly as WL color (colors only compared within a run)

**B. Build adjacency once, thread through (~12-15s savings)**
- `build_adjacency` called in: `wl_refine_colors`, `wl_refine_with_scc_fallback` (after WL), potentially `secondary_match_unresolved`
- `_build_reverse_sources` in `RootedOwnerProjector` is yet another reverse adjacency
- Fix: build adjacency + reverse in `_precompute_identity_state`, pass through all consumers

**C. Streaming structural_hash (~8-12s savings)**
- Replace `json.dumps(payload) + sha256` with streaming xxhash over entity fields
- Skip intermediate `structural_payload` dict and `strip_ref_ids` copy
- Guardrail: xxhash for fingerprinting only; structural equality verified where correctness matters

**D. Lazy `_graph_for_profile` (~5s savings)**
- Don't copy all 1M entities for semantic_stable profile
- Compute profile entities only when comparison actually needs them
- Profile hashes already computed from profiled entities; keep that, defer the entity materialization

**E. Flatten `_index_by_identity` (~4-5s savings)**
- Replace per-entity wrapper dicts with flat `by_id: dict[str, list[int]]` index
- Keep identity, profile_hash, compare_entity in separate per-step lookups
- Eliminates 2M small dict allocations

**F. Defer `_build_compare_entities` (~4-5s savings)**
- Currently eagerly normalizes ref targets for all non-H: comparable entities
- Only ~3,489 entities actually reach `entities_equal`
- Fix: normalize lazily at comparison time

**G. Parser schema caching (~3-5s savings)**
- `schema_util.get_declaration(ent)` repeats for same entity types across 1M entities
- Cache per-entity-type schema metadata (attr names, types, resolved type helpers)

**H. Minor: gc.disable during hot phases, fewer sorts (~2-3s savings)**

### Tier 2: Parse overlap (~80-95s → ~55-75s)

**Thread-overlapped parsing**
- Split `parse_graph` into `open_ifc(path)` + `extract_graph(ifc, profile)`
- Start `ifcopenshell.open(new_path)` in a background thread while extracting old graph
- If ifcopenshell.open() releases GIL (it's C++, likely does): saves ~25-35s wall clock
- No IPC, no dict transfer, minimal code change
- Risk: verify ifcopenshell GIL behavior first; check RSS doesn't double on M1 unified memory

### Tier 3: GraphIR cache (~first-run stays ~75s, repeat files → ~35-50s)

**Binary cache with content-hash key**
- After first parse, serialize GraphIR to MessagePack/CBOR keyed by file content hash (or mtime+size)
- Second encounter of same file skips 39s parse entirely
- Biggest UX win: users almost always diff against a file they've already opened
- First run: ~75s. Second run involving cached file: ~35-50s

### Tier 4: Rust PyO3 (~first-run → ~60s)

**Native module for hot inner loops**
- structural_hash: eliminate json.dumps/dict allocation overhead
- WL round payload construction + hashing
- Expected savings: ~15-20s on top of all Python fixes

## Key correctness constraints

- **WL must run on full graph**: WL is message-passing; singleton entities transmit colors to neighbors in collision classes. Cannot skip singletons without breaking correctness. This is the competitive moat vs IfcDiff.
- **Two-stage identity (hash-match first, WL only on ambiguous)**: theoretically possible but subtle — would need to prove that frozen singleton colors produce equivalent partitions. Deferred as high-risk.
- **xxhash for WL colors is safe**: colors only compared within a single run, never persisted across runs.
- **xxhash for structural identity**: use as fingerprint/bucket key only, verify equality within same-hash groups.

## Projected outcomes

| Scenario | Estimated time | Path |
|----------|---------------|------|
| After Tier 1 (pure Python fixes) | ~80-95s | Low risk, ~3-5 days work |
| + Tier 2 (parse overlap) | ~55-75s | Medium risk, +0.5 day |
| + Tier 3 (GraphIR cache, repeat files) | ~35-50s | Low risk, +1 day |
| + Tier 4 (Rust PyO3) | ~55-65s first-run | High effort, +1-2 weeks |
| Tier 1+2+3 combined (repeat files) | **~35-50s** | Best near-term target |

## Decision

60s is achievable for repeat files with Tiers 1-3 (pure Python, no Rust). First-run of never-seen files: ~55-75s with Tiers 1-2. Rust (Tier 4) gets first-run to ~60s but is higher effort.

Multiprocessing for old/new precompute is deferred — pickling 1M-entity dicts likely costs 10-20s each way, erasing savings. Only reconsider after micro-profiles prove net benefit.
