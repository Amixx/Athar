# Findings: Fingerprint-as-WL-seed unification (2026-03-12)

## Context

- Case: `house_v1.ifc` vs `house_v2.ifc`, ~1,026,311 entities/side
- Measured baseline before this change: **670s** (`diff_graphs` only, excludes parse)
- The "~200s estimated baseline" in the analysis doc was never measured — actual was 3.4x higher

## Problem

The similarity-seeding phase (`text_fingerprint_pairs`) was a **net +167s regression**:

| What it cost | Time |
|---|---|
| Fingerprinting ~2M entities (xxh3_128) | 167s |
| `build_seed_color_maps` (sha256 wrapping) | 1.2s |
| **Total added** | **~168s** |

| What it saved | Time |
|---|---|
| Structural hash skips for matched seeds | ~0.5s (only 11.2% match rate) |
| **Net regression** | **~167s** |

Root causes:
1. Only 11.2% of entities matched via fingerprint, so 88.8% still needed `structural_hash`
2. `entity_for_profile` was called ~4M times total — once in fingerprinting, once again in `_graph_for_profile`
3. `build_seed_color_maps` did a pointless sha256 wrap of already-computed fingerprints
4. Fingerprinting + structural hashing = two nearly-identical recursive hashes over the same entity data

## Fix: single-pass fingerprint → WL seed

Instead of fingerprinting some entities for matching and then structural-hashing all entities for WL seeds, do one pass that serves both purposes.

### Changes

**`athar/similarity_seed.py`** — `text_fingerprint_pairs` now:
- Fingerprints **all** entities (not just non-GUID), returning `old_all_fingerprints` / `new_all_fingerprints`
- Captures profile entities during the same loop, returning `old_profile_entities` / `new_profile_entities`
- GUID-bearing entities are still excluded from cross-file matching but ARE fingerprinted
- This single pass replaces: fingerprinting + `_graph_for_profile` + `structural_hash` + `build_seed_color_maps`

**`athar/identity_pipeline.py`** — `_precompute_identity_state` now:
- Accepts `precomputed_profile_entities` parameter (skips `_graph_for_profile` when provided)
- Fast path: when `len(seeded_colors) >= len(id_entities)`, builds `profile_hashes` by simple dict lookup instead of calling `structural_hash`

**`athar/diff_engine_context.py`** — `prepare_diff_context` now:
- Uses fingerprints directly as WL initial colors (no `build_seed_color_maps`)
- Forwards precomputed profile entities to identity precompute
- Wraps hot allocation phases in `gc.disable()` / `gc.enable()` to reduce GC pauses (37s → expected ~5s for `index_by_identity`)
- Total step count reduced from 23 to 22

### What was eliminated

| Eliminated work | Estimated savings |
|---|---|
| `structural_hash` for ~1.8M unmatched entities (73s + 83s) | ~156s |
| Redundant `entity_for_profile` calls in `_graph_for_profile` (×2 sides) | ~30s |
| `build_seed_color_maps` sha256 wrapping | ~1.2s |
| GC pauses during hot dict-allocation phases | ~30s (estimated) |
| **Total estimated savings** | **~217s** |

### Correctness argument

- Text fingerprints (xxh3_128) and structural hashes (sha256) compute the same logical value: `hash(entity_type + canonical_attrs_stripped_refs + edge_multiset)`. They differ only in hash function.
- WL initial colors only need to distinguish structurally different entities within a single run — any collision-resistant hash works. xxh3_128's 128-bit output has negligible collision probability for 1M entities.
- Final entity IDs (`H:...`) change because WL converges from different initial colors, but the partition quality is equivalent — all determinism tests pass.
- Fingerprint-based cross-file matching still excludes GUID-bearing entities (unchanged).

## Key insight for future work

When two code paths compute `f(entity)` with the same logical function but different hash backends, unify them. The "seed" abstraction layer (`build_seed_color_maps`) that sha256-wrapped fingerprints was pure overhead — fingerprints ARE valid WL seeds directly.

## Remaining bottlenecks (for next optimization pass)

The ~670s baseline should drop to ~450s estimated. Remaining large costs:
- WL refinement: ~54s + ~49s (already on xxh3_64, 3-round cap for >1M entities)
- Fingerprinting pass: ~167s → should be ~80-90s now (single pass, no redundant profile work)
- `index_by_identity`: ~37s (GC fix should help significantly)
- `emit_base_changes`: ~36s
- Matching stages: fast (~2s)

Further wins require: deferred `_build_compare_entities`, flattened `_index_by_identity`, and possibly Rust for the hash inner loops.
