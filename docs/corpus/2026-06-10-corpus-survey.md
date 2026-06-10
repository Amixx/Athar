# Corpus survey — 2026-06-10

**Historical record.** Facts gathered by `scripts/explore/corpus_survey.py`
against the *pre-simplification* engine as of commit `bb4d58c` (tiers: guid /
geometry_hash / tier2_signature / spatial_fallback). Raw output:
`2026-06-10-corpus-survey.json` (same vintage). This survey motivated the
matcher simplification recorded at the end; the current engine has only the
guid and geometry_hash tiers, so the fallback-tier columns below cannot recur.
The survey script has since gained the `corpus/gni-bim-sample/` files (11
independent 2025 BIM Fundamentals models, 5 architecture/structure 2026 BIM
Projects pairs); no post-simplification survey has been generated yet.

## Inventory

Roots: repo `real-world-test/`, `data/`, `tests/fixtures/`, external
`../vscode-ifc/test-files` (override `ATHAR_EXTERNAL_CORPUS_DIR`). All repo
IFCs are git-lfs. Duplicates across roots (by content): `AdvancedProject.ifc`
(data/ = real-world-test/ = external copy ±1 trailing byte), Building-Landscaping
v0 = external `001…Building-Landscaping.ifc`, Duplex-Architecture, uni-house,
spanish, tiny.

| key | schema | MB | signatures | bundle build (s) | notes |
|---|---|---:|---:|---:|---|
| tiny_no_products | IFC4 | 0.001 | 0 | 0.0 | checked-in fixture, no products |
| cube_brep | IFC4 | 0.011 | 2 | 0.0 | external |
| sample_house_roof | IFC4 | 0.063 | 5 | 0.1 | external |
| building_arch | IFC4 | 0.23 | 22 | 0.1 | external; same sample project as bl_* |
| bl_v0..v3 | IFC4 | 1.2–1.5 | 7–9 | 0.5 | revision chain, Archicad re-exports |
| duplex_arch | IFC2X3 | 2.4 | 295 | 2.7 | |
| duplex_mech | IFC2X3 | 8.8 | 529 | 9.8 | external; same project as duplex_arch |
| revit_arc | IFC4 | 13.6 | 560 | 17.6 | external |
| duplex_mep | IFC2X3 | 17.9 | 973 | 21.8 | external; ⊇ mechanical content |
| revit_mep | IFC4 | 29.2 | 15639 | 34.1 | external; 55 sigs missing centroid |
| advanced_project | IFC2X3 | 44.3 | 2365 | 82.8 | |
| adv_changed | IFC2X3 | 43.1 | 2360 | 68.6 | real authoring revision of advanced_project |
| basic_house | IFC2X3 | 52.7 | 177 | 98.3 | geometry-heavy, few products |
| uni_house | IFC2X3 | 55.9 | 6385 | 125.8 | |
| real_world_big | IFC2X3 | 168.0 | 4680 | 954.9 | external |
| spanish | IFC2X3 | 181.8 | 44389 | 968.8 | |
| spanish_bad | IFC2X3 | 181.8 | — | — | external; one ref mangled to `#299…9` (dangling-ref probe; ifcopenshell silently repairs to `()` — see below) |

Bundle build dominates wall time: roughly 0.5–0.9 MB/s for small/medium
files, degrading to ~0.18 MB/s on the two ~170–180MB models (~16 min each).
Matcher+report stays in low seconds even at 44k signatures (spanish
same-file 1.2s, spanish↔uni 1.3s). Full survey: 2431s. Every file: 0 GUID
collisions, 0 missing GUIDs, 0 missing topology hashes, 0 parse dangling
refs/cycle breaks.

## Pair results

Same-file diffs (every usable surveyed file): zero added/deleted/modified,
100% of matches via the guid tier, `probe_capped` 0 corpus-wide.

GUID-scramble pairs (bl_v2, sample_house_roof, duplex_arch, revit_arc):
zero diff; 100% of identity recovered by `geometry_hash` full-vector
equality. `tier2_signature` and `spatial_fallback` contributed nothing.

| kind | pair | A / D / M / U | matched by tier |
|---|---|---|---|
| revision | bl_v0→v1 | 0 / 1 / 4 / 4 | guid 8 |
| revision | bl_v1→v2 | 0 / 1 / 7 / 0 | guid 7 |
| revision | bl_v2→v3 | 3 / 1 / 6 / 0 | guid 6 |
| revision | bl_v0→v3 | 3 / 3 / 6 / 0 | guid 6 |
| revision | advanced_project→adv_changed | 0 / 5 / 2329 / 31 | guid 2360 |
| discipline | building_arch→bl_v0 | 5 / 18 / 3 / 1 | guid 4 |
| discipline | duplex_arch→duplex_mech | 507 / 273 / 22 / 0 | spatial 22 (zip 16 + nearest 6) |
| discipline | duplex_mech→duplex_mep | 599 / 155 / 374 / 0 | guid 132, spatial 242 (zip 140 + nearest 102) |
| discipline | duplex_arch→duplex_mep | 951 / 273 / 22 / 0 | spatial 22 |
| discipline | revit_arc→revit_mep | 15639 / 560 / 0 / 0 | none |
| unrelated | cube_brep→sample_house_roof | 4 / 1 / 1 / 0 | spatial 1 (zip) |
| unrelated | sample_house_roof→building_arch | 21 / 4 / 1 / 0 | spatial 1 (zip) |
| unrelated | basic_house→advanced_project | 2362 / 174 / 3 / 0 | spatial 3 (zip) |
| unrelated | spanish→uni_house | 6385 / 44389 / 0 / 0 | none (fully disjoint, not even spatial) |

Observations on the real revision pair (advanced_project→adv_changed):

- Identity carried 100% by GlobalIds (2360/2360); structural tiers never
  needed. The same holds across the whole bl_v0→v3 chain.
- `modified` scope split (as first surveyed): 674 intrinsic, 1431 mixed, 224
  transitive-only, 31 fully unchanged, max placement delta "~299 km". These
  numbers were contaminated by the parser nondeterminism described below —
  phantom ×1000 attribute flips manufactured fake data/topology diffs and the
  delta unit was inflated ×1000. Re-measured post-fix: 2162 modified
  (1659 intrinsic, 446 mixed, 57 transitive-only), 198 fully unchanged, max
  placement delta 299.33 m — the revision relocates the project base point,
  so placement change is near-global. Transitive-only ripple is ~3% of
  `modified`, not the dominant component.
- Cross-class matches: 0 in every pair (engine invariant held corpus-wide).

## Suspicious patterns (pre-simplification engine)

- `tier2_signature` matched **zero** entities in every pair, including all
  scrambles. The (class, topology)-unique tier had no corpus evidence of
  value.
- `spatial_fallback` **never fired on a same-project revision pair**. Every
  firing was cross-model: 22/22/3/1/1 matches of spatial-structure containers
  (IfcSpace/IfcBuildingStorey/IfcBuilding/IfcSite at identical or near
  positions, e.g. default origin placements) across discipline exports or
  unrelated projects, plus 242 federation-style matches in
  duplex_mech→duplex_mep (which shares 132 GlobalIds with it — a
  subset/union export, not a revision). Cross-project container matches are
  false positives presented as score-0.5 "modified, mixed" entries.
- Conflict downgrades fired only on those spatial matches; the
  `tier2_signature` arm of the downgrade rule was unreachable (a tier2 match
  has equal topology by construction, so it can never be transitive/mixed).
- `modified_score_bands` was a bijective re-encoding of
  `modified_match_reasons` (each tier has a fixed score), and with the
  fallback tiers removed both stats are degenerate: `geometry_hash` matches
  are by construction `unchanged`, so every `modified` item is a guid match.
- `spatial_probe_limit` (probe_capped) never tripped on any file, including
  the 180MB model with 11.5k signatures and real_world_big with 598k.
- ifcopenshell silently repairs out-of-range entity references (the
  spanish_bad corruption) to empty aggregates: no parse error, no
  `dangling_refs` diagnostic; the damage surfaces as a legitimate transitive
  topology diff vs the clean file (covered by a default-tier test on a
  generated fixture).

## Parser nondeterminism found by the at-scale scramble test (fixed)

The 44MB AdvancedProject GUID-scramble acceptance test initially failed (1716
added/deleted instead of 0) even though every small scramble passed. Root
cause, in `athar/bottom/parser.py`:

- `_unwrap_named_type` / `_measure_type_from_attr_type` used `id()`-based
  seen-sets for cycle detection while walking EXPRESS type chains. Each
  `declared_type()` call mints a transient SWIG proxy; once the walk advanced,
  the previous proxy was freed and its heap address recycled, so `id()` of a
  fresh node could collide with a dead one — a phantom cycle that broke the
  walk at a random depth. Double-parsing the identical 44MB file produced 195
  entities whose quantized length attributes flipped ×1000 (mm→m conversion
  randomly applied or skipped) run to run.
- Worse, the call site detected the measure name from the **fully unwrapped**
  base type (`str`/`float` at the bottom of the chain), where no `*Measure`
  name exists. Unit conversion for typed scalar attributes (e.g.
  `IfcExtrudedAreaSolid.Depth`) only ever fired *through* the phantom-cycle
  bug stopping the walk early at a measure-named node (measured: ~38% of
  extractions on one entity, ~0.1% on the bare schema walk — allocator
  roulette).

Fix: hop-capped walks with no `id()` tracking, and measure detection now runs
top-down from the outermost attr type, returning the first `*Measure` name.
Consequence: typed scalar measures (and measure-typed aggregate items such as
`IfcCartesianPoint` coordinates) are now deterministically unit-converted, as
the canonicalization contract always claimed, and `placement_delta_mm` is now
actually millimetres for mm-based files (it was inflated ×1000). Per-file
signature counts and matcher tier distributions above are unaffected; the
adv revision pair's modified/unchanged/scope splits shifted once the phantom
diffs vanished (corrected inline above: 44MB scramble now zero-diff, 198
truly-unchanged matches instead of 31). Every same-file diff in the original
survey masked the bug via the in-process bundle cache (one parse, both
sides); only the two-parse at-scale scramble caught it. Regression guard:
`test_measure_detection_is_deterministic_and_drives_unit_conversion`
(default suite, file-free, hammers the walkers 5000×).

## Actions taken (see AGENTS.md for current semantics)

- Deleted matcher tiers `tier2_signature` and `spatial_fallback` plus the
  `radius_m`/`spatial_probe_limit` knobs, the `matcher_policy` plumbing, and
  the `--matcher-radius-m` CLI flag. Matching is now: unique GlobalId
  (same class), then full signature-vector equality. Everything else is
  conservative added/deleted.
- Deleted the conflict-downgrade rule and the `modified_score_bands` /
  `modified_match_reasons` / `modified_conflicts` stats.
- Kept: `change_scope` (intrinsic/transitive/mixed — the adv pair shows real
  consumers need the ripple filter), duplicate-GUID diagnostics, per-aspect
  hashes, placement deltas.
- Known accepted limitation: a pipeline that regenerates GlobalIds **and**
  edits content in the same revision now collapses to added+deleted (no
  corpus pair demonstrates that scenario; revisit only with real evidence).
