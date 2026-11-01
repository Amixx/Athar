# Core Engine Agent Notes

`athar/` is the pure engine package: parse -> signatures -> matching -> delta
report. It must stay independent of Git integration, terminal rendering,
GitHub comments, and other presentation concerns.

## Module Boundaries

- `engine.py` orchestrates the pipeline. `diff_files()` builds one
  `SignatureBundle` per input, enforces same-schema comparison, runs the
  matcher, and assembles the report. Its in-process bundle cache is keyed by
  path, mtime, and size.
- Same-schema enforcement raises `SchemaMismatchError` (a `ValueError`
  subclass carrying `old_schema`/`new_schema`). The engine never translates
  between IFC2X3 and IFC4, so a cross-schema pair is surfaced as a clear
  "schema changed, cannot semantically diff" result rather than diffed into a
  flood of false add/delete churn. The core CLI catches it, prints a
  `status: "schema_incompatible"` JSON document, and exits `3` (distinct from
  generic error exit `1`).
- `stream_diff_files()` wraps the same report as `ndjson` or `chunked_json`
  records. Keep the header audit metadata and deterministic final stats record.
- `matcher/` may depend on `athar/bottom/` types, but matching must remain
  tiered pool reduction with no candidate-list scoring pass or assignment step.
- `delta/` may depend on bottom and matcher types. It owns report sections,
  aspect states, placement deltas, data hashes, change scope, and stats.
- `check.py` evaluates JSON reports or fresh diffs against report-visible
  policy signals only. Exit `2` is policy violation; exit `1` is execution or
  configuration error. Property-level gates (`forbid_property_removal`,
  `forbid_property_value_change`) read the report's `property_deltas`, so they
  only fire when `delta/report.py` emitted them (a `property_index` was present).
  Property deltas are keyed by (pset, property name, inherited); type-level
  values are `inherited: true` entries beside the occurrence's own values.
  `forbid_property_value_change` keys are property names and match that name
  in every pset and origin; each violating entity lists all matching
  `changes` (pset, inherited, old/new value). `property_removed` lists
  removed properties with their pset and origin.
  `resolve_policy` accepts a file path or a shipped pack name; packs live as
  JSON in `athar/policies/` and are wheel-bundled package data.
- `__main__.py` is the minimal CLI for raw JSON, streaming output, generated-at
  audit timestamps, and `check`.
- `rekey.py` is an experimental serialization utility, not a semantic matcher
  tier. It anchors all unique rooted GUIDs plus the existing product matches,
  propagates same-attribute and inverse-relationship roles, and pairs remaining
  exact resource content, unique content first so roles reach duplicates before
  the rest pair in STEP order. Role guesses never affect diff reports.
  The native tokenizer preserves literal spellings while replacing ID tokens;
  IfcOpenShell verifies the entire output graph before an atomic file replace.
  Do not substitute a normal serializer: changed numeric/escape spellings can
  dominate the text diff and expose the native hasher's undecoded STEP strings.
  Unmatched rooted additions are refused before output publication: independent
  rekeys cannot deduplicate additions shared by both branches in ifcmerge.
  Changed records are refused unless ifcmerge's `_dissemble` (ported
  in `rekey.py`) splits both lines into exactly their attributes; ifcmerge
  truncates a record changed on both branches when it cannot. Records spanning
  lines are refused too, since ifcmerge's line reader drops them.
  Refusal JSON includes candidate match statistics; no merge-ready file exists.

## Matching Contract

Current tiers, strongest first:

1. `guid`: unique `GlobalId` on both sides plus same canonical class. Score
   `1.0` when the full vector is identical, otherwise `0.9`.
2. `geometry_hash`: same-class full signature-vector equality, zipped in stable
   STEP order for interchangeable entities. Score `0.8`.

Each tier only examines still-unmatched pools and emits disjoint 1:1 pairs.
Never match across canonical classes. Do not reintroduce topology-only,
spatial-nearest, candidate fan-out, or tuning-knob matching without fresh
corpus evidence and tests.

## Report Contract

- Sections are `added`, `deleted`, `modified`, and `unchanged`.
- Matched items include `match{score,reason}` plus per-aspect
  `geometry`, `data`, `topology`, and `placement` states.
- Modified items include `placement_delta_mm`, `data_hash{old,new}`, and
  `change_scope` (`intrinsic`, `transitive`, `mixed`, `none`).
- Stats include section counts, signature counts, parse diagnostics, edge
  stats, modified change-scope counts, placement cohorts, dropped matches,
  GUID collisions, and matcher diagnostics.
- `stats.placement_cohorts` groups placement-changed pairs by
  `placement_delta_mm` quantized to a fixed 0.1 mm grid: cohorts of 2+ members
  (`{delta_mm, count, members}`, `delta_mm` = the grid key, members = new-side
  step ids, largest cohort first) mark coherent group moves. It is a derived
  pattern, deliberately not a structural claim — exporters (Revit, 2026-07
  round-trip corpus) bake group moves into per-element placements, so no moved
  container exists in the file. The grid absorbs sub-0.1 mm exporter noise (a
  1 um rounding artifact split a real 45-element move) while staying
  deterministic: fixed grid, no adaptive tolerance, no chained near-matches.
  Zero-delta-on-grid placement changes (pure rotation / chain restructuring)
  and singletons never join a cohort.
- Because `geometry_hash` requires equal full vectors, every modified item is
  a GUID match by construction.
- `orphan_property_sets{added,removed}` lists property set definitions that no
  `IfcRelDefinesByProperties` or `IfcTypeObject.HasPropertySets` references.
  They have no signature, so they are compared as a multiset of content keys
  (GlobalId and OwnerHistory excluded): exporters regenerate orphan GlobalIds
  on every export (Revit, 2026-10: ~14k orphans per file). Surplus copies of
  one content are reported from the end of the file. `stats.orphan_property_sets`
  carries `{old, new, added, removed}` counts; streams emit
  `orphan_property_set_added` / `_removed` records.
- `count_changed` in `property_deltas` lists (pset, name, inherited) keys
  whose occurrence count differs, so an object gaining a second identical
  property set is not a silent data change.
