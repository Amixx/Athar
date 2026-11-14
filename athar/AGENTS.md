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
  configuration error. Policy normalization rejects unknown keys, malformed
  values and unknown IFC classes before diffing; class rules include subtypes.
  Property-level gates (`forbid_property_removal`,
  `forbid_property_value_change`, `forbid_property_addition`) require the
  report's `property_deltas` on data-changed entities. Absence is ambiguous
  (comparison unavailable or no property changes) and raises an error.
  Property deltas are keyed by (pset, property name, inherited); type-level
  values are `inherited: true` entries beside the occurrence's own values.
  `forbid_property_value_change` keys are property names and match that name
  in every pset and origin; each violating entity lists all matching
  `changes` (pset, inherited, old/new value). Removed values and remove/add
  pairs with different values violate the rule; equal-value moves do not.
  `count_changed` entries for duplicate property keys fail closed.
  `property_removed` lists removed properties with their pset and origin.
  `max_placement_cohort_share` fails when one report placement cohort exceeds
  the configured share of matched products. Schema mismatch is a policy
  failure returned by the CLI because the engine cannot compare schemas.
  `resolve_policy` accepts a file path or a shipped pack name; packs live as
  JSON in `athar/policies/` and are wheel-bundled package data.
- `__main__.py` is the top-level `athar` command. `main(argv) -> int` parses
  only the first argument against the subcommand list (`diff`, `check`, `git`,
  `view`, `store`, `rekey`) plus `--help`/`--version` (the `athar-ifc`
  distribution version), then hands the rest to the subcommand's own parser;
  integration subcommands import their packages lazily. A first argument that
  is an option, ends in `.ifc`, or names an existing path means `diff`, so
  `athar OLD NEW` stays the raw JSON diff (streaming, generated-at audit
  timestamps); any other unknown word is an argparse error.
- `rekey.py` is an experimental serialization utility, not a semantic matcher
  tier. It anchors all unique rooted GUIDs plus the existing product matches,
  propagates same-attribute and inverse-relationship roles, and pairs remaining
  exact resource content, unique content first so roles reach duplicates.
  Equal-content duplicate children use attribute positions in ordered aggregates;
  unordered child buckets must have equal sizes. Role-only child keys must be
  unique on both sides.
  Remaining equal-content twins pair only in equal-size buckets with no
  incoming NEW references and identical NEW bodies after all outgoing references
  resolve through established mappings. Leaf twins satisfy the outgoing check.
  These pairings never enter the role-propagation queue. Other twins get fresh
  ids and are counted as `unmatched_twins`. A final role pass pairs entities
  whose role key is unique among the unmatched on both sides, followed by
  unique-content recovery and propagation until no unique-content pair remains.
  No pass gives an `IfcObjectDefinition` the id of one with a different
  `GlobalId` (counted as `globalid_refusals`), Athar's
  GUID-free signature matches included: the rekey content hash of a uniquely
  GUIDed root is its GUID, so content cannot vouch for a regenerated one, and
  such a product ends as a refused rooted addition. Property definitions and
  relationships may still pair across regenerated GUIDs. Role guesses never
  affect diff reports.
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

The full vector is the three hashes plus the placement matrix or, for an
unresolved chain, its placement digest.

Each tier only examines still-unmatched pools and emits disjoint 1:1 pairs.
Never match across canonical classes. Do not reintroduce topology-only,
spatial-nearest, candidate fan-out, or tuning-knob matching without fresh
corpus evidence and tests.

## Report Contract

- Sections are `added`, `deleted`, `modified`, and `unchanged`.
- Matched items include `match{score,reason}` plus per-aspect
  `geometry`, `data`, `topology`, and `placement` states.
- Modified items include `placement_delta_mm`, `data_hash{old,new}`, and
  `change_scope` (`intrinsic`, `transitive`, `mixed`, `none`). `data` is unchanged if and only if `data_hash.old == data_hash.new`; each hash-backed aspect is changed exactly when its hash differs.
- Editing a type representation map reports `geometry` and `data` on an occurrence that maps it, with the map's `(shape)` fact in `data_delta`; an occurrence that does not map it reports `data` only. Editing a swept profile shared with a material profile reports `geometry` and `data`; an untouched equal copy in the material profile leaves `data` unchanged.
- A data change always carries a non-empty `data_delta` of
  `{path, old, new}` entries. A path is the owners (entities with a
  GlobalId below the entity) and the positions of the ordered lists passed
  on the way from the entity (`IfcPropertySet[Name=...]/`,
  `MaterialLayers[2]/`), then the class and attribute holding the value; the
  entity itself is `Class[GlobalId=...]`. Repeated paths are numbered in
  value order. A value longer than 96 bytes
  is bounded: a text keeps its first 96 bytes followed by
  `… (N bytes, sha256:<digest>)`, any other value (a colour list, a table)
  is given as its byte length and a digest. Shape content that counts as
  data (a context's `TrueNorth`, a material profile's profile) is listed in
  its canonical encoding, with one `(shape)` digest for the geometry below
  it. When every fact
  is equal and only the attachment differs (an unordered member moved between
  two owners of one class that have no GlobalId), the single entry is `<entity>.(structure)` with
  the two data hash prefixes.
- A placement chain that passes through an `IfcGridPlacement`, or that ends
  in a `PlacementRelTo` cycle, is not evaluated. The entity summary then
  carries `placement_unresolved: true`, no matrix exists,
  `placement_delta_mm` is `null`, and the `placement` aspect compares a
  digest: for a grid placement its content (intersections, axes, offsets),
  the placements of the grids owning those axes, and the local chain below
  it; for a cycle the relative placements around it.
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
- `IfcProject` is reported like any other entity. Units, representation
  contexts (`WorldCoordinateSystem`, `TrueNorth`) and georeferencing are its
  data, so a unit, north or map-conversion change modifies the project and no
  element.
- `stats.coverage` says what no signature examined, so a difference outside
  the examined graph is named instead of reported as `unchanged`:
  - `old` / `new`: per file, `unknown_relationships` (`{class: count}` of
    relationship instances whose class has no policy entry), `unreached`
    (`{class: {count, digest}}` for entities no signature root reaches,
    relationships with empty or dangling role attributes included;
    `digest` is order-independent over the instances' content) and
    `ignored_by_design` (`{class: count}`: provenance reached only through
    `OwnerHistory`, and relationship classes the policy ignores).
  - `unexamined_changes`: sorted class names whose `unreached` count or digest
    differs between the two files.
- `orphan_property_sets{added,removed}` lists property set definitions that no
  `IfcRelDefinesByProperties` or `IfcTypeObject.HasPropertySets` references.
  They have no signature, so they are compared as a multiset of content keys
  (GlobalId and OwnerHistory excluded): exporters regenerate orphan GlobalIds
  on every export (Revit, 2026-10: ~14k orphans per file). Surplus copies of
  one content are reported from the end of the file. `stats.orphan_property_sets`
  carries `{old, new, added, removed}` counts; streams emit
  `orphan_property_set_added` / `_removed` records.
- `property_deltas` compares properties per (pset name, property name). A key
  can have several definitions: on the occurrence and on its type, or in two
  sets of one name. All are kept (`PropertyEntry` carries `inherited` and the
  set's `pset_guid`). The effective value is the occurrence's when it defines
  the key, otherwise the type's; when definitions of the effective origin
  disagree it is the sorted list of their distinct values.
  - `changed`: the effective value or its unit differs. `added` / `removed`:
    the key exists on one side only. Each carries `inherited`, the origin of the
    effective value, and `old_definitions` / `new_definitions`
    (`{origin, pset_guid, value}`) when a side has more than one definition.
  - `origin_changed` (`old_origin`, `new_origin`: `occurrence` or `type`):
    the effective value comes from the other origin, reported whether or not
    the value differs. Removing an occurrence's override of a key its type
    also defines leaves the key in place with the type's value: it is an
    `origin_changed` entry and a `count_changed` entry for the occurrence,
    plus a `changed` entry when the two values differ, never `removed`.
  - `overridden_changed`: the type's value differs while the occurrence
    overrides it on both sides.
  - `count_changed`: the number of definitions of one origin
    (`inherited`) differs, so an object gaining a second identical property
    set is not a silent data change.
- Numbers shown for people are real numbers in SI units, never the quantized
  integers that are hashed. A real in `data_delta` is the hashed quantity
  printed as a decimal with the unit its measure is normalized to
  (`IFCLENGTHMEASURE: 3.25 m`, `m2`, `m3`, `rad`; a real without a measure
  has no unit), so two values print alike exactly when they hash alike.
  `property_deltas` values of length, area, volume, plane angle, time and
  mass measures and of the matching quantities are converted from the file's
  unit (or the unit the property or quantity names itself) and the entry
  carries `unit` (`m`, `m2`, `m3`, `rad`, `s`, `kg`); other values are as
  written, without `unit`. A value is also as written, without `unit`, when
  the file's unit of the measure's type, or the unit the property names, is
  not an `IfcSIUnit` or an `IfcConversionBasedUnit` (an
  `IfcContextDependentUnit` has no factor), or the file declares none. A `changed` entry whose two
  sides have different units carries `old_unit` beside `unit` (either may be
  `null`), and a value whose unit changed is `changed` even when the number
  is equal. Only single values, enumerated values and quantities are
  extracted; bounded, list and table values appear in `data_delta` only. `placement_delta_mm` is in millimetres whatever
  the file's length unit.
- `data_delta` paths list content below an owner under that owner
  (`IfcWallType[Name=...]/IfcPropertySet[Name=...]/IfcPropertySingleValue.NominalValue`),
  so a property fact names the set and, for a type-level value, the type it
  comes from. An owner is labelled by class and `Name`, not by GlobalId,
  which no hash covers: a set or type whose GlobalId was regenerated keeps
  its paths. Owners of one entity whose labels would be equal (two sets of
  one name) carry `[Name=...,GlobalId=...]`.
