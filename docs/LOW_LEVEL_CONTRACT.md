# Low-Level Contract (Phase 0 Lock)

This document defines the locked interface for Athar's low-level diff layer.

## Profiles

- `raw_exact`
  - Preserve all explicit attributes/refs as parsed.
  - No volatility suppression.
- `semantic_stable` (default)
  - Volatility suppression rules:
    - Strip attribute `OwnerHistory` from all entities.
    - Strip refs with path prefix `/OwnerHistory`.
    - Normalize `IfcOwnerHistory` entities to empty payload:
      - `attributes = {}`
      - `refs = []`

Unsupported profile values are rejected with `ValueError("Unknown profile: ...")`.

## Canonical Value/Path Rules

- Canonical values are typed records (`kind`) produced by `athar/canonical_values.py`.
- Paths are JSON-Pointer-like (`/Attr/0/SubAttr`) and include aggregate indices.
- SELECT values carry explicit branch type (`{"kind":"select","type":"IfcLabel",...}`).
- `SET` and `BAG` are deterministically ordered by canonical JSON string.
- `LIST/ARRAY` preserve order.

## Identity Tiers

- `G:<GlobalId>` for valid, unique rooted entities.
- `G!:...` for deterministic disambiguated rooted IDs when `guid_policy=disambiguate`.
- `H:<sha256>` for non-root structural identity.
- `C:<sha256>` for unresolved indistinguishable/ambiguous equivalence classes.

WL refinement round hashing policy:

- Round hashing may use fast internal backends (`xxh3_64` or `blake3`) when configured/available.
- `auto` mode resolves deterministically in this order: `xxh3_64`, `blake3`, `blake2b_64`.
- External wire IDs (`G/H/C`) remain `sha256`-based.
- SCC ambiguity fallback uses bounded local refinement with default partition cap `K=128`; unresolved SCC partitions emit deterministic `C:` IDs.

`identity.match_method` currently includes:
- `exact_guid`
- `guid_disambiguated`
- `root_remap`
- `path_propagation`
- `secondary_match`
- `equivalence_class`
- `exact_hash`

## GUID Policy

- `fail_fast` (default)
  - duplicate or invalid `GlobalId` raises a `ValueError` with diagnostics.
- `disambiguate`
  - duplicate rooted GUIDs are assigned deterministic `G!:` IDs (`G!:<guid>#<ordinal>`).
  - invalid rooted GUIDs are assigned deterministic `G!:` IDs (`G!:INVALID#<ordinal>`).
  - change identity is tagged with `match_method="guid_disambiguated"` and `matched_on.stage="guid_disambiguation"`.

## Secondary Matcher

- Unresolved non-root matching uses entity-family candidate blocking (compatible ancestry such as `IfcWallStandardCase` vs `IfcWall`).
- Blocking/scoring includes ancestry and neighborhood digests.
- Small candidate blocks use deterministic min-cost bipartite assignment.
- Ambiguous small blocks use iterative deepening (`depth 1 -> depth 2 -> depth 3`) before ambiguity rejection.

Matcher-policy overrides are supported through API/CLI:

- `root_remap`
  - `guid_overlap_threshold` (0..1)
  - `score_threshold` (0..1)
  - `score_margin` (0..1)
  - `assignment_max` (>=1)
- `secondary_match`
  - `score_threshold` (0..1)
  - `score_margin` (0..1)
  - `assignment_max` (>=1)
  - `depth2_max` (>=1)
  - `depth3_max` (>=1, must be `<= depth2_max`)

## Wire Schema (v2)

Top-level object:

- `version` (`"2"`)
- `profile`
- `schema_policy`
- `identity_policy` (`guid_policy` + resolved `matcher_policy`)
- `stats`
- `base_changes`
- `derived_markers`

`base_changes` item fields:

- `change_id`
- `op` (`ADD|REMOVE|MODIFY|CLASS_DELTA`)
- `old_entity_id`
- `new_entity_id`
- `identity`
- `field_ops`
- `old_snapshot` (REMOVE only unless stream mode suppresses)
- `new_snapshot` (ADD only unless stream mode suppresses)
- `rooted_owners`
- `change_categories`
- `equivalence_class` (for `CLASS_DELTA`)

`derived_markers` currently:

- `REPARENT` with:
  - `relation_type`
  - `child_id`
  - `old_parent_id`
  - `new_parent_id`
  - `source_change_ids`

Qualified relation types for `REPARENT`:

- `IfcRelContainedInSpatialStructure`
- `IfcRelAggregates`
- `IfcRelNests`

## Streaming Contract

- `ndjson`: `header`, `base_change`, `derived_marker`, `end` records.
- `chunked_json`: `header`, chunked `base_changes`, chunked `derived_markers`, `end`.
- Stream mode suppresses ADD/REMOVE snapshots.
