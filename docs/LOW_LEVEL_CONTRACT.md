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
- `H:<sha256>` for non-root structural identity.
- `C:<sha256>` for unresolved indistinguishable/ambiguous equivalence classes.

`identity.match_method` currently includes:
- `exact_guid`
- `root_remap`
- `path_propagation`
- `secondary_match`
- `equivalence_class`
- `exact_hash`

## Wire Schema (v2)

Top-level object:

- `version` (`"2"`)
- `profile`
- `schema_policy`
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
