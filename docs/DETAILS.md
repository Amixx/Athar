# Detailed Documentation

## How it works

### Entity matching

Entities are matched across file revisions by **GlobalId** — the stable IFC identifier that persists across saves and exports.

**Content-based fallback for regenerated GUIDs**: Some authoring tools regenerate GlobalIds on re-export, IFC roundtripping, or copy/paste operations (see [Zhao et al. 2020](https://onlinelibrary.wiley.com/doi/10.1155/2020/8782740), [buildingSMART forum discussion](https://forums.buildingsmart.org/t/guids-in-an-bim-project/2593)). When Athar detects low GUID overlap between two files (<30% of old GUIDs present in the new file), it automatically activates a three-stage content-based matching pipeline:

1. **Exact signature match**: Entities with a unique combination of (ifc_class, name, type_name, container, groups, properties) are matched directly.
2. **Positional disambiguation**: When multiple entities share the same content signature (e.g., 20 identical windows), placement position is used to break ties.
3. **Fuzzy scoring**: Remaining unmatched entities are scored on name, type, container, group overlap, property overlap, and placement proximity. Only high-confidence matches with clear winners are accepted.

The matcher is conservative: ambiguous cases are left unmatched (appearing as add+delete in the diff) rather than risking a wrong pairing. When content-based matching is used, the diff output includes `match_method: "content_fallback"` and `guid_overlap` fields.

Folder mode also uses a content-based fallback for version grouping: when GUID overlap is low, files are grouped by `(ifc_class, name)` fingerprint overlap instead.

### What it compares

- IFC class and name
- Direct attributes
- Property sets (psets) and their properties
- Placement matrices (with float tolerance) — changes described as distance + compass direction + nearby entities
- Spatial container assignment
- Type assignment

**Bulk movements**: When ≥5 entities share the same displacement vector, they are automatically grouped instead of listed individually.

### Folder mode

Pass a directory and Athar will:

1. Scan all `.ifc` files in the folder
2. Group them by **entity GUID overlap** (files sharing >50% of IfcProduct GlobalIds are versions of the same model)
3. Sort each group by **IFC header timestamp** (embedded in the file, survives transfers)
4. Diff consecutive pairs within each group, plus a cumulative first→last diff

This correctly handles folders with multiple unrelated models — each gets its own group. Files without a matching version pair are skipped.

### File metadata

The parser extracts file-level metadata (schema, timestamp, organization, application) and per-entity `OwnerHistory` (who modified each element, when, via which app). This is included in JSON output under `metadata` and on each entity under `owner_history`.

**Metadata sources (in priority order):**

- IFC header fields (`file_name.time_stamp`, etc.)
- `IfcApplication` entities (the authoring tool)

## Scripts

| Script | Description |
|--------|-------------|
| `scripts/inspect_ifc.py` | Print summary stats for an IFC file (types, counts, sample entities) |
| `scripts/make_modified_ifc.py` | Generate a modified IFC with known changes (for testing) |
| `scripts/diff_folder.py` | Diff all versions in a folder sequentially (oldest→newest). Superseded by built-in folder mode. |
| `scripts/inspect_ifc_identity.py` | Show project name/GlobalId and header timestamp for IFC files |
| `scripts/inspect_guid_overlap.py` | Show entity GUID overlap matrix between IFC files |

## Test data

- `data/` — Sample IFC files (Architecture, HVAC, Landscaping, Structural) from a single building model.
- `real-world-test/` — Three versions of Building-Landscaping, edited in Bonsai (Blender IFC plugin):
  - `v0-original` → `v1`: deleted `geo-reference` proxy, moved `origin` and `tree`
  - `v1` → `v2`: deleted `origin` proxy
