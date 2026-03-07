# Athar

Semantic IFC diff tool. Compares BIM models at the entity/property level, not text level.

*Athar (Arabic: أثار) — a trace left behind.*

## Architecture

- `athar/parser.py` — Extracts structured entity data from IFC files using ifcopenshell. Each element is keyed by GlobalId and includes type, name, attributes, property sets, placement, spatial container, type assignment, owner history, and group memberships (from `IfcRelAssignsToGroup`). Also extracts file-level metadata (schema, timestamp, org, application). Returns `{"metadata": {...}, "entities": {guid: {...}, ...}}`.
- `athar/differ.py` — Compares two parsed models. Produces added/deleted/changed/bulk_movements buckets with per-property granularity. Includes file metadata from both sides in the output. Placement changes include a human-readable `description` field. Bulk movement detection groups entities whose only change is placement and share the same displacement vector (within 1mm). Groups of ≥5 are collapsed into a single bulk movement record with class breakdown; smaller groups stay as individual changes.
- `athar/placement.py` — Human-readable descriptions of placement changes. Converts 4x4 matrix diffs into distance + compass direction ("moved 7.6m northwest") and finds nearest named entities for relative context ("from near 'tree' to near 'house'"). Uses IFC convention: +Y=north, +X=east. Distances in mm internally, displayed in meters.
- `athar/folder.py` — Folder scanning and version grouping. Groups IFC files by entity GUID overlap (>50% shared IfcProduct GlobalIds = same model). Sorts each group by IFC header timestamp. Uses union-find for transitive grouping.
- `athar/cli.py` — CLI entry point. Two modes: two-file diff, or folder mode (auto-groups and diffs versions). `--summary` shows a condensed human-readable overview (counts + IFC class breakdown + top changed fields). `--summary --verbose` expands to per-entity details. `--report FILE` writes a Markdown report.
- `athar/report.py` — Markdown report generation. Produces structured `.md` files with tables, emoji markers, and class breakdowns. Supports both two-file and folder (version history) modes. `--verbose` adds per-entity detail tables; default is class-level breakdown. No extra dependencies.

## Conventions

- Python 3.10+
- Use `ifcopenshell` for all IFC parsing. Do not parse STEP files as text.
- Match entities across files by GlobalId (the stable identifier across revisions).
- Keep diffing deterministic and algorithmic — no AI in the diff pipeline itself.
- Output structured JSON. Human-readable summaries are a presentation concern, not a diff concern.
- No deep B-rep geometry comparison. Compare placement matrices and geometric parameters only.
- Minimal dependencies. Only `ifcopenshell` and stdlib.

## Running

```bash
python -m athar old.ifc new.ifc                # two-file diff (JSON)
python -m athar old.ifc new.ifc -o diff.json   # write to file
python -m athar old.ifc new.ifc --summary      # human-readable summary (condensed)
python -m athar old.ifc new.ifc --summary --verbose  # expanded per-entity details
python -m athar old.ifc new.ifc --report diff.md     # Markdown report
python -m athar some-folder/                   # folder mode: auto-group + diff (summary by default)
python -m athar some-folder/ --report report.md      # folder mode: Markdown report
```

## Scripts

- `scripts/inspect_ifc.py` — Print summary stats for an IFC file (types, counts, sample entities).
- `scripts/make_modified_ifc.py` — Take an IFC file and produce a modified copy with known changes for testing.
- `scripts/diff_folder.py` — Diff all IFC versions in a folder sequentially (oldest→newest by mtime), prints step-by-step + cumulative summary. Superseded by built-in folder mode (`python -m athar folder/`).
- `scripts/inspect_ifc_identity.py` — Show project name/GlobalId and header timestamp for IFC files. Useful for understanding how files would be grouped.
- `scripts/inspect_guid_overlap.py` — Show entity GUID overlap matrix between IFC files. Used to validate the >50% grouping threshold.

## Testing

```bash
python -m pytest tests/
```

## Dev practices

- Don't write throwaway scripts. If you need to do something exploratory, save it as a reusable script in `scripts/`.
- **Preserve knowledge during feature work.** After implementing a feature, update README.md and AGENTS.md with: what was built, why, what was tried and didn't work, known limitations, and any domain insights learned. This is mandatory — future context depends on it.

## Known IFC domain insights

- `IfcOwnerHistory` on each element tracks per-entity modification timestamps and change actions (`ADDED`, `MODIFIED`, `NOCHANGE`). This is included in JSON output but not displayed in human-readable summaries.
- The `IfcApplication` entity identifies the tool used. When multiple tools have touched a file (e.g., SketchUp → Bonsai), multiple `IfcApplication` entries exist; the last one is typically the most recent editor.
- `IfcRelAssignsToGroup` links elements to named groups (e.g., Revit "Model Groups"). Group names often include a trailing Revit element ID (e.g., "Model Group:Mockup Buildings NDS:379102") which is stripped during parsing. Bulk movements display the common group name when all entities share one.
- IFC is standardized in schema (ISO 16739) but wildly inconsistent in practice across authoring tools. Spatial constructs like `IfcSpace`, `IfcZone`, `IfcSystem` are often absent or poorly populated. `IfcGroup` (via Revit Model Groups) is one of the more reliable grouping mechanisms in real-world files.
