# Athar Higher Layers

This directory contains the higher-level integration layers for Athar. These layers build upon the core diff engine to provide human-readable summaries, scene modeling, and folder-level versioning.

## Architecture

- `athar_layers/scene.py` — Builds a human-oriented "Scene Model" from parsed IFC data. Transforms raw entity data + relationships into labeled elements with spatial hierarchy, hosting chains (wall→opening→door/window), compass-based wall orientation, exterior/interior classification, and aggregation (roof→slabs, stair→flights). Generates human-readable labels like "exterior north-facing wall (Floor 0)" or "interior door in east-facing wall (Floor 0)". Exports `clean_name()` (public) for Revit name cleaning and `human_class()` for IFC class→category mapping. No LLM — purely rule-based and deterministic.
- `athar_layers/placement.py` — Human-readable descriptions of placement changes. Converts 4x4 matrix diffs into distance + compass direction ("moved 7.6m northwest") and finds nearest named entities for relative context ("from near 'tree' to near 'house'"). Uses IFC convention: +Y=north, +X=east. Distances in mm internally, displayed in meters. Includes `enrich_diff()` to add these descriptions back into a raw diff result.
- `athar_layers/folder.py` — Folder scanning and version grouping. Groups IFC files by entity GUID overlap (>50% shared IfcProduct GlobalIds = same model), with content fingerprint fallback (>60% `(ifc_class, name)` multiset overlap) when GUIDs are regenerated. Sorts each group by IFC header timestamp. Uses union-find for transitive grouping.
- `athar_layers/cli.py` — Full-featured CLI entry point. Two modes: two-file diff, or folder mode (auto-groups and diffs versions). `--summary` shows a condensed human-readable overview (counts + IFC class breakdown + top changed fields). `--summary --verbose` expands to per-entity details. `--report FILE` writes a Markdown report.
- `athar_layers/report.py` — Markdown report generation. Produces structured `.md` files with tables, emoji markers, and class breakdowns. Supports both two-file and folder (version history) modes. `--verbose` adds per-entity detail tables with scene labels; default is class-level breakdown with name/location sub-breakdowns in the Details column (e.g., "3× in west-facing wall (Floor 0)" for deleted windows, or name breakdown for bulk movements).

## Conventions

- Higher layers must never be imported by the core engine (`athar/`).
- Higher layers should enrich the core engine's raw JSON output for human consumption.
- Use `enrich_diff()` in `placement.py` to add human-readable descriptions to a raw diff result.

## Known IFC domain insights

- `IfcOwnerHistory` on each element tracks per-entity modification timestamps and change actions (`ADDED`, `MODIFIED`, `NOCHANGE`). This is included in JSON output but not displayed in human-readable summaries.
- The `IfcApplication` entity identifies the tool used. When multiple tools have touched a file (e.g., SketchUp → Bonsai), multiple `IfcApplication` entries exist; the last one is typically the most recent editor.
- `IfcRelAssignsToGroup` links elements to named groups (e.g., Revit "Model Groups"). Group names often include a trailing Revit element ID (e.g., "Model Group:Mockup Buildings NDS:379102") which is stripped during parsing. Bulk movements display the common group name when all entities share one.
- IFC is standardized in schema (ISO 16739) but wildly inconsistent in practice across authoring tools. Spatial constructs like `IfcSpace`, `IfcZone`, `IfcSystem` are often absent or poorly populated. `IfcGroup` (via Revit Model Groups) is one of the more reliable grouping mechanisms in real-world files.
- `IfcRelVoidsElement` + `IfcRelFillsElement` form the hosting chain: wall→opening→door/window. The scene model resolves this into direct wall→door/window relationships. Openings themselves are intermediary entities, not interesting to end users.
- Wall orientation is derived from the local X-axis of the placement matrix (which runs along the wall's length). The wall "faces" perpendicular to this direction. Works reliably for axis-aligned walls; diagonal walls get the nearest 8-point compass direction.
- `Pset_WallCommon.IsExternal`, `Pset_DoorCommon.IsExternal`, `Pset_WindowCommon.IsExternal` are the standard way to determine interior/exterior status. Swedish naming conventions ("Yttervägg"=exterior wall, "Innervägg"=interior wall, "Ytterdörr"=exterior door, "Innerdörr"=interior door) are also used as fallbacks.
- Revit element names follow the pattern "Family:Type:ElementId" (e.g., "Basic Wall:Generic - 200mm:308895"). The numeric element ID is stripped during scene model name cleaning. Some elements use a shorter "Name:Id" pattern (e.g., "Surface:1310134").
