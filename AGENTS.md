# Athar

Semantic IFC diff tool. Compares BIM models at the entity/property level, not text level.

*Athar (Arabic: أثار) — a trace left behind.*

## Architecture

Athar is split into a core diff engine and higher-level integration layers.

### Core Engine (`athar/`)

The core engine is responsible for parsing IFC files, aligning entities across models, and generating a structured JSON diff.

- `athar/parser.py` — Extracts structured entity data from IFC files using ifcopenshell. Each element is keyed by GlobalId and includes type, name, attributes, property sets, placement, spatial container, type assignment, owner history, and group memberships (from `IfcRelAssignsToGroup`). Also extracts file-level metadata (schema, timestamp, org, application) and structural relationships (voids, fills, aggregates, spatial containment). Returns `{"metadata": {...}, "entities": {guid: {...}, ...}, "relationships": {...}}`.
- `athar/matcher.py` — Entity alignment across two parsed models. Primary matching by GlobalId. When GUID overlap is low (<30%), activates a three-stage content-based fallback: (1) exact unique content signature match (ifc_class + name + type_name + container + groups + properties hash), (2) positional disambiguation for duplicate signatures using quantized placement (50mm cells), (3) fuzzy scoring on remaining candidates (weighted: placement proximity 0.30, props overlap 0.25, name 0.15, type_name 0.15, container 0.10, groups 0.05) with threshold + margin guards. Conservative: ambiguous cases left unmatched. Returns `{old_to_new: {old_guid: new_guid}, method: "guid"|"content_fallback", guid_overlap: float}`.
- `athar/differ.py` — Compares two parsed models. Uses `matcher.match_entities()` for entity alignment (supports both GUID and content-based matching). Produces added/deleted/changed/bulk_movements buckets with per-property granularity. Includes file metadata from both sides in the output. Output is raw JSON "for computers".
- `athar/__main__.py` — Minimal CLI for the core engine. Diffs two files and prints raw JSON to stdout.
- `athar/canonical_values.py` — Deterministic canonicalization of scalar and aggregate values for the low-level diff reimplementation. Preserves wrapper/select type information, enforces deterministic ordering for SET/BAG, and uses profile-driven float normalization.
- `athar/graph_parser.py` — Full-instance graph extractor for the low-level diff layer. Emits explicit attributes in canonical form plus a typed edge list labeled with JSON-Pointer-like paths.
- `athar/canonical_ids.py` — Structural hash helpers for low-level identity (`H:`) seeds and WL-style refinement scaffolding. Computes deterministic payloads that ignore STEP IDs and inverse attributes.
- `athar/semantic_signature.py` — Soft signature (`S:`) for candidate blocking. Uses attribute/aggregate shape and typed edge signatures while ignoring literal values.

### Higher Layers (`athar_layers/`)

Integration layers that build upon the core engine for human-readable output, scene modeling, and folder-level versioning.

Detailed information for these components can be found in [athar_layers/AGENTS.md](athar_layers/AGENTS.md).

- `athar_layers/scene.py` — Human-oriented "Scene Model" and labeling.
- `athar_layers/placement.py` — Human-readable placement descriptions.
- `athar_layers/folder.py` — Folder scanning and version grouping.
- `athar_layers/report.py` — Markdown report generation.
- `athar_layers/cli.py` — Full-featured CLI (Summary, Folder mode, Reports).

## Conventions

- Python 3.10+
- Core engine (`athar/`) must not depend on higher layers (`athar_layers/`).
- Higher layers enrich the raw JSON output from the core engine for human consumption.
- Use `ifcopenshell` for all IFC parsing. Do not parse STEP files as text.
- Match entities across files by GlobalId (the stable identifier across revisions).
- Keep diffing deterministic and algorithmic — no AI in the diff pipeline itself.
- Output structured JSON. Human-readable summaries are a presentation concern, not a diff concern.
- No deep B-rep geometry comparison. Compare placement matrices and geometric parameters only.
- Minimal dependencies. Only `ifcopenshell` and stdlib.

## Running

### Full Tool (Summary, Folder mode, Reports)

```bash
python -m athar_layers old.ifc new.ifc                # two-file diff summary
python -m athar_layers old.ifc new.ifc --report r.md  # Markdown report
python -m athar_layers some-folder/                   # folder mode: auto-group + diff
```

### Core Engine (Raw JSON)

```bash
python -m athar old.ifc new.ifc                       # raw JSON diff
```

## Scripts

- `scripts/inspect_ifc.py` — Print summary stats for an IFC file.
- `scripts/make_modified_ifc.py` — Produce a modified copy of an IFC file for testing.
- `scripts/inspect_ifc_identity.py` — Show project name/GlobalId and header timestamp.
- `scripts/inspect_guid_overlap.py` — Show entity GUID overlap matrix between files.
- `scripts/explore/canonical_reference_impl.py` — Executable reference for canonical value normalization (value grammar, ordering, and profiles).
- `scripts/explore/` — Exploratory/investigative scripts.

## Testing

```bash
python -m pytest tests/
```

## Dev practices

- Don't write throwaway scripts. Save exploratory ones in `scripts/explore/`.
- **Preserve knowledge during feature work.** Update README.md and AGENTS.md with what was built, why, and domain insights learned.
