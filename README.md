# Athar

Semantic diff for IFC files. Compares BIM models at the entity/property level — not line-by-line text.

*Athar (Arabic: أثار) — a trace left behind.*

## Installation

```bash
pip install -r requirements.txt
```

Requires Python 3.10+ and [ifcopenshell](https://ifcopenshell.org/).

## Usage

### Full Tool (Summary, Folder mode, Reports)

`athar_layers` is temporarily disabled while it is rewired to the graph engine.

```bash
# Two-file diff summary
python -m athar_layers old.ifc new.ifc

# Verbose summary (per-entity details)
python -m athar_layers old.ifc new.ifc --summary --verbose

# Folder mode — auto-groups versions and diffs them
python -m athar_layers some-folder/

# Export a Markdown report
python -m athar_layers old.ifc new.ifc --report diff-report.md
python -m athar_layers some-folder/ --report version-history.md
```

### Core Engine (Raw JSON)

```bash
# Two-file diff (JSON output for computers)
python -m athar old.ifc new.ifc

# Select profile
python -m athar old.ifc new.ifc --profile semantic_stable

# Stream output as NDJSON records
python -m athar old.ifc new.ifc --stream ndjson

# Stream output as chunked JSON records
python -m athar old.ifc new.ifc --stream chunked_json --chunk-size 1000
```

See the [sample report](docs/SAMPLE_REPORT.md) for what the output looks like.

## Testing

```bash
python -m pytest tests/
```

## Documentation

See [docs/DETAILS.md](docs/DETAILS.md) for detailed documentation on comparison logic, folder mode, file metadata, helper scripts, and test data.

## Engine Reimplementation (WIP)

Foundational canonical value normalization for the engine lives in `athar/canonical_values.py`, with an executable reference in `scripts/explore/canonical_reference_impl.py`. This establishes deterministic ordering for SET/BAG aggregates and preserves wrapper/select type information so hashing stays stable across STEP reorder/renumber. Full-instance extraction (explicit attributes + typed edge paths) is in progress in `athar/graph_parser.py` and now emits parse diagnostics for dangling references (`metadata.diagnostics`), and GUID-free structural hash seeds (`H:` payloads) plus WL refinement scaffolding are implemented in `athar/canonical_ids.py`. Soft candidate blocking signatures (`S:`) live in `athar/semantic_signature.py`, deterministic record serialization is in `athar/canonical_serializer.py` (ordered `G:` then `H:` then `C:`), and an initial diff engine skeleton is in `athar/diff_engine.py`.

Low-overlap rooted GUID churn now has Phase 2.5 scaffolding in `athar/root_remap.py`: deterministic GUID-overlap gating (`<30%`), GUID-independent root signatures, unique bucket remaps, and explicit ambiguity rejection. `athar/matcher_graph.py` now adds deterministic typed-path propagation from matched root pairs for unique non-root buckets, plus scored deterministic secondary matching for unresolved non-root residues (small-block assignment, fallback signature buckets for large blocks, and explicit tie/margin ambiguity rejection). `athar/diff_engine.py` applies remap + propagation + secondary matching before identity merge, emits `identity.match_method` (`root_remap`, `path_propagation`, `secondary_match`, `exact_guid`, `exact_hash`) with `match_confidence` and `matched_on` diagnostics, applies profile-driven volatility in both comparison and ID assignment (`semantic_stable` suppresses OwnerHistory-reference churn and normalizes `IfcOwnerHistory` entities; `raw_exact` preserves it), emits recursive field-level `field_ops` paths for `MODIFY`, emits `CLASS_DELTA` changes (with `equivalence_class.{id,old_count,new_count,exemplar}`) for unresolved exact-hash class-cardinality deltas, emits derived `REPARENT` markers for `IfcRelContainedInSpatialStructure`, `IfcRelAggregates`, and `IfcRelNests`, populates `rooted_owners` with deterministic sampling (`N=5`) plus exact totals, and now supports direct streaming diff computation via `stream_diff_graphs()` / `stream_diff_files()` (`ndjson` and `chunked_json` modes) without first building full `base_changes` in memory.

`athar_layers/` remains in-repo for now as a presentation/integration package, but the engine work is centered in `athar/` and is intended to stand on its own.

## License

MIT
