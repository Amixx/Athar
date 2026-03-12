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

# Set GlobalId policy (default: fail_fast)
python -m athar old.ifc new.ifc --guid-policy disambiguate

# Stream output as NDJSON records
python -m athar old.ifc new.ifc --stream ndjson

# Stream output as chunked JSON records
python -m athar old.ifc new.ifc --stream chunked_json --chunk-size 1000

# Override matcher policy knobs
python -m athar old.ifc new.ifc --secondary-score-threshold 0.65 --secondary-assignment-max 6
python -m athar old.ifc new.ifc --root-remap-score-threshold 0.7 --root-remap-score-margin 0.06
```

See the [sample report](docs/SAMPLE_REPORT.md) for what the output looks like.

## Testing

```bash
python -m pytest tests/
```

Determinism fixtures for low-level output are stored in `tests/fixtures/determinism/`.
Regenerate them after intentional contract changes with:

```bash
PYTHONPATH=. python scripts/explore/generate_determinism_fixtures.py
```

## Documentation

See [docs/LOW_LEVEL_CONTRACT.md](docs/LOW_LEVEL_CONTRACT.md) for the locked low-level wire/profile contract, and [docs/DETAILS.md](docs/DETAILS.md) for broader comparison logic, folder mode, file metadata, helper scripts, and test data.

## Engine Reimplementation (WIP)

Foundational canonical value normalization for the engine lives in `athar/canonical_values.py`, with an executable reference in `scripts/explore/canonical_reference_impl.py`. This establishes deterministic ordering for SET/BAG aggregates and preserves wrapper/select type information so hashing stays stable across STEP reorder/renumber. Full-instance extraction (explicit attributes + typed edge paths) is in progress in `athar/graph_parser.py` and now emits parse diagnostics for dangling references (`metadata.diagnostics`), and GUID-free structural hash seeds (`H:` payloads) plus WL refinement scaffolding are implemented in `athar/canonical_ids.py`. Soft candidate blocking signatures (`S:`) live in `athar/semantic_signature.py`, deterministic record serialization is in `athar/canonical_serializer.py` (ordered `G:` then `H:` then `C:`), and an initial diff engine skeleton is in `athar/diff_engine.py`.

Low-overlap rooted GUID churn now has Phase 2.5 staged remap in `athar/root_remap.py`: deterministic GUID-overlap gating (`<30%`), GUID-independent root signatures, neighbor-signature disambiguation, and bounded scored assignment for unresolved buckets (`threshold`, `margin`, `assignment_max`) with deterministic tie rejection and explicit ambiguity accounting. `athar/matcher_graph.py` now adds deterministic typed-path propagation from matched root pairs for unique non-root buckets, plus scored deterministic secondary matching for unresolved non-root residues (small-block assignment, fallback signature buckets for large blocks, and explicit tie/margin ambiguity rejection). `athar/canonical_ids.py` now adds SCC-aware ambiguity fallback so unresolved symmetric partitions can emit deterministic `C:` IDs (bounded refinement with partition cap). `athar/matcher_policy.py` now centralizes validated matcher-policy defaults/overrides (root remap + secondary matcher) and this policy is carried through API/CLI into output headers. `athar/diff_engine.py` applies remap + propagation + secondary matching before identity merge, emits `identity.match_method` (`root_remap`, `path_propagation`, `secondary_match`, `equivalence_class`, `exact_guid`, `exact_hash`) with `match_confidence` and `matched_on` diagnostics, applies profile-driven volatility in both comparison and ID assignment (`semantic_stable` suppresses OwnerHistory-reference churn and normalizes `IfcOwnerHistory` entities; `raw_exact` preserves it), enforces same-schema checks at file and graph entrypoints, emits recursive field-level `field_ops` paths for `MODIFY`, emits `CLASS_DELTA` changes (with `equivalence_class.{id,old_count,new_count,exemplar}`) for exact-hash class-cardinality deltas, SCC-ambiguous partitions, and unresolved ambiguous secondary partitions, emits derived `REPARENT` markers for `IfcRelContainedInSpatialStructure`, `IfcRelAggregates`, and `IfcRelNests`, populates `rooted_owners` with deterministic sampling (`N=5`) plus exact totals, and now supports direct streaming diff computation via `stream_diff_graphs()` / `stream_diff_files()` (`ndjson` and `chunked_json` modes) without first building full `base_changes` in memory.

`athar/canonical_ids.py` now supports pluggable fast hash backends for WL refinement rounds (`auto`, `xxh3_64`, `blake3`, `blake2b_64`, `sha256`), while external/wire identity IDs remain `sha256`.

Secondary matching now uses stronger blocking and solver logic: entity-family blocking (e.g. `IfcWallStandardCase`/`IfcWall` compatibility), ancestry-aware feature buckets, deterministic min-cost bipartite assignment, and iterative deepening (`depth 1 -> 2 -> 3`) for small ambiguous blocks.

Duplicate/invalid rooted `GlobalId` handling is now explicit and configurable: `fail_fast` (default, raises diagnostics) or `disambiguate` (assigns deterministic `G!:` IDs and marks identity as `guid_disambiguated`).

`athar_layers/` remains in-repo for now as a presentation/integration package, but the engine work is centered in `athar/` and is intended to stand on its own.

## License

MIT
