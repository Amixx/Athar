# Athar

Semantic IFC diff tool. Compares BIM models at the entity/property level, not text level.

*Athar (Arabic: أثار) — a trace left behind.*

## Architecture

Athar contains the core diff engine plus a transitional integration package.

### Core Engine (`athar/`)

The core engine is responsible for parsing IFC files, aligning entities across models, and generating a structured JSON diff.

- `athar/__main__.py` — Minimal CLI for the core engine. Diffs two files and prints raw JSON to stdout.
- `athar/canonical_values.py` — Deterministic canonicalization of scalar and aggregate values for the graph engine. Preserves wrapper/select type information, enforces deterministic ordering for SET/BAG, and uses profile-driven float normalization.
- `athar/profile_policy.py` — Central profile contract (`raw_exact`, `semantic_stable`) with validation and volatility filtering rules used by parser/diff paths.
- `athar/graph_parser.py` — Full-instance graph extractor for the engine. Emits explicit attributes in canonical form plus a typed edge list labeled with JSON-Pointer-like paths, and parse diagnostics for malformed/dangling references (`metadata.diagnostics`). Parser is covered with schema tests for `IFC2X3`, `IFC4`, and `IFC4X3`.
- `athar/structural_hash.py` — Structural hash helpers for engine identity (`H:`) seeds. Computes deterministic payloads that ignore STEP IDs and inverse attributes.
- `athar/wl_refinement.py` — WL-style refinement and SCC-aware ambiguity fallback. WL round hashing is pluggable (`auto`, `xxh3_64`, `blake3`, `blake2b_64`, `sha256`) while external IDs stay `sha256`.
- `athar/guid_policy.py` — GlobalId quality policy (`fail_fast` or `disambiguate`) with deterministic `G!:` disambiguation and diagnostics.
- `athar/matcher_policy.py` — Validated matcher policy defaults/overrides for rooted remap and secondary matcher stages.
- `athar/semantic_signature.py` — Soft signature (`S:`) for candidate blocking. Uses attribute/aggregate shape and typed edge signatures while ignoring literal values.
- `athar/root_remap.py` — Phase 2.5 rooted remap for low GUID overlap. Uses staged matching: GUID-independent root signatures, neighbor-signature disambiguation, then bounded scored assignment (`threshold`, `margin`, `assignment_max`) for unresolved buckets, with deterministic tie rejection and ambiguity accounting.
- `athar/matcher_graph.py` — Matching stages for graph diffing. Implements deterministic typed-path propagation from matched root pairs (unique 1:1 buckets only, with matched-on path diagnostics) and scored secondary matching for unresolved non-root entities (entity-family compatibility, ancestry-aware blocking/features, deterministic min-cost bipartite assignment, iterative deepening for small ambiguous blocks, large-block signature fallback, and explicit tie/margin ambiguity rejection, with per-pair score diagnostics).
- `athar/types.py` — Shared `TypedDict` contracts (`GraphIR`, `EntityIR`, `IdentityInfo`, `DiffContext`) used across engine modules to reduce nested-dict contract drift.
- `athar/canonical_serializer.py` — Deterministic serializer for identity/class records with total ordering `G:` then `H:` then `C:`.
- `athar/diff_engine.py` — Core diff engine skeleton that merges identity sets and emits `base_changes` in the new wire format. Integrates rooted remap planning + typed-path propagation + secondary matching before ID assignment/merge, records `match_method` (`exact_guid`, `guid_disambiguated`, `root_remap`, `path_propagation`, `secondary_match`, `equivalence_class`, `exact_hash`) plus `match_confidence` and `matched_on` diagnostics per change, applies profile-driven volatility filtering in both comparison and H-hash assignment (`semantic_stable` suppresses OwnerHistory-reference churn and normalizes `IfcOwnerHistory`; `raw_exact` preserves it), enforces same-schema policy via shared validation, enforces explicit GlobalId policy (`fail_fast` default, optional deterministic disambiguation to `G!:` IDs), accepts validated matcher-policy overrides for rooted remap/secondary matching, emits recursive field-level `field_ops` for `MODIFY`, emits `CLASS_DELTA` with `equivalence_class` counts/exemplar for unresolved exact-hash class-cardinality deltas, SCC-ambiguous partitions, and unresolved ambiguous secondary partitions, emits relation-typed derived `REPARENT` markers for `IfcRelContainedInSpatialStructure`, `IfcRelAggregates`, and `IfcRelNests`, populates `rooted_owners` with deterministic sampling (`N=5`) plus exact totals, includes dangling-reference counts in output stats, and emits stream events for framing.
- `athar/diff_engine_context.py` — Context prep and equality semantics; `G:` entity equality compares attribute/ref targets via matched identity IDs rather than raw STEP IDs to reduce renumber-only false positives.
- `athar/diff_engine_markers.py` — Derived-marker and rooted-owner helpers, including lazy rooted-owner projection and optional disk-backed owner-index spill (`ATHAR_OWNER_INDEX_DISK_THRESHOLD`) for large owner closures.
- `athar/diff_engine_streaming.py` — Single source of stream framing/protocol (`ndjson` and `chunked_json`), including deterministic `end` counters (`base_change_count`, `derived_marker_count`, `op_counts`) for both full-result and event-stream paths.
- `athar/__main__.py` — CLI is graph-engine only, with streamed output modes via `--stream ndjson|chunked_json`, `--chunk-size`, explicit matcher-policy overrides for rooted remap/secondary matcher tuning, owner-index spill control via `--owner-index-disk-threshold`, and optional runtime timings via `--timings`.

### Higher Layers (`athar_layers/`)

Integration layers that build upon the core engine for human-readable output, scene modeling, and folder-level versioning. This package is transitional and may be moved out of this repository. The `athar_layers` CLI is currently disabled pending rewiring to the graph engine.

Detailed information for these components can be found in [athar_layers/AGENTS.md](athar_layers/AGENTS.md).

- `athar_layers/scene.py` — Human-oriented "Scene Model" and labeling.
- `athar_layers/placement.py` — Human-readable placement descriptions.
- `athar_layers/folder.py` — Folder scanning and version grouping.
- `athar_layers/report.py` — Markdown report generation.
- `athar_layers/cli.py` — Full-featured CLI (Summary, Folder mode, Reports).

## Conventions

- Python 3.10+
- Engine modules (`athar/`) must not depend on integration/presentation modules (`athar_layers/`).
- Integration/presentation layers enrich the raw JSON output from the engine for human consumption.
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
- `scripts/explore/generate_determinism_fixtures.py` — Regenerate frozen golden outputs for deterministic low-level diff/stream payloads and environment fingerprint fixture.
- `scripts/explore/benchmark_diff_engine.py` — Reproducible runtime/peak-memory benchmark harness for `diff_graphs` and streaming modes (`ndjson`, `chunked_json`) on default or explicit IFC case pairs; captures per-case parser timings (`parse_ms`) and optional `--engine-timings` per-stage `diff_graphs` timing breakdowns from engine stats.
- `scripts/explore/benchmark_wl_backends.py` — WL refinement backend benchmark harness (`auto`, `sha256`, `xxh3_64`, `blake3`, `blake2b_64`) with runtime/peak-memory summaries.
- `scripts/explore/check_wl_backend_consistency.py` — WL backend consistency checker that compares compact color/class partition fingerprints against `sha256` baseline per graph.
- `scripts/explore/benchmark_owner_projection.py` — Rooted-owner projection benchmark comparing in-memory index vs disk-spill mode (`ATHAR_OWNER_INDEX_DISK_THRESHOLD`).
- `scripts/explore/evaluate_matcher_quality.py` — Deterministic matcher quality harness (precision/recall/F1) across rooted remap, typed-path propagation, and secondary matching scenarios; prints per-scenario progress to stderr.
- `scripts/explore/stress_determinism.py` — Repeated-run hash stability harness for `diff_graphs` and both stream modes; prints per-round progress (configurable via `--progress-every`) to stderr.
- `scripts/explore/render_perf_summary.py` — Render benchmark/quality/stability JSON artifacts into a concise markdown summary, including optional `diff_graphs` stage timing tables when baseline artifacts include `engine_timings_ms`.
- `scripts/explore/run_perf_suite.py` — Sequential overnight runner for baseline, WL benchmark, matcher quality, determinism stress, and final summary generation; supports bounded execution via per-step timeout, scoped WL graph inputs, optional baseline stage-timing capture (`--baseline-engine-timings`), and resumable step skipping via `--resume` with incremental manifest checkpoints.
- `scripts/explore/` — Exploratory/investigative scripts.
- Benchmark harnesses should emit visible progress logs (graph/case/backend + iteration) for long runs.

## Testing

```bash
python -m pytest tests/
```

## Dev practices

- Don't write throwaway scripts. Save exploratory ones in `scripts/explore/`.
- **Preserve knowledge during feature work.** Update README.md and AGENTS.md with what was built, why, and domain insights learned.
