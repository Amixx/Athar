# Athar

Semantic diff for IFC files. Compares BIM models at the entity/property level — not line-by-line text.

*Athar (Arabic: أثار) — a trace left behind.*

## Installation

```bash
pip install -e .
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

# Spill rooted-owner indexing to disk above threshold (estimated owner pairs)
python -m athar old.ifc new.ifc --owner-index-disk-threshold 500000

# Stream output as NDJSON records
python -m athar old.ifc new.ifc --stream ndjson

# Stream output as chunked JSON records
python -m athar old.ifc new.ifc --stream chunked_json --chunk-size 1000

# Include runtime timing breakdown in stats (non-deterministic, profiling only)
python -m athar old.ifc new.ifc --timings

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
python -m scripts.explore.generate_determinism_fixtures
```

Benchmark baselines (runtime + peak Python memory) for `diff_graphs` and stream modes:

```bash
python -m scripts.explore.benchmark_diff_engine --warmup 1 --iterations 2 --out docs/perf/batch11_baseline_YYYY-MM-DD.json
```

Include per-stage `diff_graphs` timings (from engine `stats.timings_ms`) when bottlenecking:

```bash
python -m scripts.explore.benchmark_diff_engine --case ifchouse:data/BasicHouse.ifc:data/BasicHouse.ifc --warmup 0 --iterations 1 --engine-timings --out docs/perf/batch11_baseline_YYYY-MM-DD.json
```

Show liveness during long metric iterations:

```bash
python -m scripts.explore.benchmark_diff_engine --case ifchouse:data/BasicHouse.ifc:data/BasicHouse.ifc --warmup 0 --iterations 1 --engine-timings --heartbeat-s 15 --out docs/perf/batch11_baseline_YYYY-MM-DD.json
```

Heartbeat lines include coarse `progress~...` and `eta~...` estimates (stage-aware where available), with ETA shown in human duration format (`h m s` when needed).
For `diff_graphs`, stage-aware progress includes context pipeline steps, base-change scan progress, and derived-marker completion.
For stream metrics, heartbeat also reports emitted-record progress (`items=...`) and ETA when expected record counts are known.

Write live progress snapshots to a sidecar JSON:

```bash
python -m scripts.explore.benchmark_diff_engine --case ifchouse:data/BasicHouse.ifc:data/BasicHouse.ifc --warmup 0 --iterations 1 --engine-timings --heartbeat-s 15 --progress-file /tmp/ifchouse-progress.json --out docs/perf/batch11_baseline_YYYY-MM-DD.json
```

Watch that sidecar in another terminal:

```bash
python -m scripts.explore.watch_progress --file /tmp/ifchouse-progress.json --interval-s 2
```

Watcher output includes stream item counters and emitted bytes when available.

Baseline reports also include parser timings per case under `parse_ms` (`old_graph`, `new_graph`, `total`).

WL backend benchmark (`auto`, `sha256`, `xxh3_64`, `blake3`, `blake2b_64`):

```bash
python -m scripts.explore.benchmark_wl_backends --warmup 1 --iterations 2 --out docs/perf/wl_backend_benchmark_YYYY-MM-DD.json
```

WL backend consistency check against `sha256` partition baseline:

```bash
python -m scripts.explore.check_wl_backend_consistency --out docs/perf/wl_backend_consistency_YYYY-MM-DD.json
```

The consistency report stores compact partition fingerprints (hash + size stats), not full partition listings, to keep artifacts small.

Owner projection benchmark (in-memory vs disk-spill rooted-owner index):

```bash
python -m scripts.explore.benchmark_owner_projection --warmup 1 --iterations 2 --out docs/perf/owner_projection_benchmark_YYYY-MM-DD.json
```

Matcher quality report (precision/recall/F1 over deterministic stage scenarios):

```bash
python -m scripts.explore.evaluate_matcher_quality --out docs/perf/matcher_quality_YYYY-MM-DD.json
```

Determinism stress report (repeated-run output hash stability):

```bash
python -m scripts.explore.stress_determinism --rounds 25 --out docs/perf/determinism_stress_YYYY-MM-DD.json
```

Render collected perf artifacts into markdown:

```bash
python -m scripts.explore.render_perf_summary --baseline docs/perf/batch11_baseline_YYYY-MM-DD.json --wl-benchmark docs/perf/wl_backend_benchmark_YYYY-MM-DD.json --wl-consistency docs/perf/wl_backend_consistency_YYYY-MM-DD.json --owner-projection docs/perf/owner_projection_benchmark_YYYY-MM-DD.json --matcher-quality docs/perf/matcher_quality_YYYY-MM-DD.json --determinism docs/perf/determinism_stress_YYYY-MM-DD.json --out docs/perf/SUMMARY.md
```

If the baseline report was produced with `--engine-timings`, the summary also includes a `Diff Stage Timings (diff_graphs)` section.
The summary includes a `Parse Timings` section when baseline artifacts include `parse_ms`.
If provided with a suite manifest (`--suite-manifest`), the summary includes a `Perf Suite Run` section with step status and elapsed times.
If the manifest contains live heartbeat probe snapshots, the same section includes baseline case/metric/stage/progress/eta details.

Run the full perf suite in one command (sequential, overnight-friendly):

```bash
python -m scripts.explore.run_perf_suite --tag YYYY-MM-DD
```

Recommended bounded run (single WL graph + per-step timeout):

```bash
python -m scripts.explore.run_perf_suite --tag YYYY-MM-DD --wl-graph data/BasicHouse.ifc --wl-consistency-graph data/BasicHouse.ifc --step-timeout-s 7200
```

Include `diff_graphs` stage timing breakdown in the suite baseline step:

```bash
python -m scripts.explore.run_perf_suite --tag YYYY-MM-DD --baseline-engine-timings
```

Add suite-level heartbeat logs while a step is running:

```bash
python -m scripts.explore.run_perf_suite --tag YYYY-MM-DD --heartbeat-s 30
```

Forward baseline benchmark progress sidecar through the suite runner:

```bash
python -m scripts.explore.run_perf_suite --tag YYYY-MM-DD --baseline-progress-file /tmp/baseline-progress.json
```

With `--heartbeat-s`, suite heartbeats include nested baseline detail from that sidecar (case/metric/stage/progress/eta).
The suite manifest’s `current_step` also tracks the latest heartbeat/probe snapshot during execution.

Resume an interrupted suite run (skips steps that already completed successfully and still have artifacts):

```bash
python -m scripts.explore.run_perf_suite --tag YYYY-MM-DD --resume
```

All long-running benchmark scripts emit progress logs to stderr (graph/case/backend and iteration-level status) so stalls are visible immediately.
`stress_determinism` also supports `--progress-every N` for round-level heartbeat control.

## Documentation

See [docs/LOW_LEVEL_CONTRACT.md](docs/LOW_LEVEL_CONTRACT.md) for the locked low-level wire/profile contract, and [docs/DETAILS.md](docs/DETAILS.md) for broader comparison logic, folder mode, file metadata, helper scripts, and test data.
Performance harness notes and outputs live under `docs/perf/`.

## Engine Reimplementation (WIP)

Foundational canonical value normalization for the engine lives in `athar/canonical_values.py`, with an executable reference in `scripts/explore/canonical_reference_impl.py`. This establishes deterministic ordering for SET/BAG aggregates and preserves wrapper/select type information so hashing stays stable across STEP reorder/renumber. Full-instance extraction (explicit attributes + typed edge paths) is in progress in `athar/graph_parser.py` and now emits parse diagnostics for dangling references (`metadata.diagnostics`), and GUID-free structural hash seeds (`H:` payloads) plus WL refinement scaffolding are implemented in `athar/structural_hash.py` and `athar/wl_refinement.py`. Soft candidate blocking signatures (`S:`) live in `athar/semantic_signature.py`, deterministic record serialization is in `athar/canonical_serializer.py` (ordered `G:` then `H:` then `C:`), and an initial diff engine skeleton is in `athar/diff_engine.py`.

Low-overlap rooted GUID churn now has Phase 2.5 staged remap in `athar/root_remap.py`: deterministic GUID-overlap gating (`<30%`), GUID-independent root signatures, neighbor-signature disambiguation, and bounded scored assignment for unresolved buckets (`threshold`, `margin`, `assignment_max`) with deterministic tie rejection and explicit ambiguity accounting. `athar/matcher_graph.py` now adds deterministic typed-path propagation from matched root pairs for unique non-root buckets, plus scored deterministic secondary matching for unresolved non-root residues (small-block assignment, fallback signature buckets for large blocks, and explicit tie/margin ambiguity rejection). `athar/wl_refinement.py` now adds SCC-aware ambiguity fallback so unresolved symmetric partitions can emit deterministic `C:` IDs (bounded refinement with partition cap). `athar/matcher_policy.py` now centralizes validated matcher-policy defaults/overrides (root remap + secondary matcher) and this policy is carried through API/CLI into output headers. `athar/diff_engine.py` applies remap + propagation + secondary matching before identity merge, emits `identity.match_method` (`root_remap`, `path_propagation`, `secondary_match`, `equivalence_class`, `exact_guid`, `exact_hash`) with `match_confidence` and `matched_on` diagnostics, applies profile-driven volatility in both comparison and ID assignment (`semantic_stable` suppresses OwnerHistory-reference churn and normalizes `IfcOwnerHistory` entities; `raw_exact` preserves it), enforces same-schema checks at file and graph entrypoints, emits recursive field-level `field_ops` paths for `MODIFY`, emits `CLASS_DELTA` changes (with `equivalence_class.{id,old_count,new_count,exemplar}`) for exact-hash class-cardinality deltas, SCC-ambiguous partitions, and unresolved ambiguous secondary partitions, emits derived `REPARENT` markers for `IfcRelContainedInSpatialStructure`, `IfcRelAggregates`, and `IfcRelNests`, populates `rooted_owners` with deterministic sampling (`N=5`) plus exact totals, and now supports direct streaming diff computation via `stream_diff_graphs()` / `stream_diff_files()` (`ndjson` and `chunked_json` modes) without first building full `base_changes` in memory.

`athar/diff_engine_context.py` now normalizes `G:` entity attribute/ref targets to matched identity IDs during equality checks, preventing false `MODIFY` noise from pure STEP-ID renumbering.

Stream framing is now centralized in `athar/diff_engine_streaming.py` for both full-result and live event paths, and shared `TypedDict` contracts (`GraphIR`, `EntityIR`, `IdentityInfo`, `DiffContext`) live in `athar/types.py` to reduce nested-dict drift across engine modules.

`athar/wl_refinement.py` now supports pluggable fast hash backends for WL refinement rounds (`auto`, `xxh3_64`, `blake3`, `blake2b_64`, `sha256`), while external/wire identity IDs remain `sha256`.

Rooted-owner projection now supports disk-backed fallback for large closure cardinalities: set `ATHAR_OWNER_INDEX_DISK_THRESHOLD` (estimated owner pairs) to spill owner indexing to a temporary SQLite index instead of keeping full closure sets in memory.

Secondary matching now uses stronger blocking and solver logic: entity-family blocking (e.g. `IfcWallStandardCase`/`IfcWall` compatibility), ancestry-aware feature buckets, deterministic min-cost bipartite assignment, and iterative deepening (`depth 1 -> 2 -> 3`) for small ambiguous blocks.

Duplicate/invalid rooted `GlobalId` handling is now explicit and configurable: `fail_fast` (default, raises diagnostics) or `disambiguate` (assigns deterministic `G!:` IDs and marks identity as `guid_disambiguated`).

`athar_layers/` remains in-repo for now as a presentation/integration package, but the engine work is centered in `athar/` and is intended to stand on its own.

## License

MIT
