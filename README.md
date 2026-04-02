# Athar

Semantic diff for IFC files. Compares BIM models at the entity/property level — not line-by-line text.

*Athar (Arabic: أثار) — a trace left behind.*

## Engine Status

Athar now ships the current engine path as the default CLI flow:

- Bottom layer: `athar/bottom/` (`index.py`, `parser.py`, `link_inversion.py`, `edge_policy.py`, `merkle.py`, `wl_gossip.py`, `spatial.py`, `signatures.py`)
- Matcher layer: `athar/matcher/` (`candidates.py`, `scoring.py`, `assignment.py`)
- Delta layer: `athar/delta/report.py`
- Orchestration: `athar/engine.py`

Schema support is **IFC4 and IFC2X3**, with a same-schema requirement per run (cross-schema translation is out of scope).
Spatial fallback uses world-space centroid/AABB features (geometry points transformed by resolved `ObjectPlacement` matrices), and parser scalar canonicalization preserves numeric string literals like `"0"`/`"1"` as strings.
This runtime is intentionally breaking: legacy graph-engine CLI flags are removed from the primary interface.

## Installation

```bash
pip install -e .
make native-dev
```

Requires Python 3.10+, [ifcopenshell](https://ifcopenshell.org/), `xxhash`, and the built `athar._native._core` extension. Athar no longer ships Python fallback paths for the native diff hot loops: entity text fingerprinting, combined forward/reverse adjacency building inside diff preparation, and the `xxh3_64` WL refinement path all require the Rust extension.

Recommended local dev flow is a repo-local virtualenv plus a separate native build step:

```bash
python -m venv .venv
source .venv/bin/activate
make dev-setup
make native-dev
make test-native
make test-perf
```

For performance runs, prefer the release-mode install path so the native extension is benchmarked optimized:

```bash
make native-dev-release
make perf-native-check
```

`athar/_native/pyproject.toml` is configured as a mixed Python/Rust project (`python-source = "../.."`), so `maturin develop --manifest-path athar/_native/Cargo.toml` installs the extension at `athar._native._core` inside the repo package rather than as a top-level `_core` module.
`make perf-native-check` always refreshes that extension in release mode first, then runs the default `diff_graphs` benchmark case set twice: once with `ATHAR_PARALLEL=0` and once with `ATHAR_PARALLEL=1`.
Outside explicit benchmark forcing, `ATHAR_PARALLEL` now behaves as `auto` by default: Athar keeps the current sequential path for small or cached diffs, but enables the existing side-parallel text-fingerprint + identity-precompute path on large uncached graph pairs when fork-based multiprocessing is available. Set `ATHAR_PARALLEL=0` to force serial or `ATHAR_PARALLEL=1` to force parallel.
When `benchmark_diff_engine.py` is run without `--out`, it now writes a timestamped JSON artifact under `docs/perf/`; `ATHAR_BENCHMARK_NAME` controls the filename prefix.

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

# Control spatial fallback radius for matching (meters)
python -m athar old.ifc new.ifc --matcher-radius-m 0.5

# Stream output as NDJSON records
python -m athar old.ifc new.ifc --stream ndjson

# Stream output as chunked JSON records
python -m athar old.ifc new.ifc --stream chunked_json --chunk-size 1000
```

See the [sample report](docs/SAMPLE_REPORT.md) for what the output looks like.

## Testing

```bash
python -m pytest tests/
python -m pytest tests/test_engine.py -q
python -m pytest tests/test_engine_contracts.py -q
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

Run only one metric (recommended during active tuning loops):

```bash
python -m scripts.explore.benchmark_diff_engine --case ifchouse:data/BasicHouse.ifc:data/BasicHouse.ifc --metric diff_graphs --warmup 0 --iterations 1 --engine-timings --heartbeat-s 15 --out /tmp/ifchouse-diff-only.json
```

Profile `prepare_context` in isolation (parse once, context stage only):

```bash
python -m scripts.explore.profile_prepare_context --old tests/fixtures/house_v1.ifc --new tests/fixtures/house_v2.ifc --warmup 0 --iterations 1 --heartbeat-s 15 --cprofile --out /tmp/house-v1-v2-prepare-context.json
```

Show liveness during long metric iterations:

```bash
python -m scripts.explore.benchmark_diff_engine --case ifchouse:data/BasicHouse.ifc:data/BasicHouse.ifc --warmup 0 --iterations 1 --engine-timings --heartbeat-s 15 --out docs/perf/batch11_baseline_YYYY-MM-DD.json
```

Heartbeat lines include coarse `progress~...` and `eta~...` estimates (stage-aware where available), with ETA shown in human duration format (`h m s` when needed).
Console logs for parse/heartbeat/metric means now print elapsed durations as `Xm Ys Zms` (not millisecond-only values).
For `diff_graphs`, stage-aware progress includes context pipeline steps, base-change scan progress, and derived-marker completion.
For stream metrics, heartbeat also reports emitted-record progress (`items=...`) and ETA when expected record counts are known.
Benchmark JSON reports include end-to-end run wall time under `run_summary.total_elapsed_ms` and `run_summary.total_elapsed_text`.

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

The core engine is now split into explicit internal packages: `athar/graph/` for canonical GraphIR extraction/serialization and `athar/diff/` for identity, matching, and diff orchestration. Foundational canonical value normalization lives in `athar/graph/canonical_values.py`, with an executable reference in `scripts/explore/canonical_reference_impl.py`. This establishes deterministic ordering for SET/BAG aggregates and preserves wrapper/select type information so hashing stays stable across STEP reorder/renumber. Full-instance extraction (explicit attributes + typed edge paths) lives in `athar/graph/graph_parser.py` and emits parse diagnostics for dangling references (`metadata.diagnostics`); GUID-free structural hash seeds (`H:` payloads) are implemented in `athar/graph/structural_hash.py`; deterministic record serialization is in `athar/graph/canonical_serializer.py` (ordered `G:` then `H:` then `C:`).

Diff strategy now lives under `athar/diff/`: low-overlap rooted GUID churn uses staged remap in `athar/diff/root_remap.py`; `athar/diff/matcher_graph.py` handles deterministic typed-path propagation plus scored secondary matching for unresolved residues; `athar/diff/wl_refinement.py` adds SCC-aware ambiguity fallback so unresolved symmetric partitions can emit deterministic `C:` IDs; and `athar/diff/matcher_policy.py` centralizes validated matcher-policy defaults/overrides. `athar/diff/engine.py` applies remap + propagation + secondary matching before identity merge, emits `identity.match_method` (`root_remap`, `path_propagation`, `secondary_match`, `text_fingerprint`, `equivalence_class`, `exact_guid`, `exact_hash`) with `match_confidence` and `matched_on` diagnostics, applies profile-driven volatility in both comparison and ID assignment (`semantic_stable` suppresses OwnerHistory-reference churn and normalizes `IfcOwnerHistory`; `raw_exact` preserves it), enforces same-schema checks at file and graph entrypoints, emits recursive field-level `field_ops` paths for `MODIFY`, emits `CLASS_DELTA` changes (with `equivalence_class.{id,old_count,new_count,exemplar}`) for exact-hash class-cardinality deltas, SCC-ambiguous partitions, and unresolved ambiguous secondary partitions, emits derived `REPARENT` markers for `IfcRelContainedInSpatialStructure`, `IfcRelAggregates`, and `IfcRelNests`, populates `rooted_owners` with deterministic sampling (`N=5`) plus exact totals, and supports direct streaming diff computation via `stream_diff_graphs()` / `stream_diff_files()` (`ndjson` and `chunked_json` modes) without first building full `base_changes` in memory.

`athar/diff/context.py` normalizes `G:` entity attribute/ref targets to matched identity IDs during equality checks, preventing false `MODIFY` noise from pure STEP-ID renumbering. It also runs an early conservative similarity seeding pass (`athar/diff/similarity_seed.py` + `athar._native.native_entity_fingerprint`): non-GUID text fingerprints are pre-matched (unique buckets first, then small ambiguous-bucket neighbor refinement) and used as precompute seeds so unchanged entities can skip structural hash work, while GUID-bearing entities remain on GUID/root/path match paths first. Early GUID-anchored typed-path seeding is gated by whole-graph GUID anchor coverage rather than `unique_guid_overlap`, so sparse-GUID large models do not trigger a costly propagation pass from a very small root set. High-GUID-overlap early typed-path seeding now uses a lighter propagation mode that avoids per-match nested diagnostic dict allocation during the seed phase and only materializes full path diagnostics later if those matches are reused. Stream framing is centralized in `athar/diff/streaming.py` for both full-result and live event paths, while graph-only `TypedDict` contracts live in `athar/graph/types.py` and diff-layer contracts live in `athar/diff/types.py`.

There is now a first-pass query index layer under `athar/index/` built directly on GraphIR. `athar/index/store.py` provides `GraphIndex` with ambiguity-aware GUID lookup, exact and subtype-inclusive type queries when schema metadata is available, canonical attribute presence/value lookup, and labeled forward/reverse relationship indices that preserve attribute-path provenance (`attr_name`, aggregate-vs-direct edge shape). `athar/index/query.py` stays intentionally thin and only composes those primitives, so query acceleration does not fork semantics away from the shared GraphIR contract.

`athar/diff/wl_refinement.py` supports pluggable fast hash backends for WL refinement rounds (`auto`, `xxh3_64`, `blake3`, `blake2b_64`, `sha256`), while external/wire identity IDs remain `sha256`; WL round payload construction avoids per-node JSON/dict allocations in the hot loop, and `auto` now resolves directly to the required native `xxh3_64` path. The native path uses the multi-round Rust entrypoint rather than keeping a Python or single-round fallback branch. The graph-layer adjacency helpers remain deterministic Python reference builders, while the diff hot paths call the required native combined adjacency builder directly during identity precompute and SCC setup. `athar/graph/structural_hash.py` hashes canonical entity fields directly in the hot path (deterministic streaming hash over entity type/attributes/edge multiset) instead of building per-entity JSON payloads. `athar/diff/engine.py` overlaps opening the second IFC while extracting the first graph (`graph_parser.open_ifc()` + `graph_parser.graph_from_ifc()`), reducing `diff_files`/`stream_diff_files` parse wall time on large pairs, and also short-circuits same-graph inputs (including same-path parses reused as one graph object) to immediate empty diff/stream output after schema/profile/GUID/matcher-policy validation.

The native seam under `athar/_native/` now contains the required Rust implementations for entity text fingerprinting, combined adjacency/reverse-adjacency construction, and the `xxh3_64` WL refinement loop. Missing the extension is a hard installation error, not a slower compatibility mode.

Rooted-owner projection is demand-driven by default (reverse reachability per changed step with caching). Set `ATHAR_OWNER_INDEX_DISK_THRESHOLD` to opt into eager full owner-index mode; when estimated owner pairs exceed that threshold, indexing spills to a temporary SQLite store instead of keeping full closure sets in memory.

`prepare_context()` no longer forces an unconditional full `gc.collect()` at the end of every diff run; set `ATHAR_FORCE_GC_COLLECT=1` to restore the previous behavior for memory experiments. `diff_graphs()` / `stream_diff_graphs()` now also keep cyclic GC disabled across base-change and derived-marker emission so large mostly-unchanged runs do not pay long GC pauses inside emit-time compare normalization.

Secondary matching now uses stronger blocking and solver logic: entity-family blocking (e.g. `IfcWallStandardCase`/`IfcWall` compatibility), ancestry-aware feature buckets, deterministic min-cost bipartite assignment, and iterative deepening (`depth 1 -> 2 -> 3`) for small ambiguous blocks.

Duplicate/invalid rooted `GlobalId` handling is now explicit and configurable: `fail_fast` (default, raises diagnostics) or `disambiguate` (assigns deterministic `G!:` IDs and marks identity as `guid_disambiguated`).

`athar_layers/` remains in-repo for now as a presentation/integration package, but the engine work is centered in `athar/` and is intended to stand on its own.

## License

MIT
