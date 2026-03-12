# Athar

Semantic IFC diff tool. Compares BIM models at the entity/property level, not text level.

*Athar (Arabic: أثار) — a trace left behind.*

## Architecture

Athar contains the core diff engine plus a transitional integration package.

### Core Engine (`athar/`)

The core engine is responsible for parsing IFC files, aligning entities across models, and generating a structured JSON diff.

- `athar/__main__.py` — Minimal CLI for the core engine. Diffs two files and prints raw JSON to stdout.
- `athar/_native/` — Optional PyO3/maturin native-accelerator scaffold for hot loops. Intended to provide `athar._native._core` with drop-in helpers such as native entity fingerprinting/WL round logic while keeping pure-Python fallbacks in place when the extension is not built.
- `athar/graph/types.py` — Graph-layer `TypedDict` contracts (`GraphIR`, `EntityIR`, `EntityRefIR`) shared by graph extraction and diff consumers.
- `athar/graph/canonical_values.py` — Deterministic canonicalization of scalar and aggregate values for the graph layer. Preserves wrapper/select type information, enforces deterministic ordering for SET/BAG, and applies profile-driven measure-aware normalization (length/area/volume/angle/derived via unit context) for `semantic_stable`.
- `athar/graph/profile_policy.py` — Central profile contract (`raw_exact`, `semantic_stable`) with validation and volatility filtering rules used by parser/diff paths.
- `athar/graph/graph_parser.py` — Full-instance graph extractor. Emits explicit attributes in canonical form plus a typed edge list labeled with JSON-Pointer-like paths, parse diagnostics for malformed/dangling references (`metadata.diagnostics`), and extracted unit assignment context (`metadata.units`) used for semantic measure normalization. Exposes `open_ifc()` + `graph_from_ifc()` so callers can overlap open/extract work across file pairs.
- `athar/graph/graph_utils.py` — Shared graph primitives such as adjacency builders, edge signatures, stripping ref IDs, and deterministic JSON hashing helpers.
- `athar/graph/structural_hash.py` — Structural hash helpers for engine identity (`H:`) seeds. Computes deterministic hashes that ignore STEP IDs/inverse attributes; hot path now hashes canonical entity fields directly (no `json.dumps` payload serialization in the per-entity loop).
- `athar/graph/canonical_serializer.py` — Deterministic serializer for identity/class records with total ordering `G:` then `H:` then `C:`.
- `athar/graph/determinism.py` — Canonical JSON serialization helpers plus environment fingerprint reporting for determinism fixtures/tools.
- `athar/diff/types.py` — Diff-layer `TypedDict` contracts (`IdentityInfo`, `ContextEntityItem`, `DiffContext`) layered on top of graph types.
- `athar/diff/engine.py` — Core diff engine orchestration that merges identity sets and emits the v2 wire format. Integrates rooted remap planning + typed-path propagation + secondary matching before ID assignment/merge, records `match_method` (`exact_guid`, `guid_disambiguated`, `root_remap`, `path_propagation`, `secondary_match`, `text_fingerprint`, `equivalence_class`, `exact_hash`) plus `match_confidence` and `matched_on` diagnostics per change, applies profile-driven volatility filtering in both comparison and H-hash assignment (`semantic_stable` suppresses OwnerHistory-reference churn and normalizes `IfcOwnerHistory`; `raw_exact` preserves it), enforces same-schema policy, includes dangling-reference counts in stats, short-circuits same-graph inputs after validation, and overlaps opening the second IFC while extracting the first graph to reduce parse wall-time.
- `athar/diff/context.py` — Context prep and equality semantics; `G:` entity equality compares attribute/ref targets via matched identity IDs rather than raw STEP IDs to reduce renumber-only false positives, threads precomputed graph adjacency/reverse-adjacency from identity precompute into secondary matching and owner projection, and runs early similarity seeding (unique GUID diagnostics, optional GUID-anchored propagation probe, conservative text-fingerprint matches) before identity precompute so unchanged non-GUID entities can skip structural hash calculation.
- `athar/diff/changes.py` — Base change assembly helpers, including rooted-owner enrichment and field-op shaping for `MODIFY`.
- `athar/diff/markers.py` — Derived-marker and rooted-owner helpers. Default rooted-owner projection is demand-driven (reverse reachability per changed step with caching) to avoid full closure materialization; setting `ATHAR_OWNER_INDEX_DISK_THRESHOLD` enables eager full owner-index mode with optional disk spill for very large closures.
- `athar/diff/streaming.py` — Single source of stream framing/protocol (`ndjson` and `chunked_json`), including deterministic `end` counters (`base_change_count`, `derived_marker_count`, `op_counts`) for both full-result and event-stream paths.
- `athar/diff/stats.py` — Shared diff statistics helpers, including root `GlobalId` quality summaries.
- `athar/diff/identity_pipeline.py` — Identity assignment internals for structural hashes, WL colors, SCC ambiguity classes, and GUID policy application.
- `athar/diff/root_remap.py` — Phase 2.5 rooted remap for low GUID overlap. Uses staged matching: GUID-independent root signatures, neighbor-signature disambiguation, then bounded scored assignment (`threshold`, `margin`, `assignment_max`) for unresolved buckets, with deterministic tie rejection and ambiguity accounting.
- `athar/diff/wl_refinement.py` — WL-style refinement and SCC-aware ambiguity fallback. WL round hashing is pluggable (`auto`, `xxh3_64`, `blake3`, `blake2b_64`, `sha256`) while external IDs stay `sha256`; fast backends now run backend-native colors during rounds and normalize to sha256 once at output boundary.
- `athar/diff/matcher_graph.py` — Matching stages for graph diffing. Implements deterministic typed-path propagation from matched root pairs and scored secondary matching for unresolved non-root entities.
- `athar/diff/matcher_graph_scoring.py` — Feature extraction and scoring helpers used by secondary matching and rooted remap assignment.
- `athar/diff/matcher_assignment.py` — Deterministic min-cost bipartite assignment helpers used by matcher scoring.
- `athar/diff/matcher_policy.py` — Validated matcher policy defaults/overrides for rooted remap and secondary matcher stages, including secondary unresolved-set gating (`unresolved_limit`) and unresolved pair-product gating (`unresolved_pair_limit`) for very large unmatched populations.
- `athar/diff/guid_policy.py` — GlobalId quality policy (`fail_fast` or `disambiguate`) with deterministic `G!:` disambiguation and diagnostics.
- `athar/diff/equivalence_classes.py` — Equivalence-class assignment for unresolved exact-hash or SCC-ambiguous partitions.
- `athar/diff/semantic_signature.py` — Soft signature (`S:`) for candidate blocking. Uses attribute/aggregate shape and typed edge signatures while ignoring literal values.
- `athar/diff/text_fingerprint.py` — Fast GUID-independent entity fingerprint (`xxh3_128` when available, deterministic fallback otherwise) for cross-file similarity seeding; fingerprints include entity type, canonicalized attributes with ref IDs stripped, and edge path/type multiset.
- `athar/diff/similarity_seed.py` — Early seed builders for unique GUID pairing diagnostics and conservative text-fingerprint pairing/refinement before full identity precompute.
- `athar/diff/geometry_policy.py` — Geometry representation policy contract (`strict_syntax`, `invariant_probe`) with validation.
- `athar/diff/geometry_invariants.py` — Coarse representation-invariant probe helpers (point-count, bbox, centroid from representation subgraphs).
- `athar/diff/graph_cache.py` — Binary disk cache for parsed GraphIR + precomputed identity state. Keyed by file content hash (`xxh3_128`) + profile; uses pickle (internal, not a wire format). Cache location: `~/.cache/athar/` (override via `ATHAR_CACHE_DIR`). Disable via `ATHAR_CACHE=0`. LRU eviction at 64 entries. Saves after fresh identity precompute; loads skip parse + identity computation entirely on repeat-file hits.
- `athar/__main__.py` — CLI is graph-engine only, with streamed output modes via `--stream ndjson|chunked_json`, `--chunk-size`, explicit matcher-policy overrides for rooted remap/secondary matcher tuning, geometry policy selection via `--geometry-policy`, owner-index spill control via `--owner-index-disk-threshold`, graph cache control via `--no-cache`/`--cache-dir`/`--clear-cache`, and optional runtime timings via `--timings`.

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
- `athar/diff/` may depend on `athar/graph/` and optional low-level accelerators under `athar/_native/`; `athar/graph/` must remain independent of both.
- Integration/presentation layers enrich the raw JSON output from the engine for human consumption.
- Use `ifcopenshell` for all IFC parsing. Do not parse STEP files as text.
- Match entities across files by GlobalId (the stable identifier across revisions).
- Keep diffing deterministic and algorithmic — no AI in the diff pipeline itself.
- Output structured JSON. Human-readable summaries are a presentation concern, not a diff concern.
- No deep B-rep geometry comparison. Compare placement matrices and geometric parameters only.
- Minimal dependencies. Core runtime deps are `ifcopenshell` and `xxhash`.
- Native acceleration is optional. When working on `athar/_native/`, keep Python fallback behavior intact and validate parity against the pure-Python implementations.
- Prefer a repo-local `.venv` for development. `pyproject.toml` defines packaging/build metadata; it does not replace environment isolation. Verified local workflow is `make dev-setup`, `make native-dev`, `make test-native`, `make test-perf`.
- Matching quality is prioritized over throughput: preserve `MODIFY` recovery (correct entity alignment) rather than collapsing into `ADD/REMOVE`; keep matcher cutoffs conservative and only use aggressive gates as safety valves for pathological inputs.

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
- `scripts/explore/benchmark_diff_engine.py` — Reproducible runtime/peak-memory benchmark harness for `diff_graphs` and streaming modes (`ndjson`, `chunked_json`) on default or explicit IFC case pairs; captures per-case parser timings (`parse_ms`), supports iteration heartbeat logs (`--heartbeat-s`) with stage-aware coarse progress/ETA estimates (ETA rendered as `h m s` when needed), prints parse/heartbeat/mean/total durations in human form (`Xm Ys Zms`), includes end-to-end run totals in report `run_summary`, supports optional metric selection (`--metric`) to avoid unnecessary full reruns, and optional `--engine-timings` per-stage `diff_graphs` timing breakdowns from engine stats.
- `scripts/explore/benchmark_diff_engine.py` also supports live progress sidecar output (`--progress-file`) with run state + current case/metric/stage progress for external monitors.
- For stream metrics, `benchmark_diff_engine` heartbeat uses emitted-record count progress (`items=...`) with ETA from observed throughput when expected stream record counts are derivable.
- `athar/diff/engine.py` + `athar/diff/context.py` expose stage-progress callbacks used by benchmark heartbeats (`prepare_context` substeps, base-change scan progress, derived-marker completion).
- `scripts/explore/benchmark_wl_backends.py` — WL refinement backend benchmark harness (`auto`, `sha256`, `xxh3_64`, `blake3`, `blake2b_64`) with runtime/peak-memory summaries.
- `scripts/explore/check_wl_backend_consistency.py` — WL backend consistency checker that compares compact color/class partition fingerprints against `sha256` baseline per graph.
- `scripts/explore/benchmark_owner_projection.py` — Rooted-owner projection benchmark comparing in-memory index vs disk-spill mode (`ATHAR_OWNER_INDEX_DISK_THRESHOLD`).
- `scripts/explore/profile_prepare_context.py` — Isolated prepare-context profiler (parse once, benchmark `prepare_diff_context` only) with heartbeat, per-stage timing summaries, and optional `cProfile` top tables.
- `scripts/explore/evaluate_matcher_quality.py` — Deterministic matcher quality harness (precision/recall/F1) across rooted remap, typed-path propagation, and secondary matching scenarios; prints per-scenario progress to stderr.
- `scripts/explore/stress_determinism.py` — Repeated-run hash stability harness for `diff_graphs` and both stream modes; prints per-round progress (configurable via `--progress-every`) to stderr.
- `scripts/explore/render_perf_summary.py` — Render benchmark/quality/stability JSON artifacts into a concise markdown summary, including optional `diff_graphs` stage timing tables when baseline artifacts include `engine_timings_ms`, parse timing tables, and suite-step status summaries when passed a perf-suite manifest.
- `scripts/explore/render_perf_summary.py` also renders live suite heartbeat probe snapshots (when present in manifest `current_step`) for in-progress baseline visibility.
- `scripts/explore/run_perf_suite.py` — Sequential overnight runner for baseline, WL benchmark, matcher quality, determinism stress, and final summary generation; supports bounded execution via per-step timeout, suite-level heartbeat logs (`--heartbeat-s`), scoped WL graph inputs, optional baseline stage-timing capture (`--baseline-engine-timings`), and resumable step skipping via `--resume` with incremental manifest checkpoints and run-state metadata.
- `scripts/explore/run_perf_suite.py` can forward baseline sidecar progress output via `--baseline-progress-file`.
- With `--heartbeat-s`, `run_perf_suite` heartbeat output can include nested baseline sidecar details (case/metric/stage/progress/eta).
- While running, `run_perf_suite` manifest `current_step` can include latest heartbeat/probe snapshots for file-based monitoring.
- `scripts/explore/watch_progress.py` — Poll and render concise live status from benchmark sidecar progress JSON (`--file`, `--interval-s`, optional `--follow`).
- Watcher/status output includes stream counters (`items`) and emitted `bytes` when present in sidecar snapshots.
- `scripts/explore/` — Exploratory/investigative scripts.
- Benchmark harnesses should emit visible progress logs (graph/case/backend + iteration) for long runs.

## Testing

```bash
python -m pytest tests/                          # full suite
python -m pytest tests/test_graph_cache.py -q    # focused: cache module
python -m pytest tests/test_diff_engine.py -q    # focused: engine core
```

During active development, run only the focused tests relevant to your changes rather than the full suite. Run the full suite before committing.

## Dev practices

- Don't write throwaway scripts. Save exploratory ones in `scripts/explore/`.
- **Preserve knowledge during feature work.** Update README.md and AGENTS.md with what was built, why, and domain insights learned.
- When a perf investigation yields concrete bottlenecks or measured stage timings, save a concise findings note under `docs/perf/` (facts only: command/context, key numbers, hotspots, and chosen follow-up actions).
- Don't state obvious operational facts to the user (for example, that an already-running process won't pick up new code until restarted).
