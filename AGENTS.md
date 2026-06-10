# Athar

Semantic IFC diff tool. Compares BIM models at the entity/property level, not text level.

*Athar (Arabic: أثار) — a trace left behind.*

## Architecture

Athar contains the core diff engine plus a transitional integration package.

### Core Engine (`athar/`)

Pure Python. Pipeline: parse → signatures → tiered matching → delta report.

- `athar/engine.py` — Orchestration. `diff_files()` builds a `SignatureBundle` per file (with an in-process cache keyed by path/mtime/size), enforces the same-schema policy, runs the matcher, and assembles the report with `guid_collisions` and `matcher_diagnostics` stats. `stream_diff_files()` wraps the same report as `ndjson` or `chunked_json` records with a deterministic `end` stats record.
- `athar/bottom/` — Signature pipeline for one IFC file (`build_signature_bundle`):
  - `index.py` — Byte-offset STEP index for random-access diagnostics.
  - `parser.py` — Schema-aware parse via ifcopenshell into `ParsedEntity` records with canonicalized scalar attributes (unit-normalized quantized lengths, numeric string literals like `"0"`/`"1"` preserved as strings, NFC text). Spatial tagging supports both IFC4 (`IfcSpatialElement`) and IFC2X3 (`IfcSpatialStructureElement`) roots.
  - `link_inversion.py` — Reverse-reference maps for parsed entities.
  - `edge_policy.py` — Declarative edge classification table (relationship → include/context, geometry/data/placement/spatial domain). Property/quantity subtrees (`HasProperties`, `Quantities`, `HasQuantities`, `HasPropertySets`) are include/data edges so property *values* reach `vh_data` (canon v2 — before that the data Merkle stopped at the pset shell and value edits were invisible); other generic non-geometry refs stay ignored.
  - `merkle.py` — Bottom-up sha256 Merkle hashing over domain subgraphs. Produces location-independent `vh_geometry` (placement excluded, so repeated identical components share it across locations) and `vh_data`. GlobalId and OwnerHistory never enter any hash.
  - `wl_gossip.py` — WL-style topology hash `vh_topology` from a self seed (`class|vh_geometry|vh_data`) plus context (k=1) and spatial (k=2) neighbor seeds.
  - `spatial.py` — Resolves `ObjectPlacement` matrix chains; emits world-space quantized placement matrix, centroid, and AABB per product.
  - `signatures.py` — Assembles per-product/spatial `SignatureVector`s into a `SignatureBundle` (signatures + diagnostics + edge stats only; the full parse result is not retained).
- `athar/matcher/core.py` — Tiered pool-reduction matching (`match_signatures`, no tuning knobs). Each tier examines only still-unmatched pools and emits disjoint 1:1 pairs; there are no candidate lists, no scoring pass, and no assignment step, so memory stays O(N) and ambiguity is resolved by construction. Tiers, strongest first: unique GlobalId + same class (score 1.0 when the full vector is identical, else 0.9); same-class full signature-vector equality zipped in step order (`geometry_hash`, 0.8). Anything weaker is reported as added+deleted rather than guessed — the 2026-06 corpus survey (`docs/corpus/2026-06-10-corpus-survey.md`) showed the former topology-unique and spatial-nearest fallback tiers never fired on a real revision pair and only produced cross-model container matches. Every tier keys on canonical class, so matched pairs never cross classes. Duplicated GlobalIds are never identity evidence — those entities fall through to the vector tier.
- `athar/delta/report.py` — Per-aspect change report assembly. Sections `added/deleted/modified/unchanged`; matched items carry `match{score,reason}`, per-aspect `changed/unchanged` for `geometry/data/topology/placement` plus `placement_delta_mm`, `data_hash{old,new}`, and `change_scope` (`intrinsic|transitive|mixed|none`). Stats cover section counts, signature counts, parse diagnostics, edge stats, `modified_change_scope`, and `dropped_matches`. Because `geometry_hash` matches equal full vectors, every `modified` item is by construction a guid match with score 0.9.
- `athar/__main__.py` — Minimal CLI: `--stream ndjson|chunked_json`, `--chunk-size`. Errors print to stderr and exit 1.

Schema policy: IFC4 and IFC2X3 are both supported, but only same-schema comparisons in one run (no IFC2X3↔IFC4 translation).

### Higher Layers (`athar_layers/`)

Integration layers that build upon the core engine for human-readable output, scene modeling, and folder-level versioning. This package is transitional and may be moved out of this repository. The `athar_layers` CLI is currently disabled pending rewiring to the current engine.

Detailed information for these components can be found in [athar_layers/AGENTS.md](athar_layers/AGENTS.md).

## Conventions

- Python 3.10+, pure Python. The only core runtime dependency is `ifcopenshell`.
- Engine modules (`athar/`) must not depend on integration/presentation modules (`athar_layers/`).
- Layering inside the engine: `athar/bottom/` is self-contained; `athar/matcher/` may depend on `athar/bottom/` types; `athar/delta/` may depend on both; `athar/engine.py` orchestrates all three.
- Use `ifcopenshell` for all IFC parsing. Do not parse STEP files as text.
- Keep diffing deterministic and algorithmic — no AI in the diff pipeline itself.
- Identity is evidence-tiered and conservative: unique GlobalIds are the strongest signal, duplicated GlobalIds must never become identity evidence, and GUID-free recovery is limited to full signature-vector equality — 1:1 by construction, no ambiguous fan-out. Weaker evidence (shared topology, spatial proximity) is reported as added+deleted, not matched; do not reintroduce inference tiers without corpus evidence of a real revision pair that needs them.
- Matching quality is prioritized over throughput: preserve correct entity alignment (`modified`) for evidenced matches rather than collapsing into `added/deleted`, but never invent alignment from weak evidence.
- Output structured JSON. Human-readable summaries are a presentation concern, not a diff concern.
- No deep B-rep geometry comparison. Signatures hash geometry-domain subgraphs and compare placement matrices/spatial features only.
- Prefer a repo-local `.venv` for development. Verified local workflow is `make dev-setup` then `make test`.

## Running

### Core Engine (Raw JSON)

```bash
python -m athar old.ifc new.ifc                          # raw JSON diff
python -m athar old.ifc new.ifc --stream ndjson          # NDJSON records
python -m athar old.ifc new.ifc --stream chunked_json --chunk-size 1000
```

### Full Tool (`athar_layers`)

Currently disabled pending rewiring to the current engine.

## Scripts

- `scripts/inspect_ifc.py` — Print summary stats for an IFC file.
- `scripts/inspect_ifc_identity.py` — Show project name/GlobalId and header timestamp.
- `scripts/inspect_guid_overlap.py` — Show entity GUID overlap matrix between files.
- `scripts/explore/` — Exploratory/investigative scripts (entity/relationship/pset inspectors; `corpus_survey.py` regenerates the corpus survey JSON behind `docs/corpus/`).

## Testing

```bash
make test                                          # full default suite (~20s)
python -m pytest tests/test_matcher_core.py -q     # focused: matcher tiers
python -m pytest tests/test_engine.py -q           # focused: engine end-to-end
python -m pytest tests/test_corpus_invariants.py -q  # focused: corpus invariants
make test-large-acceptance                         # opt-in large IFC acceptance
```

The corpus registry lives in `tests/corpus.py`: repo LFS files (fail loudly on
unfetched pointers) plus an optional external corpus (default
`../vscode-ifc/test-files`, override `ATHAR_EXTERNAL_CORPUS_DIR`; absent files
skip). It also provides the shared report-invariant assertions (accounting,
1:1 matching, class safety, score/reason consistency, change-scope
consistency) used by both tiers.

The default tier (`tests/test_corpus_invariants.py` plus the engine/matcher/
report tests) runs invariants over every small corpus file (≤2.4MB: the
Building-Landscaping v0→v3 IFC4 revision chain, Duplex-Architecture IFC2X3,
two GNI Revit IFC4 samples under `corpus/gni-bim-sample/`, small external
samples, `tests/fixtures/tiny_no_products.ifc`): same-file zero diff,
revision-pair invariants, GUID-scramble metamorphics, generated
duplicate-GUID and dangling-ref variants, cross-schema rejection.

`tests/test_semantic_scenarios.py` adds known-edit semantic scenarios on top:
`tests/mutations.py` applies one constructed edit to a real seed (GUID
scramble, single-product move by a known vector, leaf-product delete, pset
value edit, rename, duplicated GUID) and returns a `Mutation` manifest whose
expectations (victim section, aspect states, `placement_delta_mm` norm) are
derived from the edit itself — never from blessing engine output. Pairs are
generated in pytest tmp dirs through one shared ifcopenshell write path, so
the constructed edit is the only delta; victim pickers only select exclusive
(unshared) placements/psets/properties so each mutation is provably a
single-entity edit. There are no multi-minute fixtures in the default path;
the determinism test re-runs the full pipeline with the bundle cache cleared
and asserts byte-identical output.

The large acceptance tier is opt-in via `ATHAR_RUN_LARGE_ACCEPTANCE=1` (see
`tests/test_acceptance_large_ifc.py`): same-file invariants over the
medium/large corpus (8MB–182MB), the real `AdvancedProject.ifc` → `adv proj
changed.ifc` 44MB revision pair, discipline pairs (Duplex, Revit), unrelated
pairs (`real-world-spanish-180mb.ifc` ↔ `uni-project-house-50mb.ifc`,
`BasicHouse` ↔ `AdvancedProject`), and a 44MB GUID-scramble. Missing files
skip individually. Optional per-test wall-clock bound:
`ATHAR_ACCEPTANCE_TIMEOUT_S` (seconds). Corpus facts (sizes, schemas,
signature counts, tier distributions, runtimes): `docs/corpus/`.

During active development, run only the focused tests relevant to your changes rather than the full suite. Run the full suite before committing.

## Dev practices

- Don't write throwaway scripts. Save exploratory ones in `scripts/explore/`.
- **Preserve knowledge during feature work.** Update README.md and AGENTS.md with what was built, why, and domain insights learned.
- When a perf investigation yields concrete bottlenecks or measured stage timings, save a concise findings note under `docs/perf/` (facts only: command/context, key numbers, hotspots, and chosen follow-up actions). Notes under `docs/perf/` are dated historical records; `docs/perf/STATUS.md` states what is still current. Corpus measurements (file inventory, pair shapes, tier distributions) follow the same pattern under `docs/corpus/`.
- Don't state obvious operational facts to the user (for example, that an already-running process won't pick up new code until restarted).
