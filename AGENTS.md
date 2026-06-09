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
  - `edge_policy.py` — Declarative edge classification table (relationship → include/context, geometry/data/placement/spatial domain).
  - `merkle.py` — Bottom-up sha256 Merkle hashing over domain subgraphs. Produces location-independent `vh_geometry` (placement excluded, so repeated identical components share it across locations) and `vh_data`. GlobalId and OwnerHistory never enter any hash.
  - `wl_gossip.py` — WL-style topology hash `vh_topology` from a self seed (`class|vh_geometry|vh_data`) plus context (k=1) and spatial (k=2) neighbor seeds.
  - `spatial.py` — Resolves `ObjectPlacement` matrix chains; emits world-space quantized placement matrix, centroid, and AABB per product.
  - `signatures.py` — Assembles per-product/spatial `SignatureVector`s into a `SignatureBundle` (signatures + diagnostics + edge stats only; the full parse result is not retained).
- `athar/matcher/core.py` — Tiered pool-reduction matching (`match_signatures`). Each tier examines only still-unmatched pools and emits disjoint 1:1 pairs; there are no candidate lists, no scoring pass, and no assignment step, so memory stays O(N) and ambiguity is resolved by construction. Tiers, strongest first: unique GlobalId + same class (score 1.0 when the full vector is identical, else 0.9); same-class full signature-vector equality zipped in step order (`geometry_hash`, 0.8); globally unique `(canonical_class, vh_topology)` bucket with proximity sanity (`tier2_signature`, 0.7); class-compatible spatial fallback via exact-centroid zip or uniform-grid nearest neighbour within `radius_m` (`spatial_fallback`, 0.5). Every tier keys on canonical class, so matched pairs never cross classes. Duplicated GlobalIds are never identity evidence — those entities fall through to the structural tiers. The only safety valve is `spatial_probe_limit` (default 128 grid probes per entity, policy key `spatial_probe_limit`): a capped entity is left unmatched rather than matched approximately, surfaced in diagnostics as `spatial.probe_capped`.
- `athar/delta/report.py` — Per-aspect change report assembly. Sections `added/deleted/modified/unchanged`; matched items carry `match{score,reason}`, per-aspect `changed/unchanged` for `geometry/data/topology/placement` plus `placement_delta_mm`, `data_hash{old,new}`, `change_scope` (`intrinsic|transitive|mixed|none`), and conservative `conflict` downgrade metadata for low-confidence fallback transitive/mixed matches. Stats cover section counts, signature counts, parse diagnostics, edge stats, `modified_change_scope`, `modified_conflicts`, `modified_match_reasons`, `modified_score_bands` (`high|medium|low`), and `dropped_matches`.
- `athar/__main__.py` — Minimal CLI: `--stream ndjson|chunked_json`, `--chunk-size`, `--matcher-radius-m`. Errors print to stderr and exit 1.

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
- Identity is evidence-tiered: unique GlobalIds are the strongest signal, but duplicated GlobalIds must never become identity evidence, and GUID-free recovery (vector/topology/spatial tiers) must stay conservative — 1:1 by construction, no ambiguous fan-out.
- Matching quality is prioritized over throughput: preserve correct entity alignment (`modified`) rather than collapsing into `added/deleted`; keep cutoffs conservative and use safety valves (`spatial_probe_limit`) only for pathological inputs, surfaced via diagnostics.
- Output structured JSON. Human-readable summaries are a presentation concern, not a diff concern.
- No deep B-rep geometry comparison. Signatures hash geometry-domain subgraphs and compare placement matrices/spatial features only.
- Prefer a repo-local `.venv` for development. Verified local workflow is `make dev-setup` then `make test`.

## Running

### Core Engine (Raw JSON)

```bash
python -m athar old.ifc new.ifc                          # raw JSON diff
python -m athar old.ifc new.ifc --stream ndjson          # NDJSON records
python -m athar old.ifc new.ifc --stream chunked_json --chunk-size 1000
python -m athar old.ifc new.ifc --matcher-radius-m 0.5   # spatial fallback radius
```

### Full Tool (`athar_layers`)

Currently disabled pending rewiring to the current engine.

## Scripts

- `scripts/inspect_ifc.py` — Print summary stats for an IFC file.
- `scripts/inspect_ifc_identity.py` — Show project name/GlobalId and header timestamp.
- `scripts/inspect_guid_overlap.py` — Show entity GUID overlap matrix between files.
- `scripts/explore/` — Exploratory/investigative scripts (entity/relationship/pset inspectors).

## Testing

```bash
make test                                        # full default suite (~3s)
python -m pytest tests/test_matcher_core.py -q   # focused: matcher tiers
python -m pytest tests/test_engine.py -q         # focused: engine end-to-end
make test-large-acceptance                       # opt-in large IFC acceptance
```

The default suite is fast because engine end-to-end tests run on small real
fixtures: the `real-world-test/Building-Landscaping-v1/v2.ifc` IFC4 pair
(~1.2MB each), `real-world-test/Duplex-Architecture.ifc` (2.3MB IFC2X3 — the
only default-tier run through the IFC2X3 parse path), and
`tests/fixtures/tiny_no_products.ifc` (empty-model edge case), plus tiny
synthetic inputs. There are no multi-minute fixtures in the default path. The
GUID-scramble metamorphic test generates its own scrambled variant of the small
pair at test time, and the determinism test re-runs the full pipeline with the
bundle cache cleared and asserts byte-identical output.

Large acceptance tier is opt-in via `ATHAR_RUN_LARGE_ACCEPTANCE=1` (see `tests/test_acceptance_large_ifc.py`). Current repo-default corpus paths are:
- `real-world-test/real-world-spanish-180mb.ifc` (primary large acceptance model)
- `real-world-test/uni-project-house-50mb.ifc` (smaller unrelated companion model)
Path overrides: `ATHAR_ACCEPTANCE_HOLY_GRAIL_PATH`, `ATHAR_ACCEPTANCE_SIMPLIFIED_PATH`. Optional per-test wall-clock bound: `ATHAR_ACCEPTANCE_TIMEOUT_S` (seconds).

During active development, run only the focused tests relevant to your changes rather than the full suite. Run the full suite before committing.

## Dev practices

- Don't write throwaway scripts. Save exploratory ones in `scripts/explore/`.
- **Preserve knowledge during feature work.** Update README.md and AGENTS.md with what was built, why, and domain insights learned.
- When a perf investigation yields concrete bottlenecks or measured stage timings, save a concise findings note under `docs/perf/` (facts only: command/context, key numbers, hotspots, and chosen follow-up actions). Notes under `docs/perf/` are dated historical records; `docs/perf/STATUS.md` states what is still current.
- Don't state obvious operational facts to the user (for example, that an already-running process won't pick up new code until restarted).
