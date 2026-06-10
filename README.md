# Athar

Semantic diff for IFC files. Compares BIM models at the entity/property level — not line-by-line text.

*Athar (Arabic: أثار) — a trace left behind.*

## How it works

For each IFC file, Athar builds a **signature vector** per product/spatial entity:

- `vh_geometry` — sha256 Merkle hash over the entity's geometry-domain subgraph, **excluding placement**, so identical components share it across locations.
- `vh_data` — Merkle hash over the data-domain subgraph (properties, materials, typing). GlobalId and OwnerHistory never enter any hash.
- `vh_topology` — WL-style gossip hash over context and spatial neighborhoods.
- World-space placement matrix (quantized), centroid, and AABB from the resolved `ObjectPlacement` chain.

Old/new entities are then matched by **tiered pool reduction**: each tier only
looks at still-unmatched entities and emits disjoint 1:1 pairs, so there is no
candidate explosion and no ambiguous fan-out by construction.

| Tier | Evidence | Score |
|------|----------|-------|
| `guid` | GlobalId unique on both sides + same class | 1.0 (identical vector) / 0.9 |
| `geometry_hash` | same class + full signature-vector equality (interchangeable entities, zipped 1:1) | 0.8 |

Anything weaker is reported as added+deleted rather than guessed: a corpus
survey over real revision, discipline, and unrelated model pairs
(`docs/corpus/2026-06-10-corpus-survey.md`) showed that topology-only and
spatial-proximity fallback matching never fired on a genuine revision pair and
only manufactured cross-model matches, so those tiers were removed. Duplicated
GlobalIds are never trusted as identity — those entities fall through to the
vector tier. Matching is deterministic and entirely algorithmic, with no
tuning knobs.

The delta report classifies every entity as `added`, `deleted`, `modified`, or
`unchanged`, with per-aspect detail (`geometry/data/topology/placement`,
`placement_delta_mm`), `change_scope` (`intrinsic` = the entity itself changed,
`transitive` = only its neighborhood changed, `mixed`), and matcher
diagnostics (per-tier match counts, duplicate-GUID counts).

Schema support: **IFC4 and IFC2X3**, same-schema comparisons only (no
IFC2X3↔IFC4 translation).

## Installation

```bash
pip install -e .
```

Pure Python. Requires Python 3.10+ and [ifcopenshell](https://ifcopenshell.org/).

Local development:

```bash
python -m venv .venv
source .venv/bin/activate
make dev-setup
make test
```

## Usage

```bash
# Two-file diff (JSON output)
python -m athar old.ifc new.ifc

# Stream output as NDJSON records (header, one record per entity, end with stats)
python -m athar old.ifc new.ifc --stream ndjson

# Stream output as chunked JSON
python -m athar old.ifc new.ifc --stream chunked_json --chunk-size 1000
```

The `athar_layers` package (human-readable summaries, folder mode, Markdown
reports) is temporarily disabled while it is rewired to the current engine.

## Testing

```bash
make test                    # full default suite (~20 seconds)
make test-large-acceptance   # opt-in large IFC acceptance checks
```

The default suite runs corpus-wide invariant tests (same-file zero diff,
stats accounting, no cross-class matches, duplicate-GUID and dangling-ref
behavior, schema policy) over every small real IFC in the corpus — the
Building-Landscaping v0→v3 revision chain, the IFC2X3 Duplex architecture
model, two GNI BIM Fundamentals samples, and small external samples — plus
metamorphic
GUID-scramble tests that prove identity recovery without GlobalId evidence.
It also generates known-edit semantic scenarios from real seeds (GUID
scramble, moving one product by a known vector, deleting a leaf product,
editing a pset value, renaming, duplicating a GUID) in temp dirs and asserts
the report against expectations derived from the constructed edit itself,
never from blessing engine output. Small files from the optional external
corpus (default `../vscode-ifc/test-files`, override via
`ATHAR_EXTERNAL_CORPUS_DIR`) skip when absent.

Large acceptance checks are opt-in so day-to-day runs stay fast:

```bash
ATHAR_RUN_LARGE_ACCEPTANCE=1 python -m pytest tests/test_acceptance_large_ifc.py -q
```

The acceptance tier covers the medium/large corpus (8MB–182MB; same-file
invariants, a real 44MB revision pair, discipline pairs, unrelated pairs, and
a GUID-scramble at scale). Files that are missing or unfetched LFS pointers
skip individually. Bound wall-clock per test via `ATHAR_ACCEPTANCE_TIMEOUT_S`.

At every tier, tests assert structural invariants and expectations derived
from the inputs themselves — never exact whole-report JSON goldens. Corpus
files too big for the repo will be described by a checked-in manifest rather
than stored (design: `docs/corpus/REMOTE_CORPUS_MANIFEST.md`; not yet
implemented).

## Documentation

Architecture and conventions live in [AGENTS.md](AGENTS.md). Dated performance
findings live under `docs/perf/`, with `docs/perf/STATUS.md` stating what is
still current. Corpus survey findings live under `docs/corpus/`.

## License

MIT
