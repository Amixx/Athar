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
| `tier2_signature` | globally unique `(class, topology-hash)` bucket + proximity sanity | 0.7 |
| `spatial_fallback` | same class, same world centroid or nearest neighbour within radius | 0.5 |

Duplicated GlobalIds are never trusted as identity — those entities fall
through to the structural tiers. Matching is deterministic and entirely
algorithmic.

The delta report classifies every entity as `added`, `deleted`, `modified`, or
`unchanged`, with per-aspect detail (`geometry/data/topology/placement`,
`placement_delta_mm`), `change_scope` (`intrinsic` = the entity itself changed,
`transitive` = only its neighborhood changed, `mixed`), conservative `conflict`
downgrades for low-confidence fallback matches, and matcher diagnostics
(per-tier match counts, duplicate-GUID counts, spatial probe stats).

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

# Control the spatial fallback radius (meters)
python -m athar old.ifc new.ifc --matcher-radius-m 0.5

# Stream output as NDJSON records (header, one record per entity, end with stats)
python -m athar old.ifc new.ifc --stream ndjson

# Stream output as chunked JSON
python -m athar old.ifc new.ifc --stream chunked_json --chunk-size 1000
```

The `athar_layers` package (human-readable summaries, folder mode, Markdown
reports) is temporarily disabled while it is rewired to the current engine.

## Testing

```bash
make test                    # full default suite (~3 seconds)
make test-large-acceptance   # opt-in large IFC acceptance checks
```

The default suite runs engine end-to-end tests on a small real IFC pair
(`real-world-test/Building-Landscaping-v1/v2.ifc`, ~1.2MB each), including a
metamorphic GUID-scramble test that proves identity recovery without GlobalId
evidence.

Large acceptance checks are opt-in so day-to-day runs stay fast:

```bash
ATHAR_RUN_LARGE_ACCEPTANCE=1 python -m pytest tests/test_acceptance_large_ifc.py -q
```

Default acceptance corpus paths (override via `ATHAR_ACCEPTANCE_HOLY_GRAIL_PATH`
/ `ATHAR_ACCEPTANCE_SIMPLIFIED_PATH`; bound wall-clock per test via
`ATHAR_ACCEPTANCE_TIMEOUT_S`):

- `real-world-test/real-world-spanish-180mb.ifc` (primary large model)
- `real-world-test/uni-project-house-50mb.ifc` (smaller unrelated companion)

## Documentation

Architecture and conventions live in [AGENTS.md](AGENTS.md). Dated performance
findings live under `docs/perf/`, with `docs/perf/STATUS.md` stating what is
still current.

## License

MIT
