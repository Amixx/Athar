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
diagnostics (per-tier match counts, duplicate-GUID counts). Reports also carry
an `audit` block with producer/canon metadata plus old/new input provenance:
path, schema, byte size, and sha256. That makes the raw JSON suitable for
archiving as milestone evidence even before a visual viewer or PDF/HTML export
exists. Default output stays byte-deterministic; pass `--generated-at now` or
an explicit timestamp when an archived copy should embed a generation time.

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

# Include an audit generation timestamp for an archived report
python -m athar old.ifc new.ifc --generated-at now

# CI policy gate over a fresh diff; exits 2 on policy violations
python -m athar check old.ifc new.ifc --policy athar-policy.json

# CI policy gate over an existing JSON report
python -m athar check --report report.json --policy athar-policy.json

# Stream output as NDJSON records (header, one record per entity, end with stats)
python -m athar old.ifc new.ifc --stream ndjson

# Stream output as chunked JSON
python -m athar old.ifc new.ifc --stream chunked_json --chunk-size 1000
```

### CI Policy Gates

`athar check` evaluates an Athar JSON report against a small JSON policy and
prints structured JSON: `{ "ok": bool, "violations": [...], "summary": ... }`.
It exits `0` when the policy passes, `2` when the diff is valid but violates
policy, and `1` for execution/configuration errors.

Example policy:

```json
{
  "forbid_schema_change": true,
  "max_deleted": 0,
  "max_deleted_by_class": {
    "IfcWall": 0
  },
  "max_modified_change_scope": {
    "intrinsic": 10,
    "mixed": 0
  },
  "forbid_site_placement_change": true,
  "max_placement_delta_mm": 50,
  "max_placement_delta_mm_by_class": {
    "IfcSite": 0
  },
  "forbid_aspect_changes": {
    "data": ["IfcWall"],
    "placement": "*"
  }
}
```

The gate is report-driven and CI-platform-agnostic. Current policies can only
inspect signals present in the report: schema equality, section counts,
class-specific counts, modified change scopes, changed aspects, and placement
translation deltas. Fine-grained checks such as “property `FireRating` was
removed” require future report detail beyond the current data hash.

### Git IFC Diff Driver

Athar can be installed as a Git diff driver for `.ifc` files. The driver uses
the same semantic engine, renders a deterministic terminal summary, and caches
signature bundles on disk by Git blob id (falling back to a content hash for
working-tree files).

```bash
# Configure the current repository and add .gitattributes guidance
athar git install

# Manual terminal rendering without Git plumbing
athar git diff old.ifc new.ifc
```

`athar git install` sets `diff.athar.command` to
`athar git diff --external` and appends:

```gitattributes
*.ifc diff=athar -merge
```

The `-merge` marker is intentional. IFC is STEP text, but Git's default text
merge can silently splice two exported models into a corrupt file with
duplicate or dangling STEP ids. Athar provides semantic diffing; it does not
attempt semantic merge.

The persistent signature cache is versioned and deterministic, but v1 does not
evict old entries. Set `ATHAR_CACHE_DIR` to isolate or periodically clear a
large repository's cache.

### GitHub PR Comments

Athar also includes a thin GitHub PR bot command for CI. It discovers `.ifc`
files changed between two Git refs, runs the same cached semantic diff for
modified/renamed pairs, and creates or updates one stable PR comment marked by
an HTML sentinel.

```bash
athar git pr-comment \
  --base "$BASE_SHA" \
  --head "$HEAD_SHA" \
  --repo owner/repo \
  --pr 123
```

Use `--dry-run` to print the Markdown comment body without calling GitHub.
When `--policy-result athar-check.json` is supplied, the comment includes a
pass/fail summary from an `athar check` result, but policy enforcement remains
separate. The checked-in `.github/workflows/athar-pr-diff.yml` workflow shows
the default pull-request setup using `GITHUB_TOKEN`.

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
editing a pset value, editing a type-level pset value inherited by its
occurrences, renaming, duplicating a GUID) in temp dirs and asserts
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

Agent-facing architecture and conventions are split by ownership: start at
[AGENTS.md](AGENTS.md), then follow the nearest subdirectory `AGENTS.md` for
core engine, Git integration, tests, docs, and scripts. Dated performance
findings live under `docs/perf/`, with `docs/perf/STATUS.md` stating what is
still current. Corpus survey findings live under `docs/corpus/`.

## License

MIT
