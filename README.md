# Athar

Semantic diff for IFC files. Compares BIM models at the entity/property level — not line-by-line text.

*Athar (Arabic: أثار) — a trace left behind.*

## How it works

For each IFC file, Athar builds a **signature vector** per product, spatial element and the project:

- `vh_geometry` — sha256 Merkle hash over the entity's geometry-domain subgraph, **excluding placement**, so identical components share it across locations. A host element also covers its openings and projections and where each sits relative to it.
- `vh_data` — Merkle hash over everything else the entity owns or uses: attributes, properties, materials, classifications, groups, typing, styles; for the project also units, contexts and georeferencing. GlobalId and OwnerHistory never enter any hash.
- `vh_topology` — WL-style gossip hash over context and spatial neighborhoods.
- World-space placement matrix (quantized) from the resolved `ObjectPlacement` chain; a digest of the chain's content where it cannot be resolved (grid placements, placement cycles). Coordinates are converted to metres before composing the chain; matrices are quantized only after composition at one micrometre for translations and `1e-6` for rotation entries.

Coverage is the default: every attribute and reference is examined unless an explicit table excludes it with a reason. What no signature reaches is counted and digested per class, and the report's `stats.coverage.unexamined_changes` names the classes that differ there.

Old/new entities are then matched by **tiered pool reduction**: each tier only
looks at still-unmatched entities and emits disjoint 1:1 pairs, so there is no
candidate explosion and no ambiguous fan-out by construction.

| Tier | Evidence | Score |
|------|----------|-------|
| `guid` | GlobalId unique on both sides + same class | 1.0 (identical vector) / 0.9 |
| `geometry_hash` | same class + full signature-vector equality; entities sharing a vector pair by equal relation targets, then in STEP order | 0.8 |
| `guid_reclassified` | GlobalId unique on both sides + different class, both unmatched above (reported `modified` with a `class` aspect) | 0.7 |

Anything weaker is reported as added+deleted rather than guessed: a corpus
survey over revision, discipline, and unrelated model pairs
(`docs/corpus/2026-06-10-corpus-survey.md`) found that topology-only and
spatial-proximity fallback matching never fired on its revision pairs, which
are the author's own re-saves of sample models, and only manufactured
cross-model matches, so those tiers were removed. Duplicated
GlobalIds are never trusted as identity — those entities fall through to the
vector tier. Matching is deterministic and entirely algorithmic, with no
tuning knobs.

The delta report classifies every entity as `added`, `deleted`, `modified`, or
`unchanged`, with per-aspect detail (`geometry/data/topology/placement/relations`,
`placement_delta_mm`), `change_scope` (`intrinsic` = the entity itself changed,
`transitive` = only its neighborhood changed, `mixed`), and matcher
diagnostics (per-tier match counts, duplicate-GUID counts). Every data change
lists what differs in `data_delta`. The `relations` aspect compares what an
entity names through its relationships (container, whole, type, host, the
other end of a 1:1 relationship) by matched identity, and a change lists the
relationship with its old and new targets: an element moved to another storey
is `modified` itself, the storeys only `transitive`. Relations that differ
only in added or deleted targets (a port whose host was deleted) count as
`transitive` too. The `data` aspect is unchanged if and only if
`data_hash.old == data_hash.new`. Geometry and data hashes depend on content,
not instance sharing: a shape reached through data contributes its full content.
Editing a type map reports both `geometry` and `data` on an occurrence that maps
it, including the map's `(shape)` fact in `data_delta`; an occurrence that does
not map it reports `data` only. Editing a swept profile shared with a material
profile reports both aspects; an untouched equal copy in the material profile
leaves `data` unchanged. Merging or splitting equal copies changes neither hash.
An entity whose `GlobalId` was regenerated and that is matched by content
stays `unchanged` but carries `identity.guid_changed` with both ids; the
report lists these pairs under `guid_changed` and counts them in
`stats.guid_changed`.
An element whose world placement changed only because an ancestor in its
placement chain moved is `transitive` and names that ancestor in `moved_with`;
`stats.placement_cohorts` names the shared `root_cause` of a group move.
An element placed through an
`IfcGridPlacement` is marked `placement_unresolved`: grid geometry is not
evaluated, so it has no coordinates and its placement aspect compares the grid
placement's content. Reports also carry
an `audit` block with producer/canon metadata plus old/new input provenance:
path, schema, byte size, and sha256. That makes the raw JSON suitable for
archiving as milestone evidence even before a visual viewer or PDF/HTML export
exists. Default output stays byte-deterministic; pass `--generated-at now` or
an explicit timestamp when an archived copy should embed a generation time.

Schema support: **IFC4 and IFC2X3**, same-schema comparisons only (no
IFC2X3↔IFC4 translation).

IfcOpenShell 0.9.0.post1+ supplies unit conversion factors, including SI prefixes, conversion-based chains and derived units. Property measures with a unit type are compared in SI using the file's unit or the property's own unit, and reported with the value as written where it differs. Context-dependent units and conversions that require an offset have no multiplicative factor: their values stay unscaled and unlabelled. Missing native unit factors likewise leave signature numbers unscaled.

## Installation

```bash
pip install athar-ifc     # into the active virtualenv
pipx install athar-ifc    # or as a standalone command
```

Either installs the `athar` command (`athar --help` lists the subcommands,
`athar --version` prints the installed version). The PyPI distribution is `athar-ifc`; the
Python packages import as `athar`, `athar_git`, and so on. Wheels ship the Rust
signature pipeline (`athar_native`) and the viewer bundle for Linux (x86_64,
aarch64), macOS (arm64, x86_64), and Windows (x86_64); other platforms build
from the sdist, which needs a Rust toolchain. Requires Python 3.10+ and
[ifcopenshell](https://ifcopenshell.org/).

Local development (needs a Rust toolchain):

```bash
python -m venv .venv
source .venv/bin/activate
make dev-setup
make native-build   # builds athar_native and installs Athar editable
make test
```

## Usage

```bash
# Two-file diff (JSON output); `athar diff old.ifc new.ifc` is the same
athar old.ifc new.ifc

# Include an audit generation timestamp for an archived report
athar old.ifc new.ifc --generated-at now

# CI policy gate over a fresh diff; exits 2 on policy violations
athar check old.ifc new.ifc --policy athar-policy.json

# CI policy gate over an existing JSON report
athar check --report report.json --policy athar-policy.json

# Stream output as NDJSON records (header, one record per entity, end with stats)
athar old.ifc new.ifc --stream ndjson

# Stream output as chunked JSON
athar old.ifc new.ifc --stream chunked_json --chunk-size 1000
```

`athar COMMAND --help` documents each subcommand (`diff`, `check`, `git`,
`view`, `store`, `rekey`).

### Experimental STEP ID Restoration

```bash
athar rekey base.ifc exported.ifc -o rekeyed.ifc
```

This experimental command restores BASE's STEP IDs on matched entities in NEW
and assigns unmatched entities IDs above BASE's maximum. It uses unique GUIDs,
Athar's product matches, same-parent attribute roles, inverse relationship
roles, and exact resource content. Identical duplicates are paired through their
matched parents: ordered duplicate children use attribute positions, and
unordered duplicate child buckets must have equal sizes. Remaining equal-content
twins pair only when the buckets have
matching sizes, no incoming NEW references, and every NEW body resolves through
established mappings to the same values at every reference position. Leaf twins
satisfy the outgoing-reference check. These pairings do not propagate role
identity to descendants. Other twins get fresh IDs and are counted as unmatched
twins. Shared resources remain shared; conflicting role proposals are not
forced into a many-to-one match. A last role pass pairs entities whose class,
name and named children are unique among the unmatched entities of both files.
Unique-content recovery and role propagation then run to a fixed point.
An object (product, type, spatial element) never takes the ID of one with a
different GlobalId, so a product whose exporter regenerated its GUID is refused
as an unmatched rooted addition; property sets and relationships still pair
across regenerated GUIDs. Resource role recovery is a serialization heuristic, not an
extension of the semantic diff matcher's identity policy.

The native tokenizer rewrites ID tokens and orders records by output ID without
changing NEW's literal spellings. Before publishing the output, IfcOpenShell
reopens it and every entity's class, attributes, and mapped references are
checked against NEW. Inputs cannot be overwritten. Schemas must match.
Matching statistics are printed as JSON: per-class counts, final role-pass
matches and ambiguous candidates, pairings refused for differing GlobalIds, and
unmatched equal-content twins.

Unmatched rooted additions are refused (exit 1, JSON `status: "refused"`,
no output replacement). Two independent rekeys cannot tell whether both
branches added the same root; ifcmerge copies both additions even when their
GlobalIds agree. Any DATA line in BASE or the rekeyed output that is not one
whole record, a one-line comment or blank is also refused: ifcmerge's line
reader drops records spanning lines and truncates one whose first line holds a
`;` inside a string.
So is a matched record whose text differs from BASE when ifcmerge's attribute
tokenizer cannot split the BASE or rekeyed line into exactly its attributes
(nested lists, doubled quotes, lists of more than 16,383 plain members, the
limit of Perl before 5.30): ifcmerge rebuilds a record changed on both
branches from those tokens and silently truncates it. Refusal diagnostics
include candidate matching statistics and the offending NEW ids (BASE ids for
BASE records not alone on their line).

This is not a general merge-safety guarantee: ambiguous resources, changed
sharing, unordered collections, regenerated GUIDs, and geometry restructuring
can still produce text churn or misleading identity continuity. Use the result
as an experiment, not as an unattended merge driver. The repeatable study runner
is `scripts/explore/rekey_study.py`; it requires a local corpus and an external
evidence directory. Its merge demo includes an explicitly constructed fork.

### CI Policy Gates

`athar check` evaluates an Athar JSON report against a small JSON policy and
prints structured JSON: `{ "ok": bool, "violations": [...], "summary": ... }`.
It exits `0` when the policy passes, `2` when the diff is valid but violates
policy, and `1` for execution/configuration errors.

Example policy:

```json
{
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

Policies are validated before diffing. Unknown keys, malformed values, negative
limits, unknown aspects/scopes, and misspelled IFC classes are errors. Classes
are case-insensitive, normalized to report classes, and include subtypes;
`IfcWallStandardCase` rules apply to `IfcWall`. For placement limits the nearest
class override wins over a supertype or global limit.

The gate requires an Athar report header, supported schema, four complete sections,
and consistent counts. Missing detail needed by a rule is an error, not a pass.
Schema mismatch is a failed gate with exit `2`, because Athar cannot semantically
diff different schemas; `forbid_schema_change` is not a policy key.

`forbid_property_value_change` maps property names to `"*"`, a class, or a list
of classes. It checks matched entities across all psets and occurrence/type
origins, on the effective value of each key (the occurrence's when it defines
the key, the type's otherwise). Changed effective values, removals, and
remove/add pairs with different values violate it; equal-value moves are
allowed, and so are a value moving between occurrence and type unchanged and a
type value changing under an occurrence override. A duplicated key's effective
value is the sorted list of its distinct values, and a duplicated key that
gains or loses a copy fails closed. Added values alone are checked only by
`forbid_property_addition` (the same shape). Property
rules require `property_deltas` on data-changed entities: the engine can omit
it both when comparison is unavailable and when no property changed, so these
ambiguous reports produce exit `1`. No engine changes are implied by this gate.

`max_placement_cohort_share` is a fraction from 0 to 1. A cohort exceeds the
budget when its member count divided by all matched products (modified plus
unchanged) is strictly greater than the limit. Violations carry translation,
size, share, and affected entities. Count-limit violations include up to 50
entity references plus a `truncated` count; text output prints up to ten.

The `georeferencing` pack forbids site placement/data changes, added/deleted
sites, replaced site GUIDs on matched sites, and translation cohorts covering
more than half the matched products. Its project-data ban applies only if a
report contains `IfcProject`; it does not cover map conversions.

`forbid_aspect_changes` takes the aspects `geometry`, `data`, `topology`,
`placement`, `relations` (re-contained, re-typed, re-hosted) and `class`
(reclassified).

Use `--policy PACK --explain` to print the normalized policy without diffing;
`--list-packs` prints shipped pack descriptions. `--format text` renders a
human-readable verdict; errors print a clear error line.

### Git IFC Diff Driver

Athar can be installed as a Git diff driver for `.ifc` files. The driver uses
the same semantic engine, renders a deterministic terminal summary, and caches
signature bundles on disk by Git blob id (falling back to a content hash for
working-tree files).

```bash
# Configure the current repository and add the .gitattributes rule
athar git install

# Manual terminal rendering without Git plumbing
athar git diff old.ifc new.ifc
```

`athar git install` sets `diff.athar.command` to an absolute command for the
installation that ran it: the `athar` console script that installation recorded,
or that interpreter running a bootstrap that drops the current directory from
`sys.path` when the record lists no script on disk. Git runs the
driver from the repository's top level, so the command never imports modules
from the repository being diffed, and it works without activating the
virtualenv or having `athar` on `PATH`; run it again after moving or
recreating that environment. It must run inside a repository and appends this
rule to the `.gitattributes` at the repository's top level:

```gitattributes
*.ifc diff=athar -merge
```

The `-merge` marker is intentional. IFC is STEP text, but Git's default text
merge can silently splice two exported models into a corrupt file with
duplicate or dangling STEP ids. Athar provides semantic diffing; it does not
attempt semantic merge.

The persistent signature cache is versioned (by the installed `athar-ifc`
version) and deterministic, but v1 does not evict old entries. Set
`ATHAR_CACHE_DIR` to isolate or periodically clear a large repository's cache.
Cache entries are Python pickles, so the cache directory is trusted local
state: Athar creates it with mode 0700, refuses one that another user owns or
that group or others can write, and it must never be shared with or restored
from a source you do not control.

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

`--policy-result IFC_PATH=RESULT_JSON` attaches an `athar check` JSON result
to the section of one changed file; repeat it once per file. Each such file
shows its pass/fail verdict and violation messages, and a pass/fail count
heads the comment. A result for a path the pull request does not change is an
error. Policy enforcement stays with `athar check`:

```bash
athar check base/model.ifc models/model.ifc --policy no-deletions > model.check.json
athar git pr-comment --base "$BASE_SHA" --head "$HEAD_SHA" \
  --policy-result models/model.ifc=model.check.json
```

The comment stays under 60,000 characters: per-entity detail rows, then
modified entities, placement-cohort lines and added/deleted entities are cut
in that order, each cut list ending in a "not shown" line, while totals,
per-file count tables and policy verdicts are always kept.

To run it in another repository, add `.github/workflows/athar-ifc-diff.yml`.
It installs the published wheel, comments on pull requests from the same
repository, and writes the comment to the job summary for fork and Dependabot
pull requests, whose token cannot comment:

```yaml
name: Athar IFC PR Diff

on:
  pull_request:
    paths: ["**.ifc", "**.IFC"]

permissions:
  contents: read
  pull-requests: write

concurrency:
  group: athar-ifc-diff-${{ github.event.pull_request.number }}
  cancel-in-progress: true

jobs:
  athar-ifc-diff:
    runs-on: ubuntu-latest
    timeout-minutes: 30
    env:
      BASE_SHA: ${{ github.event.pull_request.base.sha }}
      HEAD_SHA: ${{ github.event.pull_request.head.sha }}
      PR_NUMBER: ${{ github.event.pull_request.number }}
      REPOSITORY: ${{ github.repository }}
    steps:
      - uses: actions/checkout@11d5960a326750d5838078e36cf38b85af677262 # v4.4.0
        with:
          fetch-depth: 0
          lfs: true

      - uses: actions/setup-python@a26af69be951a213d495a4c3e4e4022e16d87065 # v5.6.0
        with:
          python-version: "3.12"

      - run: python -I -m pip install athar-ifc

      - name: Post IFC diff comment
        if: github.event.pull_request.head.repo.full_name == github.repository && github.actor != 'dependabot[bot]'
        env:
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
        run: >-
          athar git pr-comment
          --base "$BASE_SHA" --head "$HEAD_SHA" --repo "$REPOSITORY" --pr "$PR_NUMBER"

      - name: Write IFC diff to the job summary
        if: github.event.pull_request.head.repo.full_name != github.repository || github.actor == 'dependabot[bot]'
        run: >-
          athar git pr-comment --dry-run
          --base "$BASE_SHA" --head "$HEAD_SHA" --repo "$REPOSITORY" --pr "$PR_NUMBER"
          >> "$GITHUB_STEP_SUMMARY"
```

`python -I` keeps the checked-out pull request's files off `sys.path`, so a
`pip/` directory in the pull request cannot replace pip; the `athar` console
script never imports from the current directory.

This repository's own `.github/workflows/athar-pr-diff.yml` runs the same
steps against an editable install of the checkout, with pip and cargo caches.

### Visual 3D Diff Viewer

`athar view` overlays both models in one 3D scene in the browser, colored by
the semantic report: green added, red deleted, blue modified, gray ghosted
unchanged, amber displacement lines for placement moves. An old↔new slider
crossfades between revisions; clicking an entity shows its class, GUID,
changed aspects, placement delta, and match evidence.

```bash
athar view old.ifc new.ifc                        # build report, serve, open browser
athar view old.ifc new.ifc --no-open --port 4799  # print the URL instead
athar git install --difftool                      # then: git difftool -t athar -- model.ifc
```

The browser does all tessellation (`@ifc-lite/geometry` WASM); rendering is
three.js. The local server listens on 127.0.0.1 only, answers only requests
addressed to its own origin, and serves the models and report under a secret
per-launch path that is part of the printed URL. Cross-origin viewer development
requires explicit `--dev-origin URL`, which authorizes exactly that origin; no
private-network permission is granted. For example, use
`--viewer-url http://localhost:5173/ --dev-origin http://localhost:5173` with
Vite. `--viewer-url` (or `ATHAR_VIEWER_URL`) selects the viewer but cannot grant
access by itself: `athar view` refuses to start unless `--dev-origin` equals
that viewer's origin, or `--offline` is given. The viewer fetches models from localhost via `?src=`; nothing
is uploaded. Offline mode serves a local build from
`ATHAR_VIEWER_DIST`, a release-packaged `athar_view/static`, or a freshly built
`viewer/dist`. The same SPA also accepts plain drag-drop of `old.ifc`,
`new.ifc`, and a report JSON — no launcher needed.

Viewer development requires [bun](https://bun.sh) and lives entirely under
`viewer/` (see [viewer/README.md](viewer/README.md)). Build the local bundle
with `make viewer-build`; `make viewer-package-static` copies it into
`athar_view/static`, which the release workflow does before building wheels.

## Publishing

Dispatch `.github/workflows/python-release.yml` on a release tag matching the
package version (for example `0.1.1`), then run `make publish RUN=<id>`.
Publishing requires a successful release-workflow run whose ref is a tag of
this repository that, read through the GitHub API (annotated tags
dereferenced), points at the run's commit, and wheels and an sdist whose
package metadata all identify `athar-ifc` at the tag's version. Artifacts are
validated in a temporary directory before Twine uploads them to PyPI.

## Testing

```bash
make test                    # full default suite (~20 seconds)
make test-large-acceptance   # opt-in large IFC acceptance checks
# IfcOpenShell oracle checks (parser, placements, frames); include local IfcOpenShell manifest files via ATHAR_IFCOS_CORPUS_DIR:
ATHAR_RUN_LARGE_ACCEPTANCE=1 python -m pytest tests/test_ifcopenshell_crosscheck.py -q
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

LGPL-3.0-or-later: see `COPYING.LESSER` and `COPYING`.
