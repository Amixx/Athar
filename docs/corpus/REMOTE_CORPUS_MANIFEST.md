# Remote corpus manifest — design

Status: design only (2026-06-10). No download/object-store infrastructure is
implemented; this records the manifest shape so future corpus growth (huge
IFCs that cannot live in the repo, even via LFS) stays describable without
inventing a new format per batch. The repo stores manifests and assertions,
not huge IFC files.

## Goals

- One declarative place that describes every corpus file Athar's tests and
  surveys may use: tracked LFS samples, tiny checked-in fixtures, and
  external/object-store-only files.
- Tests and the survey script derive expectations from the manifest (schema,
  pair shape, integrity) plus engine invariants — never from full-report JSON
  goldens.
- Deterministic missing-data behavior: tracked files fail loudly when broken,
  optional remote files skip clearly unless opted in.

## Non-goals

- No S3/GCS/R2 hosting, downloader, cache manager, or CI fetch step yet
  (deferred — see the dfc "defer big corpus infrastructure" bucket).
- No per-file expected diff counts. Assertions stay invariant-based.

## Manifest shape

One JSON (or TOML) document, checked into the repo, e.g.
`corpus/manifest.json`. Two top-level sections: `files` and `cases`.

### File entry

```jsonc
{
  "id": "gni_2025_model_190",          // stable, snake_case, never reused
  "path": "corpus/gni-bim-sample/2025_BIMfundamentals/model_190.ifc",
                                        // repo-relative; present only for tracked files
  "uris": [                             // present only for external/remote files;
    "https://zenodo.org/records/19722012/files/2025_BIMfundamentals.zip#model_190.ifc"
  ],                                    // ordered fallbacks, first reachable wins
  "sha256": "<hex>",                    // preferred integrity check for the .ifc itself
  "size_bytes": 577059,
  "schema": "IFC4",
  "source": "gni-bim-dataset",          // key into a sources table (license/notice refs)
  "tags": ["default-small", "gni", "2025-fundamentals"]
}
```

Field rules:

- `id` is the stable handle used by tests, cases, and survey output. Renaming
  a file must not change its id; retiring a file retires the id.
- At least one of `path` or `uris` is required. Remote-only files use `uris`;
  tracked files use `path` and may also list `uris` as provenance.
- `sha256` is the integrity field consumers verify. `md5` is allowed only as
  an additional upstream *fact* (e.g. Zenodo publishes archive md5s) and never
  as the verification mechanism. Archive-level checksums belong on the source
  entry, not the per-file entry, when the upstream unit is a zip.
- `size_bytes` and `schema` let tests size-gate and schema-gate without
  opening the file.
- `source` points at a `sources` table entry carrying license identifier,
  NOTICE/LICENSE file paths in-repo, and upstream citation. Example below.

### Tags / subsets

Tags select files into tiers and groups; a file may carry several:

- Tier tags: `default-small` (default suite), `default-medium` (default
  boundary / corpus invariants where cheap), `large` (opt-in acceptance),
  `external` (lives outside the repo; skip when absent).
- Provenance/group tags: `gni`, `2025-fundamentals`, `2026-projects`.
- Discipline tags: `architecture`, `structure`.

### Case entry

Cases describe *how* files are exercised, separately from the files:

```jsonc
{
  "id": "gni_pair_3",
  "kind": "discipline-pair",            // see kinds below
  "old": "gni_2026_model_3_arc",
  "new": "gni_2026_model_3_structure",
  "tags": ["gni", "2026-projects"]
}
```

Case kinds and the assertion each implies:

- `same-file` — diff a file against itself; must be a zero diff plus all
  report invariants.
- `revision-pair` / `discipline-pair` — real pairs of the same project.
  Revision pairs must stay matched (guid-tier dominant); discipline pairs
  must stay mostly disjoint (conservative tiers must not manufacture
  cross-model matches).
- `unrelated-pair` — different projects; guid tier silent, both sides mostly
  added/deleted.
- `mutation-seed` — the file is an input to `tests/mutations.py`; expectations
  come from the constructed edit's manifest, not from the file pair itself.

### Assertion style

All consumers assert:

1. the shared report invariants in `tests/corpus.py` (accounting, 1:1
   matching, class safety, score/reason and change-scope consistency), and
2. manifest-derived expectations (schema, case-kind shape, mutation
   manifests).

Full-report JSON goldens are explicitly out: real exports churn on re-export
and goldens would bless engine output instead of deriving truth from inputs.

### Missing-data behavior

- Tracked file absent or an unfetched git-lfs pointer: **fail loudly** in the
  default tier (a broken checkout must not be silently green). The acceptance
  tier may downgrade to per-file skip so partial corpora give partial signal
  (current `corpus_path` vs `acceptance_path` split in `tests/corpus.py`).
- Remote-only file not present in the local cache: **skip with a clear
  reason**, unless an explicit opt-in (env var, e.g. a future
  `ATHAR_REMOTE_CORPUS_DIR`) says the cache should be complete — then fail.
- Integrity mismatch (sha256) on any present file: always fail, never skip.

## Current precedent: the GNI sample

The GNI-BIM dataset import already follows the intended split and is the
concrete model for `sources`:

- Tracked reusable seeds: `corpus/gni-bim-sample/` (Git LFS; 21 IFCs, ~217MB
  total — 11 independent 2025 BIM Fundamentals models, 5 architecture +
  structure 2026 BIM Projects pairs).
- Raw/full upstream archives: local-only under gitignored
  `corpus-data/gni-bim/archives/`; never assumed present in CI.
- Attribution/citation/disclaimer: `corpus/gni-bim-sample/NOTICE.md`;
  upstream license text: `corpus/gni-bim-sample/LICENSE.GNI-BIM-Dataset`
  (CC BY 4.0).
- Known upstream archive md5 facts (archive-level metadata, not file
  integrity): 2025 zip `bab737c3cfb3ff0443f1cc43355b589c`, 2026 zip
  `bc4bfb90313162c68a3ec4e24a0d33cf`.

Example `sources` entry:

```jsonc
"gni-bim-dataset": {
  "license": "CC-BY-4.0",
  "license_file": "corpus/gni-bim-sample/LICENSE.GNI-BIM-Dataset",
  "notice_file": "corpus/gni-bim-sample/NOTICE.md",
  "upstream": "https://doi.org/10.5281/zenodo.19722012",
  "archives": [
    {"name": "2025_BIMfundamentals.zip", "md5": "bab737c3cfb3ff0443f1cc43355b589c"},
    {"name": "2026_BIMprojects.zip", "md5": "bc4bfb90313162c68a3ec4e24a0d33cf"}
  ]
}
```

## Adoption path (later, separate tasks)

1. Generate `corpus/manifest.json` covering today's registry
   (`tests/corpus.py` CORPUS dict + GNI sample + external files), with
   sha256s computed once at import time.
2. Point `tests/corpus.py` and `scripts/explore/corpus_survey.py` at the
   manifest instead of their hardcoded dicts.
3. Only then consider a fetch/cache layer for remote-only files.
