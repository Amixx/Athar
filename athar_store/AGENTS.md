# Baseline Store Agent Notes

`athar_store/` is the persistence layer that turns one-shot diffs into an
accumulating, per-project record: accepted-version **baselines**, immutable
**approval/sign-off** records, and an append-only **review history**. The diff
itself is replicable by anyone; the defensible value is this compounding data,
so the store is designed to be the durable system of record.

It is an integration package. It may depend on `athar.engine` (`diff_files`)
and `athar.check` (`evaluate_report`), never the reverse. The core engine under
`athar/` must not import `athar_store`.

## What It Stores

Identity over time is `project_id` + `model_key` (a lineage, e.g. project
`acme-tower`, model `architecture`). Each accepted snapshot is identified by the
content `sha256` of its IFC file. The store keeps three durable things:

- **Content-addressed artifacts** — the actual blessed IFC files, so any future
  candidate can be re-diffed against the accepted baseline without the customer
  retaining old exports. This self-containment is the lock-in.
- **A materialized baseline pointer** per `model_key` — the current accepted
  version (its artifact, who approved it, when, and which approval event).
- **An append-only event log** (`history.jsonl`) — the source of truth. Every
  review verdict, approval, and rejection is recorded and never mutated.

## On-Disk Layout

```
<root>/projects/<project_id>/
  project.json                     # name, created
  artifacts/<sha256>.ifc           # raw bytes, no sidecar (written by release 0.1.0)
  artifacts/<sha256>.ifc.zst       # full zstd snapshot
  artifacts/<sha256>.ifc.patch.zst # zstd --patch-from delta against base_sha256
  artifacts/<sha256>.meta.json     # encoding, base, chain depth, sizes
  baselines/<model_key>.json       # current accepted-version pointer (derived head)
  reports/<review_id>.json.gz      # full delta report of each review (deterministic gzip)
  history.jsonl                    # append-only event log (source of truth)
  pending-pointer.json             # pointer journal, present only mid-write
  .lock                            # flock target for project writers and readers
```

## Delta Artifact Pack (`pack.py`)

Artifacts are stored git-style — a lineage costs one full snapshot plus small
per-revision deltas — but without any Git dependency, via `ArtifactPack`:

- Identity is always the sha256 of the exact decompressed IFC bytes.
- A review candidate is ingested with the current baseline as delta base
  (`zstd --patch-from`); `set_baseline` starts a fresh chain with a full
  snapshot. Chains are capped at `MAX_CHAIN_DEPTH` (8); the next ingest past
  the cap stores a full snapshot and roots a new chain.
- Every delta and full snapshot is decoded and sha-verified at ingest before
  the pack commits to it. An over-large delta (≥50% of logical size) or a
  missing or full-depth base is stored as a full zstd snapshot instead; an
  encoding that fails its round-trip check raises `PackError`.
- Writing any artifact and reading a compressed one require the external
  `zstd` executable (1.5+, for `--patch-from`). Its absence raises
  `PackError` with an install hint; there is no uncompressed fallback. It is a
  system binary, so the wheel metadata cannot declare it. Raw `<sha>.ifc`
  files without a sidecar, as written by release 0.1.0, are read and
  hash-verified; sidecars only ever carry `zstd` or `zstd-delta`. Semantic
  deltas are *not* used for storage — the engine's diff report is not
  byte-invertible, so reconstruction correctness comes from byte deltas.
- Materialization walks the chain iteratively through two scratch files beside
  the destination, regardless of depth, and replaces the destination only after
  the final link's hash check, so a failed export leaves it untouched. The
  destination gets the mode a plain `open` would create (0666 masked by the
  process umask), not the scratch files' 0600. Every link is hash-verified.
  Cyclic chains, inconsistent depths, and depths beyond eight are refused. Ingest past eight stores a full snapshot;
  there is no separate repack command.
- Reads go through `BaselineStore.export_artifact` / `ArtifactPack.open`
  (temporary materialization); `review` materializes the baseline the same
  way. `athar store export` and `athar store stats` expose this on the CLI.

`project_id` and `model_key` are validated to `[A-Za-z0-9._-]` to keep them
safe as path components. The default root is `~/.athar/store` (override with
`ATHAR_STORE_ROOT` or the CLI `--root`).

## Event Model

Every line in `history.jsonl` is one event with `event`, project-scoped
sequential `id` (`evt-000001`), `model_key`, `at` (UTC), nonblank `actor`,
`source_name` (the caller's delivery filename), `athar_version`, and
`canon_version`. Decision events retain the reviewed delivery's filename.
Pack sidecars carry `format: 1`; other format versions are refused.

Decisions are accepted only for pending reviews against the current baseline.
Conflicts name the event that approved, rejected, or made the review stale;
the HTTP API returns 409. Missing or blank actors fail with HTTP 400.

Event kinds:

- `baseline_set` — bootstrap the first accepted version for a `model_key`. No
  diff; records the artifact (`sha256`, `schema`, `size_bytes`).
- `review` — diff a candidate against the **current** baseline and evaluate the
  optional policy. Records both artifacts, the diff stats summary, the
  `evaluate_report` verdict, and `verdict` = `pass|fail`. A review **does not**
  promote; it only records. The candidate is ingested into `artifacts/` at
  review time so the verdict is reproducible and approval is a pointer flip.
  The full delta report is persisted to `reports/<review_id>.json.gz`
  (`review_report` reads it back; release 0.1.0 reviews have none and return None). The
  event also carries `conflicts`: for every *pending* review (same model_key,
  same current baseline, not yet approved/rejected — see `pending_reviews`),
  the overlap between both candidates' changed GUIDs (adds, deletes, and
  non-transitive modifications; pure ripples are excluded so disjoint edits
  in one storey do not conflict). Conflict *detection* is deliberately the
  ceiling: semantic IFC merge stays out of scope.
- `approval` — promote a prior review's candidate to the new baseline. Carries
  `review_id`, approver, and note. Updates the baseline pointer.
- `rejection` — reject a prior review's candidate. References `review_id`;
  leaves the baseline pointer unchanged.

## Locking and durability

Each project has a `.lock` file. Store mutations take an exclusive `fcntl.flock`;
all but `init_project` and `set_baseline` refuse an unknown project before
locking, so they create nothing on disk;
artifact materialization and record reads take a shared lock. A review copies
the candidate into its temporary directory and diffs and ingests that copy, so
the recorded artifact is the bytes the report describes. It holds the shared
lock only to read the baseline pointer and materialize the baseline; the diff
and the candidate's compression, round-trip decode and hashing run with no lock
held, and the artifact's schema comes from the diff's own parse. Its exclusive
lock covers only the baseline-currency check, conflict detection, the report
write and the history append, and it refuses to commit if the baseline event
changed. Artifacts are content-addressed, so a refused or interrupted review
leaves at most an unreferenced artifact.

Store files are written to unique sibling temporary paths, fsynced, replaced
atomically, and followed by a directory fsync. New directory entries are also
fsynced. The artifact and full report are durable before their history event.
The history is logically append-only, published as an atomic replacement.
Baseline pointers move last. Each mutation reads `history.jsonl` once under its
exclusive lock and derives the new event id from it. `pending-pointer.json`
journals pointer transitions;
the next locked operation recovers the committed pointer after an interrupted
writer. Unreferenced artifacts from an interrupted operation are not history
and may remain on disk. A review that stops after its report is durable and
before its event is appended leaves a report numbered past the last event;
every writer, under its exclusive lock and before writing its report or event,
removes report files numbered past the last event, so a new event never
inherits a stale report.

`BaselineStore(..., fault_hook=...)` exposes `candidate_compared` (the review's
diff is done) and durable boundaries for crash tests: `artifact_durable`,
`report_durable`, `pointer_prepared`, `history_durable`, and `pointer_durable`.
Every artifact read, including release 0.1.0 raw files, verifies its content
hash.

## Verification

`BaselineStore.verify(project_id)` checks consecutive unique event IDs, hashes
of every referenced artifact, readable gzip/JSON review reports, report files
that no review event references (`review_report` refuses any id that is not a
review event, so such a file is never served), and complete baseline pointer
equality with history replay. A report numbered past the last event is a note,
"report of an interrupted review; removed at the next write". A report for an
existing event that is not a review cannot arise from a crash, so it is a
problem. It returns `{"problems": [...], "notes": [...]}`.
A review event without a `conflicts` field was written before reports were kept
(release 0.1.0 recorded neither, and every later review records both); its
missing report is a note, "no stored report (written before reports were
kept)", not a problem. A missing report for any other review is a problem.
`athar store verify PROJECT` emits that object, with exit 0 when `problems` is
empty and 2 otherwise. `--rebuild-pointers` prints changes before writing under
the same exclusive lock, repairs missing/corrupt pointers, and removes pointers
absent from history. Invalid history cannot be used for repair. Evidence is not repaired.

## Determinism

`BaselineStore(root, clock=...)` takes an injectable UTC clock so tests pin
timestamps; event ids are derived from the per-project event count, so a fixed
clock yields byte-stable history. Artifact ingestion is content-addressed and
idempotent (re-ingesting identical bytes is a no-op).

## Extensibility (not yet built)

The event log is intentionally open: an accepted version can later carry a
`metrics` blob (quantity takeoff, embodied-carbon baseline) without a schema
break, so the carbon/QA-history wedges layer on top of the same lineage. Do not
add those fields speculatively — wait for the wedge that needs them.
