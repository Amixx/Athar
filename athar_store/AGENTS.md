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
  artifacts/<sha256>.ifc           # raw bytes (legacy layout / no-zstd fallback)
  artifacts/<sha256>.ifc.zst       # full zstd snapshot
  artifacts/<sha256>.ifc.patch.zst # zstd --patch-from delta against base_sha256
  artifacts/<sha256>.meta.json     # encoding, base, chain depth, sizes
  baselines/<model_key>.json       # current accepted-version pointer (derived head)
  history.jsonl                    # append-only event log (source of truth)
```

## Delta Artifact Pack (`pack.py`)

Artifacts are stored git-style — a lineage costs one full snapshot plus small
per-revision deltas — but without any Git dependency, via `ArtifactPack`:

- Identity is always the sha256 of the exact decompressed IFC bytes.
- A review candidate is ingested with the current baseline as delta base
  (`zstd --patch-from`); `set_baseline` starts a fresh chain with a full
  snapshot. Chains are capped at `MAX_CHAIN_DEPTH` (8); the next ingest past
  the cap stores a full snapshot and roots a new chain.
- Every delta is decoded and sha-verified at ingest before the pack commits to
  it; a failed verification, an over-large delta (≥50% of logical size), or a
  missing base falls back to a full zstd snapshot, then to a raw copy.
- The `zstd` executable is an optional external tool: without it the pack
  degrades to raw copies (the legacy layout, which stays readable forever);
  reading compressed artifacts requires it again. Semantic deltas are *not*
  used for storage — the engine's diff report is not byte-invertible, so
  reconstruction correctness comes from byte deltas.
- Reads go through `BaselineStore.export_artifact` / `ArtifactPack.open`
  (temporary materialization); `review` materializes the baseline the same
  way. `athar store export` and `athar store stats` expose this on the CLI.

`project_id` and `model_key` are validated to `[A-Za-z0-9._-]` to keep them
safe as path components. The default root is `~/.athar/store` (override with
`ATHAR_STORE_ROOT` or the CLI `--root`).

## Event Model

Every line in `history.jsonl` is one event with `event`, project-scoped
sequential `id` (`evt-000001`), `model_key`, `at` (UTC), and `actor`:

- `baseline_set` — bootstrap the first accepted version for a `model_key`. No
  diff; records the artifact (`sha256`, `schema`, `size_bytes`).
- `review` — diff a candidate against the **current** baseline and evaluate the
  optional policy. Records both artifacts, the diff stats summary, the
  `evaluate_report` verdict, and `verdict` = `pass|fail`. A review **does not**
  promote; it only records. The candidate is ingested into `artifacts/` at
  review time so the verdict is reproducible and approval is a pointer flip.
- `approval` — promote a prior review's candidate to the new baseline. Carries
  `review_id`, approver, and note. Updates the baseline pointer.
- `rejection` — reject a prior review's candidate. References `review_id`;
  leaves the baseline pointer unchanged.

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
