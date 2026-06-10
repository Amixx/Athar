# Core Engine Agent Notes

`athar/` is the pure engine package: parse -> signatures -> matching -> delta
report. It must stay independent of Git integration, terminal rendering,
GitHub comments, and other presentation concerns.

## Module Boundaries

- `engine.py` orchestrates the pipeline. `diff_files()` builds one
  `SignatureBundle` per input, enforces same-schema comparison, runs the
  matcher, and assembles the report. Its in-process bundle cache is keyed by
  path, mtime, and size.
- `stream_diff_files()` wraps the same report as `ndjson` or `chunked_json`
  records. Keep the header audit metadata and deterministic final stats record.
- `matcher/` may depend on `athar/bottom/` types, but matching must remain
  tiered pool reduction with no candidate-list scoring pass or assignment step.
- `delta/` may depend on bottom and matcher types. It owns report sections,
  aspect states, placement deltas, data hashes, change scope, and stats.
- `check.py` evaluates JSON reports or fresh diffs against report-visible
  policy signals only. Exit `2` is policy violation; exit `1` is execution or
  configuration error.
- `__main__.py` is the minimal CLI for raw JSON, streaming output, generated-at
  audit timestamps, and `check`.

## Matching Contract

Current tiers, strongest first:

1. `guid`: unique `GlobalId` on both sides plus same canonical class. Score
   `1.0` when the full vector is identical, otherwise `0.9`.
2. `geometry_hash`: same-class full signature-vector equality, zipped in stable
   STEP order for interchangeable entities. Score `0.8`.

Each tier only examines still-unmatched pools and emits disjoint 1:1 pairs.
Never match across canonical classes. Do not reintroduce topology-only,
spatial-nearest, candidate fan-out, or tuning-knob matching without fresh
corpus evidence and tests.

## Report Contract

- Sections are `added`, `deleted`, `modified`, and `unchanged`.
- Matched items include `match{score,reason}` plus per-aspect
  `geometry`, `data`, `topology`, and `placement` states.
- Modified items include `placement_delta_mm`, `data_hash{old,new}`, and
  `change_scope` (`intrinsic`, `transitive`, `mixed`, `none`).
- Stats include section counts, signature counts, parse diagnostics, edge
  stats, modified change-scope counts, dropped matches, GUID collisions, and
  matcher diagnostics.
- Because `geometry_hash` requires equal full vectors, every modified item is
  a GUID match by construction.
