# Performance Work Status

Date: 2026-06-09

## Current State

The graph engine that all earlier notes in this folder measured (`athar/diff/`,
`athar/graph/`, `athar/index/`, `athar/_native/`) was deleted on 2026-06-09 and
replaced by the current engine path (`athar/bottom/` signature pipeline +
`athar/matcher/core.py` tiered pool-reduction matcher + `athar/delta/report.py`).
Everything in this folder dated before 2026-06-09 is a historical record of the
old engine and does not describe current behavior.

Open items from the 2026-04-02 status are closed:

- `FINDINGS_prepare_context_parallel_seed_wait_2026-04-02.md` (multiprocessing
  hang in `_prepare_seeded_sides_parallel`): resolved by removal — the entire
  code path was deleted with the old engine. The current engine has no
  multiprocessing.
- Pending rewrite re-baseline artifacts (`baseline_rewrite_2026-04-02.json`,
  prepare-context profiles): obsolete — the benchmark harnesses they referred
  to were deleted with the old engine.

## Current engine numbers (2026-06-09, this machine)

- `build_signature_bundle` on `real-world-test/Building-Landscaping-v1.ifc`
  (1.2MB): ~0.7s.
- `build_signature_bundle` on a 50MB / ~1M-entity model: ~90s (pure-Python
  pipeline; parse + merkle + WL + spatial). This is why the default test suite
  uses the small pair — full suite runs in ~3s.
- Matching + report on top of built bundles is sub-second for these sizes
  (tiered pool reduction is O(N) in pool size; no candidate materialization).

See `FINDINGS_engine_rewrite_acceptance_2026-06-09.md` for large-model
acceptance numbers.

## Next perf steps (when needed)

- The bundle build is native (Rust, `athar/_native`): tokenize → canonicalize →
  edges → merkle → WL → spatial, returning only signatures. ~4.4× faster with
  lower peak RSS than a pure-Python pipeline — see
  `DESIGN_native_stage_b_2026-06-26.md` for measured numbers. Profile on the
  Rust side.
- No disk cache exists in the current engine; if repeat-diff latency on large
  files becomes a real workflow problem, a content-hash-keyed bundle cache is
  the obvious first move.
