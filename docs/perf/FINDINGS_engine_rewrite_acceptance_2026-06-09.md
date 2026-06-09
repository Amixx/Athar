# Findings: engine rewrite — test tiers + large acceptance (2026-06-09)

## Context

First acceptance run of the rewritten engine (bottom signature pipeline +
tiered pool-reduction matcher + delta report), same day the old graph engine
was deleted. Also covers the test-fixture swap that fixed default-suite
runtime.

## Commands

```bash
python -m pytest tests/ -q
# 60 passed, 2 skipped in 6.45s

ATHAR_RUN_LARGE_ACCEPTANCE=1 ATHAR_ACCEPTANCE_TIMEOUT_S=1800 \
  python -m pytest tests/test_acceptance_large_ifc.py -q
# 2 passed in 737.74s (0:12:17)
```

## Key numbers

- Default suite: 6.45s total (was 10+ minutes before the fixture swap).
  - `build_signature_bundle` on `Building-Landscaping-v1.ifc` (1.2MB, IFC4): ~0.7s.
  - `Duplex-Architecture.ifc` (2.3MB, IFC2X3, 38,898 entities, 295 products): ~2.6s.
  - Old default fixtures (50MB house pair, ~1M entities each): ~93s per bundle
    for 177 product signatures — deleted; large coverage moved to the opt-in tier.
- Large acceptance (one pytest process, bundle cache shared): 12m17s total for
  both tests. Per-test split was not instrumented in this run; `--durations=5`
  is now part of `make test-large-acceptance`.
  - `real-world-spanish-180mb.ifc`: IFC2X3, ~2.4M entities, 44,389 products.
  - `uni-project-house-50mb.ifc`: IFC2X3, ~1.05M entities, 6,385 products.
  - Product GUID overlap between the two: 0/50,774 (unrelated projects).
- Cross-project diff (spanish → uni) facts: guid tier matched 0, conservatism
  asserts held (`deleted >= old_signatures/2`, `added >= new_signatures/2`),
  `spatial.probe_capped == 0`, stats internally consistent.
- Same-file 180MB diff: zero added/deleted/modified, unchanged > 100, no
  probe-cap trips.

## Hotspot

Bundle build (parse → edges → merkle → WL → spatial, pure Python) dominates
wall time at large scale; matching + report are sub-second even at 44k vs 6.4k
signatures. Any future perf work starts at the bundle build.

## Chosen follow-ups

- Default tier stays on small real fixtures: Building-Landscaping IFC4 pair,
  Duplex-Architecture IFC2X3 model, `tests/fixtures/tiny_no_products.ifc`
  empty-model edge case, plus a GUID-scrambled variant generated at test time.
- Large tier unchanged: the 180MB same-file gate is intentionally slow and
  opt-in; shrinking its input would make it not a large-model test.
