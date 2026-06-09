# Hardening + Selective Phase 2 Execution Plan

Date: 2026-04-02

## Goal

Harden rewrite runtime truth first (perf + tests), then take only high-value result-quality work from Phase 2.

## Milestones

1. Rewrite perf re-baseline
- Produce fresh baseline artifact(s) for the current engine path.
- Keep `docs/perf/STATUS.md` and `docs/perf/SUMMARY.md` aligned with current observations.
- Record concrete hotspot findings under `docs/perf/FINDINGS_*.md`.

2. Test pyramid hardening
- Small synthetic layer (fast matcher/delta edge cases).
- Medium regression layer (existing house fixtures).
- Large acceptance/perf layer (opt-in real large IFC pairs).

3. Selective Phase 2 (Priority A only)
- A2 first (Tier 2 signatures).
- A1 next (intrinsic vs transitive classification).
- A4 next (conflict downgrade rules).
- A3 only if needed (structured data hash visibility).

4. Re-evaluate heavy work
- Decide on B1/B2/B4 based on evidence from corpus pain, not roadmap inertia.

## Proposed PR Order

1. PR: `hardening/perf-doc-rebaseline`
- Perf harness commands refreshed.
- Perf status/summary rewritten around rewrite runtime.
- At least one new hotspot findings note.

2. PR: `hardening/test-pyramid`
- Add/expand small synthetic tests.
- Add medium regression test(s).
- Add opt-in large acceptance test(s) and documented corpus paths.

3. PR: `phase2a/a2-tier2-signatures`
- Implement + unit tests.
- Validate no determinism regressions.

4. PR: `phase2a/a1-intrinsic-vs-transitive`
- Implement classification + tests.
- Confirm no material false-positive growth on house fixtures.

5. PR: `phase2a/a4-conflict-downgrade`
- Implement downgrade rules + focused edge-case tests.

6. Optional PR: `phase2a/a3-structured-data-hash`
- Only when user-facing need is confirmed.

## Operational Commands (Current)

```bash
make test-small
make test-medium
make test-large-acceptance

make perf-rewrite-bg
make perf-rewrite-watch

make perf-holy-grail-serial
make perf-holy-grail-parallel
```

## Decision Gate Before Heavy Phase 2

Require at least one of:
- Demonstrated user pain from unresolved ambiguity/splits/merges.
- Corpus-level measurable quality delta that Priority A cannot close.
- Reproducible benchmark evidence that assignment/provenance redesign is net-positive.
