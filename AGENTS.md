# Athar Agent Notes

Athar is a semantic IFC diff tool. It compares BIM models at the entity and
property level, not as STEP text.

This root file is intentionally lean. More specific instructions live next to
the code they govern:

- `athar/AGENTS.md` — core engine, matcher, delta report, and policy gates.
- `athar/bottom/AGENTS.md` — IFC parsing and signature construction.
- `athar_git/AGENTS.md` — Git diff driver, cache, renderer, and PR comments.
- `athar_view/AGENTS.md` — visual 3D diff viewer launcher and SPA.
- `athar_store/AGENTS.md` — persistent baselines, approvals, and review history.
- `athar_qa/AGENTS.md` — human-readable QA gate reports and review timelines.
- `tests/AGENTS.md` — corpus, mutation, invariant, and acceptance testing.
- `docs/AGENTS.md` — corpus/performance notes and historical records.
- `scripts/AGENTS.md` — inspection and exploratory scripts.

## Global Contracts

- Python 3.10+, pure Python. The only core runtime dependency is
  `ifcopenshell`.
- Use `ifcopenshell` for IFC parsing. Do not parse STEP files as text.
- Keep the diff pipeline deterministic and algorithmic. Do not add AI or
  probabilistic matching to engine behavior.
- Core engine modules under `athar/` must not depend on Git, rendering, CI
  presentation, or other integration packages.
- IFC4 and IFC2X3 are supported, but one diff run only compares files with the
  same schema. Do not add IFC2X3<->IFC4 translation inside the diff engine.
- Output structured JSON from the engine. Human-readable summaries belong in
  presentation/integration code.
- No deep B-rep comparison. Athar hashes geometry-domain subgraphs and compares
  placement/spatial features.
- JavaScript/TypeScript is confined to `viewer/`; bun is its package manager and
  test runner. pip users never need bun — `viewer/dist` is committed and served
  by `athar view`.

## Identity Policy

Matching is conservative by design:

- Unique `GlobalId` plus same canonical class is the strongest identity signal.
- Duplicated `GlobalId` values are never identity evidence.
- GUID-free recovery is limited to same-class full signature-vector equality,
  emitted as disjoint 1:1 pairs.
- Weaker evidence, including shared topology or spatial proximity, is reported
  as added+deleted unless a future corpus study proves a real revision case
  needs it.

## Development

- Prefer a repo-local `.venv`.
- Typical setup: `make dev-setup`.
- During active development, run focused tests relevant to the change. Run
  `make test` before committing.
- Preserve useful project knowledge when feature work changes architecture or
  domain behavior: update the nearest `AGENTS.md` and relevant README/docs.
- Do not write throwaway scripts. Put exploratory code in `scripts/explore/`.
