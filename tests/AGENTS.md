# Test Agent Notes

Tests should prove invariants and behavior derived from inputs, not bless
entire JSON reports as goldens.

## Focused Runs

```bash
python -m pytest tests/test_matcher_core.py -q
python -m pytest tests/test_engine.py -q
python -m pytest tests/test_corpus_invariants.py -q
make test
make test-large-acceptance
```

During active development, run the focused tests that cover the touched code.
Run the full default suite before committing.

## Corpus Rules

- `tests/corpus.py` is the registry for repo LFS files and optional external
  corpus files. LFS pointers should fail loudly; missing optional external
  files should skip.
- The optional external corpus defaults to `../vscode-ifc/test-files` and can
  be overridden with `ATHAR_EXTERNAL_CORPUS_DIR`.
- Shared report invariants include accounting, 1:1 matching, class safety,
  score/reason consistency, and change-scope consistency.
- Default corpus tests cover small files only so normal runs stay fast.

## Semantic Scenario Rules

- `tests/mutations.py` creates one constructed edit per scenario through a
  shared `ifcopenshell` write path.
- Mutation expectations come from the edit manifest itself: victim section,
  aspect states, and placement delta norms.
- Pick victims with exclusive placements, psets, or properties when a scenario
  claims to be a single-entity edit.
- Type-level/inherited psets are covered through `IfcTypeObject.HasPropertySets`
  edits and should manifest as data changes on defining occurrences.

## Large Acceptance

- `tests/test_acceptance_large_ifc.py` is opt-in via
  `ATHAR_RUN_LARGE_ACCEPTANCE=1`.
- It covers medium/large same-file invariants, real revision pairs, discipline
  pairs, unrelated pairs, and GUID-scramble at scale.
- Optional per-test bound: `ATHAR_ACCEPTANCE_TIMEOUT_S`.
