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
  score/reason consistency, change-scope consistency, and the placement-delta
  contract (a placement reported `unchanged` carries no translation delta; the
  converse is not asserted, since a pure rotation is a change with zero
  translation).
- Default corpus tests cover small files only so normal runs stay fast.

## Semantic Scenario Rules

- `athar_dev/ifc_mutations.py` creates one constructed edit per scenario
  through a shared `ifcopenshell` write path. It is shared with exploratory
  benchmark scripts so generated synthetic benchmark pairs use the same ground
  truth as the semantic tests.
- Mutation expectations come from the edit manifest itself: victim section,
  aspect states, and placement delta norms.
- Pick victims with exclusive placements, psets, or properties when a scenario
  claims to be a single-entity edit.
- Type-level/inherited psets are covered through `IfcTypeObject.HasPropertySets`
  edits and should manifest as data changes on defining occurrences.
- `add_product` is the symmetric counterpart to `delete_product`: it inserts one
  fresh proxy into an existing spatial container, so exactly one entity is added
  and the container may only ripple transitively.
- Edits whose ground truth needs authored geometry/units use controlled
  synthetic pairs instead of seed mutators, because real-seed geometry is too
  heterogeneous to target reliably:
  - `test_geometry_change.py` moves one explicit mesh vertex (the path that
    feeds `vh_geometry` — parametric scalars like extrusion `Depth` deliberately
    do not) to prove a clean geometry change, which is also the positive case
    for `mixed` change_scope (intrinsic geometry + transitive topology self-seed).
  - `test_unit_normalization.py` builds the same model in metres and millimetres
    to prove length quantization is unit-normalized (zero diff).

## Large Acceptance

- `tests/test_acceptance_large_ifc.py` is opt-in via
  `ATHAR_RUN_LARGE_ACCEPTANCE=1`.
- It covers medium/large same-file invariants, real revision pairs, discipline
  pairs, unrelated pairs, and GUID-scramble at scale.
- Optional per-test bound: `ATHAR_ACCEPTANCE_TIMEOUT_S`.
