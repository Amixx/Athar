# Script Agent Notes

Scripts are for inspection and repeatable investigation.

- Use `ifcopenshell` for IFC access. Do not parse STEP text directly.
- Keep reusable inspectors at `scripts/`.
- Put exploratory or investigative scripts in `scripts/explore/`.
- Do not add one-off throwaway scripts at repo root.
- Existing inspectors:
  - `inspect_ifc.py` prints IFC summary stats.
  - `inspect_ifc_identity.py` shows project name, `GlobalId`, and header
    timestamp.
  - `inspect_guid_overlap.py` shows entity GUID overlap between files.
  - `make_viewer_fixture.py` regenerates the deterministic viewer e2e fixture
    pair + report under `viewer/e2e/fixtures/`.
  - `explore/corpus_survey.py` regenerates the corpus survey JSON behind
    `docs/corpus/`.
  - `explore/benchmark_competitors.py` benchmarks Athar against other IFC
    diff tools on curated pairs with known ground truth.
    - Count-level ground truth goes in `Expected.counts` (aggregate
      added/deleted/modified, with `_min`/`_max` suffixes for bounds).
    - GlobalId-level ground truth goes in `Expected.changed_guids`
      (`{"added": [...], "deleted": [...], "modified": [...]}`). When present,
      every runner that emits `reported_guids` is scored for precision/recall/
      F1 against it via `athar_dev.changeset_scoring.score_changeset`; results
      land in each pair's `changeset_scores`. Only Athar emits `reported_guids`
      today. Precision counts a transitive/indirect change as a false positive
      unless the truth set names it, so a real revision's ground truth must
      enumerate every genuinely-changed entity, not just the primary edit.
  - `explore/speckle_diff_runner.py` runs Speckle's diff semantics locally:
    speckleifc conversion (ships in `specklepy`) + serializer hashes +
    applicationId/id classification. No Speckle server involved.
  - `explore/ifcgit_diff_runner.py` is a headless port of Bonsai IfcGit's
    diff (git text diff of STEP lines, propagated to products); the original
    is Blender-bound. Both runners print one JSON object to stdout and are
    invoked as subprocess tools by `benchmark_competitors.py`.
