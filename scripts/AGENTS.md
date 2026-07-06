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
    - Each `Pair` carries a `set_name` (JSON key `"set"`); the default set is
      `"synthetic"`. The `"revit"` set is 9 same-schema pairs built from the
      real Revit round-trip corpus at `corpus/roundtrip-revit-2026-07-06/`
      (see that directory's `MANIFEST.md`/`FINDINGS.md`) — opt-in via
      `--revit`, same pattern as `--large`. Its ground truth lives in that
      corpus directory's `ground_truth.json` (one entry per pair name, with
      optional `counts` and `changed_guids`); the source `.ifc` exports are
      committed as `.zst` and the pair builder decompresses them on demand
      via `zstd -d -k` (kept, not consumed). Cross-schema pairs (original/r1
      vs r2+) are deliberately NOT benchmarked: Athar refuses them (covered
      by `tests/test_cli_engine.py`), competitors emit meaningless churn on
      them, and there is no conclusion to draw. `_run_athar` still maps CLI
      exit 3 to `{"status": "refused", "reason": "schema_incompatible"}` so
      an unexpected refusal shows up as `"unexpected"` rather than an error.
    - `--only SUBSTRING` restricts a run to pairs whose name contains the
      substring (combine with `--revit`/`--large` to reach one gated pair
      cheaply, e.g. `--revit --only r8_r9 --repeats 1`).
  - `explore/speckle_diff_runner.py` runs Speckle's diff semantics locally:
    speckleifc conversion (ships in `specklepy`) + serializer hashes +
    applicationId/id classification. No Speckle server involved.
  - `explore/ifcgit_diff_runner.py` is a headless port of Bonsai IfcGit's
    diff (git text diff of STEP lines, propagated to products); the original
    is Blender-bound. Both runners print one JSON object to stdout and are
    invoked as subprocess tools by `benchmark_competitors.py`.
