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
    diff tools on curated pairs. Truth independent of every tool's output
    exists for 13 default pairs (4 no-op rewrites, the GUID scramble, 8
    mutation pairs whose truth comes from the mutation manifest) and the 2
    `--large` same-file pairs; the `bl_v1→v2` revision is `unscored`, and the
    9 Revit pairs are scored against Athar-derived truth (see below).
    - Count-level ground truth goes in `Expected.counts` (aggregate
      added/deleted/modified, with `_min`/`_max` suffixes for bounds).
    - GlobalId-level ground truth goes in `Expected.changed_guids`
      (`{"added": [...], "deleted": [...], "modified": [...]}`). When present,
      every runner that emits `reported_guids` is scored for precision/recall/
      F1 against it via `athar_dev.changeset_scoring.score_changeset`; results
      land in each pair's `changeset_scores`. Athar, `speckle_local`,
      `ifcgit_port`, `ifcdiff_default`/`ifcdiff_relationships`, and `ifcfast`
      all emit `reported_guids` (ifcfast's runner passes `sample=0` so its
      added/removed/changed arrays are unclipped, since its `sample=5` default
      truncates them while counts stay exact). A pair whose truth is empty
      (re-exports, no-op rewrites) is scored too: an empty report scores
      precision = recall = F1 = 1, any reported change is a false positive
      and scores F1 0. Precision counts a
      transitive/indirect change as a false positive
      unless the truth set names it, so a real revision's ground truth must
      enumerate every genuinely-changed entity, not just the primary edit.
      Truth sets may carry `optional_<category>` GUIDs — direct containers
      whose membership factually changed (derived from the files, never from
      a tool's output): reporting one is not a false positive, omitting one
      is not a false negative. `counts.modified` bounds derive from the truth
      sets (`|required|`..`|required|+|optional|`), so a tool that inflates
      `modified` fails assessment even where GUID scoring is absent.
    - Every tool, Athar included (`python -m athar old new`), runs as a
      subprocess through `_run_subprocess`: wall time includes interpreter
      start and imports, peak RSS is `ru_maxrss` from `wait4` (process plus
      waited-for descendants), GB-seconds integrate the sampled summed RSS of
      the tool's process group. Each tool runs in its own session; after the
      tool is reaped (normal exit, error exit or timeout) whatever remains of
      its process group is killed. Same-file
      pairs diff a byte-identical copy at a second path, since
      `diff_files` parses once when both arguments name one file.
    - Run statuses: `ok`, `refused` (Athar's schema refusal), `error`,
      `timeout`, `not_installed`. `not_installed` is shown and left out of
      every average. `error` and `timeout` are a miss on that pair: assessed
      as such, counted in the judged pairs, and scored precision = recall =
      F1 = 0 where GUID truth exists. A failure on any repeat is the tool's
      outcome for the pair. The JSON's `aggregates[set][tool]` holds the
      per-tool figures the dashboard shows, including coverage (pairs
      completed out of pairs attempted), changeset precision/recall/F1 both
      macro (mean of per-pair values) and micro (from TP/FP/FN summed over
      pairs), and time/peak-RSS/GB-seconds as the geometric mean of per-pair
      ratios to Athar. Status, counts and reported GUID sets must agree
      across repeats; a disagreement sets `nondeterministic` on the run.
    - Each `Pair` carries a `set_name` (JSON key `"set"`); the default set is
      `"synthetic"`. The `"revit"` set is 9 same-schema pairs built from the
      real Revit round-trip corpus at `corpus/roundtrip-revit-2026-07-06/`
      (see that directory's `MANIFEST.md`/`FINDINGS.md`), selected by
      `--revit`, which `make benchmark-run` passes; `--large` adds the 76 MB
      and 180 MB same-file stress pairs (`BENCHMARK_ARGS=--large` through
      make). Its truth lives in that corpus directory's `ground_truth.json`
      (one entry per pair name, with optional `counts` and `changed_guids`).
      That file was written from an Athar report, so scoring Athar against it
      is circular; `explore/derive_revit_truth.py` re-derives each pair's
      change set from the IFC content with `ifcopenshell` alone, as a check
      on it. The source `.ifc` exports are
      committed as `.zst` and the pair builder decompresses them on demand
      via `zstd -d -k` (kept, not consumed). Cross-schema pairs (original/r1
      vs r2+) are deliberately NOT benchmarked: Athar refuses them (covered
      by `tests/test_cli_engine.py`), competitors emit meaningless churn on
      them, and there is no conclusion to draw. `_run_athar` still maps CLI
      exit 3 to `{"status": "refused", "reason": "schema_incompatible"}` so
      an unexpected refusal shows up as `"unexpected"` rather than an error.
    - The JSON's `provenance` (shown on the dashboard) records the Athar
      commit (`-dirty` when tracked files differ from it), `CANON_VERSION`,
      each tool's installed version and the run date. Competitor versions are
      pinned in the `benchmark` extra (ifcdiff, ifcfast) and in
      `explore/speckle-requirements.txt` (Speckle).
    - `--only SUBSTRING` restricts a run to pairs whose name contains the
      substring (combine with `--revit`/`--large` to reach one gated pair
      cheaply, e.g. `--revit --only r8_r9 --repeats 1`).
  - `explore/speckle_diff_runner.py` runs Speckle's diff semantics locally:
    speckleifc conversion (ships in `specklepy`) + serializer hashes +
    applicationId/id classification. No Speckle server involved. It runs in
    its own venv (`make benchmark-setup` builds `.athar-benchmark/speckle-venv`,
    passed as `--speckle-python`) because `specklepy[speckleifc]` needs an older
    `ifcopenshell` than Athar's. A missing interpreter records the runs as
    `not_installed` with the reason.
  - `explore/ifcgit_diff_runner.py` is a headless port of Bonsai IfcGit's
    diff (git text diff of STEP lines, propagated to products); the original
    is Blender-bound. Both runners print one JSON object to stdout and are
    invoked as subprocess tools by `benchmark_competitors.py`.
