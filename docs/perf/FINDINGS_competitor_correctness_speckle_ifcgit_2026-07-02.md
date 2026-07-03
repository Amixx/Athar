# Findings: Speckle + IfcGit added to the competitor benchmark

Date: 2026-07-02

## Context

The 2026-06-27 benchmark covered ifcdiff and ifcfast but not Speckle or
IfcGit, because neither is CLI-runnable as shipped. Both are now wrapped as
local CLI runners and registered in
`scripts/explore/benchmark_competitors.py` as subprocess tools with the same
RSS/GB·s sampling as every other tool:

- `scripts/explore/speckle_diff_runner.py` — Speckle's production IFC
  importer (`speckleifc`, ships inside `specklepy`, itself ifcopenshell-based)
  converts each file locally; specklepy's serializer computes the object
  hashes a Speckle server would store; classification uses Speckle's own
  version-diff semantics (`applicationId` = GlobalId correlation, hash
  equality decides unchanged/modified). No server involved.
- `scripts/explore/ifcgit_diff_runner.py` — faithful port of Bonsai
  `tool/ifcgit.py` (`ifc_diff_ids` + `get_modified_step_ids`, IfcOpenShell
  v0.8.0). The IfcGit algorithm is a git text diff of STEP lines regexed for
  `+#N=` / `-#N=`, propagated to products through inverse traversal. The
  original imports `bpy` and cannot run outside Blender.

A `reserialize_asymmetric` pair (original file vs its IfcOpenShell rewrite)
was added; the existing `reserialize` pair rewrites both sides and therefore
produces byte-identical inputs that cannot expose text-diff churn.

`specklepy` (2026.6.0 at time of writing) was added to the `[benchmark]`
extra.

## Correctness (gni_190 pairs, ground truth known by construction)

Counts are added/deleted/modified.

| Pair (truth) | Athar | speckle_local | ifcgit_port |
|---|---|---|---|
| same_file (0 changes) | 0/0/0 ✅ | 0/0/0 ✅ | 0/0/0 ✅ |
| reserialize_asymmetric (0 changes) | 0/0/0 ✅ | 0/0/0 ✅ | 0/0/**138** ❌ |
| owner_history_churn (0 changes) | 0/0/0 ✅ | 0/0/0 ✅ | 0/0/0 ✅ |
| guid_scramble (0 changes) | 0/0/0 ✅ | **103/103/0** ❌ | 0/0/**138** ❌ |
| duplicate_guid (0 changes) | 0/0/0 ✅ | 1/1/0 ❌ | 0/0/1 ❌ |
| rename_product (1 modified) | 0/0/1 ✅ | 0/0/1 ✅ | 0/0/1 ✅ |
| pset_value (1 modified) | 0/0/1 ✅ | 0/0/1 ✅ | 0/0/1 ✅ |
| type_pset_value (28 inherited modified) | 0/0/28 ✅ | 0/0/28 ✅ | 0/0/**0** ❌ FN |
| move_product (1 modified) | 0/0/1 ✅ | 0/0/1 ✅ | 0/0/1 ✅ |
| delete_product (1 deleted) | 0/1/15* ✅ | 0/1/0 ✅ | 0/1/0 ✅ |
| add_product (1 added) | 1/0/16* ✅ | 1/0/0 ✅ | 1/0/0 ✅ |

\* Athar's extra modified are transitive change-scope fallout, reported as
such (`change_scope: transitive`).

## Findings

- **Speckle false-positives totally on GUID churn.** All 103 elements report
  added + deleted, zero matched. This was predicted architecturally
  (applicationId *is* the GlobalId, so scrambled GUIDs destroy all
  correlation) but is now measured, not hypothesized. Same failure on
  duplicated GUIDs.
- **IfcGit false-positives on pure reserialization**: 138 phantom "modified"
  products (6 927 changed step ids) when the only difference is STEP line
  layout/ordering from an IfcOpenShell rewrite with zero semantic change.
  It also false-positives on GUID scramble (the GlobalId attribute line
  changes) and duplicate GUID.
- **IfcGit false-negatives on type-level property edits.** A type pset value
  change that Athar and Speckle both report as 28 inherited modifications is
  invisible to IfcGit: its propagation walks `IfcPropertySet →
  DefinesOccurrence` but type psets hang off `IfcTypeProduct.HasPropertySets`
  / `IfcRelDefinesByType`, a path its `collect()` does not cover.
- **Athar is the only tool correct on all pairs.**
- **Speckle's importer is nondeterministic**: proxy lists (render materials,
  instance definitions) on Collections have unstable ordering, so Collection
  hashes differ between two imports of the identical file. Speckle itself
  would show the project container as changed on re-import. The runner
  therefore counts DataObjects only (the elements the Speckle viewer diff
  colors).
- Speckle element granularity is coarser than Athar signatures (103 elements
  vs 138 signature entities on gni_190): only IfcRoot entities under the
  project tree become elements, and spatial containers become Collections.

## Performance

Small pairs (gni_190, ~1 MB, median of 3, this machine): Athar ~0.57 s /
126 MB peak; ifcgit_port ~0.46 s / 104 MB; ifcdiff_default ~1.29 s / 221 MB;
speckle_local ~1.49 s / 187 MB (it tessellates all geometry).

Large same-file (model_5_arc.ifc, 76 MB IFC4, one-off `/usr/bin/time -l`):

| Tool | Result | Wall | Peak RSS |
|---|---|---|---|
| ifcgit_port | 0/0/0 ✅ | 9.5 s | 1.22 GB |
| speckle_local | 0/0/0 ✅ (5140 elements) | 146.7 s | 3.07 GB |
| Athar (2026-06-27 note) | 0/0/0 ✅ | 17.7 s | 3.13 GB |
| ifcdiff_default (2026-06-27 note) | 0/0/0 ✅ | 75.1 s | 3.05 GB |

Speckle's conversion (~70 s per side at 76 MB) makes it ~8× slower than
Athar at equal memory. ifcgit_port is fast and light because it never builds
semantics — which is also why it false-positives on churn and misses type
property edits.

## Reproduce

```bash
python -m pip install '.[benchmark]'
python scripts/explore/benchmark_competitors.py --out /tmp/athar_benchmark.json
# individual runners
python scripts/explore/speckle_diff_runner.py old.ifc new.ifc
python scripts/explore/ifcgit_diff_runner.py old.ifc new.ifc
```

## Follow-ups

- Fold these results into the positioning material; the "Speckle was not
  benchmarked" caveat can be dropped.
- Run the `--large` tier with the new tools (Speckle tessellates all
  geometry, so expect it to be slow there; raise `--timeout-s`).
- Candidate additional competitors (2026-07-02 survey): BIMserver Model
  Compare (AGPL, Java, headless-able), CBIMS.IFCNormalization + git diff
  (LGPL/GPL, .NET, the normalize-then-text-diff baseline), Speckle
  server-side `/diff` endpoint. Cloud CDE diffs (Autodesk APS, Trimble
  Connect) are not locally benchmarkable.
