# GNI-BIM sample corpus

Small, tracked IFC sample extracted from the external GNI-BIM-Dataset archives for future Athar corpus layers. The full Zenodo archives are cached locally under `corpus-data/gni-bim/` and are intentionally gitignored.

Source: GNI BIM Dataset, Zenodo `10.5281/zenodo.19722011` / record `19722012`, CC BY 4.0. See [`NOTICE.md`](NOTICE.md) for upstream attribution, disclaimer, and requested citation; see [`LICENSE.GNI-BIM-Dataset`](LICENSE.GNI-BIM-Dataset) for the upstream dataset license text.

## Why these files are tracked

These are whole IFC files, not hand-cut fragments. Arbitrary IFC excerpts are easy to make invalid because entity references form a graph; partial slices would be misleading unless produced by a real IFC minimizer.

This subset is small enough to keep in the repo via Git LFS and covers the future validation layers:

- Layer 1 — same-file/invariant seeds from real IFC exports.
- Layer 2 — generated mutation seeds for GUID scramble, move, property edit, delete, duplicate-GUID scenarios.
- Layer 3 — same-building architecture/structure pair stress cases.

Keep future tests tiered: the smallest files can run in the default suite, while the larger files here are better as opt-in/default-boundary stress seeds.

## Files

### `2025_BIMfundamentals/`

Independent student models from the 2025 BIM Fundamentals archive. Useful for same-file tests, unrelated-pair tests, generated mutation seeds, and size-spread stress.

- `model_190.ifc` — 577,059 bytes
- `model_9.ifc` — 601,356 bytes
- `model_50.ifc` — 909,988 bytes
- `model_62.ifc` — 1,501,127 bytes
- `model_149.ifc` — 1,538,884 bytes
- `model_18.ifc` — 1,707,831 bytes
- `model_115.ifc` — 2,101,918 bytes
- `model_89.ifc` — 2,692,605 bytes
- `model_169.ifc` — 3,188,039 bytes
- `model_77.ifc` — 22,145,516 bytes
- `model_68.ifc` — 45,334,489 bytes

### `2026_BIMprojects/`

Small/medium architecture + structure pairs from the 2026 BIM Projects archive. Useful for same-building cross-discipline stress tests.

- Pair 0:
  - `model_0_arc.ifc` — 7,050,075 bytes
  - `model_0_structure.ifc` — 5,752,411 bytes
- Pair 3:
  - `model_3_arc.ifc` — 13,802,280 bytes
  - `model_3_structure.ifc` — 409,916 bytes
- Pair 5:
  - `model_5_arc.ifc` — 76,612,522 bytes
  - `model_5_structure.ifc` — 5,310,508 bytes
- Pair 7:
  - `model_7_arc.ifc` — 9,390,720 bytes
  - `model_7_structure.ifc` — 3,077,417 bytes
- Pair 8:
  - `model_8_arc.ifc` — 22,059,457 bytes
  - `model_8_structure.ifc` — 1,928,533 bytes

Total extracted sample size: about 217 MB.

## Non-goals

- This folder is not the full GNI corpus.
- These files are not revision pairs and should not be treated as ground-truth before/after diffs.
- Large/outlier GNI files stay external/opt-in.
