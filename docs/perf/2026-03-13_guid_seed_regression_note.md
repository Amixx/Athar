## 2026-03-13 GUID Seed Regression Note

- Context: `basichouse_v1_v2` regression observed in `docs/perf/native_check_rust_serial_2026-03-13_02-27-01.json` and still present, though reduced, in `docs/perf/native_check_rust_serial_2026-03-13_02-55-13.json`.
- Primary trigger: early GUID-path seeding gate in `athar/diff/context.py` switched from `guid_seed_diagnostics["coverage"]` to `guid_seed_diagnostics["unique_guid_overlap"]`.
- Measured effect on `basichouse_v1_v2`:
  - `seed_guid_path_propagation`: `~0ms -> 27.9s`, later `20.1s`
  - `seed_text_fingerprints.matched`: `114,512 -> 530`
  - `prepare_context`: `155.5s -> 314.7s`, later `259.5s`
  - `base_change_count`: `133 -> 126`, later `120`
- Interpretation: `unique_guid_overlap` stayed near `1.0` on a large sparse-GUID graph, so early propagation ran despite GUID anchors covering only a tiny fraction of the full graph.
- Secondary observation from a focused local diagnostic on `tests/fixtures/BasicHouse.ifc` vs `tests/fixtures/BasicHouse_modified.ifc`:
  - emit phase scanned `3510` candidate IDs and produced `113` actual changes
  - `3446` candidate IDs were `G:` identities
  - two extreme emit stalls (`~36.5s`, `~9.5s`) dominated wall time, consistent with cyclic-GC pauses landing inside emit-time compare normalization
- Follow-up implemented:
  - restore whole-graph `coverage` gating for early GUID path seeding
  - keep cyclic GC disabled across base-change/derived-marker emission paths
