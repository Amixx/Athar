# Revit round-trip corpus — 2026-07-06

Real-world Archicad-authored IFC2X3 model ("real-world-spanish", 173 MB,
44,389 products) opened in Revit and re-exported through a chain of setting
variations and atomic UI edits. Raw `.ifc` files are zstd-compressed
(`zstd -d --rm exports/*.zst` to unpack). The native `.rvt` snapshot
(171 MB, taken immediately after import, before any edit) lives in the
personal Google Drive `Athar/` folder, not in git — it can regenerate any
export variant without redoing the import.

## Environment

- Revit 26.0.4.409, 20250227_1515(x64), fresh project with no template
- Import: File → Open → IFC, default options
- Import warnings: `revit_import_error_report.html` (~26 join warnings,
  no reported drops)
- Exports r4–r10 share one duplicated setup: IFC4 Reference View
  [architecture] + "keep GUIDs" + "export Revit property sets"

## Chain (each step is one export; edits are cumulative)

| file | schema | action |
|------|--------|--------|
| original | IFC2X3 | Archicad-authored source model |
| r1 | IFC2X3 | export, default in-session settings, no edits |
| r2 | IFC4 | setup: IFC4 Reference View [architecture], no edits |
| r3 | IFC4 | + "keep GUIDs" export option, no edits |
| r4 | IFC4 | + "export Revit property sets", no edits |
| r5 | IFC4 | moved whole extruded facade away from building |
| r6 | IFC4 | deleted a row of windows (one merged proxy) |
| r7 | IFC4 | one door: instance property "Frame type" = "Test frame type" |
| r8 | IFC4 | duplicated that door twice onto other walls, same level |
| r9 | IFC4 | no changes (same-settings determinism pair with r8) |
| r10 | IFC4 | same door: edit type, one type property value changed |

## Reports (`reports/`, engine JSON)

Same-schema consecutive pairs: original→r1, r2→r3, r3→r4, r4→r5, r5→r6,
r6→r7, r7→r8, r8→r9, r9→r10. original→r2+ is refused
(`schema_incompatible`) because the chain switches to IFC4 at r2.

## Extras

- `guid_chain_trace.txt` — sample GUID + express-id trace across all 11
  files, plus pairwise GUID-set/renumbering stability.
- `class_counts_orig_vs_r1.txt` — per-class product counts and raw GUID
  overlap, original vs r1.
- `FINDINGS.md` — preliminary takeaways from the day.
