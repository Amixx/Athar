# Preliminary findings — Revit round-trip study, 2026-07-06

## 1. GUIDs never scrambled; STEP ids always did

`GlobalId` survived every hop with zero collisions: 100% of shared products
keep their GUID across all ten exports, including the Archicad→Revit
conversion (688/688 of the products Revit kept as discrete elements).
Express (STEP numeric) ids churned at nearly every step — ~98% renumbered
between most consecutive exports, 77 renumbered even for a one-property
edit. The lone exception (r2→r3, zero renumbering) shows the exporter is
deterministic when model state is truly identical. Consequence: numeric-id
keyed tools (ifcmerge preconditions, text diff) fail after the first
export in this workflow; GUID+class identity carried the entire chain.

## 2. Revit import destroys granularity of non-core classes, silently

Original 44,389 products → 886 in every Revit export. Core architecture
survived 1:1 with GUIDs (walls 154/154, columns 158/158, doors 57/57,
curtain walls 21/21, beams 198/201, all 13 storeys). Detailed classes were
annihilated: IfcMember 18,939→0, IfcCovering 16,120→25,
IfcMechanicalFastener 6,024→0, IfcBuildingElementPart 1,342→0, IfcPlate
1,112→0, IfcStairFlight 24→0. ~80% of geometry edge volume survived,
lumped into Generic Model family instances (proxies 26→121): clicking the
facade in Revit selects one building-sized element. The merge happens at
import (family-fication), so no export setting can recover it. The import
error report showed only ~26 benign join warnings — the loss is silent.

## 3. Same-settings Revit exports are not noise-free

r8→r9 (zero model changes, identical setup) still reports 177 modified
(175 transitive topology, 2 mixed) out of 889 — ~20% false-modified noise
floor from relationship-graph churn between exports. The same ~177
signature appears in r2→r3. Root-causing which context/topology edges
churn (spatial containment rels? type rels?) is the highest-value
precision follow-up in the reports.

## 4. Atomic edits were detected surgically (identity level)

- r4→r5 facade move: 0 added/deleted; placement changes on the moved
  elements, identity held through a large translation.
- r5→r6 window-row delete: exactly 1 deletion (one merged proxy —
  granularity finding #2 made visible).
- r6→r7 instance property edit: exactly 1 intrinsic data modification.
- r7→r8 paste door twice: +2 IfcDoor +2 IfcOpeningElement, fresh GUIDs.
- r9→r10 type property edit: 11 intrinsic data modifications — the type
  edit fanned out to all instances, attributed per-occurrence as designed.

## 5. Engine behavior held up

44k-product pair diffed in 72 s; cross-schema pair correctly refused
(exit 3, `schema_incompatible`); guid tier matched everything, geometry
hash tier never fired (nothing survives re-serialization byte-identical —
expected for cross-tool churn); zero dropped matches, zero GUID
collisions.

## Open questions

- Root-cause the 177-element transitive noise floor (finding 3).
- Do the merged facade proxies keep stable GUIDs across *edit* exports?
  (They appear to — the moved facade stayed matched in r4→r5.)
- IFC2X3 export with the r4 setup would give a properly-configured
  original-comparable pair (r1 is defaults-only).
- Same protocol through Archicad (import this model back, or start from
  the .pln side) and Bonsai as the Native IFC control group.
