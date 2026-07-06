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

**Root-caused and fixed 2026-07-06 (canon-v5).** The noise was not
relationship churn: Revit re-exports the geometry of exactly 2 elements
nondeterministically (IfcSlab `04S5ZSnP1438qLTWHYFlNV` on Level 1, IfcWall
`3LpkEFuRv1RfddUahE4zQR` on Level 6; identical GUID sets in r2→r3 and
r8→r9). WL topology seeds contained each neighbor's `vh_geometry`, and
spatial gossip was undirected with radius 2 (element→storey→all siblings),
so 2 real geometry wobbles flipped the topology hash of both full storeys
plus the building — `scripts/explore/wl_blast_radius.py` reproduces the
175-element transitive set exactly (175/175 predicted). Fix: class-only WL
seeds + gossip radius 1, i.e. `vh_topology` now means "the class-multiset
of my direct relationship neighborhood changed"; a neighbor's content
change is reported on the neighbor only. Post-fix chain (was → is):
r2→r3 177→2, r3→r4 840→840, r4→r5 264→49 (exactly the moved facade),
r5→r6 264→3+1del (container ripple only), r6→r7 178→3, r7→r8 126→4+4add,
r8→r9 177→2, r9→r10 83→13. The residual 2 on no-op pairs are the wobble
pair itself, now correctly attributed as intrinsic geometry changes (see
the representation-equivalence limitation in `athar/bottom/AGENTS.md`).
The committed `reports/*.json` are the pre-fix (canon-v4) outputs kept as
the study record.

**Wobble eliminated (canon-v6, same day).** Tessellated meshes now hash as
order-canonical expanded triangles (winding preserved), so the reorder
wobble is invisible by construction: both no-op pairs report exactly
0/0/0, r4→r5 reports exactly the 48 movers, r9→r10 exactly the 12
occurrences. Scoping this also uncovered and fixed a converse gap:
`IfcPolygonalFaceSet.Faces` never entered the geometry Merkle, so a face
`CoordIndex` edit (7,775 such facesets per export — the dominant
representation) produced a zero diff; it is now an include/geometry edge.
Pinned by `tests/test_mesh_canonicalization.py`.

**Wobble mechanism (audited).** The slab and wall are the only two products
in the Revit exports carrying `IfcTriangulatedFaceSet` meshes (everything
else is parametric). Revit re-serializes those meshes with permuted
`CoordList`/`CoordIndex` order — coordinate *values* are byte-identical,
only list order changes, and each of the two meshes independently may or
may not permute on any given export (slab wobbled on 6/8 consecutive
pairs, wall on 6/8, never anything else). Quantization cannot absorb this
(there is no numeric delta); the only cure would be canonicalizing mesh
vertex/triangle order before hashing, which is deliberately out of scope
(no deep geometry). `scripts/explore/roundtrip_wobble_classify.py`
separates reorder-wobble from genuine mesh change; the r4→r5 "49 movers"
are precisely 48 facade elements (45 placement deltas of
(3848.7, -783.1, 0) mm + 3 slabs with the move baked into vertices) plus
1 coincidental wall wobble.

**Known blind spot of the canon-v5 topology semantics:** an element
re-contained into a different same-class container with no
placement/data/geometry change no longer flips its own topology hash; it
surfaces on the two containers instead (both storeys' child multisets
change). No such case exists anywhere in this corpus
(`scripts/explore/roundtrip_recontain_scan.py`).

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

- Root-cause the 177-element transitive noise floor (finding 3): done, see
  above — remaining thread is why Revit re-tessellates those 2 specific
  elements differently on every export.
- Do the merged facade proxies keep stable GUIDs across *edit* exports?
  (They appear to — the moved facade stayed matched in r4→r5.)
- IFC2X3 export with the r4 setup would give a properly-configured
  original-comparable pair (r1 is defaults-only).
- Same protocol through Archicad (import this model back, or start from
  the .pln side) and Bonsai as the Native IFC control group.
