# Signature Pipeline Agent Notes

`athar/bottom/` builds a `SignatureBundle` for one IFC file. It is
self-contained and should not depend on matcher, delta, Git, or presentation
code.

## Pipeline (native-only)

The bottom layer is implemented in Rust (`athar/_native`, exposed as
`athar_native`). It is required — there is no Python fallback. The Python
surface is thin glue:

- `_native.py` — discovers the compiled module (`native()`).
- `parser.py` — file-independent schema/unit glue only: schema-support guard,
  measure-type helpers, `IfcUnitAssignment` factor extraction.
- `native_schema.py` — builds the per-class descriptor map (JSON, cached per
  schema) that Rust applies to the STEP records.
- `signatures.py` — opens the file once via `ifcopenshell` for the schema name
  and unit factors, releases it, runs the native pipeline, and wraps the result
  in `SignatureVector` / `SignatureBundle`.

The Rust side (`athar/_native/src/`): `step.rs` tokenize → `canon.rs`
canonicalize (NFC, banker's-rounded quantization, `data_facts`) → `edges.rs`
relationship/attribute classification → Merkle (geometry/data) → WL topology
gossip → `spatial.rs` placement/centroid/AABB. The edge-policy table lives in
`edges.rs`. `GlobalId` and `OwnerHistory` never enter hashes; spatial tagging
supports IFC4 `IfcSpatialElement` and IFC2X3 `IfcSpatialStructureElement` roots.

## Edge Policy Contracts

- Property and quantity subtrees (`HasProperties`, `Quantities`,
  `HasQuantities`, `HasPropertySets`) are include/data edges so property value
  edits reach `vh_data`.
- `IfcRelDefinesByType` is both context/topology and include/data from
  occurrence to type. Type objects carry no signatures, so type-level property
  values must be attributed to each occurrence's `vh_data`.
- Other generic non-geometry references stay ignored unless there is a clear
  semantic reason and coverage proving the change.
- Placement is excluded from `vh_geometry`; identical components at different
  locations should share the same geometry hash.
- `IfcPolygonalFaceSet.Faces` is an include/geometry edge (canon-v6): before
  it, face `CoordIndex` edits were invisible to `vh_geometry` because the
  face entities never reached the product Merkle. Face-list order stays
  neutral through the Merkle's sorted child hashing.

## Tessellated-Mesh Canonicalization (canon-v6)

`IfcTriangulatedFaceSet` is hashed as order-canonical expanded triangles:
each triangle becomes its three quantized vertex triples (Coordinates and
PnIndex resolved away, no edge to the point list), cyclically rotated to the
lexicographically-smallest form and emitted one part per triangle so the
sorted parts make the triangle list a set. Vertex order and triangle order
are serialization noise (Revit permutes both between otherwise-identical
exports — 2026-07 round-trip corpus), but a reversed winding is a real
orientation change and must stay visible. Facesets carrying `Normals` fall
back to the raw order-sensitive encoding. Point lists consumed by
index-order-sensitive entities (e.g. `IfcIndexedPolyCurve`) are untouched.
Pinned by `tests/test_mesh_canonicalization.py`.

## WL Topology Contract

`vh_topology` answers "did the class-multiset of my direct relationship
neighborhood change": WL seeds are canonical-class-only and both gossip radii
are 1 (direct context neighbors, direct spatial parents/children). Neighbor
*content* (geometry/data) must never enter the seeds — a neighbor's content
change is already reported on that neighbor as intrinsic, and content-bearing
seeds fan a single change out storey-wide (2026-07 Revit round-trip corpus:
2 re-tessellated elements false-flagged 175 storey siblings per export pair).
Consequences to preserve: a pure content edit keeps the victim's own topology
aspect `unchanged`; add/delete flips only direct containers and directly
connected elements, not siblings.

## Known Limitation: Representation Equivalence

`vh_geometry` hashes the geometry-domain subgraph (representation items,
profiles, points), not a resolved solid. The same shape stored two ways — e.g.
an `IfcExtrudedAreaSolid` in one file and an `IfcTriangulatedFaceSet` /
`IfcFacetedBrep` of the identical solid in another — produces different
subgraphs and therefore a different geometry hash, so the engine reports it as
geometry-changed even though the realized BRep is the same.

This is intentional, not a bug: the engine does no deep B-rep comparison (a
core contract), so it cannot resolve representations to a canonical solid.
Identity still holds — a GUID-matched product survives the swap as `modified`
with the geometry aspect flipped, never as added+deleted. The gap is pinned by
`tests/test_geometry_representation_gap.py` (a strict `xfail`); closing it would
require canonical solid resolution through a geometry kernel.
