# Signature Pipeline Agent Notes

`athar/bottom/` builds a `SignatureBundle` for one IFC file. It is
self-contained and should not depend on matcher, delta, Git, or presentation
code.

## Pipeline

- `index.py` builds a byte-offset STEP index for random-access diagnostics.
- `parser.py` parses through `ifcopenshell` into `ParsedEntity` records.
  Canonicalize scalar attributes, preserve numeric string literals such as
  `"0"` and `"1"` as strings, normalize text to NFC, and normalize/quantize
  lengths through unit handling.
- Spatial tagging supports IFC4 `IfcSpatialElement` and IFC2X3
  `IfcSpatialStructureElement` roots.
- `link_inversion.py` owns reverse-reference maps.
- `edge_policy.py` is the declarative relationship classification table.
- `merkle.py` computes bottom-up sha256 Merkle hashes for geometry and data
  domains. `GlobalId` and `OwnerHistory` never enter hashes.
- `wl_gossip.py` computes topology hashes from `class|vh_geometry|vh_data`
  self seeds plus context and spatial neighbor seeds.
- `spatial.py` resolves `ObjectPlacement` chains and emits quantized
  world-space placement matrices, centroids, and AABBs.
- `signatures.py` assembles product/spatial `SignatureVector` objects and
  diagnostics. Do not retain the full parse result in the bundle.

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
