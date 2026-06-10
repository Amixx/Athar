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
