"""Shared constants for the current engine runtime."""

from __future__ import annotations

CANON_VERSION = "athar-canon-v9"  # v9: scalar attributes of shape-model entities (Depth, profile dims, Scale) and axis placements reached from geometry enter vh_geometry; v8: unset ($) and derived (*) values hash alike at attributes the entity redeclares as DERIVE; v7: STEP string control directives (\X\, \X2\, \X4\, \S\) are decoded before hashing; v6: triangulated meshes hash as order-canonical expanded triangles (winding preserved) and IfcPolygonalFaceSet Faces enter the geometry Merkle; v5: WL topology seeds are class-only and gossip radius is 1 (vh_topology = direct-neighborhood structure); v4: native-only bottom pipeline canonical form; v3: type data included in occurrence vh_data; v2: property/quantity subtrees included in the data Merkle domain.
SUPPORTED_SCHEMAS = ("IFC4", "IFC2X3")
