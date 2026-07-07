"""Shared constants for the current engine runtime."""

from __future__ import annotations

CANON_VERSION = "athar-canon-v6"  # v6: triangulated meshes hash as order-canonical expanded triangles (winding preserved) and IfcPolygonalFaceSet Faces enter the geometry Merkle; v5: WL topology seeds are class-only and gossip radius is 1 (vh_topology = direct-neighborhood structure); v4: native-only bottom pipeline canonical form; v3: type data included in occurrence vh_data; v2: property/quantity subtrees included in the data Merkle domain.
SUPPORTED_SCHEMA_PREFIXES = ("IFC4", "IFC2X3")
