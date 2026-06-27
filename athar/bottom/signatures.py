"""Core-engine signature assembly pipeline.

The bottom layer is native-only: ``athar_native`` tokenizes the STEP file and
runs the whole parse → canonicalize → edges → Merkle → WL → spatial pipeline in
Rust, returning only product/spatial signatures. The file's millions of
entities never materialize as Python objects. ifcopenshell is used solely for
the file-independent schema descriptors and unit factors.
"""

from __future__ import annotations

from ._native import native
from .constants import CANON_VERSION
from .types import ParseDiagnostics, SignatureBundle, SignatureVector


def build_signature_bundle(filepath: str) -> SignatureBundle:
    """Run bottom-layer extraction and return first-class signatures."""
    native_mod = native()
    if native_mod is None:
        raise RuntimeError(
            "The native extension `athar_native` is required but is not built. "
            "Build it with `make native-build`."
        )
    return _build_signature_bundle_native(filepath, native_mod)


def _build_signature_bundle_native(filepath: str, native_mod) -> SignatureBundle:
    import ifcopenshell

    from .native_schema import schema_descriptors_json
    from .parser import _assert_supported_schema, _extract_unit_context
    from .properties import extract_properties

    # ifcopenshell is used for the schema name, unit factors, and raw property
    # values (relationship/pset traversal only, not the mesh primitives); it is
    # released before the native parse so the file's geometry entities are never
    # held as Python/C++ model state during signature building.
    ifc = ifcopenshell.open(filepath)
    schema = str(ifc.schema or "")
    _assert_supported_schema(schema)
    unit_factors = _extract_unit_context(ifc).get("unit_factors", {})
    property_index = extract_properties(ifc)
    del ifc

    schema_json = schema_descriptors_json(schema)
    sigs, edge_stats_map, (dangling, cycle_breaks, warnings) = native_mod.build_signature_bundle(
        filepath, schema_json, unit_factors
    )

    signatures: dict[int, SignatureVector] = {}
    for (
        step_id,
        guid,
        name,
        entity_type,
        canonical_class,
        vh_geometry,
        vh_data,
        vh_topology,
        placement,
        centroid,
        aabb,
        data_facts,
    ) in sigs:
        signatures[step_id] = SignatureVector(
            step_id=step_id,
            guid=guid,
            name=name,
            entity_type=entity_type,
            canonical_class=canonical_class,
            vh_geometry=vh_geometry,
            vh_data=vh_data,
            vh_topology=vh_topology,
            placement=tuple(placement) if placement is not None else None,
            centroid=tuple(centroid) if centroid is not None else None,
            aabb=tuple(aabb) if aabb is not None else None,
            canon_version=CANON_VERSION,
            data_facts=tuple((path, value) for path, value in data_facts),
        )

    diagnostics = ParseDiagnostics(
        dangling_refs=dangling,
        cycle_breaks=cycle_breaks,
        warnings=list(warnings),
    )
    return SignatureBundle(
        filepath=filepath,
        schema=schema,
        canon_version=CANON_VERSION,
        signatures=signatures,
        diagnostics=diagnostics,
        edge_stats=dict(edge_stats_map),
        property_index=property_index,
    )
