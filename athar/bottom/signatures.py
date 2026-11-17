"""Core-engine signature assembly pipeline.

``athar_native`` tokenizes the STEP file and runs the whole parse →
canonicalize → edges → Merkle → WL → spatial pipeline in Rust, returning the
root signatures and the coverage account. The file's entities never
materialize as Python objects. ifcopenshell supplies the schema descriptors,
unit factors and raw property values.
"""

from __future__ import annotations

from ._native import native
from .constants import CANON_VERSION
from .types import (
    Coverage,
    ParseDiagnostics,
    RelatedEntity,
    RelationTarget,
    SignatureBundle,
    SignatureVector,
)


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
    from .native_schema import schema_descriptors_json
    from .parser import _assert_supported_schema, _extract_unit_context, open_ifc
    from .properties import extract_orphan_property_sets, extract_properties, own_unit_factors

    # The ifcopenshell model is released before the native parse so both
    # representations of the file are never resident together.
    ifc = open_ifc(filepath)
    schema = str(ifc.schema or "")
    _assert_supported_schema(schema)
    unit_context = _extract_unit_context(ifc)
    unit_factors = unit_context.get("unit_factors", {})
    record_unit_factors = {**unit_context["record_unit_factors"], **own_unit_factors(ifc)}
    property_index = extract_properties(ifc, unit_context["property_unit_factors"])
    orphan_property_sets = extract_orphan_property_sets(ifc)
    del ifc

    schema_json = schema_descriptors_json(schema)
    (
        sigs,
        edge_stats_map,
        (dangling, cycle_breaks, warnings),
        (unknown_relationships, unreached, ignored_by_design),
        (relation_labels, related_entities),
    ) = native_mod.build_signature_bundle(filepath, schema_json, unit_factors, record_unit_factors)

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
        (placement, placement_unresolved, local_placement, placement_parent),
        (data_facts, relation_targets),
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
            canon_version=CANON_VERSION,
            data_facts=tuple((path, value) for path, value in data_facts),
            placement_unresolved=placement_unresolved,
            local_placement=tuple(local_placement) if local_placement is not None else None,
            placement_parent=placement_parent,
            relation_targets=tuple(
                RelationTarget(*relation_labels[label], target) for label, target in relation_targets
            ),
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
        coverage=Coverage(
            unknown_relationships=dict(unknown_relationships),
            unreached={name: (count, digest) for name, (count, digest) in unreached.items()},
            ignored_by_design=dict(ignored_by_design),
        ),
        property_index=property_index,
        orphan_property_sets=orphan_property_sets,
        related_entities={step: RelatedEntity(*label) for step, label in related_entities.items()},
    )
