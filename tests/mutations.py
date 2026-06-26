"""Known-edit mutation harness: semantic ground truth from constructed edits.

Each mutator takes an open ifcopenshell model, applies exactly one constructed
edit, and returns a ``Mutation`` manifest stating what was done and what a
correct diff must therefore report. Expectations come from the edit itself,
never from blessing current engine output.

Victim pickers are deterministic (first qualifying candidate in step order)
and pick only *exclusive* targets — placements, psets, and properties not
shared with any other entity — so each mutation is provably a single-entity
edit and the manifest's expectations are airtight. Pickers fail loudly when a
seed has no qualifying candidate so a seed swap cannot silently hollow out the
scenario suite.

Pair-building stays in the tests: write the baseline and the mutated copy
through the same ifcopenshell write path so the constructed edit is the only
delta between the two files.
"""

from __future__ import annotations

import math
import uuid
from collections import Counter
from dataclasses import dataclass, field

import ifcopenshell
import ifcopenshell.guid
import ifcopenshell.util.unit

_GUID_NAMESPACE = uuid.UUID("8a3279cf-25b0-4480-bbcf-cd166bf2cb44")
_MOVE_OFFSET_MM = (123.0, -45.0, 67.0)

# Inverse relationships through which a leaf product may be referenced and
# still be removable as a pure single-product deletion (victim only ever sits
# in the listed aggregate attribute). Anything else — aggregation, voids,
# fills, connectivity — makes the removal more than a one-entity edit, so the
# deletion picker skips such products.
_SAFE_DELETE_RELS = {
    "IfcRelContainedInSpatialStructure": "RelatedElements",
    "IfcRelDefinesByProperties": "RelatedObjects",
    "IfcRelDefinesByType": "RelatedObjects",
    "IfcRelAssociatesMaterial": "RelatedObjects",
}


@dataclass(frozen=True)
class Mutation:
    """Manifest of one constructed edit and the report it must produce."""

    operation: str
    victim_guid: str | None = None
    victim_type: str | None = None
    # Section the victim must land in ("modified"/"deleted"); None for
    # whole-file operations like the GUID scramble.
    expected_victim_section: str | None = None
    # Aspect states the victim's matched item must satisfy (subset; aspects
    # the mutation makes no claim about — e.g. topology, whose self-seed
    # legitimately absorbs data changes — are left unconstrained).
    expected_victim_aspects: dict[str, str] = field(default_factory=dict)
    # Euclidean norm of the world-space move in mm (move operations only).
    # Frame-independent: rigid parent placements preserve the norm.
    expected_delta_mm: float | None = None
    # duplicate_guid only: the GlobalId the victim was overwritten with.
    partner_guid: str | None = None
    # edit_type_pset_value only: GlobalIds of the products inheriting the
    # edited type pset. Each must land in expected_victim_section with
    # expected_victim_aspects; the type entity itself carries no signature.
    affected_guids: tuple[str, ...] = ()


def scramble_guids(model: ifcopenshell.file) -> Mutation:
    """Rewrite every GlobalId to a fresh deterministic value.

    Truth: GlobalIds are identity labels, not content — nothing was added,
    deleted, or modified, and the rewritten ids must not pass as identity
    evidence.
    """
    rewritten = 0
    for entity in model:
        if not hasattr(entity, "GlobalId"):
            continue
        guid = entity.GlobalId
        if isinstance(guid, str) and guid.strip():
            entity.GlobalId = ifcopenshell.guid.compress(uuid.uuid5(_GUID_NAMESPACE, guid).hex)
            rewritten += 1
    assert rewritten > 0, "seed has no GlobalIds to scramble"
    return Mutation(operation="scramble_guids")


def duplicate_guid(model: ifcopenshell.file) -> Mutation:
    """Overwrite one product's GlobalId with another product's GlobalId.

    Truth: content is untouched, so nothing may be reported added/deleted/
    modified; the collision must be surfaced and the duplicated id must never
    be used as identity evidence for the two entities now sharing it.
    """
    counts = _product_guid_counts(model)
    assert counts and max(counts.values()) == 1, "seed must start without duplicate product GlobalIds"
    keeper, victim, *_ = (p for p in _products_in_step_order(model) if p.GlobalId)
    original = victim.GlobalId
    victim.GlobalId = keeper.GlobalId
    return Mutation(
        operation="duplicate_guid",
        victim_guid=original,
        victim_type=victim.is_a(),
        partner_guid=keeper.GlobalId,
    )


def move_product(model: ifcopenshell.file, offset_mm: tuple[float, float, float] = _MOVE_OFFSET_MM) -> Mutation:
    """Translate one product's exclusive local placement by a known offset.

    Truth: placement is the only thing that changed, on exactly one product.
    A correct report has no additions/deletions, exactly one modified entity
    (the victim, matched by GlobalId) whose only changed aspect is placement,
    with |placement_delta_mm| equal to |offset|.
    """
    victim, placement, axis_placement = _pick_exclusively_placed_product(model)
    # Model length unit -> meters; the offset is specified in world mm.
    unit_scale = ifcopenshell.util.unit.calculate_unit_scale(model)
    offset_model = tuple(component / 1000.0 / unit_scale for component in offset_mm)
    old_point = axis_placement.Location
    new_point = model.create_entity(
        "IfcCartesianPoint",
        tuple(c + d for c, d in zip(old_point.Coordinates, offset_model)),
    )
    # Fresh point + axis placement: the old ones may be shared with other
    # placements, the local placement itself is exclusive (picker-verified).
    placement.RelativePlacement = model.create_entity(
        "IfcAxis2Placement3D", new_point, axis_placement.Axis, axis_placement.RefDirection
    )
    return Mutation(
        operation="move_product",
        victim_guid=victim.GlobalId,
        victim_type=victim.is_a(),
        expected_victim_section="modified",
        expected_victim_aspects={
            "geometry": "unchanged",
            "data": "unchanged",
            "topology": "unchanged",
            "placement": "changed",
        },
        expected_delta_mm=math.hypot(*offset_mm),
    )


def delete_product(model: ifcopenshell.file) -> Mutation:
    """Remove one leaf product (and its slots in safe relationships).

    Truth: exactly the victim disappears. A correct report deletes only the
    victim, adds nothing, and any other flagged entity may only carry the
    relational fallout of the removal (transitive change), never an intrinsic
    one.
    """
    victim = _pick_leaf_product(model)
    guid, victim_type = victim.GlobalId, victim.is_a()
    for rel in list(model.get_inverse(victim)):
        attr_name = _SAFE_DELETE_RELS[rel.is_a()]
        remaining = tuple(obj for obj in (getattr(rel, attr_name) or ()) if obj != victim)
        if remaining:
            setattr(rel, attr_name, remaining)
        else:
            model.remove(rel)
    # The victim's private placement/representation subgraph stays behind as
    # unrooted entities; those carry no signatures, so they are not part of
    # the semantic truth of this edit.
    model.remove(victim)
    return Mutation(
        operation="delete_product",
        victim_guid=guid,
        victim_type=victim_type,
        expected_victim_section="deleted",
    )


def edit_pset_value(model: ifcopenshell.file) -> Mutation:
    """Rewrite one string property value in one product's exclusive pset.

    Truth: a pure data edit on exactly one product. The victim must be
    matched by GlobalId with data changed and geometry/placement unchanged;
    no claim is made about its topology aspect (the WL self-seed legitimately
    absorbs the data change). Everything else may only change transitively.
    """
    victim, prop = _pick_exclusive_string_property(model)
    nominal = prop.NominalValue
    prop.NominalValue = model.create_entity(nominal.is_a(), nominal.wrappedValue + " [athar-edited]")
    return Mutation(
        operation="edit_pset_value",
        victim_guid=victim.GlobalId,
        victim_type=victim.is_a(),
        expected_victim_section="modified",
        expected_victim_aspects={
            "geometry": "unchanged",
            "data": "changed",
            "placement": "unchanged",
        },
    )


def edit_type_pset_value(model: ifcopenshell.file) -> Mutation:
    """Rewrite one leaf value in a type object's exclusive property set.

    Truth: the edited value is inherited by every occurrence linked to the
    type via IfcRelDefinesByType, so the effective data of exactly those
    products changed. A correct report adds/deletes nothing and shows each
    occurrence as modified (matched by GlobalId) with data changed and
    geometry/placement unchanged; the type object itself carries no signature,
    so the occurrences are where the change must surface. No claim is made
    about topology (the WL self-seed legitimately absorbs the data change),
    and anything outside the occurrence set may only change transitively.
    """
    type_obj, prop, occurrences = _pick_exclusive_type_property(model)
    nominal = prop.NominalValue
    value = nominal.wrappedValue
    if isinstance(value, bool):
        new_value = not value
    elif isinstance(value, str):
        new_value = value + " [athar-edited]"
    else:
        # A whole-unit bump can never be absorbed by canonical quantization
        # (the smallest scale in play is 1e3 after unit conversion).
        new_value = value + 1
    prop.NominalValue = model.create_entity(nominal.is_a(), new_value)
    return Mutation(
        operation="edit_type_pset_value",
        victim_guid=type_obj.GlobalId,
        victim_type=type_obj.is_a(),
        expected_victim_section="modified",
        expected_victim_aspects={
            "geometry": "unchanged",
            "data": "changed",
            "placement": "unchanged",
        },
        affected_guids=tuple(sorted(occ.GlobalId for occ in occurrences)),
    )


def rename_product(model: ifcopenshell.file) -> Mutation:
    """Rewrite one product's Name attribute.

    Same truth shape as edit_pset_value, through the other data path: the
    entity's own scalar attribute instead of an attached property subtree.
    """
    counts = _product_guid_counts(model)
    for product in _products_in_step_order(model):
        if counts[product.GlobalId] != 1:
            continue
        product.Name = f"{product.Name or ''} [athar-renamed]"
        return Mutation(
            operation="rename_product",
            victim_guid=product.GlobalId,
            victim_type=product.is_a(),
            expected_victim_section="modified",
            expected_victim_aspects={
                "geometry": "unchanged",
                "data": "changed",
                "placement": "unchanged",
            },
        )
    raise AssertionError("seed has no uniquely-identified product to rename")


def add_product(model: ifcopenshell.file) -> Mutation:
    """Insert one fresh leaf product, contained in an existing spatial structure.

    The symmetric counterpart to delete_product: exactly one product appears
    that was not there before. Truth: a correct report adds only the new proxy,
    deletes nothing, and shows no *intrinsic* modification anywhere — the only
    permitted ripple is the spatial container gaining a child (a transitive
    change on the container).
    """
    container = _pick_spatial_container(model)
    placement = model.create_entity(
        "IfcLocalPlacement",
        RelativePlacement=model.create_entity(
            "IfcAxis2Placement3D",
            Location=model.create_entity("IfcCartesianPoint", (0.0, 0.0, 0.0)),
        ),
    )
    new_guid = ifcopenshell.guid.compress(uuid.uuid5(_GUID_NAMESPACE, "added-product").hex)
    proxy = model.create_entity(
        "IfcBuildingElementProxy",
        GlobalId=new_guid,
        Name="athar-added-proxy",
        ObjectPlacement=placement,
    )
    container.RelatedElements = tuple(container.RelatedElements or ()) + (proxy,)
    return Mutation(
        operation="add_product",
        victim_guid=new_guid,
        victim_type=proxy.is_a(),
        expected_victim_section="added",
    )


def _pick_spatial_container(model: ifcopenshell.file):
    """An existing IfcRelContainedInSpatialStructure to append a child to."""
    rels = sorted(
        model.by_type("IfcRelContainedInSpatialStructure"), key=lambda entity: entity.id()
    )
    if rels:
        return rels[0]
    raise AssertionError("seed has no spatial containment relationship to extend")


def _products_in_step_order(model: ifcopenshell.file) -> list:
    return sorted(model.by_type("IfcProduct"), key=lambda entity: entity.id())


def _product_guid_counts(model: ifcopenshell.file) -> Counter:
    return Counter(p.GlobalId for p in model.by_type("IfcProduct") if p.GlobalId)


def _pick_exclusively_placed_product(model: ifcopenshell.file):
    counts = _product_guid_counts(model)
    for product in _products_in_step_order(model):
        if counts[product.GlobalId] != 1:
            continue
        placement = product.ObjectPlacement
        if placement is None or not placement.is_a("IfcLocalPlacement"):
            continue
        # Exclusive frame: referenced only by the victim — moving it cannot
        # drag along co-placed products or child placements.
        if len(model.get_inverse(placement)) != 1:
            continue
        axis_placement = placement.RelativePlacement
        if axis_placement is None or not axis_placement.is_a("IfcAxis2Placement3D"):
            continue
        point = axis_placement.Location
        if point is None or not point.is_a("IfcCartesianPoint") or len(point.Coordinates) != 3:
            continue
        return product, placement, axis_placement
    raise AssertionError("seed has no product with an exclusive 3D local placement")


def _pick_leaf_product(model: ifcopenshell.file):
    counts = _product_guid_counts(model)
    for product in _products_in_step_order(model):
        if counts[product.GlobalId] != 1 or _is_spatial(product):
            continue
        inverses = model.get_inverse(product)
        if inverses and all(
            rel.is_a() in _SAFE_DELETE_RELS
            and product in (getattr(rel, _SAFE_DELETE_RELS[rel.is_a()]) or ())
            for rel in inverses
        ):
            return product
    raise AssertionError("seed has no leaf product that is safe to delete")


def _pick_exclusive_string_property(model: ifcopenshell.file):
    counts = _product_guid_counts(model)
    for rel in sorted(model.by_type("IfcRelDefinesByProperties"), key=lambda entity: entity.id()):
        related = rel.RelatedObjects or ()
        if len(related) != 1:
            continue
        product = related[0]
        if not product.is_a("IfcProduct") or counts[product.GlobalId] != 1:
            continue
        pset = rel.RelatingPropertyDefinition
        if pset is None or not pset.is_a("IfcPropertySet"):
            continue
        if len(model.get_inverse(pset)) != 1:
            continue
        for prop in pset.HasProperties or ():
            if (
                prop.is_a("IfcPropertySingleValue")
                and prop.NominalValue is not None
                and isinstance(prop.NominalValue.wrappedValue, str)
                and prop.NominalValue.wrappedValue
                and len(model.get_inverse(prop)) == 1
            ):
                return product, prop
    raise AssertionError("seed has no exclusive single-product pset with a string property")


def _pick_exclusive_type_property(model: ifcopenshell.file):
    """First type object (step order) with an exclusively-owned editable leaf.

    Exclusivity mirrors _pick_exclusive_string_property: the pset is referenced
    only by the type and the property only by the pset, so the edit is provably
    a single-type edit. Every linked occurrence must be a uniquely-identified
    product so the manifest can demand a GlobalId match for each. String, bool,
    and numeric leaves are all editable: real seeds share bool/string property
    instances across dozens of psets, so the only exclusive leaf may well be a
    numeric one.
    """
    counts = _product_guid_counts(model)
    for type_obj in sorted(model.by_type("IfcTypeObject"), key=lambda entity: entity.id()):
        occurrences = []
        for rel in model.get_inverse(type_obj):
            if rel.is_a("IfcRelDefinesByType") and rel.RelatingType == type_obj:
                occurrences.extend(rel.RelatedObjects or ())
        if not occurrences or not all(
            occ.is_a("IfcProduct") and counts[occ.GlobalId] == 1 for occ in occurrences
        ):
            continue
        for pset in type_obj.HasPropertySets or ():
            if not pset.is_a("IfcPropertySet"):
                continue
            if len(model.get_inverse(pset)) != 1:
                continue
            for prop in pset.HasProperties or ():
                if (
                    prop.is_a("IfcPropertySingleValue")
                    and prop.NominalValue is not None
                    and isinstance(prop.NominalValue.wrappedValue, (str, bool, int, float))
                    and prop.NominalValue.wrappedValue != ""
                    and len(model.get_inverse(prop)) == 1
                ):
                    return type_obj, prop, occurrences
    raise AssertionError("seed has no type object with an exclusive editable pset property and product occurrences")


def _is_spatial(entity) -> bool:
    for type_name in ("IfcSpatialElement", "IfcSpatialStructureElement"):
        try:
            if entity.is_a(type_name):
                return True
        except Exception:
            continue
    return False
