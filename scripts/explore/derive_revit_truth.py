#!/usr/bin/env python3
"""Independent GUID-level IFC product comparison, without tessellation.

Compares own attributes (excluding identity, OwnerHistory export metadata,
placement and representation), inherited property/quantity values, assigned type,
spatial containment, world placement and representation graphs. Graph comparison
preserves LIST order, so mesh reindexing can be reported despite equal geometry.
Such differences require investigation; this is not a semantic geometry oracle.

Numeric graph/property values use absolute tolerance 1e-8 in declared model units
and no relative tolerance. World matrix translations use 1e-6 metres; rotation
entries use 1e-9. Files must have the same schema and length units. Missing or
duplicate product GlobalIds fail, rather than inventing correspondence.
"""
from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

import ifcopenshell
import ifcopenshell.util.element
import ifcopenshell.util.placement
import ifcopenshell.util.unit

NUMERIC_TOLERANCE = 1e-8
PLACEMENT_METRES_TOLERANCE = 1e-6
ROTATION_TOLERANCE = 1e-9


def _equal(left, right, seen=None):
    if isinstance(left, ifcopenshell.entity_instance) or isinstance(right, ifcopenshell.entity_instance):
        if not isinstance(left, ifcopenshell.entity_instance) or not isinstance(right, ifcopenshell.entity_instance):
            return False
        if left.is_a() != right.is_a() or len(left) != len(right):
            return False
        seen = set() if seen is None else seen
        pair = (left.id(), right.id())
        if left.id() and right.id():
            if pair in seen:
                return True
            seen.add(pair)
        return all(_equal(a, b, seen) for a, b in zip(left, right))
    if isinstance(left, dict) and isinstance(right, dict):
        return left.keys() == right.keys() and all(_equal(left[key], right[key], seen) for key in left)
    if isinstance(left, (list, tuple)) and isinstance(right, (list, tuple)):
        return len(left) == len(right) and all(_equal(a, b, seen) for a, b in zip(left, right))
    if isinstance(left, float) or isinstance(right, float):
        return (isinstance(left, (int, float)) and isinstance(right, (int, float))
                and math.isclose(left, right, abs_tol=NUMERIC_TOLERANCE, rel_tol=0))
    return type(left) is type(right) and left == right


def _products(model):
    products = {}
    for product in model.by_type("IfcProduct"):
        guid = product.GlobalId
        if not guid or guid in products:
            raise ValueError(f"missing or duplicate product GlobalId: {guid!r}")
        products[guid] = product
    return products


def _psets(product):
    # get_psets includes export-local STEP ids, which are not property values.
    def values(value):
        if isinstance(value, dict):
            return {key: values(item) for key, item in value.items() if key != "id"}
        if isinstance(value, (list, tuple)):
            return [values(item) for item in value]
        return value
    return values(ifcopenshell.util.element.get_psets(product))


def _attributes(product):
    excluded = {"id", "type", "GlobalId", "OwnerHistory", "ObjectPlacement", "Representation"}
    return {key: value for key, value in product.get_info().items() if key not in excluded}


def _type(product):
    assigned = ifcopenshell.util.element.get_type(product)
    if assigned is None:
        return None
    return assigned.is_a(), assigned.GlobalId


def _containment(product):
    return sorted(rel.RelatingStructure.GlobalId for rel in getattr(product, "ContainedInStructure", ()))


def _placement_equal(left, right, scale):
    a = ifcopenshell.util.placement.get_local_placement(left.ObjectPlacement)
    b = ifcopenshell.util.placement.get_local_placement(right.ObjectPlacement)
    for row in range(4):
        for col in range(4):
            factor = scale if col == 3 and row < 3 else 1.0
            tolerance = PLACEMENT_METRES_TOLERANCE if col == 3 and row < 3 else ROTATION_TOLERANCE
            if not math.isclose(float(a[row, col]) * factor, float(b[row, col]) * factor,
                                abs_tol=tolerance, rel_tol=0):
                return False
    return True


def derive_truth(old_path: str | Path, new_path: str | Path) -> dict:
    old, new = ifcopenshell.open(str(old_path)), ifcopenshell.open(str(new_path))
    if old.schema != new.schema:
        raise ValueError(f"schema mismatch: {old.schema} != {new.schema}")
    old_scale = ifcopenshell.util.unit.calculate_unit_scale(old)
    new_scale = ifcopenshell.util.unit.calculate_unit_scale(new)
    if old_scale != new_scale:
        raise ValueError(f"length unit mismatch: {old_scale} != {new_scale}")
    before, after = _products(old), _products(new)
    aspects = {}
    for guid in sorted(before.keys() & after.keys()):
        left, right = before[guid], after[guid]
        changed = []
        if left.is_a() != right.is_a() or not _equal(_attributes(left), _attributes(right)):
            changed.append("attributes")
        if not _equal(_psets(left), _psets(right)):
            changed.append("properties_quantities")
        if _type(left) != _type(right):
            changed.append("type_assignment")
        if _containment(left) != _containment(right):
            changed.append("containment")
        if not _placement_equal(left, right, old_scale):
            changed.append("world_placement")
        if not _equal(left.Representation, right.Representation):
            changed.append("representation")
        if changed:
            aspects[guid] = changed
    guids = {"added": sorted(after.keys() - before.keys()),
             "deleted": sorted(before.keys() - after.keys()), "modified": sorted(aspects)}
    return {"changed_guids": guids, "counts": {key: len(value) for key, value in guids.items()},
            "aspects": aspects,
            "tolerances": {"numeric_model_units": NUMERIC_TOLERANCE,
                           "placement_metres": PLACEMENT_METRES_TOLERANCE, "rotation": ROTATION_TOLERANCE},
            "scope": "IfcProduct; structural representation comparison preserves list order"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("old", type=Path)
    parser.add_argument("new", type=Path)
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()
    output = json.dumps(derive_truth(args.old, args.new), indent=2, sort_keys=True)
    if args.out:
        args.out.write_text(output + "\n", encoding="utf-8")
    else:
        print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
