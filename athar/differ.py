"""Compare two parsed IFC models and produce a structured diff."""

from __future__ import annotations

import math
from collections import Counter

from athar.matcher import match_entities

# Relative tolerance for floating-point comparisons
_RTOL = 1e-6
# Absolute tolerance (catches near-zero drifts from serialization)
_ATOL = 1e-6

# Minimum number of entities sharing a displacement to count as bulk movement
_BULK_THRESHOLD = 5


def diff(old: dict, new: dict) -> dict:
    """Diff two parsed IFC models.

    Accepts the output of parser.parse() — a dict with "metadata" and "entities".

    Returns a dict with keys: metadata, added, deleted, changed, bulk_movements, summary.
    """
    old_entities = old["entities"]
    new_entities = new["entities"]

    # Build entity alignment (GUID-based or content-based fallback)
    alignment = match_entities(old, new)
    old_to_new = alignment["old_to_new"]

    matched_old = set(old_to_new.keys())
    matched_new = set(old_to_new.values())

    # Find openings that are filled by a door/window — these are implementation
    # details, not user-facing changes.  Standalone voids (no fill) are kept.
    filled_openings = _filled_opening_guids(old, new)

    added = [
        _summary(guid, new_entities[guid])
        for guid in sorted(set(new_entities) - matched_new)
        if guid not in filled_openings
    ]

    deleted = [
        _summary(guid, old_entities[guid])
        for guid in sorted(set(old_entities) - matched_old)
        if guid not in filled_openings
    ]

    changed = []
    for old_guid in sorted(matched_old):
        new_guid = old_to_new[old_guid]
        if old_guid in filled_openings or new_guid in filled_openings:
            continue
        changes = _diff_entity(old_entities[old_guid], new_entities[new_guid],
                               all_entities=new_entities)
        if changes:
            entry = _summary(new_guid, new_entities[new_guid])
            entry["changes"] = changes
            if old_guid != new_guid:
                entry["old_guid"] = old_guid
            changed.append(entry)

    # Detect bulk movements: many entities moved by the same vector
    bulk_movements, remaining_changed = _detect_bulk_movements(changed, new_entities)

    result = {
        "metadata": {
            "old": old["metadata"],
            "new": new["metadata"],
        },
        "added": added,
        "deleted": deleted,
        "changed": remaining_changed,
        "bulk_movements": bulk_movements,
        "summary": {
            "added": len(added),
            "deleted": len(deleted),
            "changed": len(remaining_changed),
            "bulk_moved": sum(len(bm["entities"]) for bm in bulk_movements),
            "unchanged": len(matched_old) - len(changed),
        },
    }

    if alignment["method"] == "content_fallback":
        result["match_method"] = "content_fallback"
        result["guid_overlap"] = alignment["guid_overlap"]

    return result


def _filled_opening_guids(old: dict, new: dict) -> set[str]:
    """Collect IfcOpeningElement GUIDs that are filled by a door/window.

    An opening with a fill relationship in either model version is an
    intermediary (wall→opening→door/window) — not interesting on its own.
    Standalone voids with no fill in either version are kept.
    """
    filled = set()
    for model in (old, new):
        for fill in model.get("relationships", {}).get("fills", []):
            filled.add(fill["opening_guid"])
    return filled


def _detect_bulk_movements(changed: list[dict],
                           new_entities: dict) -> tuple[list[dict], list[dict]]:
    """Find groups of entities that all moved by the same displacement vector.

    Entities whose only change is placement AND share the same displacement
    vector (within tolerance) are grouped into bulk movements.

    Returns (bulk_movements, remaining_changed).
    """
    # Separate placement-only changes from entities with other changes
    placement_only = []  # (entity, displacement_vector)
    other_changed = []

    for entity in changed:
        changes = entity["changes"]
        placement_changes = [c for c in changes if c["field"] == "placement"]
        non_placement = [c for c in changes if c["field"] != "placement"]

        if len(placement_changes) == 1 and not non_placement:
            old_m = placement_changes[0]["old"]
            new_m = placement_changes[0]["new"]
            if old_m and new_m:
                dx = new_m[0][3] - old_m[0][3]
                dy = new_m[1][3] - old_m[1][3]
                dz = new_m[2][3] - old_m[2][3]
                placement_only.append((entity, (dx, dy, dz)))
            else:
                other_changed.append(entity)
        else:
            other_changed.append(entity)

    if not placement_only:
        return [], changed

    # Group by quantized displacement vector (round to 1mm — sub-mm is noise)
    groups: dict[tuple, list[dict]] = {}
    for entity, (dx, dy, dz) in placement_only:
        key = (round(dx), round(dy), round(dz))
        groups.setdefault(key, []).append(entity)

    bulk_movements = []
    for (dx, dy, dz), entities in groups.items():
        if len(entities) >= _BULK_THRESHOLD:
            class_counts = Counter(e["ifc_class"] for e in entities)
            # Find common groups across all entities in this bulk movement
            entity_guids = [e["guid"] for e in entities]
            common_groups = _find_common_groups(entity_guids, new_entities)
            bm_entry = {
                "displacement": [dx, dy, dz],
                "count": len(entities),
                "class_breakdown": dict(class_counts.most_common()),
                "entities": [
                    {"guid": e["guid"], "ifc_class": e["ifc_class"], "name": e["name"]}
                    for e in entities
                ],
            }
            if common_groups:
                bm_entry["groups"] = common_groups
            bulk_movements.append(bm_entry)
        else:
            # Below threshold — keep as individual changes
            other_changed.extend(entities)

    # Sort remaining by guid for determinism
    other_changed.sort(key=lambda e: e["guid"])

    return bulk_movements, other_changed


def _find_common_groups(guids: list[str], entities: dict) -> list[str]:
    """Find group names shared by all entities in the list."""
    group_sets = []
    for guid in guids:
        ent = entities.get(guid, {})
        group_sets.append(set(ent.get("groups", [])))
    if not group_sets:
        return []
    common = group_sets[0]
    for gs in group_sets[1:]:
        common &= gs
    return sorted(common)


def _summary(guid: str, entity: dict) -> dict:
    return {
        "guid": guid,
        "ifc_class": entity["ifc_class"],
        "name": entity["name"],
    }


def _diff_entity(old: dict, new: dict,
                 all_entities: dict | None = None) -> list[dict]:
    """Compare two entity dicts and return a list of changes."""
    changes = []

    # Compare simple fields
    for field in ("ifc_class", "name", "container", "type_name"):
        if not _values_equal(old.get(field), new.get(field)):
            changes.append({
                "field": field,
                "old": old.get(field),
                "new": new.get(field),
            })

    # Compare placement
    if not _values_equal(old.get("placement"), new.get("placement")):
        entry = {
            "field": "placement",
            "old": old.get("placement"),
            "new": new.get("placement"),
        }
        changes.append(entry)

    # Compare attributes
    attr_changes = _diff_dict(old.get("attributes", {}), new.get("attributes", {}))
    for key, (old_val, new_val) in attr_changes.items():
        changes.append({
            "field": f"attribute.{key}",
            "old": old_val,
            "new": new_val,
        })

    # Compare property sets
    old_psets = old.get("property_sets", {})
    new_psets = new.get("property_sets", {})

    for pset_name in sorted(set(old_psets.keys()) | set(new_psets.keys())):
        if pset_name not in old_psets:
            changes.append({
                "field": f"pset.{pset_name}",
                "old": None,
                "new": new_psets[pset_name],
            })
        elif pset_name not in new_psets:
            changes.append({
                "field": f"pset.{pset_name}",
                "old": old_psets[pset_name],
                "new": None,
            })
        else:
            prop_changes = _diff_dict(old_psets[pset_name], new_psets[pset_name])
            for prop, (old_val, new_val) in prop_changes.items():
                changes.append({
                    "field": f"pset.{pset_name}.{prop}",
                    "old": old_val,
                    "new": new_val,
                })

    return changes


def _diff_dict(old: dict, new: dict) -> dict[str, tuple]:
    """Compare two flat dicts, return {key: (old_val, new_val)} for differences."""
    changes = {}
    for key in sorted(set(old.keys()) | set(new.keys())):
        old_val = old.get(key)
        new_val = new.get(key)
        if not _values_equal(old_val, new_val):
            changes[key] = (old_val, new_val)
    return changes


def _values_equal(a, b) -> bool:
    """Compare values with tolerance for floats."""
    if a is b:
        return True
    if type(a) != type(b):
        # Allow int/float cross-comparison
        if isinstance(a, (int, float)) and isinstance(b, (int, float)):
            return math.isclose(float(a), float(b), rel_tol=_RTOL, abs_tol=_ATOL)
        return a == b
    if isinstance(a, float):
        return math.isclose(a, b, rel_tol=_RTOL, abs_tol=_ATOL)
    if isinstance(a, list):
        if len(a) != len(b):
            return False
        return all(_values_equal(x, y) for x, y in zip(a, b))
    if isinstance(a, dict):
        if a.keys() != b.keys():
            return False
        return all(_values_equal(a[k], b[k]) for k in a)
    return a == b
