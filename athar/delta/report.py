"""Per-aspect delta report assembly."""

from __future__ import annotations

from athar.bottom.types import PropertyEntry, SignatureBundle, SignatureVector
from athar.matcher.types import MatchedPair


def build_delta_report(
    old_bundle: SignatureBundle,
    new_bundle: SignatureBundle,
    matches: list[MatchedPair],
    unmatched_old: list[int],
    unmatched_new: list[int],
) -> dict:
    """Build a structured change report."""
    old_signatures = old_bundle.signatures
    new_signatures = new_bundle.signatures
    old_props = old_bundle.property_index
    new_props = new_bundle.property_index

    modified: list[dict] = []
    unchanged: list[dict] = []
    dropped_matches = 0

    for pair in matches:
        old_sig = old_signatures.get(pair.old_step)
        new_sig = new_signatures.get(pair.new_step)
        if old_sig is None or new_sig is None:
            dropped_matches += 1
            continue
        aspects = _aspect_diff(old_sig, new_sig)
        item = {
            "old": _entity_summary(old_sig),
            "new": _entity_summary(new_sig),
            "match": {"score": pair.score, "reason": pair.reason},
            "aspects": aspects,
            "data_hash": {
                "old": old_sig.vh_data,
                "new": new_sig.vh_data,
            },
            "change_scope": _change_scope(aspects),
        }
        if aspects["data"] == "changed":
            item["data_delta"] = _data_delta(old_sig, new_sig)
            if old_props is not None and new_props is not None:
                deltas = _property_deltas(
                    old_props.get(pair.old_step, []),
                    new_props.get(pair.new_step, []),
                )
                if deltas:
                    item["property_deltas"] = deltas
        if any(value == "changed" for key, value in aspects.items() if key != "placement_delta_mm"):
            modified.append(item)
        else:
            unchanged.append(item)

    added = [_entity_summary(new_signatures[step]) for step in unmatched_new if step in new_signatures]
    deleted = [_entity_summary(old_signatures[step]) for step in unmatched_old if step in old_signatures]

    modified.sort(key=lambda item: (item["new"]["class"], item["new"]["step_id"]))
    unchanged.sort(key=lambda item: (item["new"]["class"], item["new"]["step_id"]))
    added.sort(key=lambda item: (item["class"], item["step_id"]))
    deleted.sort(key=lambda item: (item["class"], item["step_id"]))

    return {
        "engine": "athar",
        "canon_version": old_bundle.canon_version,
        "schemas": {"old": old_bundle.schema, "new": new_bundle.schema},
        "stats": {
            "added": len(added),
            "deleted": len(deleted),
            "modified": len(modified),
            "unchanged": len(unchanged),
            "old_signatures": len(old_signatures),
            "new_signatures": len(new_signatures),
            "old_diagnostics": _diagnostic_summary(old_bundle),
            "new_diagnostics": _diagnostic_summary(new_bundle),
            "old_edge_stats": old_bundle.edge_stats,
            "new_edge_stats": new_bundle.edge_stats,
            "modified_change_scope": _scope_stats(modified),
            "dropped_matches": dropped_matches,
        },
        "added": added,
        "deleted": deleted,
        "modified": modified,
        "unchanged": unchanged,
    }


def _entity_summary(sig: SignatureVector) -> dict:
    return {
        "step_id": sig.step_id,
        "guid": sig.guid,
        "name": sig.name,
        "class": sig.canonical_class,
        "entity_type": sig.entity_type,
    }


def _aspect_diff(old_sig: SignatureVector, new_sig: SignatureVector) -> dict:
    placement_delta = _placement_delta_mm(old_sig.placement, new_sig.placement)
    return {
        "geometry": _changed(old_sig.vh_geometry, new_sig.vh_geometry),
        "data": _changed(old_sig.vh_data, new_sig.vh_data),
        "topology": _changed(old_sig.vh_topology, new_sig.vh_topology),
        "placement": _changed(old_sig.placement, new_sig.placement),
        "placement_delta_mm": placement_delta,
    }


def _changed(old, new) -> str:
    return "unchanged" if old == new else "changed"


def _data_delta(old_sig: SignatureVector, new_sig: SignatureVector) -> list[dict]:
    old_facts = dict(old_sig.data_facts)
    new_facts = dict(new_sig.data_facts)
    out: list[dict] = []
    for path in sorted(set(old_facts) | set(new_facts)):
        old_value = old_facts.get(path)
        new_value = new_facts.get(path)
        if old_value == new_value:
            continue
        out.append({"path": path, "old": old_value, "new": new_value})
    return out


def _placement_delta_mm(old: tuple[int, ...] | None, new: tuple[int, ...] | None) -> tuple[float, float, float] | None:
    if old is None or new is None:
        return None
    if len(old) < 12 or len(new) < 12:
        return None
    # Translation entries in row-major 4x4 matrix.
    dx_m = (new[3] - old[3]) / 1_000_000.0
    dy_m = (new[7] - old[7]) / 1_000_000.0
    dz_m = (new[11] - old[11]) / 1_000_000.0
    return (round(dx_m * 1000.0, 3), round(dy_m * 1000.0, 3), round(dz_m * 1000.0, 3))


def _diagnostic_summary(bundle: SignatureBundle) -> dict:
    return {
        "dangling_refs": bundle.diagnostics.dangling_refs,
        "cycle_breaks": bundle.diagnostics.cycle_breaks,
        "warnings": list(bundle.diagnostics.warnings),
    }


def _change_scope(aspects: dict) -> str:
    intrinsic_changed = any(
        aspects.get(name) == "changed"
        for name in ("geometry", "data", "placement")
    )
    transitive_changed = aspects.get("topology") == "changed"
    if intrinsic_changed and transitive_changed:
        return "mixed"
    if intrinsic_changed:
        return "intrinsic"
    if transitive_changed:
        return "transitive"
    return "none"


def _scope_stats(modified: list[dict]) -> dict[str, int]:
    out = {"intrinsic": 0, "transitive": 0, "mixed": 0}
    for item in modified:
        scope = item.get("change_scope")
        if scope in out:
            out[scope] += 1
    return out


def _property_deltas(
    old_entries: list[PropertyEntry],
    new_entries: list[PropertyEntry],
) -> dict | None:
    """Compare old and new property lists and return per-property deltas."""
    old_map: dict[str, str | int | float | bool | list[str]] = {}
    for entry in old_entries:
        old_map[entry.name] = entry.value

    new_map: dict[str, str | int | float | bool | list[str]] = {}
    for entry in new_entries:
        new_map[entry.name] = entry.value

    changed: list[dict] = []
    added: list[dict] = []
    removed: list[dict] = []

    for name, new_val in new_map.items():
        if name in old_map:
            old_val = old_map[name]
            if old_val != new_val:
                changed.append({"name": name, "old_value": old_val, "new_value": new_val})
        else:
            added.append({"name": name, "new_value": new_val})

    for name, old_val in old_map.items():
        if name not in new_map:
            removed.append({"name": name, "old_value": old_val})

    if not (changed or added or removed):
        return None

    result: dict[str, list[dict]] = {}
    if changed:
        result["changed"] = sorted(changed, key=lambda x: x["name"])
    if added:
        result["added"] = sorted(added, key=lambda x: x["name"])
    if removed:
        result["removed"] = sorted(removed, key=lambda x: x["name"])

    return result
