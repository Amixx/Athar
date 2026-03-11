"""Change/snapshot diff helpers for graph diff output."""

from __future__ import annotations

from typing import Any

from .diff_engine_context import entity_for_profile
from .diff_engine_markers import summarize_rooted_owners


def snapshot(entity: dict | None, *, profile: str) -> dict | None:
    if entity is None:
        return None
    normalized = entity_for_profile(entity, profile=profile)
    return {
        "entity_type": normalized.get("entity_type"),
        "attributes": normalized.get("attributes"),
        "refs": normalized.get("refs"),
    }


def make_change(
    change_id: int,
    *,
    op: str,
    old_entity_id: str | None,
    new_entity_id: str | None,
    old_ent: dict | None,
    new_ent: dict | None,
    identity: dict[str, Any],
    rooted_owners: dict[str, Any],
    profile: str,
) -> dict[str, Any]:
    field_ops = []
    if op == "MODIFY":
        field_ops = diff_values(
            snapshot(old_ent, profile=profile),
            snapshot(new_ent, profile=profile),
            path="",
        )
    return {
        "change_id": f"chg-{change_id:06d}",
        "op": op,
        "old_entity_id": old_entity_id,
        "new_entity_id": new_entity_id,
        "identity": {
            "stability_tier": "G" if (old_entity_id or new_entity_id or "").startswith("G:") else "H",
            "match_method": identity.get("match_method", "exact_hash"),
            "match_confidence": float(identity.get("match_confidence", 1.0)),
            "matched_on": identity.get("matched_on"),
        },
        "field_ops": field_ops,
        "old_snapshot": snapshot(old_ent, profile=profile) if op == "REMOVE" else None,
        "new_snapshot": snapshot(new_ent, profile=profile) if op == "ADD" else None,
        "rooted_owners": rooted_owners,
        "change_categories": [],
        "equivalence_class": None,
    }


def make_class_delta_change(
    change_id: int,
    *,
    entity_id: str,
    old_items: list[dict],
    new_items: list[dict],
    owner_ids: set[str],
    profile: str,
) -> dict[str, Any]:
    exemplar_entity = (new_items[0]["entity"] if new_items else old_items[0]["entity"])
    class_id = f"C:{entity_id[2:]}"
    return {
        "change_id": f"chg-{change_id:06d}",
        "op": "CLASS_DELTA",
        "old_entity_id": entity_id,
        "new_entity_id": entity_id,
        "identity": {
            "stability_tier": "C",
            "match_method": "equivalence_class",
            "match_confidence": 1.0,
            "matched_on": None,
        },
        "field_ops": [],
        "old_snapshot": None,
        "new_snapshot": None,
        "rooted_owners": summarize_rooted_owners(owner_ids),
        "change_categories": [],
        "equivalence_class": {
            "id": class_id,
            "old_count": len(old_items),
            "new_count": len(new_items),
            "exemplar": snapshot(exemplar_entity, profile=profile),
        },
    }


def diff_values(old: Any, new: Any, *, path: str) -> list[dict[str, Any]]:
    if old == new:
        return []

    if type(old) is not type(new):
        return [{
            "path": norm_path(path),
            "op": "replace",
            "old": old,
            "new": new,
        }]

    if isinstance(old, dict):
        ops: list[dict[str, Any]] = []
        old_keys = set(old)
        new_keys = set(new)
        for key in sorted(old_keys | new_keys):
            child_path = f"{path}/{key}"
            if key not in old:
                ops.append({
                    "path": norm_path(child_path),
                    "op": "add",
                    "old": None,
                    "new": new[key],
                })
            elif key not in new:
                ops.append({
                    "path": norm_path(child_path),
                    "op": "remove",
                    "old": old[key],
                    "new": None,
                })
            else:
                ops.extend(diff_values(old[key], new[key], path=child_path))
        return ops

    if isinstance(old, list):
        ops: list[dict[str, Any]] = []
        common = min(len(old), len(new))
        for idx in range(common):
            ops.extend(diff_values(old[idx], new[idx], path=f"{path}/{idx}"))
        for idx in range(common, len(old)):
            ops.append({
                "path": norm_path(f"{path}/{idx}"),
                "op": "remove",
                "old": old[idx],
                "new": None,
            })
        for idx in range(common, len(new)):
            ops.append({
                "path": norm_path(f"{path}/{idx}"),
                "op": "add",
                "old": None,
                "new": new[idx],
            })
        return ops

    return [{
        "path": norm_path(path),
        "op": "replace",
        "old": old,
        "new": new,
    }]


def norm_path(path: str) -> str:
    return path or "/"
