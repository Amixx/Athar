"""Core diff engine skeleton built on graph parsing and canonical IDs."""

from __future__ import annotations

from typing import Any

from .canonical_ids import structural_hash, wl_refine_colors
from .graph_parser import parse_graph
from .root_remap import plan_root_remap


def diff_files(old_path: str, new_path: str, *, profile: str = "semantic_stable") -> dict:
    old_graph = parse_graph(old_path, profile=profile)
    new_graph = parse_graph(new_path, profile=profile)
    return diff_graphs(old_graph, new_graph, profile=profile)


def diff_graphs(old_graph: dict, new_graph: dict, *, profile: str = "semantic_stable") -> dict:
    _validate_schema(old_graph, new_graph)

    remap = plan_root_remap(old_graph, new_graph)
    old_ids, old_methods = _assign_ids(old_graph, root_remap=remap["old_to_new"])
    new_ids, new_methods = _assign_ids(new_graph)

    old_by_id = _index_by_identity(old_graph, old_ids, old_methods)
    new_by_id = _index_by_identity(new_graph, new_ids, new_methods)

    base_changes = []
    change_id = 0

    for entity_id in sorted(set(old_by_id) | set(new_by_id)):
        old_item = old_by_id.get(entity_id)
        new_item = new_by_id.get(entity_id)
        old_ent = old_item["entity"] if old_item else None
        new_ent = new_item["entity"] if new_item else None
        if old_item is None:
            change_id += 1
            base_changes.append(_make_change(
                change_id,
                op="ADD",
                old_entity_id=None,
                new_entity_id=entity_id,
                old_ent=None,
                new_ent=new_ent,
                match_method=_resolve_match_method(None, new_item),
            ))
            continue
        if new_item is None:
            change_id += 1
            base_changes.append(_make_change(
                change_id,
                op="REMOVE",
                old_entity_id=entity_id,
                new_entity_id=None,
                old_ent=old_ent,
                new_ent=None,
                match_method=_resolve_match_method(old_item, None),
            ))
            continue
        if not _entities_equal(old_ent, new_ent):
            change_id += 1
            base_changes.append(_make_change(
                change_id,
                op="MODIFY",
                old_entity_id=entity_id,
                new_entity_id=entity_id,
                old_ent=old_ent,
                new_ent=new_ent,
                match_method=_resolve_match_method(old_item, new_item),
            ))

    return {
        "version": "2",
        "profile": profile,
        "schema_policy": {
            "mode": "same_schema_only",
            "old_schema": old_graph["metadata"]["schema"],
            "new_schema": new_graph["metadata"]["schema"],
        },
        "stats": {
            "old_entities": len(old_graph.get("entities", {})),
            "new_entities": len(new_graph.get("entities", {})),
            "matched": len(set(old_by_id) & set(new_by_id)),
            "ambiguous": remap["ambiguous"],
        },
        "base_changes": base_changes,
        "derived_markers": [],
    }


def _validate_schema(old_graph: dict, new_graph: dict) -> None:
    old_schema = old_graph.get("metadata", {}).get("schema")
    new_schema = new_graph.get("metadata", {}).get("schema")
    if old_schema != new_schema:
        raise ValueError(f"Schema mismatch: {old_schema} vs {new_schema}")


def _assign_ids(
    graph: dict,
    *,
    root_remap: dict[str, str] | None = None,
) -> tuple[dict[int, str], dict[int, str]]:
    entities = graph.get("entities", {})
    colors = wl_refine_colors(graph)
    guid_index = _guid_index(entities)
    ids: dict[int, str] = {}
    methods: dict[int, str] = {}
    for step_id, entity in entities.items():
        gid = entity.get("global_id")
        if gid and guid_index.get(gid, 0) == 1:
            mapped_gid = (root_remap or {}).get(gid, gid)
            ids[step_id] = f"G:{mapped_gid}"
            methods[step_id] = "root_remap" if mapped_gid != gid else "exact_guid"
        else:
            ids[step_id] = f"H:{colors.get(step_id) or structural_hash(entity)}"
            methods[step_id] = "exact_hash"
    return ids, methods


def _guid_index(entities: dict[int, dict]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for entity in entities.values():
        gid = entity.get("global_id")
        if gid:
            counts[gid] = counts.get(gid, 0) + 1
    return counts


def _index_by_identity(
    graph: dict,
    ids: dict[int, str],
    methods: dict[int, str],
) -> dict[str, dict]:
    by_id: dict[str, dict] = {}
    for step_id, entity in graph.get("entities", {}).items():
        by_id[ids[step_id]] = {
            "entity": entity,
            "match_method": methods.get(step_id, "exact_hash"),
        }
    return by_id


def _resolve_match_method(old_item: dict | None, new_item: dict | None) -> str:
    old_method = (old_item or {}).get("match_method")
    new_method = (new_item or {}).get("match_method")
    if old_method == "root_remap" or new_method == "root_remap":
        return "root_remap"
    if old_method and old_method == new_method:
        return old_method
    return old_method or new_method or "exact_hash"


def _entities_equal(old_ent: dict, new_ent: dict) -> bool:
    return (
        old_ent.get("entity_type") == new_ent.get("entity_type")
        and old_ent.get("attributes") == new_ent.get("attributes")
        and old_ent.get("refs") == new_ent.get("refs")
    )


def _snapshot(entity: dict | None) -> dict | None:
    if entity is None:
        return None
    return {
        "entity_type": entity.get("entity_type"),
        "attributes": entity.get("attributes"),
        "refs": entity.get("refs"),
    }


def _make_change(
    change_id: int,
    *,
    op: str,
    old_entity_id: str | None,
    new_entity_id: str | None,
    old_ent: dict | None,
    new_ent: dict | None,
    match_method: str,
) -> dict[str, Any]:
    field_ops = []
    if op == "MODIFY":
        field_ops = [{
            "path": "/",
            "op": "replace",
            "old": _snapshot(old_ent),
            "new": _snapshot(new_ent),
        }]
    return {
        "change_id": f"chg-{change_id:06d}",
        "op": op,
        "old_entity_id": old_entity_id,
        "new_entity_id": new_entity_id,
        "identity": {
            "stability_tier": "G" if (old_entity_id or new_entity_id or "").startswith("G:") else "H",
            "match_method": match_method,
            "match_confidence": 1.0,
        },
        "field_ops": field_ops,
        "old_snapshot": _snapshot(old_ent) if op == "REMOVE" else None,
        "new_snapshot": _snapshot(new_ent) if op == "ADD" else None,
        "rooted_owners": {"sample": [], "total": 0},
        "change_categories": [],
        "equivalence_class": None,
    }
