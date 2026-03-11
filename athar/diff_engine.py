"""Core diff engine skeleton built on graph parsing and canonical IDs."""

from __future__ import annotations

from typing import Any

from .canonical_ids import structural_hash, wl_refine_colors
from .graph_parser import parse_graph
from .matcher_graph import propagate_matches_by_typed_path, secondary_match_unresolved
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
    root_pairs = _match_root_steps(old_graph, new_graph, remap["old_to_new"])
    exact_pairs = _match_steps_by_unique_id(old_ids, new_ids)
    pre_old = set(root_pairs) | set(exact_pairs)
    pre_new = set(root_pairs.values()) | set(exact_pairs.values())
    path_propagation = propagate_matches_by_typed_path(
        old_graph,
        new_graph,
        root_pairs,
        pre_matched_old=pre_old,
        pre_matched_new=pre_new,
    )
    _apply_step_matches(
        old_ids,
        old_methods,
        new_ids,
        path_propagation["old_to_new"],
        method="path_propagation",
    )
    matched_after_path = _match_steps_by_unique_id(old_ids, new_ids)
    secondary = secondary_match_unresolved(
        old_graph,
        new_graph,
        pre_matched_old=set(matched_after_path),
        pre_matched_new=set(matched_after_path.values()),
    )
    _apply_step_matches(
        old_ids,
        old_methods,
        new_ids,
        secondary["old_to_new"],
        method="secondary_match",
    )

    old_by_id = _index_by_identity(old_graph, old_ids, old_methods)
    new_by_id = _index_by_identity(new_graph, new_ids, new_methods)

    base_changes = []
    change_id = 0

    for entity_id in sorted(set(old_by_id) | set(new_by_id)):
        old_items = sorted(old_by_id.get(entity_id, []), key=lambda item: item["step_id"])
        new_items = sorted(new_by_id.get(entity_id, []), key=lambda item: item["step_id"])
        paired = min(len(old_items), len(new_items))

        for i in range(paired):
            old_item = old_items[i]
            new_item = new_items[i]
            old_ent = old_item["entity"]
            new_ent = new_item["entity"]
            if _entities_equal(old_ent, new_ent):
                continue
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

        for old_item in old_items[paired:]:
            old_ent = old_item["entity"]
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

        for new_item in new_items[paired:]:
            new_ent = new_item["entity"]
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
            "matched": _matched_occurrence_count(old_by_id, new_by_id),
            "ambiguous": remap["ambiguous"] + path_propagation["ambiguous"] + secondary["ambiguous"],
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
) -> dict[str, list[dict]]:
    by_id: dict[str, list[dict]] = {}
    for step_id, entity in graph.get("entities", {}).items():
        by_id.setdefault(ids[step_id], []).append({
            "step_id": step_id,
            "entity": entity,
            "match_method": methods.get(step_id, "exact_hash"),
        })
    return by_id


def _matched_occurrence_count(old_by_id: dict[str, list[dict]], new_by_id: dict[str, list[dict]]) -> int:
    total = 0
    for entity_id in set(old_by_id) & set(new_by_id):
        total += min(len(old_by_id[entity_id]), len(new_by_id[entity_id]))
    return total


def _resolve_match_method(old_item: dict | None, new_item: dict | None) -> str:
    old_method = (old_item or {}).get("match_method")
    new_method = (new_item or {}).get("match_method")
    if old_method == "root_remap" or new_method == "root_remap":
        return "root_remap"
    if old_method == "path_propagation" or new_method == "path_propagation":
        return "path_propagation"
    if old_method == "secondary_match" or new_method == "secondary_match":
        return "secondary_match"
    if old_method and old_method == new_method:
        return old_method
    return old_method or new_method or "exact_hash"


def _match_root_steps(
    old_graph: dict,
    new_graph: dict,
    root_remap: dict[str, str],
) -> dict[int, int]:
    old_roots = _unique_guid_step_index(old_graph.get("entities", {}))
    new_roots = _unique_guid_step_index(new_graph.get("entities", {}))
    pairs: dict[int, int] = {}
    for old_gid, old_step in sorted(old_roots.items()):
        mapped_gid = root_remap.get(old_gid, old_gid)
        new_step = new_roots.get(mapped_gid)
        if new_step is not None:
            pairs[old_step] = new_step
    return pairs


def _unique_guid_step_index(entities: dict[int, dict]) -> dict[str, int]:
    counts = _guid_index(entities)
    out: dict[str, int] = {}
    for step_id, entity in entities.items():
        gid = entity.get("global_id")
        if gid and counts.get(gid, 0) == 1:
            out[gid] = step_id
    return out


def _match_steps_by_unique_id(old_ids: dict[int, str], new_ids: dict[int, str]) -> dict[int, int]:
    old_by_id: dict[str, list[int]] = {}
    new_by_id: dict[str, list[int]] = {}
    for step_id, entity_id in old_ids.items():
        old_by_id.setdefault(entity_id, []).append(step_id)
    for step_id, entity_id in new_ids.items():
        new_by_id.setdefault(entity_id, []).append(step_id)

    pairs: dict[int, int] = {}
    for entity_id in sorted(set(old_by_id) & set(new_by_id)):
        old_steps = old_by_id[entity_id]
        new_steps = new_by_id[entity_id]
        if len(old_steps) == 1 and len(new_steps) == 1:
            pairs[old_steps[0]] = new_steps[0]
    return pairs


def _apply_step_matches(
    old_ids: dict[int, str],
    old_methods: dict[int, str],
    new_ids: dict[int, str],
    step_matches: dict[int, int],
    *,
    method: str,
) -> None:
    for old_step, new_step in step_matches.items():
        new_entity_id = new_ids.get(new_step)
        if new_entity_id is None:
            continue
        old_ids[old_step] = new_entity_id
        old_methods[old_step] = method


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
        field_ops = _diff_values(_snapshot(old_ent), _snapshot(new_ent), path="")
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


def _diff_values(old: Any, new: Any, *, path: str) -> list[dict[str, Any]]:
    if old == new:
        return []

    # Structural mismatch -> single replace.
    if type(old) is not type(new):
        return [{
            "path": _norm_path(path),
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
                    "path": _norm_path(child_path),
                    "op": "add",
                    "old": None,
                    "new": new[key],
                })
            elif key not in new:
                ops.append({
                    "path": _norm_path(child_path),
                    "op": "remove",
                    "old": old[key],
                    "new": None,
                })
            else:
                ops.extend(_diff_values(old[key], new[key], path=child_path))
        return ops

    if isinstance(old, list):
        ops: list[dict[str, Any]] = []
        common = min(len(old), len(new))
        for idx in range(common):
            ops.extend(_diff_values(old[idx], new[idx], path=f"{path}/{idx}"))
        for idx in range(common, len(old)):
            ops.append({
                "path": _norm_path(f"{path}/{idx}"),
                "op": "remove",
                "old": old[idx],
                "new": None,
            })
        for idx in range(common, len(new)):
            ops.append({
                "path": _norm_path(f"{path}/{idx}"),
                "op": "add",
                "old": None,
                "new": new[idx],
            })
        return ops

    return [{
        "path": _norm_path(path),
        "op": "replace",
        "old": old,
        "new": new,
    }]


def _norm_path(path: str) -> str:
    return path or "/"
