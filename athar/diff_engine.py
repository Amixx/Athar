"""Core diff engine skeleton built on graph parsing and canonical IDs."""

from __future__ import annotations

from typing import Any

from .canonical_ids import structural_hash, wl_refine_colors
from .graph_parser import parse_graph
from .matcher_graph import propagate_matches_by_typed_path, secondary_match_unresolved
from .root_remap import plan_root_remap

_REPARENT_RELATION_TYPES = frozenset({
    "IfcRelContainedInSpatialStructure",
    "IfcRelAggregates",
    "IfcRelNests",
})


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
        if _should_emit_class_delta(entity_id, old_items, new_items):
            change_id += 1
            base_changes.append(_make_class_delta_change(
                change_id,
                entity_id=entity_id,
                old_items=old_items,
                new_items=new_items,
            ))
            continue

        paired = min(len(old_items), len(new_items))

        for i in range(paired):
            old_item = old_items[i]
            new_item = new_items[i]
            old_ent = old_item["entity"]
            new_ent = new_item["entity"]
            if _entities_equal(entity_id, old_ent, new_ent):
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

    derived_markers = _build_derived_markers(
        old_graph=old_graph,
        new_graph=new_graph,
        old_ids=old_ids,
        new_ids=new_ids,
        base_changes=base_changes,
    )

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
        "derived_markers": derived_markers,
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


def _should_emit_class_delta(
    entity_id: str,
    old_items: list[dict],
    new_items: list[dict],
) -> bool:
    if not entity_id.startswith("H:"):
        return False
    if not old_items or not new_items:
        return False
    if len(old_items) == len(new_items):
        return False

    # Conservative gating: class delta is only used for exact-hash buckets
    # that were not overridden by propagation/secondary matching.
    all_methods = [item.get("match_method") for item in old_items + new_items]
    return all(method == "exact_hash" for method in all_methods)


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


def _entities_equal(entity_id: str, old_ent: dict, new_ent: dict) -> bool:
    # H identities are STEP-ID independent signatures; compare the same way
    # to avoid false MODIFYs from pure renumbering.
    if entity_id.startswith("H:"):
        return structural_hash(old_ent) == structural_hash(new_ent)
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


def _make_class_delta_change(
    change_id: int,
    *,
    entity_id: str,
    old_items: list[dict],
    new_items: list[dict],
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
        },
        "field_ops": [],
        "old_snapshot": None,
        "new_snapshot": None,
        "rooted_owners": {"sample": [], "total": 0},
        "change_categories": [],
        "equivalence_class": {
            "id": class_id,
            "old_count": len(old_items),
            "new_count": len(new_items),
            "exemplar": _snapshot(exemplar_entity),
        },
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


def _build_derived_markers(
    *,
    old_graph: dict,
    new_graph: dict,
    old_ids: dict[int, str],
    new_ids: dict[int, str],
    base_changes: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    old_links = _extract_parent_links(old_graph, old_ids)
    new_links = _extract_parent_links(new_graph, new_ids)
    change_index = _build_change_index(base_changes)

    markers: list[dict[str, Any]] = []
    for key in sorted(set(old_links) & set(new_links)):
        relation_type, child_id = key
        old_link = old_links[key]
        new_link = new_links[key]
        old_parent_id = old_link["parent_id"]
        new_parent_id = new_link["parent_id"]
        if old_parent_id == new_parent_id:
            continue

        source_change_ids = sorted(set(
            change_index.get(old_link["relation_id"], [])
            + change_index.get(new_link["relation_id"], [])
            + change_index.get(child_id, [])
            + change_index.get(old_parent_id, [])
            + change_index.get(new_parent_id, [])
        ))
        markers.append({
            "marker_type": "REPARENT",
            "relation_type": relation_type,
            "child_id": child_id,
            "old_parent_id": old_parent_id,
            "new_parent_id": new_parent_id,
            "source_change_ids": source_change_ids,
        })
    return markers


def _extract_parent_links(graph: dict, ids: dict[int, str]) -> dict[tuple[str, str], dict[str, str]]:
    links: dict[tuple[str, str], dict[str, str]] = {}
    ambiguous: set[tuple[str, str]] = set()

    for step_id, entity in graph.get("entities", {}).items():
        relation_type = entity.get("entity_type")
        if relation_type not in _REPARENT_RELATION_TYPES:
            continue
        relation_id = ids.get(step_id)
        if relation_id is None:
            continue

        parent_steps = sorted({
            ref.get("target")
            for ref in entity.get("refs", [])
            if str(ref.get("path", "")).startswith("/Relating")
            and ref.get("target") is not None
        })
        child_steps = sorted({
            ref.get("target")
            for ref in entity.get("refs", [])
            if str(ref.get("path", "")).startswith("/Related")
            and ref.get("target") is not None
        })
        if len(parent_steps) != 1 or not child_steps:
            continue

        parent_id = ids.get(parent_steps[0])
        if parent_id is None:
            continue

        for child_step in child_steps:
            child_id = ids.get(child_step)
            if child_id is None:
                continue
            key = (relation_type, child_id)
            existing = links.get(key)
            if existing and existing["parent_id"] != parent_id:
                ambiguous.add(key)
                continue
            links[key] = {
                "parent_id": parent_id,
                "relation_id": relation_id,
            }

    for key in ambiguous:
        links.pop(key, None)
    return links


def _build_change_index(base_changes: list[dict[str, Any]]) -> dict[str, list[str]]:
    index: dict[str, list[str]] = {}
    for change in base_changes:
        change_id = change["change_id"]
        for entity_id in (change.get("old_entity_id"), change.get("new_entity_id")):
            if entity_id is None:
                continue
            index.setdefault(entity_id, []).append(change_id)
    return index
