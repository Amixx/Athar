"""Context building and identity/matching helpers for graph diff."""

from __future__ import annotations

from typing import Any

from .canonical_ids import structural_hash, wl_refine_with_scc_fallback
from .equivalence_classes import apply_ambiguous_equivalence_classes
from .graph_parser import count_dangling_refs
from .guid_policy import (
    GUID_POLICY_FAIL_FAST,
    enforce_or_disambiguate_guid_policy,
    validate_guid_policy,
)
from .matcher_graph import propagate_matches_by_typed_path, secondary_match_unresolved
from .matcher_policy import resolve_matcher_policy
from .root_remap import plan_root_remap
from .diff_engine_markers import RootedOwnerProjector
from .diff_engine_stats import build_stats
from .profile_policy import entity_for_profile, validate_profile


def prepare_diff_context(
    old_graph: dict,
    new_graph: dict,
    *,
    profile: str,
    guid_policy: str = GUID_POLICY_FAIL_FAST,
    matcher_policy: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    validate_profile(profile)
    validate_guid_policy(guid_policy)
    _validate_schema(old_graph, new_graph)

    resolved_matcher_policy = resolve_matcher_policy(matcher_policy)

    remap = plan_root_remap(
        old_graph,
        new_graph,
        **resolved_matcher_policy["root_remap"],
    )
    old_ids, old_identity = _assign_ids(
        old_graph,
        profile=profile,
        guid_policy=guid_policy,
        root_remap=remap["old_to_new"],
        root_remap_diagnostics=remap.get("diagnostics", {}),
        side="old",
    )
    new_ids, new_identity = _assign_ids(
        new_graph,
        profile=profile,
        guid_policy=guid_policy,
        side="new",
    )
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
        old_identity,
        new_ids,
        path_propagation["old_to_new"],
        method="path_propagation",
        diagnostics=path_propagation.get("diagnostics", {}),
    )
    matched_after_path = _match_steps_by_unique_id(old_ids, new_ids)
    secondary = secondary_match_unresolved(
        old_graph,
        new_graph,
        pre_matched_old=set(matched_after_path),
        pre_matched_new=set(matched_after_path.values()),
        **resolved_matcher_policy["secondary_match"],
    )
    _apply_step_matches(
        old_ids,
        old_identity,
        new_ids,
        secondary["old_to_new"],
        method="secondary_match",
        diagnostics=secondary.get("diagnostics", {}),
    )
    apply_ambiguous_equivalence_classes(
        old_ids=old_ids,
        old_identity=old_identity,
        new_ids=new_ids,
        new_identity=new_identity,
        partitions=secondary.get("ambiguous_partitions", []),
    )

    old_by_id = _index_by_identity(old_graph, old_ids, old_identity)
    new_by_id = _index_by_identity(new_graph, new_ids, new_identity)
    old_owner_projector = RootedOwnerProjector(old_graph, old_ids)
    new_owner_projector = RootedOwnerProjector(new_graph, new_ids)

    return {
        "version": "2",
        "profile": profile,
        "old_graph": old_graph,
        "new_graph": new_graph,
        "old_ids": old_ids,
        "new_ids": new_ids,
        "old_by_id": old_by_id,
        "new_by_id": new_by_id,
        "old_owner_projector": old_owner_projector,
        "new_owner_projector": new_owner_projector,
        "schema_policy": {
            "mode": "same_schema_only",
            "old_schema": old_graph["metadata"]["schema"],
            "new_schema": new_graph["metadata"]["schema"],
        },
        "identity_policy": {
            "guid_policy": guid_policy,
            "matcher_policy": resolved_matcher_policy,
        },
        "stats": build_stats(
            old_graph=old_graph,
            new_graph=new_graph,
            old_by_id=old_by_id,
            new_by_id=new_by_id,
            remap_ambiguous=remap["ambiguous"],
            path_ambiguous=path_propagation["ambiguous"],
            secondary_ambiguous=secondary["ambiguous"],
            remap_matches=len(remap["old_to_new"]),
            path_matches=len(path_propagation["old_to_new"]),
            secondary_matches=len(secondary["old_to_new"]),
            old_dangling_refs=_dangling_ref_count(old_graph),
            new_dangling_refs=_dangling_ref_count(new_graph),
        ),
    }


def build_result(
    context: dict[str, Any],
    *,
    base_changes: list[dict[str, Any]],
    derived_markers: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        **result_header(context),
        "base_changes": base_changes,
        "derived_markers": derived_markers,
    }


def result_header(context: dict[str, Any]) -> dict[str, Any]:
    return {
        "version": context["version"],
        "profile": context["profile"],
        "schema_policy": context["schema_policy"],
        "identity_policy": context.get("identity_policy"),
        "stats": context["stats"],
    }


def index_change(change_index: dict[str, list[str]], change: dict[str, Any]) -> None:
    change_id = change["change_id"]
    for entity_id in (change.get("old_entity_id"), change.get("new_entity_id")):
        if entity_id is None:
            continue
        change_index.setdefault(entity_id, []).append(change_id)


def entities_equal(entity_id: str, old_ent: dict, new_ent: dict, *, profile: str) -> bool:
    old_norm = entity_for_profile(old_ent, profile=profile)
    new_norm = entity_for_profile(new_ent, profile=profile)
    if entity_id.startswith("H:"):
        return structural_hash(old_norm) == structural_hash(new_norm)
    return (
        old_norm.get("entity_type") == new_norm.get("entity_type")
        and old_norm.get("attributes") == new_norm.get("attributes")
        and old_norm.get("refs") == new_norm.get("refs")
    )


def should_emit_class_delta(
    entity_id: str,
    old_items: list[dict],
    new_items: list[dict],
) -> bool:
    if not old_items or not new_items:
        return False
    if len(old_items) == len(new_items):
        return False

    if entity_id.startswith("C:"):
        return True
    if not entity_id.startswith("H:"):
        return False

    all_methods = [
        item.get("identity", {}).get("match_method")
        for item in old_items + new_items
    ]
    return all(method == "exact_hash" for method in all_methods)


def resolve_identity(old_item: dict | None, new_item: dict | None) -> dict[str, Any]:
    old_identity = (old_item or {}).get("identity")
    new_identity = (new_item or {}).get("identity")
    if old_identity is None and new_identity is None:
        return {"match_method": "exact_hash", "match_confidence": 1.0, "matched_on": None}
    if old_identity is None:
        return dict(new_identity)
    if new_identity is None:
        return dict(old_identity)

    old_method = old_identity.get("match_method", "exact_hash")
    new_method = new_identity.get("match_method", "exact_hash")
    old_priority = _identity_priority(old_method)
    new_priority = _identity_priority(new_method)
    if old_priority > new_priority:
        return dict(old_identity)
    if new_priority > old_priority:
        return dict(new_identity)

    if old_identity.get("match_confidence", 0.0) >= new_identity.get("match_confidence", 0.0):
        return dict(old_identity)
    return dict(new_identity)


def _validate_schema(old_graph: dict, new_graph: dict) -> None:
    old_schema = old_graph.get("metadata", {}).get("schema")
    new_schema = new_graph.get("metadata", {}).get("schema")
    if old_schema != new_schema:
        raise ValueError(f"Schema mismatch: {old_schema} vs {new_schema}")


def _assign_ids(
    graph: dict,
    *,
    profile: str,
    guid_policy: str,
    root_remap: dict[str, str] | None = None,
    root_remap_diagnostics: dict[str, dict[str, Any]] | None = None,
    side: str,
) -> tuple[dict[int, str], dict[int, dict[str, Any]]]:
    entities = graph.get("entities", {})
    id_graph = _graph_for_profile(graph, profile=profile)
    id_entities = id_graph.get("entities", {})
    colors, scc_classes = wl_refine_with_scc_fallback(id_graph)
    guid_policy_out = enforce_or_disambiguate_guid_policy(
        entities,
        policy=guid_policy,
        side=side,
    )
    guid_index = _guid_index(entities)
    disambiguated = guid_policy_out["disambiguated"]
    ids: dict[int, str] = {}
    identity: dict[int, dict[str, Any]] = {}
    for step_id, entity in entities.items():
        disambiguated_guid = disambiguated.get(step_id)
        if disambiguated_guid:
            ids[step_id] = disambiguated_guid["entity_id"]
            identity[step_id] = {
                "match_method": "guid_disambiguated",
                "match_confidence": 0.0,
                "matched_on": {
                    "stage": "guid_disambiguation",
                    "reason": disambiguated_guid["reason"],
                    "guid": disambiguated_guid["guid"],
                    "ordinal": disambiguated_guid["ordinal"],
                },
            }
            continue
        gid = entity.get("global_id")
        if gid and guid_index.get(gid, 0) == 1:
            mapped_gid = (root_remap or {}).get(gid, gid)
            remap_diag = (root_remap_diagnostics or {}).get(gid, {})
            ids[step_id] = f"G:{mapped_gid}"
            identity[step_id] = {
                "match_method": "root_remap" if mapped_gid != gid else "exact_guid",
                "match_confidence": 1.0,
                "matched_on": (
                    {
                        "stage": "root_remap",
                        "from": gid,
                        "to": mapped_gid,
                        **({"remap_stage": remap_diag.get("stage")} if remap_diag.get("stage") else {}),
                    }
                    if mapped_gid != gid
                    else {"stage": "guid", "guid": gid}
                ),
            }
        else:
            hash_entity = id_entities.get(step_id, entity)
            class_id = scc_classes.get(step_id)
            if class_id:
                ids[step_id] = class_id
                identity[step_id] = {
                    "match_method": "equivalence_class",
                    "match_confidence": 0.0,
                    "matched_on": {"stage": "scc_ambiguity"},
                }
            else:
                ids[step_id] = f"H:{colors.get(step_id) or structural_hash(hash_entity)}"
                identity[step_id] = {
                    "match_method": "exact_hash",
                    "match_confidence": 1.0,
                    "matched_on": {"stage": "structural_hash"},
                }
    return ids, identity


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
    identity: dict[int, dict[str, Any]],
) -> dict[str, list[dict]]:
    by_id: dict[str, list[dict]] = {}
    for step_id, entity in graph.get("entities", {}).items():
        by_id.setdefault(ids[step_id], []).append({
            "step_id": step_id,
            "entity": entity,
            "identity": identity.get(step_id, {
                "match_method": "exact_hash",
                "match_confidence": 1.0,
                "matched_on": None,
            }),
        })
    return by_id


def _identity_priority(match_method: str) -> int:
    order = {
        "root_remap": 5,
        "path_propagation": 4,
        "secondary_match": 3,
        "exact_guid": 2,
        "guid_disambiguated": 2,
        "exact_hash": 1,
        "equivalence_class": 0,
    }
    return order.get(match_method, 0)


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
    old_identity: dict[int, dict[str, Any]],
    new_ids: dict[int, str],
    step_matches: dict[int, int],
    *,
    method: str,
    diagnostics: dict[int, dict[str, Any]] | None = None,
) -> None:
    for old_step, new_step in step_matches.items():
        new_entity_id = new_ids.get(new_step)
        if new_entity_id is None:
            continue
        old_ids[old_step] = new_entity_id
        diag = (diagnostics or {}).get(old_step, {})
        old_identity[old_step] = {
            "match_method": method,
            "match_confidence": float(diag.get("match_confidence", 1.0)),
            "matched_on": diag.get("matched_on"),
        }


def _graph_for_profile(graph: dict, *, profile: str) -> dict:
    if profile != "semantic_stable":
        return graph
    entities = {
        step_id: entity_for_profile(entity, profile=profile)
        for step_id, entity in graph.get("entities", {}).items()
    }
    return {"entities": entities}


def _dangling_ref_count(graph: dict) -> int:
    meta_count = (
        graph.get("metadata", {})
        .get("diagnostics", {})
        .get("dangling_refs")
    )
    if isinstance(meta_count, int):
        return meta_count
    return count_dangling_refs(graph)
