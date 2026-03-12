"""Identity assignment and step-matching internals for diff context prep."""

from __future__ import annotations

from typing import Any

from .structural_hash import structural_hash
from .wl_refinement import wl_refine_with_scc_fallback
from .guid_policy import enforce_or_disambiguate_guid_policy
from .profile_policy import entity_for_profile
from .types import GraphIR, IdentityInfo


def _precompute_identity_state(graph: GraphIR, *, profile: str) -> dict[str, Any]:
    entities = graph.get("entities", {})
    id_graph = _graph_for_profile(graph, profile=profile)
    id_entities = id_graph.get("entities", {})
    colors, scc_classes = wl_refine_with_scc_fallback(id_graph)
    return {
        "entities": entities,
        "id_entities": id_entities,
        "colors": colors,
        "scc_classes": scc_classes,
    }


def _assign_ids(
    graph: GraphIR,
    *,
    profile: str,
    guid_policy: str,
    root_remap: dict[str, str] | None = None,
    root_remap_diagnostics: dict[str, dict[str, Any]] | None = None,
    side: str,
    precomputed: dict[str, Any] | None = None,
) -> tuple[dict[int, str], dict[int, IdentityInfo]]:
    identity_state = precomputed if precomputed is not None else _precompute_identity_state(
        graph, profile=profile
    )
    entities = identity_state["entities"]
    id_entities = identity_state["id_entities"]
    colors = identity_state["colors"]
    scc_classes = identity_state["scc_classes"]
    guid_policy_out = enforce_or_disambiguate_guid_policy(
        entities,
        policy=guid_policy,
        side=side,
    )
    guid_index = _guid_index(entities)
    disambiguated = guid_policy_out["disambiguated"]
    ids: dict[int, str] = {}
    identity: dict[int, IdentityInfo] = {}
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


def _unique_guid_step_index(entities: dict[int, dict]) -> dict[str, int]:
    counts = _guid_index(entities)
    out: dict[str, int] = {}
    for step_id, entity in entities.items():
        gid = entity.get("global_id")
        if gid and counts.get(gid, 0) == 1:
            out[gid] = step_id
    return out


def _match_root_steps(
    old_graph: GraphIR,
    new_graph: GraphIR,
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
    old_identity: dict[int, IdentityInfo],
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


def _graph_for_profile(graph: GraphIR, *, profile: str) -> GraphIR:
    if profile != "semantic_stable":
        return graph
    entities = {
        step_id: entity_for_profile(entity, profile=profile)
        for step_id, entity in graph.get("entities", {}).items()
    }
    return {"entities": entities}


def _index_by_identity(
    graph: GraphIR,
    ids: dict[int, str],
    identity: dict[int, IdentityInfo],
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
    for items in by_id.values():
        items.sort(key=lambda item: item["step_id"])
    return by_id
