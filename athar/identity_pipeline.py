"""Identity assignment and step-matching internals for diff context prep."""

from __future__ import annotations

from typing import Any

from .graph_utils import build_adjacency, build_reverse_adjacency
from .structural_hash import structural_hash
from .wl_refinement import wl_refine_with_scc_fallback
from .guid_policy import enforce_or_disambiguate_guid_policy
from .profile_policy import entity_for_profile
from .types import GraphIR, IdentityInfo

_DEFAULT_EXACT_HASH_IDENTITY: IdentityInfo = {
    "match_method": "exact_hash",
    "match_confidence": 1.0,
    "matched_on": None,
}


def _precompute_identity_state(
    graph: GraphIR,
    *,
    profile: str,
    seeded_colors: dict[int, str] | None = None,
    precomputed_profile_entities: dict[int, dict] | None = None,
) -> dict[str, Any]:
    entities = graph.get("entities", {})
    guid_counts, guid_quality = _guid_quality(entities)
    unique_guid_steps = _unique_guid_step_index(entities, guid_counts=guid_counts)
    graph_adjacency = build_adjacency(entities)
    graph_reverse_adjacency = build_reverse_adjacency(entities, graph_adjacency)
    if precomputed_profile_entities is not None:
        id_entities = precomputed_profile_entities
    else:
        id_graph = _graph_for_profile(graph, profile=profile)
        id_entities = id_graph.get("entities", {})
    if id_entities is entities:
        id_adjacency = graph_adjacency
        id_reverse_adjacency = graph_reverse_adjacency
    else:
        # Derive profile adjacency from full-graph adjacency.
        # entity_for_profile returns the same object when no filtering was needed,
        # so we can reuse edges for the majority of entities (those without
        # OwnerHistory).  Only rebuild edges for entities whose refs changed.
        id_adjacency: dict[int, list[tuple[str, str | None, int]]] = {}
        for step_id in id_entities:
            profile_ent = id_entities[step_id]
            orig_ent = entities[step_id]
            if profile_ent is orig_ent or profile_ent.get("refs") is orig_ent.get("refs"):
                id_adjacency[step_id] = graph_adjacency.get(step_id, [])
            else:
                edges: list[tuple[str, str | None, int]] = []
                for ref in profile_ent.get("refs", []):
                    target = ref.get("target")
                    if target in id_entities:
                        edges.append((ref.get("path", ""), ref.get("target_type"), target))
                edges.sort(key=lambda item: (item[0], item[1] or "", item[2]))
                id_adjacency[step_id] = edges
        id_reverse_adjacency = build_reverse_adjacency(id_entities, id_adjacency)
    seeded_colors = seeded_colors or {}
    if seeded_colors and len(seeded_colors) >= len(id_entities):
        profile_hashes = {step_id: seeded_colors[step_id] for step_id in id_entities}
    else:
        profile_hashes = {}
        for step_id, entity in id_entities.items():
            seeded = seeded_colors.get(step_id)
            if seeded is not None:
                profile_hashes[step_id] = seeded
                continue
            profile_hashes[step_id] = structural_hash(entity)
    id_graph = {"entities": id_entities}
    wl_diagnostics: dict[str, Any] = {}
    colors, scc_classes = wl_refine_with_scc_fallback(
        id_graph,
        initial_colors=profile_hashes,
        adjacency=id_adjacency,
        reverse_adjacency=id_reverse_adjacency,
        diagnostics=wl_diagnostics,
    )
    return {
        "entities": entities,
        "guid_counts": guid_counts,
        "guid_quality": guid_quality,
        "unique_guid_steps": unique_guid_steps,
        "graph_adjacency": graph_adjacency,
        "graph_reverse_adjacency": graph_reverse_adjacency,
        "id_entities": id_entities,
        "id_adjacency": id_adjacency,
        "id_reverse_adjacency": id_reverse_adjacency,
        "profile_hashes": profile_hashes,
        "colors": colors,
        "scc_classes": scc_classes,
        "wl_diagnostics": wl_diagnostics,
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
    diagnostics: dict[str, Any] | None = None,
) -> tuple[dict[int, str], dict[int, IdentityInfo]]:
    identity_state = precomputed if precomputed is not None else _precompute_identity_state(
        graph, profile=profile
    )
    entities = identity_state["entities"]
    id_entities = identity_state["id_entities"]
    colors = identity_state["colors"]
    scc_classes = identity_state["scc_classes"]
    if diagnostics is not None and isinstance(identity_state.get("wl_diagnostics"), dict):
        wl_diag = identity_state["wl_diagnostics"]
        if isinstance(wl_diag.get("wl"), dict):
            diagnostics["wl"] = dict(wl_diag["wl"])
    guid_policy_out = enforce_or_disambiguate_guid_policy(
        entities,
        policy=guid_policy,
        side=side,
    )
    guid_index = identity_state.get("guid_counts") or _guid_index(entities)
    disambiguated = guid_policy_out["disambiguated"]
    root_remap = root_remap or {}
    root_remap_diagnostics = root_remap_diagnostics or {}
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
            mapped_gid = root_remap.get(gid, gid)
            remap_diag = root_remap_diagnostics.get(gid, {})
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
    return ids, identity


def _guid_index(entities: dict[int, dict]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for entity in entities.values():
        gid = entity.get("global_id")
        if gid:
            counts[gid] = counts.get(gid, 0) + 1
    return counts


def _guid_quality(entities: dict[int, dict]) -> tuple[dict[str, int], dict[str, int]]:
    """Compute GUID counts and quality metrics in a single pass.

    Returns (guid_counts, quality_dict) where quality_dict matches the
    ``root_guid_quality`` output shape.
    """
    counts: dict[str, int] = {}
    invalid = 0
    for entity in entities.values():
        gid = entity.get("global_id")
        if gid is None:
            continue
        if not isinstance(gid, str) or gid.strip() == "":
            invalid += 1
            continue
        counts[gid] = counts.get(gid, 0) + 1
    valid_total = sum(counts.values())
    unique_valid = sum(1 for c in counts.values() if c == 1)
    duplicate_ids = sum(1 for c in counts.values() if c > 1)
    duplicate_occurrences = sum(c for c in counts.values() if c > 1)
    return counts, {
        "valid_total": valid_total,
        "unique_valid": unique_valid,
        "duplicate_ids": duplicate_ids,
        "duplicate_occurrences": duplicate_occurrences,
        "invalid": invalid,
    }


def _unique_guid_step_index(
    entities: dict[int, dict],
    guid_counts: dict[str, int] | None = None,
) -> dict[str, int]:
    counts = guid_counts if guid_counts is not None else _guid_index(entities)
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
    *,
    old_unique_guid_steps: dict[str, int] | None = None,
    new_unique_guid_steps: dict[str, int] | None = None,
) -> dict[int, int]:
    old_roots = (
        old_unique_guid_steps
        if old_unique_guid_steps is not None
        else _unique_guid_step_index(old_graph.get("entities", {}))
    )
    new_roots = (
        new_unique_guid_steps
        if new_unique_guid_steps is not None
        else _unique_guid_step_index(new_graph.get("entities", {}))
    )
    pairs: dict[int, int] = {}
    for old_gid, old_step in sorted(old_roots.items()):
        mapped_gid = root_remap.get(old_gid, old_gid)
        new_step = new_roots.get(mapped_gid)
        if new_step is not None:
            pairs[old_step] = new_step
    return pairs


def _match_steps_by_unique_id(old_ids: dict[int, str], new_ids: dict[int, str]) -> dict[int, int]:
    old_first: dict[str, int] = {}
    old_counts: dict[str, int] = {}
    for step_id, entity_id in old_ids.items():
        old_counts[entity_id] = old_counts.get(entity_id, 0) + 1
        old_first.setdefault(entity_id, step_id)
    new_first: dict[str, int] = {}
    new_counts: dict[str, int] = {}
    for step_id, entity_id in new_ids.items():
        new_counts[entity_id] = new_counts.get(entity_id, 0) + 1
        new_first.setdefault(entity_id, step_id)

    pairs: dict[int, int] = {}
    for entity_id in sorted(set(old_counts) & set(new_counts)):
        if old_counts[entity_id] == 1 and new_counts[entity_id] == 1:
            pairs[old_first[entity_id]] = new_first[entity_id]
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
        "text_fingerprint": 3,
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
    *,
    profile_entities: dict[int, dict] | None = None,
    compare_entities: dict[int, dict] | None = None,
    profile_hashes: dict[int, str] | None = None,
) -> dict[str, list[dict]]:
    by_id: dict[str, list[dict]] = {}
    entities = graph.get("entities", {})
    profile_entities = profile_entities or entities
    compare_entities = compare_entities or {}
    for step_id, entity in entities.items():
        profile_entity = profile_entities.get(step_id, entity)
        identity_item = identity.get(step_id)
        if identity_item is None:
            identity_item = _DEFAULT_EXACT_HASH_IDENTITY
        item = {
            "step_id": step_id,
            "entity": entity,
            "identity": identity_item,
        }
        if profile_entity is not entity:
            item["profile_entity"] = profile_entity
        if step_id in compare_entities:
            item["compare_entity"] = compare_entities[step_id]
        if profile_hashes is not None and step_id in profile_hashes:
            item["profile_hash"] = profile_hashes[step_id]
        by_id.setdefault(ids[step_id], []).append(item)
    return by_id
