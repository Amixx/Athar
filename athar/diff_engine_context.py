"""Context building and identity/matching helpers for graph diff."""

from __future__ import annotations

from collections.abc import Callable
import time
from typing import Any

from .structural_hash import structural_hash
from .equivalence_classes import apply_ambiguous_equivalence_classes
from .graph_parser import count_dangling_refs
from .guid_policy import GUID_POLICY_FAIL_FAST, validate_guid_policy
from .identity_pipeline import (
    _apply_step_matches,
    _assign_ids,
    _identity_priority,
    _index_by_identity,
    _match_root_steps,
    _match_steps_by_unique_id,
    _precompute_identity_state,
)
from .matcher_graph import propagate_matches_by_typed_path, secondary_match_unresolved
from .matcher_policy import resolve_matcher_policy
from .root_remap import plan_root_remap
from .diff_engine_markers import RootedOwnerProjector
from .diff_engine_stats import build_stats
from .profile_policy import entity_for_profile, validate_profile
from .types import DiffContext, EntityIR, GraphIR, IdentityInfo

ProgressCallback = Callable[[dict[str, Any]], None]
_PREPARE_CONTEXT_TOTAL_STEPS = 15


def prepare_diff_context(
    old_graph: GraphIR,
    new_graph: GraphIR,
    *,
    profile: str,
    guid_policy: str = GUID_POLICY_FAIL_FAST,
    matcher_policy: dict[str, dict[str, Any]] | None = None,
    timing_collector: dict[str, float] | None = None,
    progress_callback: ProgressCallback | None = None,
) -> DiffContext:
    validate_profile(profile)
    validate_guid_policy(guid_policy)
    _validate_schema(old_graph, new_graph)

    resolved_matcher_policy = resolve_matcher_policy(matcher_policy)
    identity_precompute_cache: dict[tuple[int, str], dict[str, Any]] = {}

    def _identity_precompute(graph: GraphIR) -> dict[str, Any]:
        key = (id(graph), profile)
        cached = identity_precompute_cache.get(key)
        if cached is not None:
            return cached
        precomputed = _precompute_identity_state(graph, profile=profile)
        identity_precompute_cache[key] = precomputed
        return precomputed

    _emit_progress(progress_callback, {
        "status": "start",
        "completed_steps": 0,
        "total_steps": _PREPARE_CONTEXT_TOTAL_STEPS,
        "stage_progress": 0.0,
    })
    completed_steps = 0

    def _step_done(step: str) -> None:
        nonlocal completed_steps
        completed_steps += 1
        _emit_progress(progress_callback, {
            "status": "running",
            "step": step,
            "completed_steps": completed_steps,
            "total_steps": _PREPARE_CONTEXT_TOTAL_STEPS,
            "stage_progress": round(completed_steps / _PREPARE_CONTEXT_TOTAL_STEPS, 6),
        })

    stage_started = time.perf_counter()
    remap = plan_root_remap(
        old_graph,
        new_graph,
        **resolved_matcher_policy["root_remap"],
    )
    _record_timing(timing_collector, "root_remap", stage_started)
    _step_done("root_remap")
    stage_started = time.perf_counter()
    old_assign_diagnostics: dict[str, Any] = {}
    old_ids, old_identity = _assign_ids(
        old_graph,
        profile=profile,
        guid_policy=guid_policy,
        root_remap=remap["old_to_new"],
        root_remap_diagnostics=remap.get("diagnostics", {}),
        side="old",
        precomputed=_identity_precompute(old_graph),
        diagnostics=old_assign_diagnostics,
    )
    _record_timing(timing_collector, "assign_old_ids", stage_started)
    _record_identity_diagnostics(timing_collector, "assign_old_ids", old_assign_diagnostics)
    _step_done("assign_old_ids")
    stage_started = time.perf_counter()
    new_assign_diagnostics: dict[str, Any] = {}
    new_ids, new_identity = _assign_ids(
        new_graph,
        profile=profile,
        guid_policy=guid_policy,
        side="new",
        precomputed=_identity_precompute(new_graph),
        diagnostics=new_assign_diagnostics,
    )
    _record_timing(timing_collector, "assign_new_ids", stage_started)
    _record_identity_diagnostics(timing_collector, "assign_new_ids", new_assign_diagnostics)
    _step_done("assign_new_ids")
    stage_started = time.perf_counter()
    root_pairs = _match_root_steps(old_graph, new_graph, remap["old_to_new"])
    _record_timing(timing_collector, "match_root_steps", stage_started)
    _step_done("match_root_steps")
    stage_started = time.perf_counter()
    exact_pairs = _match_steps_by_unique_id(old_ids, new_ids)
    _record_timing(timing_collector, "match_unique_ids", stage_started)
    _step_done("match_unique_ids")
    pre_old = set(root_pairs) | set(exact_pairs)
    pre_new = set(root_pairs.values()) | set(exact_pairs.values())
    stage_started = time.perf_counter()
    path_propagation = propagate_matches_by_typed_path(
        old_graph,
        new_graph,
        root_pairs,
        pre_matched_old=pre_old,
        pre_matched_new=pre_new,
    )
    _record_timing(timing_collector, "path_propagation", stage_started)
    _step_done("path_propagation")
    stage_started = time.perf_counter()
    _apply_step_matches(
        old_ids,
        old_identity,
        new_ids,
        path_propagation["old_to_new"],
        method="path_propagation",
        diagnostics=path_propagation.get("diagnostics", {}),
    )
    _record_timing(timing_collector, "apply_path_matches", stage_started)
    _step_done("apply_path_matches")
    stage_started = time.perf_counter()
    matched_after_path = _match_steps_by_unique_id(old_ids, new_ids)
    _record_timing(timing_collector, "match_after_path", stage_started)
    _step_done("match_after_path")
    stage_started = time.perf_counter()
    secondary = secondary_match_unresolved(
        old_graph,
        new_graph,
        pre_matched_old=set(matched_after_path),
        pre_matched_new=set(matched_after_path.values()),
        **resolved_matcher_policy["secondary_match"],
    )
    _record_timing(timing_collector, "secondary_match", stage_started)
    _step_done("secondary_match")
    stage_started = time.perf_counter()
    _apply_step_matches(
        old_ids,
        old_identity,
        new_ids,
        secondary["old_to_new"],
        method="secondary_match",
        diagnostics=secondary.get("diagnostics", {}),
    )
    _record_timing(timing_collector, "apply_secondary_matches", stage_started)
    _step_done("apply_secondary_matches")
    stage_started = time.perf_counter()
    apply_ambiguous_equivalence_classes(
        old_ids=old_ids,
        old_identity=old_identity,
        new_ids=new_ids,
        new_identity=new_identity,
        partitions=secondary.get("ambiguous_partitions", []),
    )
    _record_timing(timing_collector, "apply_equivalence_classes", stage_started)
    _step_done("apply_equivalence_classes")

    stage_started = time.perf_counter()
    old_by_id = _index_by_identity(old_graph, old_ids, old_identity)
    _record_timing(timing_collector, "index_old_by_identity", stage_started)
    _step_done("index_old_by_identity")
    stage_started = time.perf_counter()
    new_by_id = _index_by_identity(new_graph, new_ids, new_identity)
    _record_timing(timing_collector, "index_new_by_identity", stage_started)
    _step_done("index_new_by_identity")
    stage_started = time.perf_counter()
    old_owner_projector = RootedOwnerProjector(old_graph, old_ids)
    new_owner_projector = RootedOwnerProjector(new_graph, new_ids)
    _record_timing(timing_collector, "build_owner_projectors", stage_started)
    _step_done("build_owner_projectors")

    stage_started = time.perf_counter()
    stats = build_stats(
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
    )
    _record_timing(timing_collector, "build_stats", stage_started)
    _step_done("build_stats")
    _emit_progress(progress_callback, {
        "status": "done",
        "completed_steps": _PREPARE_CONTEXT_TOTAL_STEPS,
        "total_steps": _PREPARE_CONTEXT_TOTAL_STEPS,
        "stage_progress": 1.0,
    })

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
        "stats": stats,
    }


def build_result(
    context: DiffContext,
    *,
    base_changes: list[dict[str, Any]],
    derived_markers: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        **result_header(context),
        "base_changes": base_changes,
        "derived_markers": derived_markers,
    }


def result_header(context: DiffContext) -> dict[str, Any]:
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


def entities_equal(
    entity_id: str,
    old_ent: EntityIR,
    new_ent: EntityIR,
    *,
    profile: str,
    old_ids: dict[int, str] | None = None,
    new_ids: dict[int, str] | None = None,
) -> bool:
    old_norm = entity_for_profile(old_ent, profile=profile)
    new_norm = entity_for_profile(new_ent, profile=profile)
    if entity_id.startswith("H:"):
        return structural_hash(old_norm) == structural_hash(new_norm)
    if old_ids is not None:
        old_norm = _normalize_entity_ref_targets(old_norm, old_ids)
    if new_ids is not None:
        new_norm = _normalize_entity_ref_targets(new_norm, new_ids)
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


def resolve_identity(old_item: dict | None, new_item: dict | None) -> IdentityInfo:
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


def _validate_schema(old_graph: GraphIR, new_graph: GraphIR) -> None:
    old_schema = old_graph.get("metadata", {}).get("schema")
    new_schema = new_graph.get("metadata", {}).get("schema")
    if old_schema != new_schema:
        raise ValueError(f"Schema mismatch: {old_schema} vs {new_schema}")


def _dangling_ref_count(graph: dict) -> int:
    meta_count = (
        graph.get("metadata", {})
        .get("diagnostics", {})
        .get("dangling_refs")
    )
    if isinstance(meta_count, int):
        return meta_count
    return count_dangling_refs(graph)


def _record_timing(target: dict[str, float] | None, key: str, started: float) -> None:
    if target is None:
        return
    target[key] = round((time.perf_counter() - started) * 1000.0, 3)


def _record_identity_diagnostics(
    target: dict[str, float] | None,
    stage_key: str,
    diagnostics: dict[str, Any],
) -> None:
    if target is None:
        return
    wl = diagnostics.get("wl")
    if not isinstance(wl, dict):
        return
    total_ms = wl.get("total_ms")
    rounds = wl.get("executed_rounds")
    if isinstance(total_ms, (int, float)):
        target[f"{stage_key}.wl_total_ms"] = round(float(total_ms), 3)
    if isinstance(rounds, int):
        target[f"{stage_key}.wl_rounds"] = float(rounds)


def _emit_progress(callback: ProgressCallback | None, payload: dict[str, Any]) -> None:
    if callback is None:
        return
    try:
        callback(payload)
    except Exception:
        return


def _normalize_entity_ref_targets(entity: EntityIR, ids_by_step: dict[int, str]) -> EntityIR:
    refs = []
    for ref in entity.get("refs", []):
        target = ref.get("target")
        refs.append({
            **ref,
            "target": _map_ref_target(target, ids_by_step),
        })
    attrs = _normalize_attr_refs(entity.get("attributes", {}), ids_by_step)
    return {
        **entity,
        "attributes": attrs,
        "refs": refs,
    }


def _normalize_attr_refs(value: Any, ids_by_step: dict[int, str]) -> Any:
    if isinstance(value, dict):
        if value.get("kind") == "ref":
            return {
                **value,
                "id": _map_ref_target(value.get("id"), ids_by_step),
            }
        return {k: _normalize_attr_refs(v, ids_by_step) for k, v in value.items()}
    if isinstance(value, list):
        return [_normalize_attr_refs(item, ids_by_step) for item in value]
    return value


def _map_ref_target(target: Any, ids_by_step: dict[int, str]) -> Any:
    if isinstance(target, int):
        return ids_by_step.get(target, f"STEP:{target}")
    return target
