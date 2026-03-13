"""Context building and identity/matching helpers for graph diff."""

from __future__ import annotations

from collections.abc import Callable
import gc
import multiprocessing
import os
import time
from typing import Any

from ..graph.structural_hash import structural_hash
from .equivalence_classes import apply_ambiguous_equivalence_classes
from .graph_cache import save_cached
from ..graph.graph_parser import count_dangling_refs
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
from .geometry_policy import GEOMETRY_POLICY_STRICT_SYNTAX
from .root_remap import plan_root_remap
from .similarity_seed import (
    collect_text_fingerprint_side,
    match_text_fingerprint_collections,
    text_fingerprint_pairs,
    unique_guid_pairs,
)
from .markers import RootedOwnerProjector
from .stats import build_stats
from ..graph.profile_policy import entity_for_profile, validate_profile
from .types import DiffContext, EntityIR, GraphIR, IdentityInfo

ProgressCallback = Callable[[dict[str, Any]], None]
_PREPARE_CONTEXT_TOTAL_STEPS = 22
_GUID_SEED_PATH_PROPAGATION_THRESHOLD = 0.05
_PARALLEL_ENV = "ATHAR_PARALLEL"
_FORCE_GC_COLLECT_ENV = "ATHAR_FORCE_GC_COLLECT"


def prepare_diff_context(
    old_graph: GraphIR,
    new_graph: GraphIR,
    *,
    profile: str,
    geometry_policy: str = GEOMETRY_POLICY_STRICT_SYNTAX,
    guid_policy: str = GUID_POLICY_FAIL_FAST,
    matcher_policy: dict[str, dict[str, Any]] | None = None,
    timing_collector: dict[str, float] | None = None,
    progress_callback: ProgressCallback | None = None,
    cached_identity_old: dict[str, Any] | None = None,
    cached_identity_new: dict[str, Any] | None = None,
    file_hashes: tuple[str | None, str | None] | None = None,
) -> DiffContext:
    validate_profile(profile)
    validate_guid_policy(guid_policy)
    _validate_schema(old_graph, new_graph)

    resolved_matcher_policy = resolve_matcher_policy(matcher_policy)
    old_file_hash = file_hashes[0] if file_hashes else None
    new_file_hash = file_hashes[1] if file_hashes else None
    identity_precompute_cache: dict[tuple[int, str, int], dict[str, Any]] = {}

    def _identity_precompute(
        graph: GraphIR,
        *,
        disk_cached: dict[str, Any] | None = None,
        file_hash: str | None = None,
        seeded_colors: dict[int, str] | None = None,
        precomputed_profile_entities: dict[int, dict] | None = None,
    ) -> dict[str, Any]:
        key = (id(graph), profile, id(seeded_colors) if seeded_colors is not None else 0)
        cached = identity_precompute_cache.get(key)
        if cached is not None:
            return cached
        if disk_cached is not None:
            identity_precompute_cache[key] = disk_cached
            return disk_cached
        precomputed = _precompute_identity_state(
            graph,
            profile=profile,
            seeded_colors=seeded_colors,
            precomputed_profile_entities=precomputed_profile_entities,
        )
        identity_precompute_cache[key] = precomputed
        # Save to disk cache for future runs (skip when seeded — state is pair-dependent)
        if file_hash is not None and not seeded_colors:
            save_cached(file_hash, profile, graph=graph, identity_state=precomputed)
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
    seed_guid_pairs: dict[int, int] = {}
    seed_path_pairs: dict[int, int] = {}
    seed_path_diagnostics: dict[int, dict[str, Any]] = {}
    seed_path_ambiguous = 0
    seed_text_pairs: dict[int, int] = {}
    seed_text_diagnostics: dict[int, dict[str, Any]] = {}
    old_seed_colors: dict[int, str] = {}
    new_seed_colors: dict[int, str] = {}
    old_precomputed_profile: dict[int, dict] | None = None
    new_precomputed_profile: dict[int, dict] | None = None
    seeding_enabled = cached_identity_old is None or cached_identity_new is None
    old_state: dict[str, Any] | None = None
    new_state: dict[str, Any] | None = None

    if seeding_enabled:
        seed_guid_pairs, guid_seed_diagnostics = unique_guid_pairs(old_graph, new_graph)
    else:
        guid_seed_diagnostics = {
            "matched": 0,
            "coverage": 0.0,
            "unique_guid_overlap": 0.0,
            "old_unique": 0,
            "new_unique": 0,
        }
    _record_timing(timing_collector, "seed_guid_pairs", stage_started)
    if timing_collector is not None:
        timing_collector["seed_guid_pairs.matched"] = float(guid_seed_diagnostics.get("matched", 0))
        timing_collector["seed_guid_pairs.coverage"] = round(
            float(guid_seed_diagnostics.get("coverage", 0.0)),
            6,
        )
        timing_collector["seed_guid_pairs.unique_guid_overlap"] = round(
            float(guid_seed_diagnostics.get("unique_guid_overlap", 0.0)),
            6,
        )
    _step_done("seed_guid_pairs")

    stage_started = time.perf_counter()
    if (
        seed_guid_pairs
        and float(guid_seed_diagnostics.get("unique_guid_overlap", 0.0))
        >= _GUID_SEED_PATH_PROPAGATION_THRESHOLD
    ):
        early_guid_path = propagate_matches_by_typed_path(
            old_graph,
            new_graph,
            seed_guid_pairs,
            pre_matched_old=set(seed_guid_pairs),
            pre_matched_new=set(seed_guid_pairs.values()),
            collect_diagnostics=False,
        )
        seed_path_pairs = early_guid_path.get("old_to_new", {})
        seed_path_diagnostics = early_guid_path.get("match_info", {})
        seed_path_ambiguous = int(early_guid_path.get("ambiguous", 0))
    _record_timing(timing_collector, "seed_guid_path_propagation", stage_started)
    _step_done("seed_guid_path_propagation")

    # Disable cyclic GC during the allocation-heavy identity/matching phases
    # to avoid expensive GC pauses on millions of small dicts.
    gc.disable()

    stage_started = time.perf_counter()
    parallel_seed_results: tuple[dict[str, Any], dict[str, Any]] | None = None
    if seeding_enabled:
        exclude_old = set(seed_guid_pairs) | set(seed_path_pairs)
        exclude_new = set(seed_guid_pairs.values()) | set(seed_path_pairs.values())
        if (
            cached_identity_old is None
            and cached_identity_new is None
            and _parallel_enabled()
        ):
            parallel_seed_results = _prepare_seeded_sides_parallel(
                old_graph,
                new_graph,
                profile=profile,
                exclude_old=exclude_old,
                exclude_new=exclude_new,
            )
        if parallel_seed_results is not None:
            old_parallel, new_parallel = parallel_seed_results
            seed_text = match_text_fingerprint_collections(
                old_graph,
                new_graph,
                old_collection={
                    "match_buckets": old_parallel["match_buckets"],
                    "all_fingerprints": old_parallel["all_fingerprints"],
                    "profile_entities": old_parallel["identity_state"].get("id_entities", {}),
                },
                new_collection={
                    "match_buckets": new_parallel["match_buckets"],
                    "all_fingerprints": new_parallel["all_fingerprints"],
                    "profile_entities": new_parallel["identity_state"].get("id_entities", {}),
                },
            )
            old_state = old_parallel["identity_state"]
            new_state = new_parallel["identity_state"]
            old_seed_colors = old_parallel["all_fingerprints"]
            new_seed_colors = new_parallel["all_fingerprints"]
            old_precomputed_profile = old_state.get("id_entities")
            new_precomputed_profile = new_state.get("id_entities")
        else:
            seed_text = text_fingerprint_pairs(
                old_graph,
                new_graph,
                profile=profile,
                exclude_old=exclude_old,
                exclude_new=exclude_new,
            )
            old_seed_colors = seed_text.get("old_all_fingerprints", {})
            new_seed_colors = seed_text.get("new_all_fingerprints", {})
            old_precomputed_profile = seed_text.get("old_profile_entities")
            new_precomputed_profile = seed_text.get("new_profile_entities")
        seed_text_pairs = seed_text.get("old_to_new", {})
        seed_text_diagnostics = seed_text.get("diagnostics", {})
    else:
        seed_text = {"ambiguous_buckets": 0}
    _record_timing(timing_collector, "seed_text_fingerprints", stage_started)
    if timing_collector is not None:
        timing_collector["seed_text_fingerprints.matched"] = float(len(seed_text_pairs))
        timing_collector["seed_text_fingerprints.ambiguous_buckets"] = float(
            seed_text.get("ambiguous_buckets", 0),
        )
        timing_collector["seed_text_fingerprints.parallel"] = (
            1.0 if parallel_seed_results is not None else 0.0
        )
    _step_done("seed_text_fingerprints")

    if old_state is None:
        stage_started = time.perf_counter()
        old_state = _identity_precompute(
            old_graph, disk_cached=cached_identity_old, file_hash=old_file_hash,
            seeded_colors=old_seed_colors,
            precomputed_profile_entities=old_precomputed_profile,
        )
        _record_timing(timing_collector, "precompute_old_identity", stage_started)
    elif timing_collector is not None:
        timing_collector["precompute_old_identity"] = float(
            parallel_seed_results[0]["precompute_ms"]
        )
    _step_done("precompute_old_identity")
    if new_state is None:
        stage_started = time.perf_counter()
        new_state = _identity_precompute(
            new_graph, disk_cached=cached_identity_new, file_hash=new_file_hash,
            seeded_colors=new_seed_colors,
            precomputed_profile_entities=new_precomputed_profile,
        )
        _record_timing(timing_collector, "precompute_new_identity", stage_started)
    elif timing_collector is not None:
        timing_collector["precompute_new_identity"] = float(
            parallel_seed_results[1]["precompute_ms"]
        )
    _step_done("precompute_new_identity")

    stage_started = time.perf_counter()
    remap = plan_root_remap(
        old_graph,
        new_graph,
        old_colors=old_state.get("colors"),
        new_colors=new_state.get("colors"),
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
        precomputed=old_state,
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
        precomputed=new_state,
        diagnostics=new_assign_diagnostics,
    )
    _record_timing(timing_collector, "assign_new_ids", stage_started)
    _record_identity_diagnostics(timing_collector, "assign_new_ids", new_assign_diagnostics)
    _step_done("assign_new_ids")
    stage_started = time.perf_counter()
    root_pairs = _match_root_steps(
        old_graph,
        new_graph,
        remap["old_to_new"],
        old_unique_guid_steps=old_state.get("unique_guid_steps"),
        new_unique_guid_steps=new_state.get("unique_guid_steps"),
    )
    _record_timing(timing_collector, "match_root_steps", stage_started)
    _step_done("match_root_steps")
    stage_started = time.perf_counter()
    _apply_step_matches(
        old_ids,
        old_identity,
        new_ids,
        seed_text_pairs,
        method="text_fingerprint",
        diagnostics=seed_text_diagnostics,
    )
    _record_timing(timing_collector, "apply_text_fingerprint_matches", stage_started)
    _step_done("apply_text_fingerprint_matches")
    stage_started = time.perf_counter()
    exact_pairs = _match_steps_by_unique_id(old_ids, new_ids)
    _record_timing(timing_collector, "match_unique_ids", stage_started)
    _step_done("match_unique_ids")
    pre_old = set(root_pairs) | set(exact_pairs)
    pre_new = set(root_pairs.values()) | set(exact_pairs.values())
    stage_started = time.perf_counter()
    can_reuse_early_path = (
        bool(seed_path_pairs)
        and remap.get("method") == "disabled_guid_overlap"
        and root_pairs == seed_guid_pairs
    )
    if can_reuse_early_path:
        filtered_old_to_new: dict[int, int] = {}
        filtered_diagnostics: dict[int, dict[str, Any]] = {}
        for old_step, new_step in sorted(seed_path_pairs.items()):
            if old_step in pre_old or new_step in pre_new:
                continue
            filtered_old_to_new[old_step] = new_step
            diag = seed_path_diagnostics.get(old_step)
            if diag is not None:
                filtered_diagnostics[old_step] = {
                    "match_confidence": 1.0,
                    "matched_on": {
                        "stage": "typed_path",
                        "path": diag[0],
                        "target_type": diag[1],
                    },
                }
        path_propagation = {
            "old_to_new": filtered_old_to_new,
            "diagnostics": filtered_diagnostics,
            "ambiguous": seed_path_ambiguous,
        }
    else:
        path_propagation = propagate_matches_by_typed_path(
            old_graph,
            new_graph,
            root_pairs,
            pre_matched_old=pre_old,
            pre_matched_new=pre_new,
        )
    _record_timing(timing_collector, "path_propagation", stage_started)
    if timing_collector is not None:
        timing_collector["path_propagation.reused_early"] = 1.0 if can_reuse_early_path else 0.0
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
        old_adjacency=old_state.get("graph_adjacency"),
        new_adjacency=new_state.get("graph_adjacency"),
        old_reverse_adjacency=old_state.get("graph_reverse_adjacency"),
        new_reverse_adjacency=new_state.get("graph_reverse_adjacency"),
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

    old_profile_entities = old_state.get("id_entities", old_graph.get("entities", {}))
    new_profile_entities = new_state.get("id_entities", new_graph.get("entities", {}))
    stage_started = time.perf_counter()
    # Compare-entity normalization is now lazy in base-change emission.
    _record_timing(timing_collector, "build_compare_entities", stage_started)
    _step_done("build_compare_entities")

    stage_started = time.perf_counter()
    old_index_summary: dict[str, Any] = {}
    old_by_id = _index_by_identity(
        old_graph,
        old_ids,
        old_identity,
        profile_entities=old_profile_entities,
        profile_hashes=old_state.get("profile_hashes"),
        summary=old_index_summary,
        record_methods=True,
    )
    _record_timing(timing_collector, "index_old_by_identity", stage_started)
    _step_done("index_old_by_identity")
    stage_started = time.perf_counter()
    new_index_summary: dict[str, Any] = {}
    new_by_id = _index_by_identity(
        new_graph,
        new_ids,
        new_identity,
        profile_entities=new_profile_entities,
        profile_hashes=new_state.get("profile_hashes"),
        summary=new_index_summary,
    )
    _record_timing(timing_collector, "index_new_by_identity", stage_started)
    _step_done("index_new_by_identity")
    stage_started = time.perf_counter()
    old_owner_projector = RootedOwnerProjector(
        old_graph,
        old_ids,
        reverse_adjacency=old_state.get("graph_reverse_adjacency"),
    )
    new_owner_projector = RootedOwnerProjector(
        new_graph,
        new_ids,
        reverse_adjacency=new_state.get("graph_reverse_adjacency"),
    )
    _record_timing(timing_collector, "build_owner_projectors", stage_started)
    _step_done("build_owner_projectors")

    stage_started = time.perf_counter()
    stats = build_stats(
        old_graph=old_graph,
        new_graph=new_graph,
        old_by_id=old_by_id,
        new_by_id=new_by_id,
        old_index_summary=old_index_summary,
        new_index_summary=new_index_summary,
        remap_ambiguous=remap["ambiguous"],
        path_ambiguous=path_propagation["ambiguous"],
        secondary_ambiguous=secondary["ambiguous"],
        remap_matches=len(remap["old_to_new"]),
        path_matches=len(path_propagation["old_to_new"]),
        secondary_matches=len(secondary["old_to_new"]),
        old_dangling_refs=_dangling_ref_count(old_graph),
        new_dangling_refs=_dangling_ref_count(new_graph),
        old_guid_quality=old_state.get("guid_quality"),
        new_guid_quality=new_state.get("guid_quality"),
    )
    _record_timing(timing_collector, "build_stats", stage_started)
    _step_done("build_stats")

    # Re-enable cyclic GC after hot phases.
    gc.enable()
    if _force_gc_collect():
        gc.collect()

    _emit_progress(progress_callback, {
        "status": "done",
        "completed_steps": _PREPARE_CONTEXT_TOTAL_STEPS,
        "total_steps": _PREPARE_CONTEXT_TOTAL_STEPS,
        "stage_progress": 1.0,
    })

    return {
        "version": "2",
        "profile": profile,
        "geometry_policy": geometry_policy,
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
        "geometry_policy": context.get("geometry_policy", GEOMETRY_POLICY_STRICT_SYNTAX),
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
    old_compare_entity: EntityIR | None = None,
    new_compare_entity: EntityIR | None = None,
) -> bool:
    if old_compare_entity is not None and new_compare_entity is not None:
        return (
            old_compare_entity.get("entity_type") == new_compare_entity.get("entity_type")
            and old_compare_entity.get("attributes") == new_compare_entity.get("attributes")
            and old_compare_entity.get("refs") == new_compare_entity.get("refs")
        )
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


def _parallel_enabled() -> bool:
    raw = os.environ.get(_PARALLEL_ENV, "0").strip().lower()
    return raw not in {"", "0", "false", "no", "off"}


def _force_gc_collect() -> bool:
    raw = os.environ.get(_FORCE_GC_COLLECT_ENV, "0").strip().lower()
    return raw not in {"", "0", "false", "no", "off"}


def _prepare_seeded_side_state(
    graph: GraphIR,
    *,
    profile: str,
    exclude_steps: set[int],
) -> dict[str, Any]:
    fingerprint_started = time.perf_counter()
    collection = collect_text_fingerprint_side(
        graph,
        profile=profile,
        exclude_steps=exclude_steps,
    )
    fingerprint_ms = round((time.perf_counter() - fingerprint_started) * 1000.0, 3)
    precompute_started = time.perf_counter()
    identity_state = _precompute_identity_state(
        graph,
        profile=profile,
        seeded_colors=collection["all_fingerprints"],
        precomputed_profile_entities=collection["profile_entities"],
    )
    precompute_ms = round((time.perf_counter() - precompute_started) * 1000.0, 3)
    return {
        "match_buckets": collection["match_buckets"],
        "all_fingerprints": collection["all_fingerprints"],
        "identity_state": identity_state,
        "fingerprint_ms": fingerprint_ms,
        "precompute_ms": precompute_ms,
    }


def _seeded_side_state_worker(
    send_conn,
    *,
    graph: GraphIR,
    profile: str,
    exclude_steps: set[int],
) -> None:
    try:
        send_conn.send(
            {
                "ok": True,
                "result": _prepare_seeded_side_state(
                    graph,
                    profile=profile,
                    exclude_steps=exclude_steps,
                ),
            }
        )
    except BaseException as exc:
        send_conn.send({"ok": False, "error": repr(exc)})
    finally:
        send_conn.close()


def _prepare_seeded_sides_parallel(
    old_graph: GraphIR,
    new_graph: GraphIR,
    *,
    profile: str,
    exclude_old: set[int],
    exclude_new: set[int],
) -> tuple[dict[str, Any], dict[str, Any]] | None:
    try:
        ctx = multiprocessing.get_context("fork")
    except ValueError:
        return None

    recv_conn, send_conn = ctx.Pipe(duplex=False)
    process = ctx.Process(
        target=_seeded_side_state_worker,
        kwargs={
            "send_conn": send_conn,
            "graph": old_graph,
            "profile": profile,
            "exclude_steps": exclude_old,
        },
    )
    try:
        process.start()
        send_conn.close()
        new_result = _prepare_seeded_side_state(
            new_graph,
            profile=profile,
            exclude_steps=exclude_new,
        )
        message = recv_conn.recv()
        process.join()
        if process.exitcode != 0:
            return None
        if not message.get("ok"):
            return None
        return message["result"], new_result
    except Exception:
        if process.is_alive():
            process.terminate()
        process.join()
        return None
    finally:
        recv_conn.close()


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
    refs_in = entity.get("refs", [])
    attrs_in = entity.get("attributes", {})
    if not refs_in:
        return entity

    refs_changed = False
    refs: list[dict[str, Any]] = []
    for ref in refs_in:
        target = ref.get("target")
        mapped_target = _map_ref_target(target, ids_by_step)
        if mapped_target == target:
            refs.append(ref)
            continue
        refs_changed = True
        refs.append({
            **ref,
            "target": mapped_target,
        })

    attrs = _normalize_attr_refs(attrs_in, ids_by_step)
    if attrs is attrs_in and not refs_changed:
        return entity
    return {
        **entity,
        "attributes": attrs,
        "refs": refs if refs_changed else refs_in,
    }


def _normalize_attr_refs(value: Any, ids_by_step: dict[int, str]) -> Any:
    if isinstance(value, dict):
        kind = value.get("kind")
        if kind == "ref":
            mapped = _map_ref_target(value.get("id"), ids_by_step)
            if mapped == value.get("id"):
                return value
            out = dict(value)
            out["id"] = mapped
            return out
        if kind in {"null", "bool", "int", "real", "string"}:
            return value
        changed = False
        out: dict[str, Any] = {}
        for key, item in value.items():
            normalized = _normalize_attr_refs(item, ids_by_step)
            out[key] = normalized
            if normalized is not item:
                changed = True
        return out if changed else value
    if isinstance(value, list):
        changed = False
        out: list[Any] = []
        for item in value:
            normalized = _normalize_attr_refs(item, ids_by_step)
            out.append(normalized)
            if normalized is not item:
                changed = True
        return out if changed else value
    return value


def _map_ref_target(target: Any, ids_by_step: dict[int, str]) -> Any:
    if isinstance(target, int):
        return ids_by_step.get(target, f"STEP:{target}")
    return target


def _build_compare_entities(
    profile_entities: dict[int, EntityIR],
    ids_by_step: dict[int, str],
    *,
    comparable_ids: set[str] | None = None,
) -> dict[int, EntityIR]:
    out: dict[int, EntityIR] = {}
    for step_id, entity_id in ids_by_step.items():
        if not isinstance(entity_id, str) or entity_id.startswith("H:"):
            continue
        if comparable_ids is not None and entity_id not in comparable_ids:
            continue
        entity = profile_entities.get(step_id)
        if entity is None:
            continue
        out[step_id] = _normalize_entity_ref_targets(entity, ids_by_step)
    return out
