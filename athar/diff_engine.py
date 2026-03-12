"""Core diff engine orchestration built on graph parsing and canonical IDs."""

from __future__ import annotations

from collections.abc import Callable
import os
import time
from typing import Any

from .diff_engine_changes import make_change, make_class_delta_change
from .diff_engine_context import (
    _dangling_ref_count,
    build_result,
    entities_equal,
    index_change,
    _validate_schema,
    prepare_diff_context,
    resolve_identity,
    result_header,
    should_emit_class_delta,
)
from .diff_engine_markers import build_derived_markers, summarize_rooted_owners
from .diff_engine_stats import root_guid_quality
from .diff_engine_streaming import stream_diff_events
from .geometry_invariants import representation_invariants_match
from .geometry_policy import (
    GEOMETRY_POLICY_STRICT_SYNTAX,
    GEOMETRY_POLICY_INVARIANT_PROBE,
    validate_geometry_policy,
)
from .graph_parser import parse_graph
from .guid_policy import GUID_POLICY_FAIL_FAST, enforce_or_disambiguate_guid_policy, validate_guid_policy
from .matcher_policy import resolve_matcher_policy
from .profile_policy import DEFAULT_PROFILE, validate_profile
from .types import DiffContext, GraphIR

ProgressCallback = Callable[[dict[str, Any]], None]
_PROGRESS_PREPARE_CONTEXT_WEIGHT = 0.55
_PROGRESS_EMIT_BASE_CHANGES_WEIGHT = 0.35
_PROGRESS_EMIT_DERIVED_MARKERS_WEIGHT = 0.10
_BASE_CHANGES_PROGRESS_INTERVAL = 10000


def diff_files(
    old_path: str,
    new_path: str,
    *,
    profile: str = DEFAULT_PROFILE,
    geometry_policy: str = GEOMETRY_POLICY_STRICT_SYNTAX,
    guid_policy: str = GUID_POLICY_FAIL_FAST,
    matcher_policy: dict[str, dict[str, Any]] | None = None,
    timings: bool = False,
    progress_callback: ProgressCallback | None = None,
) -> dict:
    validate_guid_policy(guid_policy)
    validate_geometry_policy(geometry_policy)
    same_input = _paths_refer_to_same_file(old_path, new_path)
    parse_timings_ms: dict[str, float] | None = None
    if timings:
        parse_timings_ms = {}
        started = time.perf_counter()
        old_graph = parse_graph(old_path, profile=profile)
        parse_timings_ms["parse_old_graph"] = _elapsed_ms(started)
        if same_input:
            new_graph = old_graph
            parse_timings_ms["parse_new_graph"] = 0.0
        else:
            started = time.perf_counter()
            new_graph = parse_graph(new_path, profile=profile)
            parse_timings_ms["parse_new_graph"] = _elapsed_ms(started)
    else:
        old_graph = parse_graph(old_path, profile=profile)
        new_graph = old_graph if same_input else parse_graph(new_path, profile=profile)
    _validate_schema(old_graph, new_graph)
    return diff_graphs(
        old_graph,
        new_graph,
        profile=profile,
        geometry_policy=geometry_policy,
        guid_policy=guid_policy,
        matcher_policy=matcher_policy,
        timings=timings,
        parse_timings_ms=parse_timings_ms,
        progress_callback=progress_callback,
    )


def diff_graphs(
    old_graph: GraphIR,
    new_graph: GraphIR,
    *,
    profile: str = DEFAULT_PROFILE,
    geometry_policy: str = GEOMETRY_POLICY_STRICT_SYNTAX,
    guid_policy: str = GUID_POLICY_FAIL_FAST,
    matcher_policy: dict[str, dict[str, Any]] | None = None,
    timings: bool = False,
    parse_timings_ms: dict[str, float] | None = None,
    progress_callback: ProgressCallback | None = None,
) -> dict:
    validate_guid_policy(guid_policy)
    validate_geometry_policy(geometry_policy)
    validate_profile(profile)
    _validate_schema(old_graph, new_graph)
    resolved_matcher_policy = resolve_matcher_policy(matcher_policy)
    if old_graph is new_graph:
        enforce_or_disambiguate_guid_policy(
            old_graph.get("entities", {}),
            policy=guid_policy,
            side="old",
        )
        result = _same_graph_fastpath_result(
            graph=old_graph,
            profile=profile,
            geometry_policy=geometry_policy,
            guid_policy=guid_policy,
            matcher_policy=resolved_matcher_policy,
        )
        if timings:
            timings_ms: dict[str, float] = {}
            if parse_timings_ms:
                timings_ms.update(parse_timings_ms)
            timings_ms["prepare_context"] = 0.0
            timings_ms["emit_base_changes"] = 0.0
            timings_ms["emit_derived_markers"] = 0.0
            timings_ms["total"] = 0.0
            result.setdefault("stats", {})["timings_ms"] = timings_ms
        _emit_progress(progress_callback, {"stage": "done", "status": "done", "overall_progress": 1.0, "base_change_count": 0, "derived_marker_count": 0, "elapsed_ms": 0.0})
        return result
    total_started = time.perf_counter()
    _emit_progress(progress_callback, {
        "stage": "prepare_context",
        "status": "start",
        "overall_progress": 0.0,
    })
    context_timings_ms: dict[str, float] | None = {} if timings else None
    def _on_context_progress(event: dict[str, Any]) -> None:
        stage_progress = event.get("stage_progress")
        mapped = {
            **event,
            "stage": "prepare_context",
        }
        if isinstance(stage_progress, (int, float)):
            mapped["overall_progress"] = round(
                _PROGRESS_PREPARE_CONTEXT_WEIGHT * min(max(float(stage_progress), 0.0), 1.0),
                6,
            )
        _emit_progress(progress_callback, mapped)

    started = time.perf_counter()
    context = prepare_diff_context(
        old_graph,
        new_graph,
        profile=profile,
        geometry_policy=geometry_policy,
        guid_policy=guid_policy,
        matcher_policy=matcher_policy,
        timing_collector=context_timings_ms,
        progress_callback=_on_context_progress if progress_callback is not None else None,
    )
    prepare_context_ms = _elapsed_ms(started)
    _emit_progress(progress_callback, {
        "stage": "prepare_context",
        "status": "done",
        "elapsed_ms": prepare_context_ms,
        "overall_progress": _PROGRESS_PREPARE_CONTEXT_WEIGHT,
    })
    try:
        base_changes: list[dict[str, Any]] = []
        change_index: dict[str, list[str]] = {}

        started = time.perf_counter()
        for change in _iter_base_changes(
            context,
            include_snapshots=True,
            progress_callback=progress_callback,
        ):
            base_changes.append(change)
            index_change(change_index, change)
        emit_base_changes_ms = _elapsed_ms(started)

        _emit_progress(progress_callback, {
            "stage": "emit_derived_markers",
            "status": "start",
            "overall_progress": _PROGRESS_PREPARE_CONTEXT_WEIGHT + _PROGRESS_EMIT_BASE_CHANGES_WEIGHT,
        })
        started = time.perf_counter()
        derived_markers = build_derived_markers(
            old_graph=context["old_graph"],
            new_graph=context["new_graph"],
            old_ids=context["old_ids"],
            new_ids=context["new_ids"],
            change_index=change_index,
        )
        emit_derived_markers_ms = _elapsed_ms(started)
        _emit_progress(progress_callback, {
            "stage": "emit_derived_markers",
            "status": "done",
            "elapsed_ms": emit_derived_markers_ms,
            "overall_progress": (
                _PROGRESS_PREPARE_CONTEXT_WEIGHT
                + _PROGRESS_EMIT_BASE_CHANGES_WEIGHT
                + _PROGRESS_EMIT_DERIVED_MARKERS_WEIGHT
            ),
            "derived_marker_count": len(derived_markers),
        })
        result = build_result(context, base_changes=base_changes, derived_markers=derived_markers)
        if timings:
            timings_ms: dict[str, float] = {}
            if parse_timings_ms:
                timings_ms.update(parse_timings_ms)
            timings_ms["prepare_context"] = prepare_context_ms
            if context_timings_ms:
                for key in sorted(context_timings_ms):
                    timings_ms[f"context.{key}"] = context_timings_ms[key]
            timings_ms["emit_base_changes"] = emit_base_changes_ms
            timings_ms["emit_derived_markers"] = emit_derived_markers_ms
            timings_ms["total"] = _elapsed_ms(total_started)
            result.setdefault("stats", {})["timings_ms"] = timings_ms
        _emit_progress(progress_callback, {
            "stage": "done",
            "status": "done",
            "overall_progress": (
                _PROGRESS_PREPARE_CONTEXT_WEIGHT
                + _PROGRESS_EMIT_BASE_CHANGES_WEIGHT
                + _PROGRESS_EMIT_DERIVED_MARKERS_WEIGHT
            ),
            "elapsed_ms": _elapsed_ms(total_started),
            "base_change_count": len(base_changes),
            "derived_marker_count": len(derived_markers),
        })
        return result
    finally:
        _close_owner_projectors(context)


def stream_diff_files(
    old_path: str,
    new_path: str,
    *,
    profile: str = DEFAULT_PROFILE,
    geometry_policy: str = GEOMETRY_POLICY_STRICT_SYNTAX,
    guid_policy: str = GUID_POLICY_FAIL_FAST,
    matcher_policy: dict[str, dict[str, Any]] | None = None,
    mode: str = "ndjson",
    chunk_size: int = 1000,
    timings: bool = False,
):
    """Stream diff output directly from files without materializing full result."""
    validate_guid_policy(guid_policy)
    validate_geometry_policy(geometry_policy)
    same_input = _paths_refer_to_same_file(old_path, new_path)
    parse_timings_ms: dict[str, float] | None = None
    if timings:
        parse_timings_ms = {}
        started = time.perf_counter()
        old_graph = parse_graph(old_path, profile=profile)
        parse_timings_ms["parse_old_graph"] = _elapsed_ms(started)
        if same_input:
            new_graph = old_graph
            parse_timings_ms["parse_new_graph"] = 0.0
        else:
            started = time.perf_counter()
            new_graph = parse_graph(new_path, profile=profile)
            parse_timings_ms["parse_new_graph"] = _elapsed_ms(started)
    else:
        old_graph = parse_graph(old_path, profile=profile)
        new_graph = old_graph if same_input else parse_graph(new_path, profile=profile)
    _validate_schema(old_graph, new_graph)
    yield from stream_diff_graphs(
        old_graph,
        new_graph,
        profile=profile,
        geometry_policy=geometry_policy,
        guid_policy=guid_policy,
        matcher_policy=matcher_policy,
        mode=mode,
        chunk_size=chunk_size,
        timings=timings,
        parse_timings_ms=parse_timings_ms,
    )


def stream_diff_graphs(
    old_graph: GraphIR,
    new_graph: GraphIR,
    *,
    profile: str = DEFAULT_PROFILE,
    geometry_policy: str = GEOMETRY_POLICY_STRICT_SYNTAX,
    guid_policy: str = GUID_POLICY_FAIL_FAST,
    matcher_policy: dict[str, dict[str, Any]] | None = None,
    mode: str = "ndjson",
    chunk_size: int = 1000,
    timings: bool = False,
    parse_timings_ms: dict[str, float] | None = None,
):
    """Stream diff output directly from graph inputs."""
    validate_guid_policy(guid_policy)
    validate_geometry_policy(geometry_policy)
    validate_profile(profile)
    _validate_schema(old_graph, new_graph)
    resolved_matcher_policy = resolve_matcher_policy(matcher_policy)
    if old_graph is new_graph:
        enforce_or_disambiguate_guid_policy(
            old_graph.get("entities", {}),
            policy=guid_policy,
            side="old",
        )
        result = _same_graph_fastpath_result(
            graph=old_graph,
            profile=profile,
            geometry_policy=geometry_policy,
            guid_policy=guid_policy,
            matcher_policy=resolved_matcher_policy,
        )
        if timings:
            timings_ms: dict[str, float] = {}
            if parse_timings_ms:
                timings_ms.update(parse_timings_ms)
            timings_ms["prepare_context"] = 0.0
            timings_ms["emit_base_changes"] = 0.0
            timings_ms["emit_derived_markers"] = 0.0
            timings_ms["total"] = 0.0
            result.setdefault("stats", {})["timings_ms"] = timings_ms
        yield from stream_diff_events(
            iter([{"event_type": "header", "header": _result_to_header(result)}]),
            mode=mode,
            chunk_size=chunk_size,
        )
        return

    started = time.perf_counter()
    context_timings_ms: dict[str, float] | None = {} if timings else None
    context = prepare_diff_context(
        old_graph,
        new_graph,
        profile=profile,
        geometry_policy=geometry_policy,
        guid_policy=guid_policy,
        matcher_policy=matcher_policy,
        timing_collector=context_timings_ms,
    )
    if timings:
        timings_ms: dict[str, float] = {}
        if parse_timings_ms:
            timings_ms.update(parse_timings_ms)
        timings_ms["prepare_context"] = _elapsed_ms(started)
        if context_timings_ms:
            for key in sorted(context_timings_ms):
                timings_ms[f"context.{key}"] = context_timings_ms[key]
        context["stats"]["timings_ms"] = timings_ms
    try:
        events = _iter_stream_events(context)
        yield from stream_diff_events(events, mode=mode, chunk_size=chunk_size)
    finally:
        _close_owner_projectors(context)


def _iter_base_changes(
    context: DiffContext,
    *,
    include_snapshots: bool,
    progress_callback: ProgressCallback | None = None,
):
    profile = context["profile"]
    geometry_policy = context.get("geometry_policy", GEOMETRY_POLICY_STRICT_SYNTAX)
    old_by_id = context["old_by_id"]
    new_by_id = context["new_by_id"]
    old_ids = context["old_ids"]
    new_ids = context["new_ids"]
    old_owner_projector = context["old_owner_projector"]
    new_owner_projector = context["new_owner_projector"]
    old_graph = context["old_graph"]
    new_graph = context["new_graph"]

    entity_ids = sorted(set(old_by_id) | set(new_by_id))
    total = len(entity_ids)
    _emit_progress(progress_callback, {
        "stage": "emit_base_changes",
        "status": "start",
        "completed": 0,
        "total": total,
        "stage_progress": 0.0,
        "overall_progress": _PROGRESS_PREPARE_CONTEXT_WEIGHT,
    })
    change_id = 0
    for idx, entity_id in enumerate(entity_ids, start=1):
        old_items = old_by_id.get(entity_id, [])
        new_items = new_by_id.get(entity_id, [])
        if entity_id.startswith("C:") and len(old_items) == len(new_items):
            continue
        if should_emit_class_delta(entity_id, old_items, new_items):
            change_id += 1
            owner_ids = old_owner_projector.owners_for_steps([item["step_id"] for item in old_items])
            owner_ids.update(new_owner_projector.owners_for_steps([item["step_id"] for item in new_items]))
            yield make_class_delta_change(
                change_id,
                entity_id=entity_id,
                old_items=old_items,
                new_items=new_items,
                owner_ids=owner_ids,
                profile=profile,
            )
            continue

        paired = min(len(old_items), len(new_items))
        for i in range(paired):
            old_item = old_items[i]
            new_item = new_items[i]
            old_ent = old_item["entity"]
            new_ent = new_item["entity"]
            if (
                entity_id.startswith("H:")
                and old_item.get("identity", {}).get("match_method") == "exact_hash"
                and new_item.get("identity", {}).get("match_method") == "exact_hash"
            ):
                continue
            if entities_equal(
                entity_id,
                old_ent,
                new_ent,
                profile=profile,
                old_ids=old_ids,
                new_ids=new_ids,
            ):
                continue
            change_id += 1
            change = make_change(
                change_id,
                op="MODIFY",
                old_entity_id=entity_id,
                new_entity_id=entity_id,
                old_ent=old_ent,
                new_ent=new_ent,
                identity=resolve_identity(old_item, new_item),
                rooted_owners=summarize_rooted_owners(
                    old_owner_projector.owners_for_step(old_item["step_id"])
                    | new_owner_projector.owners_for_step(new_item["step_id"])
                ),
                profile=profile,
                include_snapshots=include_snapshots,
            )
            if (
                geometry_policy == GEOMETRY_POLICY_INVARIANT_PROBE
                and _is_geometry_form_swap_change(change)
                and representation_invariants_match(
                    old_ent,
                    new_ent,
                    old_graph=old_graph,
                    new_graph=new_graph,
                )
            ):
                change_id -= 1
                continue
            yield change

        for old_item in old_items[paired:]:
            old_ent = old_item["entity"]
            change_id += 1
            yield make_change(
                change_id,
                op="REMOVE",
                old_entity_id=entity_id,
                new_entity_id=None,
                old_ent=old_ent,
                new_ent=None,
                identity=resolve_identity(old_item, None),
                rooted_owners=summarize_rooted_owners(
                    old_owner_projector.owners_for_step(old_item["step_id"])
                ),
                profile=profile,
                include_snapshots=include_snapshots,
            )

        for new_item in new_items[paired:]:
            new_ent = new_item["entity"]
            change_id += 1
            yield make_change(
                change_id,
                op="ADD",
                old_entity_id=None,
                new_entity_id=entity_id,
                old_ent=None,
                new_ent=new_ent,
                identity=resolve_identity(None, new_item),
                rooted_owners=summarize_rooted_owners(
                    new_owner_projector.owners_for_step(new_item["step_id"])
                ),
                profile=profile,
                include_snapshots=include_snapshots,
            )
        if idx == 1 or idx % _BASE_CHANGES_PROGRESS_INTERVAL == 0 or idx == total:
            stage_progress = idx / total if total > 0 else 1.0
            _emit_progress(progress_callback, {
                "stage": "emit_base_changes",
                "status": "running",
                "completed": idx,
                "total": total,
                "emitted_changes": change_id,
                "stage_progress": round(stage_progress, 6),
                "overall_progress": round(
                    _PROGRESS_PREPARE_CONTEXT_WEIGHT + (_PROGRESS_EMIT_BASE_CHANGES_WEIGHT * stage_progress),
                    6,
                ),
            })

    _emit_progress(progress_callback, {
        "stage": "emit_base_changes",
        "status": "done",
        "completed": total,
        "total": total,
        "emitted_changes": change_id,
        "stage_progress": 1.0,
        "overall_progress": _PROGRESS_PREPARE_CONTEXT_WEIGHT + _PROGRESS_EMIT_BASE_CHANGES_WEIGHT,
    })


def _iter_stream_events(context: DiffContext):
    yield {"event_type": "header", "header": result_header(context)}
    change_index: dict[str, list[str]] = {}
    for change in _iter_base_changes(context, include_snapshots=False):
        index_change(change_index, change)
        yield {"event_type": "base_change", "change": change}

    derived_markers = build_derived_markers(
        old_graph=context["old_graph"],
        new_graph=context["new_graph"],
        old_ids=context["old_ids"],
        new_ids=context["new_ids"],
        change_index=change_index,
    )
    for marker in derived_markers:
        yield {"event_type": "derived_marker", "marker": marker}


def _close_owner_projectors(context: DiffContext) -> None:
    old_owner_projector = context.get("old_owner_projector")
    if old_owner_projector is not None:
        old_owner_projector.close()
    new_owner_projector = context.get("new_owner_projector")
    if new_owner_projector is not None:
        new_owner_projector.close()


def _paths_refer_to_same_file(old_path: str, new_path: str) -> bool:
    try:
        return os.path.samefile(old_path, new_path)
    except OSError:
        old_norm = os.path.normcase(os.path.abspath(old_path))
        new_norm = os.path.normcase(os.path.abspath(new_path))
        return old_norm == new_norm


def _same_graph_fastpath_result(
    *,
    graph: GraphIR,
    profile: str,
    geometry_policy: str,
    guid_policy: str,
    matcher_policy: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    entity_count = len(graph.get("entities", {}))
    stats: dict[str, Any] = {
        "old_entities": entity_count,
        "new_entities": entity_count,
        "matched": entity_count,
        "matched_by_method": {"exact_hash": entity_count} if entity_count > 0 else {},
        "root_guid_quality": {
            "old": root_guid_quality(graph),
            "new": root_guid_quality(graph),
        },
        "ambiguous": 0,
        "ambiguous_by_stage": {
            "root_remap": 0,
            "path_propagation": 0,
            "secondary_match": 0,
        },
        "stage_match_counts": {
            "root_remap": 0,
            "path_propagation": 0,
            "secondary_match": 0,
        },
        "old_dangling_refs": _dangling_ref_count(graph),
        "new_dangling_refs": _dangling_ref_count(graph),
    }
    schema = graph.get("metadata", {}).get("schema")
    return {
        "version": "2",
        "profile": profile,
        "geometry_policy": geometry_policy,
        "schema_policy": {
            "mode": "same_schema_only",
            "old_schema": schema,
            "new_schema": schema,
        },
        "identity_policy": {
            "guid_policy": guid_policy,
            "matcher_policy": matcher_policy,
        },
        "stats": stats,
        "base_changes": [],
        "derived_markers": [],
    }


def _result_to_header(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "version": result["version"],
        "profile": result["profile"],
        "geometry_policy": result.get("geometry_policy", GEOMETRY_POLICY_STRICT_SYNTAX),
        "schema_policy": result["schema_policy"],
        "identity_policy": result.get("identity_policy"),
        "stats": result["stats"],
    }


def _elapsed_ms(started: float) -> float:
    return round((time.perf_counter() - started) * 1000.0, 3)


def _emit_progress(callback: ProgressCallback | None, payload: dict[str, Any]) -> None:
    if callback is None:
        return
    try:
        callback(payload)
    except Exception:
        return


def _is_geometry_form_swap_change(change: dict[str, Any]) -> bool:
    categories = set(change.get("change_categories") or [])
    if "GEOMETRY" not in categories:
        return False
    allowed = {"GEOMETRY", "ATTRIBUTES", "RELATIONSHIP"}
    return categories <= allowed
