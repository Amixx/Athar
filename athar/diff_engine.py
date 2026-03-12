"""Core diff engine orchestration built on graph parsing and canonical IDs."""

from __future__ import annotations

from typing import Any

from .diff_engine_changes import make_change, make_class_delta_change
from .diff_engine_context import (
    build_result,
    entities_equal,
    index_change,
    prepare_diff_context,
    resolve_identity,
    result_header,
    should_emit_class_delta,
)
from .diff_engine_markers import build_derived_markers, summarize_rooted_owners
from .diff_engine_streaming import json_line, stream_diff_result
from .graph_parser import parse_graph
from .guid_policy import GUID_POLICY_FAIL_FAST, validate_guid_policy
from .profile_policy import DEFAULT_PROFILE


def diff_files(
    old_path: str,
    new_path: str,
    *,
    profile: str = DEFAULT_PROFILE,
    guid_policy: str = GUID_POLICY_FAIL_FAST,
    matcher_policy: dict[str, dict[str, Any]] | None = None,
) -> dict:
    validate_guid_policy(guid_policy)
    old_graph = parse_graph(old_path, profile=profile)
    new_graph = parse_graph(new_path, profile=profile)
    _validate_same_schema(old_graph, new_graph)
    return diff_graphs(
        old_graph,
        new_graph,
        profile=profile,
        guid_policy=guid_policy,
        matcher_policy=matcher_policy,
    )


def diff_graphs(
    old_graph: dict,
    new_graph: dict,
    *,
    profile: str = DEFAULT_PROFILE,
    guid_policy: str = GUID_POLICY_FAIL_FAST,
    matcher_policy: dict[str, dict[str, Any]] | None = None,
) -> dict:
    validate_guid_policy(guid_policy)
    context = prepare_diff_context(
        old_graph,
        new_graph,
        profile=profile,
        guid_policy=guid_policy,
        matcher_policy=matcher_policy,
    )
    base_changes: list[dict[str, Any]] = []
    change_index: dict[str, list[str]] = {}

    for change in _iter_base_changes(context, include_snapshots=True):
        base_changes.append(change)
        index_change(change_index, change)

    derived_markers = build_derived_markers(
        old_graph=context["old_graph"],
        new_graph=context["new_graph"],
        old_ids=context["old_ids"],
        new_ids=context["new_ids"],
        change_index=change_index,
    )
    return build_result(context, base_changes=base_changes, derived_markers=derived_markers)


def stream_diff_files(
    old_path: str,
    new_path: str,
    *,
    profile: str = DEFAULT_PROFILE,
    guid_policy: str = GUID_POLICY_FAIL_FAST,
    matcher_policy: dict[str, dict[str, Any]] | None = None,
    mode: str = "ndjson",
    chunk_size: int = 1000,
):
    """Stream diff output directly from files without materializing full result."""
    validate_guid_policy(guid_policy)
    old_graph = parse_graph(old_path, profile=profile)
    new_graph = parse_graph(new_path, profile=profile)
    _validate_same_schema(old_graph, new_graph)
    yield from stream_diff_graphs(
        old_graph,
        new_graph,
        profile=profile,
        guid_policy=guid_policy,
        matcher_policy=matcher_policy,
        mode=mode,
        chunk_size=chunk_size,
    )


def stream_diff_graphs(
    old_graph: dict,
    new_graph: dict,
    *,
    profile: str = DEFAULT_PROFILE,
    guid_policy: str = GUID_POLICY_FAIL_FAST,
    matcher_policy: dict[str, dict[str, Any]] | None = None,
    mode: str = "ndjson",
    chunk_size: int = 1000,
):
    """Stream diff output directly from graph inputs."""
    if chunk_size <= 0:
        raise ValueError("chunk_size must be > 0")
    validate_guid_policy(guid_policy)

    context = prepare_diff_context(
        old_graph,
        new_graph,
        profile=profile,
        guid_policy=guid_policy,
        matcher_policy=matcher_policy,
    )
    header = result_header(context)
    change_index: dict[str, list[str]] = {}

    if mode == "ndjson":
        yield json_line({"record_type": "header", **header})
        base_count = 0
        op_counts: dict[str, int] = {}
        for base_count, change in enumerate(_iter_base_changes(context, include_snapshots=False), start=1):
            index_change(change_index, change)
            op = change.get("op")
            if isinstance(op, str):
                op_counts[op] = op_counts.get(op, 0) + 1
            yield json_line({"record_type": "base_change", "index": base_count - 1, "change": change})
        derived_markers = build_derived_markers(
            old_graph=context["old_graph"],
            new_graph=context["new_graph"],
            old_ids=context["old_ids"],
            new_ids=context["new_ids"],
            change_index=change_index,
        )
        for idx, marker in enumerate(derived_markers):
            yield json_line({"record_type": "derived_marker", "index": idx, "marker": marker})
        yield json_line({
            "record_type": "end",
            "base_change_count": base_count,
            "derived_marker_count": len(derived_markers),
            "op_counts": {k: op_counts[k] for k in sorted(op_counts)},
        })
        return

    if mode == "chunked_json":
        yield json_line({"chunk_type": "header", **header})
        buffer: list[dict[str, Any]] = []
        offset = 0
        base_count = 0
        op_counts: dict[str, int] = {}
        for change in _iter_base_changes(context, include_snapshots=False):
            index_change(change_index, change)
            buffer.append(change)
            base_count += 1
            op = change.get("op")
            if isinstance(op, str):
                op_counts[op] = op_counts.get(op, 0) + 1
            if len(buffer) >= chunk_size:
                yield json_line({
                    "chunk_type": "base_changes",
                    "offset": offset,
                    "count": len(buffer),
                    "items": buffer,
                })
                offset += len(buffer)
                buffer = []
        if buffer:
            yield json_line({
                "chunk_type": "base_changes",
                "offset": offset,
                "count": len(buffer),
                "items": buffer,
            })

        derived_markers = build_derived_markers(
            old_graph=context["old_graph"],
            new_graph=context["new_graph"],
            old_ids=context["old_ids"],
            new_ids=context["new_ids"],
            change_index=change_index,
        )
        marker_offset = 0
        for idx in range(0, len(derived_markers), chunk_size):
            items = derived_markers[idx: idx + chunk_size]
            yield json_line({
                "chunk_type": "derived_markers",
                "offset": marker_offset,
                "count": len(items),
                "items": items,
            })
            marker_offset += len(items)
        yield json_line({
            "chunk_type": "end",
            "base_change_count": base_count,
            "derived_marker_count": len(derived_markers),
            "op_counts": {k: op_counts[k] for k in sorted(op_counts)},
        })
        return

    raise ValueError(f"Unknown stream mode: {mode}")


def _validate_same_schema(old_graph: dict, new_graph: dict) -> None:
    old_schema = old_graph.get("metadata", {}).get("schema")
    new_schema = new_graph.get("metadata", {}).get("schema")
    if old_schema != new_schema:
        raise ValueError(f"Schema mismatch: {old_schema} vs {new_schema}")


def _iter_base_changes(context: dict[str, Any], *, include_snapshots: bool):
    profile = context["profile"]
    old_by_id = context["old_by_id"]
    new_by_id = context["new_by_id"]
    old_owner_projector = context["old_owner_projector"]
    new_owner_projector = context["new_owner_projector"]

    change_id = 0
    for entity_id in sorted(set(old_by_id) | set(new_by_id)):
        old_items = sorted(old_by_id.get(entity_id, []), key=lambda item: item["step_id"])
        new_items = sorted(new_by_id.get(entity_id, []), key=lambda item: item["step_id"])
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
            if entities_equal(entity_id, old_ent, new_ent, profile=profile):
                continue
            change_id += 1
            yield make_change(
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
