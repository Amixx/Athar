"""Stats helpers for graph diff context."""

from __future__ import annotations

from collections import Counter
from typing import Any


def build_stats(
    *,
    old_graph: dict,
    new_graph: dict,
    old_by_id: dict[str, list[dict]],
    new_by_id: dict[str, list[dict]],
    old_index_summary: dict[str, Any] | None = None,
    new_index_summary: dict[str, Any] | None = None,
    remap_ambiguous: int,
    path_ambiguous: int,
    secondary_ambiguous: int,
    remap_matches: int,
    path_matches: int,
    secondary_matches: int,
    old_dangling_refs: int,
    new_dangling_refs: int,
    old_guid_quality: dict[str, int] | None = None,
    new_guid_quality: dict[str, int] | None = None,
) -> dict[str, Any]:
    ambiguous_total = remap_ambiguous + path_ambiguous + secondary_ambiguous
    matched_total, matched_methods = _matched_summary(
        old_by_id,
        new_by_id,
        old_index_summary=old_index_summary,
        new_index_summary=new_index_summary,
    )
    return {
        "old_entities": len(old_graph.get("entities", {})),
        "new_entities": len(new_graph.get("entities", {})),
        "matched": matched_total,
        "matched_by_method": matched_methods,
        "root_guid_quality": {
            "old": old_guid_quality if old_guid_quality is not None else root_guid_quality(old_graph),
            "new": new_guid_quality if new_guid_quality is not None else root_guid_quality(new_graph),
        },
        "ambiguous": ambiguous_total,
        "ambiguous_by_stage": {
            "root_remap": remap_ambiguous,
            "path_propagation": path_ambiguous,
            "secondary_match": secondary_ambiguous,
        },
        "stage_match_counts": {
            "root_remap": remap_matches,
            "path_propagation": path_matches,
            "secondary_match": secondary_matches,
        },
        "old_dangling_refs": old_dangling_refs,
        "new_dangling_refs": new_dangling_refs,
    }


def _matched_summary(
    old_by_id: dict[str, list[dict]],
    new_by_id: dict[str, list[dict]],
    *,
    old_index_summary: dict[str, Any] | None = None,
    new_index_summary: dict[str, Any] | None = None,
) -> tuple[int, dict[str, int]]:
    if old_index_summary is not None and new_index_summary is not None:
        old_counts = old_index_summary.get("count_by_id")
        new_counts = new_index_summary.get("count_by_id")
        old_methods = old_index_summary.get("methods_by_id")
        if isinstance(old_counts, dict) and isinstance(new_counts, dict) and isinstance(old_methods, dict):
            return _matched_summary_from_index_summaries(
                old_counts,
                new_counts,
                old_methods,
            )

    matched_total = 0
    counts: dict[str, int] = {}
    left, right = (old_by_id, new_by_id) if len(old_by_id) <= len(new_by_id) else (new_by_id, old_by_id)
    for entity_id, left_items in left.items():
        right_items = right.get(entity_id)
        if right_items is None:
            continue
        old_items = left_items if left is old_by_id else right_items
        new_items = right_items if left is old_by_id else left_items
        paired = min(len(old_items), len(new_items))
        matched_total += paired
        for idx in range(paired):
            method = old_items[idx].get("identity", {}).get("match_method", "exact_hash")
            counts[method] = counts.get(method, 0) + 1
    return matched_total, {method: counts[method] for method in sorted(counts)}


def _matched_summary_from_index_summaries(
    old_count_by_id: dict[str, int],
    new_count_by_id: dict[str, int],
    old_methods_by_id: dict[str, list[str]],
) -> tuple[int, dict[str, int]]:
    matched_total = 0
    counts: dict[str, int] = {}
    for entity_id, old_count in old_count_by_id.items():
        paired = min(old_count, new_count_by_id.get(entity_id, 0))
        if paired <= 0:
            continue
        matched_total += paired
        methods = old_methods_by_id.get(entity_id, [])
        for idx in range(paired):
            method = methods[idx] if idx < len(methods) else "exact_hash"
            counts[method] = counts.get(method, 0) + 1
    return matched_total, {method: counts[method] for method in sorted(counts)}


def matched_occurrence_count(old_by_id: dict[str, list[dict]], new_by_id: dict[str, list[dict]]) -> int:
    total, _methods = _matched_summary(old_by_id, new_by_id)
    return total


def matched_by_method(old_by_id: dict[str, list[dict]], new_by_id: dict[str, list[dict]]) -> dict[str, int]:
    _total, methods = _matched_summary(old_by_id, new_by_id)
    return methods


def root_guid_quality(graph: dict) -> dict[str, int]:
    counts: Counter[str] = Counter()
    invalid = 0
    for entity in graph.get("entities", {}).values():
        gid = entity.get("global_id")
        if gid is None:
            continue
        if not isinstance(gid, str) or gid.strip() == "":
            invalid += 1
            continue
        counts[gid] += 1

    duplicate_ids = sum(1 for c in counts.values() if c > 1)
    duplicate_occurrences = sum(c for c in counts.values() if c > 1)
    valid_total = sum(counts.values())
    unique_valid = sum(1 for c in counts.values() if c == 1)
    return {
        "valid_total": valid_total,
        "unique_valid": unique_valid,
        "duplicate_ids": duplicate_ids,
        "duplicate_occurrences": duplicate_occurrences,
        "invalid": invalid,
    }
