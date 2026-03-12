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
    remap_ambiguous: int,
    path_ambiguous: int,
    secondary_ambiguous: int,
    remap_matches: int,
    path_matches: int,
    secondary_matches: int,
    old_dangling_refs: int,
    new_dangling_refs: int,
) -> dict[str, Any]:
    ambiguous_total = remap_ambiguous + path_ambiguous + secondary_ambiguous
    return {
        "old_entities": len(old_graph.get("entities", {})),
        "new_entities": len(new_graph.get("entities", {})),
        "matched": matched_occurrence_count(old_by_id, new_by_id),
        "matched_by_method": matched_by_method(old_by_id, new_by_id),
        "root_guid_quality": {
            "old": root_guid_quality(old_graph),
            "new": root_guid_quality(new_graph),
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


def matched_occurrence_count(old_by_id: dict[str, list[dict]], new_by_id: dict[str, list[dict]]) -> int:
    total = 0
    for entity_id in set(old_by_id) & set(new_by_id):
        total += min(len(old_by_id[entity_id]), len(new_by_id[entity_id]))
    return total


def matched_by_method(old_by_id: dict[str, list[dict]], new_by_id: dict[str, list[dict]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for entity_id in set(old_by_id) & set(new_by_id):
        old_items = old_by_id[entity_id]
        new_items = new_by_id[entity_id]
        paired = min(len(old_items), len(new_items))
        for idx in range(paired):
            method = old_items[idx].get("identity", {}).get("match_method", "exact_hash")
            counts[method] = counts.get(method, 0) + 1
    return {method: counts[method] for method in sorted(counts)}


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
