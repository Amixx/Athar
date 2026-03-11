"""Matcher stages for the graph diff engine."""

from __future__ import annotations

from collections import defaultdict, deque
from typing import Any

from .matcher_graph_scoring import (
    SECONDARY_ASSIGNMENT_MAX,
    assignment_block_match,
    build_feature_vector,
    fallback_signature_block_match,
)


def propagate_matches_by_typed_path(
    old_graph: dict,
    new_graph: dict,
    root_pairs: dict[int, int],
    *,
    pre_matched_old: set[int] | None = None,
    pre_matched_new: set[int] | None = None,
) -> dict[str, Any]:
    """Match unique non-root targets by typed path propagation."""
    old_entities = old_graph.get("entities", {})
    new_entities = new_graph.get("entities", {})

    used_old = set(pre_matched_old or set())
    used_new = set(pre_matched_new or set())

    queue: deque[tuple[int, int]] = deque(sorted(root_pairs.items(), key=lambda p: p[0]))
    matches: dict[int, int] = {}
    diagnostics: dict[int, dict[str, Any]] = {}
    ambiguous = 0

    while queue:
        old_parent, new_parent = queue.popleft()
        old_buckets = _edge_buckets(old_entities, old_parent, used_old)
        new_buckets = _edge_buckets(new_entities, new_parent, used_new)

        for edge_key in sorted(set(old_buckets) & set(new_buckets)):
            old_targets = old_buckets[edge_key]
            new_targets = new_buckets[edge_key]
            if len(old_targets) == 1 and len(new_targets) == 1:
                old_target = old_targets[0]
                new_target = new_targets[0]
                if _is_root(old_entities.get(old_target)) or _is_root(new_entities.get(new_target)):
                    continue
                matches[old_target] = new_target
                diagnostics[old_target] = {
                    "match_confidence": 1.0,
                    "matched_on": {
                        "stage": "typed_path",
                        "path": edge_key[0],
                        "target_type": edge_key[1],
                    },
                }
                used_old.add(old_target)
                used_new.add(new_target)
                queue.append((old_target, new_target))
            elif old_targets and new_targets:
                ambiguous += min(len(old_targets), len(new_targets))

    return {
        "method": "typed_path_propagation",
        "old_to_new": matches,
        "diagnostics": diagnostics,
        "ambiguous": ambiguous,
    }


def secondary_match_unresolved(
    old_graph: dict,
    new_graph: dict,
    *,
    pre_matched_old: set[int] | None = None,
    pre_matched_new: set[int] | None = None,
) -> dict[str, Any]:
    """Deterministic secondary matcher for unmatched non-root entities."""
    old_entities = old_graph.get("entities", {})
    new_entities = new_graph.get("entities", {})
    used_old = set(pre_matched_old or set())
    used_new = set(pre_matched_new or set())

    old_blocks: defaultdict[str | None, list[int]] = defaultdict(list)
    new_blocks: defaultdict[str | None, list[int]] = defaultdict(list)
    old_features: dict[int, dict[str, Any]] = {}
    new_features: dict[int, dict[str, Any]] = {}

    for step_id, entity in old_entities.items():
        if step_id in used_old or _is_root(entity):
            continue
        old_blocks[entity.get("entity_type")].append(step_id)
        old_features[step_id] = build_feature_vector(entity)

    for step_id, entity in new_entities.items():
        if step_id in used_new or _is_root(entity):
            continue
        new_blocks[entity.get("entity_type")].append(step_id)
        new_features[step_id] = build_feature_vector(entity)

    matches: dict[int, int] = {}
    diagnostics: dict[int, dict[str, Any]] = {}
    ambiguous = 0
    for key in sorted(set(old_blocks) & set(new_blocks)):
        old_steps = sorted(old_blocks[key])
        new_steps = sorted(new_blocks[key])
        if not old_steps or not new_steps:
            continue

        if len(old_steps) > SECONDARY_ASSIGNMENT_MAX or len(new_steps) > SECONDARY_ASSIGNMENT_MAX:
            block_matches, block_diagnostics, block_ambiguous = fallback_signature_block_match(
                old_steps, new_steps, old_features, new_features
            )
        else:
            block_matches, block_diagnostics, block_ambiguous = assignment_block_match(
                old_steps, new_steps, old_features, new_features
            )

        for old_step, new_step in block_matches.items():
            matches[old_step] = new_step
            if old_step in block_diagnostics:
                diagnostics[old_step] = block_diagnostics[old_step]
            used_old.add(old_step)
            used_new.add(new_step)
        ambiguous += block_ambiguous

    return {
        "method": "secondary_match",
        "old_to_new": matches,
        "diagnostics": diagnostics,
        "ambiguous": ambiguous,
    }


def _edge_buckets(
    entities: dict[int, dict],
    parent_step: int,
    used_steps: set[int],
) -> dict[tuple[str, str | None], list[int]]:
    parent = entities.get(parent_step)
    buckets: defaultdict[tuple[str, str | None], list[int]] = defaultdict(list)
    if not parent:
        return {}
    for ref in parent.get("refs", []):
        target = ref.get("target")
        if target is None or target in used_steps:
            continue
        if target not in entities:
            continue
        edge_key = (ref.get("path", ""), ref.get("target_type"))
        buckets[edge_key].append(target)
    for targets in buckets.values():
        targets.sort()
    return dict(buckets)


def _is_root(entity: dict | None) -> bool:
    if not entity:
        return False
    return bool(entity.get("global_id"))
