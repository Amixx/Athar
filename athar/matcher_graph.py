"""Low-level matcher stages for the graph diff engine.

Current stage:
- deterministic typed-path propagation from already matched root pairs
"""

from __future__ import annotations

from collections import defaultdict, deque
from typing import Any

from .semantic_signature import semantic_signature


def propagate_matches_by_typed_path(
    old_graph: dict,
    new_graph: dict,
    root_pairs: dict[int, int],
    *,
    pre_matched_old: set[int] | None = None,
    pre_matched_new: set[int] | None = None,
) -> dict[str, Any]:
    """Match unique non-root targets by typed path propagation.

    For each matched parent pair, compare outgoing edge buckets keyed by
    `(path, target_type)`. Only unique 1:1 buckets are accepted; ties are
    rejected as ambiguous.
    """
    old_entities = old_graph.get("entities", {})
    new_entities = new_graph.get("entities", {})

    used_old = set(pre_matched_old or set())
    used_new = set(pre_matched_new or set())

    queue: deque[tuple[int, int]] = deque(sorted(root_pairs.items(), key=lambda p: p[0]))
    matches: dict[int, int] = {}
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
                used_old.add(old_target)
                used_new.add(new_target)
                queue.append((old_target, new_target))
            elif old_targets and new_targets:
                ambiguous += min(len(old_targets), len(new_targets))

    return {
        "method": "typed_path_propagation",
        "old_to_new": matches,
        "ambiguous": ambiguous,
    }


def secondary_match_unresolved(
    old_graph: dict,
    new_graph: dict,
    *,
    pre_matched_old: set[int] | None = None,
    pre_matched_new: set[int] | None = None,
) -> dict[str, Any]:
    """Deterministic secondary matcher for unmatched non-root entities.

    Current conservative implementation:
    - block by `(entity_type, semantic_signature)`
    - accept only unique 1:1 buckets
    - reject ties as ambiguous
    """
    old_entities = old_graph.get("entities", {})
    new_entities = new_graph.get("entities", {})
    used_old = set(pre_matched_old or set())
    used_new = set(pre_matched_new or set())

    old_blocks: defaultdict[tuple[str | None, str], list[int]] = defaultdict(list)
    new_blocks: defaultdict[tuple[str | None, str], list[int]] = defaultdict(list)

    for step_id, entity in old_entities.items():
        if step_id in used_old or _is_root(entity):
            continue
        old_blocks[_blocking_key(entity)].append(step_id)

    for step_id, entity in new_entities.items():
        if step_id in used_new or _is_root(entity):
            continue
        new_blocks[_blocking_key(entity)].append(step_id)

    matches: dict[int, int] = {}
    ambiguous = 0
    for key in sorted(set(old_blocks) & set(new_blocks)):
        old_steps = sorted(old_blocks[key])
        new_steps = sorted(new_blocks[key])
        if len(old_steps) == 1 and len(new_steps) == 1:
            matches[old_steps[0]] = new_steps[0]
            used_old.add(old_steps[0])
            used_new.add(new_steps[0])
        elif old_steps and new_steps:
            ambiguous += min(len(old_steps), len(new_steps))

    return {
        "method": "secondary_match",
        "old_to_new": matches,
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


def _blocking_key(entity: dict) -> tuple[str | None, str]:
    return (entity.get("entity_type"), semantic_signature(entity))
