"""Matcher stages for the graph diff engine.

Current stage:
- deterministic typed-path propagation from already matched root pairs
"""

from __future__ import annotations

from collections import defaultdict, deque
from typing import Any

from .semantic_signature import semantic_signature

_SECONDARY_SCORE_THRESHOLD = 0.55
_SECONDARY_SCORE_MARGIN = 0.10
_SECONDARY_ASSIGNMENT_MAX = 8


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

    Deterministic conservative implementation:
    - block by `entity_type`
    - score candidates by structural + literal overlap
    - run deterministic assignment on small buckets
    - reject ties/below-margin outcomes as ambiguous
    """
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
        old_features[step_id] = _feature_vector(entity)

    for step_id, entity in new_entities.items():
        if step_id in used_new or _is_root(entity):
            continue
        new_blocks[entity.get("entity_type")].append(step_id)
        new_features[step_id] = _feature_vector(entity)

    matches: dict[int, int] = {}
    ambiguous = 0
    for key in sorted(set(old_blocks) & set(new_blocks)):
        old_steps = sorted(old_blocks[key])
        new_steps = sorted(new_blocks[key])
        if not old_steps or not new_steps:
            continue

        if len(old_steps) > _SECONDARY_ASSIGNMENT_MAX or len(new_steps) > _SECONDARY_ASSIGNMENT_MAX:
            block_matches, block_ambiguous = _fallback_signature_block_match(
                old_steps, new_steps, old_features, new_features
            )
        else:
            block_matches, block_ambiguous = _assignment_block_match(
                old_steps, new_steps, old_features, new_features
            )

        for old_step, new_step in block_matches.items():
            matches[old_step] = new_step
            used_old.add(old_step)
            used_new.add(new_step)
        ambiguous += block_ambiguous

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


def _feature_vector(entity: dict) -> dict[str, Any]:
    return {
        "semantic_signature": semantic_signature(entity),
        "attribute_paths": _collect_attribute_paths(entity.get("attributes", {}), base_path=""),
        "literal_tokens": _collect_literal_tokens(entity.get("attributes", {})),
        "edge_labels": _edge_label_set(entity.get("refs", [])),
        "degree": len(entity.get("refs", [])),
    }


def _collect_attribute_paths(value: Any, *, base_path: str) -> set[str]:
    paths: set[str] = set()
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = f"{base_path}/{key}"
            paths.add(child_path)
            paths.update(_collect_attribute_paths(child, base_path=child_path))
    elif isinstance(value, list):
        for idx, child in enumerate(value):
            child_path = f"{base_path}/{idx}"
            paths.add(child_path)
            paths.update(_collect_attribute_paths(child, base_path=child_path))
    return paths


def _collect_literal_tokens(value: Any) -> set[str]:
    tokens: set[str] = set()
    if isinstance(value, dict):
        kind = value.get("kind")
        if kind in {"bool", "int", "real", "string"}:
            tokens.add(f"{kind}:{value.get('value')}")
        for child in value.values():
            tokens.update(_collect_literal_tokens(child))
    elif isinstance(value, list):
        for child in value:
            tokens.update(_collect_literal_tokens(child))
    return tokens


def _edge_label_set(refs: list[dict]) -> set[tuple[str, str | None]]:
    return {
        (ref.get("path", ""), ref.get("target_type"))
        for ref in refs
    }


def _similarity(old_f: dict[str, Any], new_f: dict[str, Any]) -> float:
    semantic = 1.0 if old_f["semantic_signature"] == new_f["semantic_signature"] else 0.0
    attr_paths = _jaccard(old_f["attribute_paths"], new_f["attribute_paths"])
    literals = _jaccard(old_f["literal_tokens"], new_f["literal_tokens"])
    edges = _jaccard(old_f["edge_labels"], new_f["edge_labels"])
    degree = _degree_similarity(old_f["degree"], new_f["degree"])
    return (
        0.35 * semantic
        + 0.20 * attr_paths
        + 0.15 * literals
        + 0.20 * edges
        + 0.10 * degree
    )


def _jaccard(left: set[Any], right: set[Any]) -> float:
    if not left and not right:
        return 1.0
    union = left | right
    if not union:
        return 1.0
    return len(left & right) / len(union)


def _degree_similarity(old_degree: int, new_degree: int) -> float:
    max_degree = max(1, old_degree, new_degree)
    return 1.0 - (abs(old_degree - new_degree) / max_degree)


def _fallback_signature_block_match(
    old_steps: list[int],
    new_steps: list[int],
    old_features: dict[int, dict[str, Any]],
    new_features: dict[int, dict[str, Any]],
) -> tuple[dict[int, int], int]:
    old_sig: defaultdict[str, list[int]] = defaultdict(list)
    new_sig: defaultdict[str, list[int]] = defaultdict(list)
    for step in old_steps:
        old_sig[old_features[step]["semantic_signature"]].append(step)
    for step in new_steps:
        new_sig[new_features[step]["semantic_signature"]].append(step)

    matches: dict[int, int] = {}
    ambiguous = 0
    for sig in sorted(set(old_sig) & set(new_sig)):
        left = sorted(old_sig[sig])
        right = sorted(new_sig[sig])
        if len(left) == 1 and len(right) == 1:
            matches[left[0]] = right[0]
        elif left and right:
            ambiguous += min(len(left), len(right))
    return matches, ambiguous


def _assignment_block_match(
    old_steps: list[int],
    new_steps: list[int],
    old_features: dict[int, dict[str, Any]],
    new_features: dict[int, dict[str, Any]],
) -> tuple[dict[int, int], int]:
    old_steps = sorted(old_steps)
    new_steps = sorted(new_steps)
    score_map = _score_map(old_steps, new_steps, old_features, new_features)

    best, second, tie_for_best = _enumerate_assignments(old_steps, new_steps, score_map)
    if best is None or best["match_count"] == 0:
        return {}, 0
    if tie_for_best:
        return {}, min(len(old_steps), len(new_steps))
    if second is not None and best["match_count"] == second["match_count"]:
        if (best["total_score"] - second["total_score"]) < _SECONDARY_SCORE_MARGIN:
            return {}, min(len(old_steps), len(new_steps))
    return dict(best["pairs"]), 0


def _score_map(
    old_steps: list[int],
    new_steps: list[int],
    old_features: dict[int, dict[str, Any]],
    new_features: dict[int, dict[str, Any]],
) -> dict[tuple[int, int], float]:
    scores: dict[tuple[int, int], float] = {}
    for old_step in old_steps:
        for new_step in new_steps:
            score = _similarity(old_features[old_step], new_features[new_step])
            if score >= _SECONDARY_SCORE_THRESHOLD:
                scores[(old_step, new_step)] = score
    return scores


def _enumerate_assignments(
    old_steps: list[int],
    new_steps: list[int],
    score_map: dict[tuple[int, int], float],
) -> tuple[dict[str, Any] | None, dict[str, Any] | None, bool]:
    best: dict[str, Any] | None = None
    second: dict[str, Any] | None = None
    tie_for_best = False

    def consider(candidate: dict[str, Any]) -> None:
        nonlocal best, second, tie_for_best
        if best is None:
            best = candidate
            return
        cmp = _compare_candidate(candidate, best)
        if cmp > 0:
            second = best
            best = candidate
            tie_for_best = False
            return
        if cmp == 0:
            tie_for_best = True
            if second is None or _compare_candidate(candidate, second) > 0:
                second = candidate
            return
        if second is None or _compare_candidate(candidate, second) > 0:
            second = candidate

    def walk(idx: int, used_new: set[int], pairs: list[tuple[int, int]], total_score: float) -> None:
        if idx >= len(old_steps):
            consider({
                "pairs": tuple(sorted(pairs)),
                "match_count": len(pairs),
                "total_score": total_score,
            })
            return

        old_step = old_steps[idx]
        # Option 1: skip this old step.
        walk(idx + 1, used_new, pairs, total_score)

        # Option 2: pair with eligible new steps.
        options = [
            (new_step, score_map[(old_step, new_step)])
            for new_step in new_steps
            if new_step not in used_new and (old_step, new_step) in score_map
        ]
        options.sort(key=lambda item: (-item[1], item[0]))
        for new_step, score in options:
            used_new.add(new_step)
            pairs.append((old_step, new_step))
            walk(idx + 1, used_new, pairs, total_score + score)
            pairs.pop()
            used_new.remove(new_step)

    walk(0, set(), [], 0.0)
    return best, second, tie_for_best


def _compare_candidate(left: dict[str, Any], right: dict[str, Any]) -> int:
    left_key = (left["match_count"], round(left["total_score"], 12), left["pairs"])
    right_key = (right["match_count"], round(right["total_score"], 12), right["pairs"])
    if left_key > right_key:
        return 1
    if left_key < right_key:
        return -1
    return 0
