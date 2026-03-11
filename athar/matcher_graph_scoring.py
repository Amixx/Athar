"""Secondary matcher feature extraction and scoring/assignment helpers."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from .semantic_signature import semantic_signature

SECONDARY_SCORE_THRESHOLD = 0.55
SECONDARY_SCORE_MARGIN = 0.10
SECONDARY_ASSIGNMENT_MAX = 8


def build_feature_vector(entity: dict) -> dict[str, Any]:
    return {
        "semantic_signature": semantic_signature(entity),
        "attribute_paths": _collect_attribute_paths(entity.get("attributes", {}), base_path=""),
        "literal_tokens": _collect_literal_tokens(entity.get("attributes", {})),
        "edge_labels": _edge_label_set(entity.get("refs", [])),
        "degree": len(entity.get("refs", [])),
    }


def fallback_signature_block_match(
    old_steps: list[int],
    new_steps: list[int],
    old_features: dict[int, dict[str, Any]],
    new_features: dict[int, dict[str, Any]],
) -> tuple[dict[int, int], dict[int, dict[str, Any]], int]:
    old_sig: defaultdict[str, list[int]] = defaultdict(list)
    new_sig: defaultdict[str, list[int]] = defaultdict(list)
    for step in old_steps:
        old_sig[old_features[step]["semantic_signature"]].append(step)
    for step in new_steps:
        new_sig[new_features[step]["semantic_signature"]].append(step)

    matches: dict[int, int] = {}
    diagnostics: dict[int, dict[str, Any]] = {}
    ambiguous = 0
    for sig in sorted(set(old_sig) & set(new_sig)):
        left = sorted(old_sig[sig])
        right = sorted(new_sig[sig])
        if len(left) == 1 and len(right) == 1:
            matches[left[0]] = right[0]
            diagnostics[left[0]] = {
                "match_confidence": 1.0,
                "matched_on": {
                    "stage": "signature_unique",
                    "semantic_signature": sig,
                },
            }
        elif left and right:
            ambiguous += min(len(left), len(right))
    return matches, diagnostics, ambiguous


def assignment_block_match(
    old_steps: list[int],
    new_steps: list[int],
    old_features: dict[int, dict[str, Any]],
    new_features: dict[int, dict[str, Any]],
) -> tuple[dict[int, int], dict[int, dict[str, Any]], int]:
    old_steps = sorted(old_steps)
    new_steps = sorted(new_steps)
    score_map = _score_map(old_steps, new_steps, old_features, new_features)

    best, second, tie_for_best = _enumerate_assignments(old_steps, new_steps, score_map)
    if best is None or best["match_count"] == 0:
        return {}, {}, 0
    if tie_for_best:
        return {}, {}, min(len(old_steps), len(new_steps))
    if second is not None and best["match_count"] == second["match_count"]:
        if (best["total_score"] - second["total_score"]) < SECONDARY_SCORE_MARGIN:
            return {}, {}, min(len(old_steps), len(new_steps))

    matches = dict(best["pairs"])
    diagnostics = {
        old_step: {
            "match_confidence": score_map[(old_step, new_step)],
            "matched_on": {
                "stage": "scored_assignment",
                "score": score_map[(old_step, new_step)],
            },
        }
        for old_step, new_step in best["pairs"]
    }
    return matches, diagnostics, 0


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
            if score >= SECONDARY_SCORE_THRESHOLD:
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
        walk(idx + 1, used_new, pairs, total_score)

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
