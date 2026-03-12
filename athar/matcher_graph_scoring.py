"""Secondary matcher feature extraction and scoring/assignment helpers."""

from __future__ import annotations

from collections import defaultdict, deque
import hashlib
import json
from math import inf
from typing import Any

from .semantic_signature import semantic_signature

SECONDARY_SCORE_THRESHOLD = 0.55
SECONDARY_SCORE_MARGIN = 0.10
SECONDARY_ASSIGNMENT_AMBIGUITY_MARGIN = 0.08
SECONDARY_ASSIGNMENT_MAX = 8
SECONDARY_DEEPENING_DEPTH2_MAX = 6
SECONDARY_DEEPENING_DEPTH3_MAX = 4
_DISALLOWED_COST = 1_000_000.0


def build_feature_vector(
    entity: dict,
    *,
    step_id: int | None = None,
    entities: dict[int, dict] | None = None,
    adjacency: dict[int, list[tuple[str, str | None, int]]] | None = None,
    reverse_adjacency: dict[int, list[tuple[str, str | None, int]]] | None = None,
) -> dict[str, Any]:
    entity_type = entity.get("entity_type")
    feature = {
        "entity_type": entity_type,
        "entity_family": entity_type_family(entity_type),
        "semantic_signature": semantic_signature(entity),
        "attribute_paths": _collect_attribute_paths(entity.get("attributes", {}), base_path=""),
        "literal_tokens": _collect_literal_tokens(entity.get("attributes", {})),
        "edge_labels": _edge_label_set(entity.get("refs", [])),
        "neighborhood_digest": _edge_multiset_digest(entity.get("refs", [])),
        "degree": len(entity.get("refs", [])),
        "ancestry_d1": set(),
        "ancestry_d2": set(),
        "neighbor_depth2_digest": "0" * 64,
        "neighbor_depth3_digest": "0" * 64,
    }

    if (
        step_id is not None
        and entities is not None
        and adjacency is not None
        and reverse_adjacency is not None
    ):
        ancestry_d1, ancestry_d2 = _ancestry_tokens(step_id, entities, reverse_adjacency)
        feature["ancestry_d1"] = ancestry_d1
        feature["ancestry_d2"] = ancestry_d2
        feature["neighbor_depth2_digest"] = _neighbor_depth_digest(step_id, entities, adjacency, depth=2)
        feature["neighbor_depth3_digest"] = _neighbor_depth_digest(step_id, entities, adjacency, depth=3)
    return feature


def entity_type_family(entity_type: str | None) -> str | None:
    if entity_type is None:
        return None
    if entity_type.endswith("StandardCase"):
        return entity_type[: -len("StandardCase")]
    return entity_type


def blocking_key(entity_type: str | None, features: dict[str, Any]) -> tuple[str | None, int, int, int, int, int, int]:
    """Coarse deterministic blocking key for secondary matching."""
    degree = int(features.get("degree", 0))
    edge_count = len(features.get("edge_labels", set()))
    attr_count = len(features.get("attribute_paths", set()))
    literal_count = len(features.get("literal_tokens", set()))
    ancestry_count = len(features.get("ancestry_d1", set()))
    digest = str(features.get("neighborhood_digest", "0"))
    nibble = int(digest[0], 16) if digest and digest[0] in "0123456789abcdef" else 0
    neighborhood_bucket = nibble // 4
    return (
        entity_type,
        min(8, degree // 2),
        min(8, edge_count // 2),
        min(8, attr_count // 4),
        min(8, literal_count // 4),
        min(8, ancestry_count // 2),
        neighborhood_bucket,
    )


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
                    "depth": 1,
                },
            }
        elif left and right:
            ambiguous += min(len(left), len(right))
    return matches, diagnostics, ambiguous


def iterative_assignment_block_match(
    old_steps: list[int],
    new_steps: list[int],
    old_features: dict[int, dict[str, Any]],
    new_features: dict[int, dict[str, Any]],
    *,
    score_threshold: float = SECONDARY_SCORE_THRESHOLD,
    score_margin: float = SECONDARY_ASSIGNMENT_AMBIGUITY_MARGIN,
    depth2_max: int = SECONDARY_DEEPENING_DEPTH2_MAX,
    depth3_max: int = SECONDARY_DEEPENING_DEPTH3_MAX,
) -> tuple[dict[int, int], dict[int, dict[str, Any]], int]:
    depths = [1]
    limit = min(len(old_steps), len(new_steps))
    if limit <= depth2_max:
        depths.append(2)
    if limit <= depth3_max:
        depths.append(3)

    last = ({}, {}, 0)
    for depth in depths:
        last = assignment_block_match(
            old_steps,
            new_steps,
            old_features,
            new_features,
            depth=depth,
            score_threshold=score_threshold,
            score_margin=score_margin,
        )
        matches, diagnostics, ambiguous = last
        if ambiguous == 0 or depth == depths[-1]:
            for step in list(diagnostics):
                matched_on = diagnostics[step].get("matched_on") or {}
                diagnostics[step] = {
                    **diagnostics[step],
                    "matched_on": {**matched_on, "depth": depth},
                }
            return matches, diagnostics, ambiguous
    return last


def assignment_block_match(
    old_steps: list[int],
    new_steps: list[int],
    old_features: dict[int, dict[str, Any]],
    new_features: dict[int, dict[str, Any]],
    *,
    depth: int,
    score_threshold: float = SECONDARY_SCORE_THRESHOLD,
    score_margin: float = SECONDARY_ASSIGNMENT_AMBIGUITY_MARGIN,
) -> tuple[dict[int, int], dict[int, dict[str, Any]], int]:
    old_steps = sorted(old_steps)
    new_steps = sorted(new_steps)
    score_map = _score_map(
        old_steps,
        new_steps,
        old_features,
        new_features,
        depth=depth,
        score_threshold=score_threshold,
    )
    if not score_map:
        return {}, {}, 0

    matches = _min_cost_bipartite_assignment(
        old_steps,
        new_steps,
        score_map,
        score_threshold=score_threshold,
    )
    if not matches:
        return {}, {}, 0

    if _is_assignment_ambiguous(
        old_steps,
        new_steps,
        matches,
        score_map,
        score_margin=score_margin,
    ):
        return {}, {}, min(len(old_steps), len(new_steps))

    diagnostics = {
        old_step: {
            "match_confidence": score_map[(old_step, new_step)],
            "matched_on": {
                "stage": "scored_assignment",
                "solver": "min_cost_bipartite",
                "score": score_map[(old_step, new_step)],
            },
        }
        for old_step, new_step in sorted(matches.items())
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
    return {(ref.get("path", ""), entity_type_family(ref.get("target_type"))) for ref in refs}


def _edge_multiset_digest(refs: list[dict]) -> str:
    counts: defaultdict[tuple[str, str | None], int] = defaultdict(int)
    for ref in refs:
        counts[(ref.get("path", ""), entity_type_family(ref.get("target_type")))] += 1
    payload = [
        {"path": path, "target_type": target_type, "count": count}
        for (path, target_type), count in counts.items()
    ]
    payload.sort(key=lambda item: (item["path"], item["target_type"] or "", item["count"]))
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _ancestry_tokens(
    step_id: int,
    entities: dict[int, dict],
    reverse_adjacency: dict[int, list[tuple[str, str | None, int]]],
) -> tuple[set[tuple[str, str | None]], set[tuple[str, str | None, str | None]]]:
    d1 = {
        (path, entity_type_family(source_type))
        for path, source_type, _source_step in reverse_adjacency.get(step_id, [])
    }
    d2: set[tuple[str, str | None, str | None]] = set()
    for path1, source_type1, source_step1 in reverse_adjacency.get(step_id, []):
        for path2, source_type2, _source_step2 in reverse_adjacency.get(source_step1, []):
            d2.add(
                (
                    path1,
                    entity_type_family(source_type1),
                    f"{path2}:{entity_type_family(source_type2)}",
                )
            )
    return d1, d2


def _neighbor_depth_digest(
    step_id: int,
    entities: dict[int, dict],
    adjacency: dict[int, list[tuple[str, str | None, int]]],
    *,
    depth: int,
) -> str:
    depth = max(1, depth)
    seen = {step_id}
    q: deque[tuple[int, int]] = deque([(step_id, 0)])
    labels: list[tuple[int, str, str | None, str | None]] = []
    while q:
        node, dist = q.popleft()
        if dist >= depth:
            continue
        for path, target_type, target in adjacency.get(node, []):
            target_type_family = entity_type_family(target_type)
            node_type = entity_type_family(entities.get(node, {}).get("entity_type"))
            labels.append((dist + 1, path, target_type_family, node_type))
            if target in entities and target not in seen:
                seen.add(target)
                q.append((target, dist + 1))
    labels.sort(key=lambda item: (item[0], item[1], item[2] or "", item[3] or ""))
    blob = json.dumps(labels, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _similarity(old_f: dict[str, Any], new_f: dict[str, Any], *, depth: int) -> float:
    type_score = _type_compatibility(old_f.get("entity_type"), new_f.get("entity_type"))
    semantic = 1.0 if old_f["semantic_signature"] == new_f["semantic_signature"] else 0.0
    attr_paths = _jaccard(old_f["attribute_paths"], new_f["attribute_paths"])
    literals = _jaccard(old_f["literal_tokens"], new_f["literal_tokens"])
    edges = _jaccard(old_f["edge_labels"], new_f["edge_labels"])
    degree = _degree_similarity(old_f["degree"], new_f["degree"])
    ancestry_d1 = _jaccard(old_f.get("ancestry_d1", set()), new_f.get("ancestry_d1", set()))
    score = (
        0.14 * type_score
        + 0.22 * semantic
        + 0.14 * attr_paths
        + 0.10 * literals
        + 0.13 * edges
        + 0.05 * degree
        + 0.04 * ancestry_d1
    )
    if depth >= 2:
        ancestry_d2 = _jaccard(old_f.get("ancestry_d2", set()), new_f.get("ancestry_d2", set()))
        depth2 = 1.0 if old_f.get("neighbor_depth2_digest") == new_f.get("neighbor_depth2_digest") else 0.0
        score += 0.14 * ancestry_d2 + 0.03 * depth2
    if depth >= 3:
        depth3 = 1.0 if old_f.get("neighbor_depth3_digest") == new_f.get("neighbor_depth3_digest") else 0.0
        score += 0.03 * depth3
    return min(1.0, score)


def _type_compatibility(old_type: str | None, new_type: str | None) -> float:
    if old_type == new_type and old_type is not None:
        return 1.0
    if entity_type_family(old_type) == entity_type_family(new_type) and old_type is not None and new_type is not None:
        return 0.75
    return 0.0


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
    *,
    depth: int,
    score_threshold: float,
) -> dict[tuple[int, int], float]:
    scores: dict[tuple[int, int], float] = {}
    for old_step in old_steps:
        for new_step in new_steps:
            score = _similarity(old_features[old_step], new_features[new_step], depth=depth)
            if score >= score_threshold:
                scores[(old_step, new_step)] = score
    return scores


def _min_cost_bipartite_assignment(
    old_steps: list[int],
    new_steps: list[int],
    score_map: dict[tuple[int, int], float],
    *,
    score_threshold: float,
) -> dict[int, int]:
    m = len(old_steps)
    n = len(new_steps)
    size = m + n
    cost: list[list[float]] = [[_DISALLOWED_COST for _ in range(size)] for _ in range(size)]

    for i, old_step in enumerate(old_steps):
        for j, new_step in enumerate(new_steps):
            score = score_map.get((old_step, new_step))
            if score is not None:
                cost[i][j] = 1.0 - score
        for j in range(n, size):
            cost[i][j] = _unmatched_cost(score_threshold)

    for i in range(m, size):
        for j in range(n):
            cost[i][j] = _unmatched_cost(score_threshold)
        for j in range(n, size):
            cost[i][j] = 0.0

    assignment = _hungarian_min_cost(cost)
    matches: dict[int, int] = {}
    for i, j in enumerate(assignment[:m]):
        if j < n:
            old_step = old_steps[i]
            new_step = new_steps[j]
            score = score_map.get((old_step, new_step))
            if score is not None and score >= score_threshold:
                matches[old_step] = new_step
    return matches


def _hungarian_min_cost(cost: list[list[float]]) -> list[int]:
    n = len(cost)
    u = [0.0] * (n + 1)
    v = [0.0] * (n + 1)
    p = [0] * (n + 1)
    way = [0] * (n + 1)

    for i in range(1, n + 1):
        p[0] = i
        j0 = 0
        minv = [inf] * (n + 1)
        used = [False] * (n + 1)
        while True:
            used[j0] = True
            i0 = p[j0]
            delta = inf
            j1 = 0
            for j in range(1, n + 1):
                if used[j]:
                    continue
                cur = cost[i0 - 1][j - 1] - u[i0] - v[j]
                if cur < minv[j]:
                    minv[j] = cur
                    way[j] = j0
                if minv[j] < delta:
                    delta = minv[j]
                    j1 = j
            for j in range(0, n + 1):
                if used[j]:
                    u[p[j]] += delta
                    v[j] -= delta
                else:
                    minv[j] -= delta
            j0 = j1
            if p[j0] == 0:
                break
        while True:
            j1 = way[j0]
            p[j0] = p[j1]
            j0 = j1
            if j0 == 0:
                break

    assignment = [-1] * n
    for j in range(1, n + 1):
        if p[j] > 0:
            assignment[p[j] - 1] = j - 1
    return assignment


def _is_assignment_ambiguous(
    old_steps: list[int],
    new_steps: list[int],
    matches: dict[int, int],
    score_map: dict[tuple[int, int], float],
    *,
    score_margin: float,
) -> bool:
    matched_pairs = {(o, n): score_map[(o, n)] for o, n in matches.items() if (o, n) in score_map}
    if not matched_pairs:
        return False

    for old_step, new_step in sorted(matches.items()):
        matched_score = matched_pairs.get((old_step, new_step))
        if matched_score is None:
            continue
        for alt_new in new_steps:
            if alt_new == new_step:
                continue
            alt_score = score_map.get((old_step, alt_new))
            if alt_score is not None and (matched_score - alt_score) < score_margin:
                return True
        for alt_old in old_steps:
            if alt_old == old_step:
                continue
            alt_score = score_map.get((alt_old, new_step))
            if alt_score is not None and (matched_score - alt_score) < score_margin:
                return True
    return False


def _unmatched_cost(score_threshold: float) -> float:
    return 1.0 - score_threshold + 0.05
