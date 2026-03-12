"""Deterministic assignment solver helpers for secondary matcher blocks."""

from __future__ import annotations

from math import inf

_DISALLOWED_COST = 1_000_000.0


def min_cost_bipartite_assignment(
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


def is_assignment_ambiguous(
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


def _unmatched_cost(score_threshold: float) -> float:
    return 1.0 - score_threshold + 0.05
