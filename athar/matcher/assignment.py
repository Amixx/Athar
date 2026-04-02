"""Phase 1 greedy assignment."""

from __future__ import annotations

from athar.bottom.types import SignatureVector

from .types import MatchedPair, ScoredCandidate


def greedy_assign(
    scored: list[ScoredCandidate],
    old_signatures: dict[int, SignatureVector],
    new_signatures: dict[int, SignatureVector],
    *,
    min_score: float = 0.3,
) -> tuple[list[MatchedPair], list[int], list[int]]:
    """Assign 1:1 matches greedily by descending confidence."""
    used_old: set[int] = set()
    used_new: set[int] = set()
    matches: list[MatchedPair] = []

    for cand in scored:
        if cand.score < min_score:
            continue
        if cand.old_step in used_old or cand.new_step in used_new:
            continue
        used_old.add(cand.old_step)
        used_new.add(cand.new_step)
        matches.append(
            MatchedPair(
                old_step=cand.old_step,
                new_step=cand.new_step,
                score=cand.score,
                reason=cand.reason,
            )
        )

    unmatched_old = sorted(step_id for step_id in old_signatures if step_id not in used_old)
    unmatched_new = sorted(step_id for step_id in new_signatures if step_id not in used_new)
    matches.sort(key=lambda m: (m.old_step, m.new_step))
    return matches, unmatched_old, unmatched_new

