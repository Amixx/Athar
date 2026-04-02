"""Phase 1 confidence scoring."""

from __future__ import annotations

from collections import Counter

from athar.bottom.constants import DEFAULT_MATCH_RADIUS_M
from athar.bottom.types import SignatureVector

from .types import CandidatePair, ScoredCandidate


def score_candidates(
    candidates: list[CandidatePair],
    old_signatures: dict[int, SignatureVector],
    new_signatures: dict[int, SignatureVector],
    *,
    radius_m: float = DEFAULT_MATCH_RADIUS_M,
) -> list[ScoredCandidate]:
    """Score candidates with the Phase 1 confidence tiers."""
    dirty_old = _dirty_guids(old_signatures)
    dirty_new = _dirty_guids(new_signatures)
    radius_sq = radius_m * radius_m

    out: list[ScoredCandidate] = []
    for candidate in candidates:
        old_sig = old_signatures.get(candidate.old_step)
        new_sig = new_signatures.get(candidate.new_step)
        if old_sig is None or new_sig is None:
            continue
        score = _score_pair(old_sig, new_sig, radius_sq=radius_sq)
        if (old_sig.guid and old_sig.guid in dirty_old) or (new_sig.guid and new_sig.guid in dirty_new):
            score = min(score, 0.5)
        out.append(
            ScoredCandidate(
                old_step=candidate.old_step,
                new_step=candidate.new_step,
                score=score,
                reason=candidate.reason,
            )
        )

    out.sort(key=lambda c: (-c.score, c.old_step, c.new_step, c.reason))
    return out


def _score_pair(old_sig: SignatureVector, new_sig: SignatureVector, *, radius_sq: float) -> float:
    same_guid = bool(old_sig.guid) and old_sig.guid == new_sig.guid
    same_class = old_sig.canonical_class == new_sig.canonical_class
    same_vector = _same_signature_vector(old_sig, new_sig)
    close = _close_enough(old_sig.centroid, new_sig.centroid, radius_sq=radius_sq)

    if same_guid and same_class and same_vector:
        return 1.0
    if same_guid and same_class and not same_vector:
        return 0.9
    if (not same_guid) and same_vector and close:
        return 0.8
    if (not same_guid) and (not same_vector) and same_class and close:
        return 0.5
    if same_guid and not same_class:
        return 0.1
    return 0.0


def _same_signature_vector(a: SignatureVector, b: SignatureVector) -> bool:
    return (
        a.vh_geometry == b.vh_geometry
        and a.vh_data == b.vh_data
        and a.vh_topology == b.vh_topology
        and a.placement == b.placement
    )


def _close_enough(
    a: tuple[float, float, float] | None,
    b: tuple[float, float, float] | None,
    *,
    radius_sq: float,
) -> bool:
    if a is None or b is None:
        return False
    dx = a[0] - b[0]
    dy = a[1] - b[1]
    dz = a[2] - b[2]
    return (dx * dx + dy * dy + dz * dz) <= radius_sq


def _dirty_guids(signatures: dict[int, SignatureVector]) -> set[str]:
    counter = Counter(sig.guid for sig in signatures.values() if sig.guid)
    return {guid for guid, count in counter.items() if count > 1}

