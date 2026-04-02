"""Phase 1 candidate generation."""

from __future__ import annotations

from collections import defaultdict

from athar.bottom.constants import DEFAULT_MATCH_RADIUS_M
from athar.bottom.types import SignatureVector

from .types import CandidatePair


def generate_candidates(
    old_signatures: dict[int, SignatureVector],
    new_signatures: dict[int, SignatureVector],
    *,
    radius_m: float = DEFAULT_MATCH_RADIUS_M,
) -> list[CandidatePair]:
    """Generate candidates via GUID, geometry hash, and spatial fallback."""
    candidates: dict[tuple[int, int], CandidatePair] = {}

    old_by_guid = _bucket(old_signatures, key=lambda s: s.guid)
    new_by_guid = _bucket(new_signatures, key=lambda s: s.guid)
    old_has_candidate: set[int] = set()
    new_has_candidate: set[int] = set()

    for guid, old_ids in old_by_guid.items():
        if not guid:
            continue
        new_ids = new_by_guid.get(guid, [])
        for old_step in old_ids:
            for new_step in new_ids:
                _insert(candidates, CandidatePair(old_step=old_step, new_step=new_step, reason="guid"))
                old_has_candidate.add(old_step)
                new_has_candidate.add(new_step)

    old_by_geom = _bucket(old_signatures, key=lambda s: s.vh_geometry)
    new_by_geom = _bucket(new_signatures, key=lambda s: s.vh_geometry)
    for vh, old_ids in old_by_geom.items():
        if not vh:
            continue
        new_ids = new_by_geom.get(vh, [])
        for old_step in old_ids:
            for new_step in new_ids:
                _insert(candidates, CandidatePair(old_step=old_step, new_step=new_step, reason="geometry_hash"))
                old_has_candidate.add(old_step)
                new_has_candidate.add(new_step)

    old_unmatched = [s for sid, s in old_signatures.items() if sid not in old_has_candidate]
    new_unmatched_by_class = defaultdict(list)
    for sid, sig in new_signatures.items():
        if sid in new_has_candidate:
            continue
        new_unmatched_by_class[sig.canonical_class].append(sig)

    radius_sq = radius_m * radius_m
    for old_sig in old_unmatched:
        old_centroid = old_sig.centroid
        if old_centroid is None:
            continue
        for new_sig in new_unmatched_by_class.get(old_sig.canonical_class, []):
            new_centroid = new_sig.centroid
            if new_centroid is None:
                continue
            if _dist_sq(old_centroid, new_centroid) <= radius_sq:
                _insert(
                    candidates,
                    CandidatePair(old_step=old_sig.step_id, new_step=new_sig.step_id, reason="spatial_fallback"),
                )

    out = list(candidates.values())
    out.sort(key=lambda c: (c.old_step, c.new_step, c.reason))
    return out


def _bucket(signatures: dict[int, SignatureVector], key):
    out: dict[str | None, list[int]] = defaultdict(list)
    for step_id, sig in signatures.items():
        out[key(sig)].append(step_id)
    for vals in out.values():
        vals.sort()
    return out


def _insert(store: dict[tuple[int, int], CandidatePair], candidate: CandidatePair) -> None:
    key = (candidate.old_step, candidate.new_step)
    previous = store.get(key)
    if previous is None:
        store[key] = candidate
        return
    priority = {"guid": 0, "geometry_hash": 1, "spatial_fallback": 2}
    if priority.get(candidate.reason, 99) < priority.get(previous.reason, 99):
        store[key] = candidate


def _dist_sq(a: tuple[float, float, float], b: tuple[float, float, float]) -> float:
    dx = a[0] - b[0]
    dy = a[1] - b[1]
    dz = a[2] - b[2]
    return dx * dx + dy * dy + dz * dz

