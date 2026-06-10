"""Tiered pool-reduction matching.

Matching runs as a sequence of evidence tiers. Each tier examines only the
still-unmatched pools, emits disjoint 1:1 pairs, and shrinks the pools.
There are no candidate lists, no separate scoring pass, and no assignment
step: a tier only matches when its evidence is unique or the entities are
interchangeable, so ambiguity is resolved by construction, memory stays
O(N), and the output is deterministic.

Tiers, strongest evidence first:

1. ``guid``             unique GlobalId on both sides + same canonical class.
                        Score 1.0 when the full signature vector is identical,
                        0.9 otherwise. Duplicated GlobalIds are never identity
                        evidence; their entities fall through to the vector
                        tier. A GlobalId reused across classes falls through
                        too instead of poisoning both entities.
2. ``geometry_hash``    same canonical class + full signature-vector equality
                        (geometry/data/topology hashes + quantized placement),
                        zipped 1:1 in step order per bucket. Equal placement
                        implies co-location, so bucket members are
                        semantically interchangeable. Score 0.8.

Anything weaker is reported as added+deleted rather than guessed. The 2026-06
corpus survey (docs/corpus/2026-06-10-corpus-survey.md) showed that the former
topology-unique and spatial-nearest fallback tiers never fired on a real
revision pair and only produced cross-model container matches, so they were
removed.
"""

from __future__ import annotations

from collections import defaultdict

from athar.bottom.types import SignatureVector

from .types import MatchedPair


def match_signatures(
    old_signatures: dict[int, SignatureVector],
    new_signatures: dict[int, SignatureVector],
) -> tuple[list[MatchedPair], list[int], list[int], dict]:
    """Match old/new signatures 1:1; returns (matches, unmatched_old, unmatched_new, diagnostics)."""
    pool_old = set(old_signatures)
    pool_new = set(new_signatures)
    matches: list[MatchedPair] = []
    matched_by_tier = {"guid": 0, "geometry_hash": 0}

    def _match(old_step: int, new_step: int, score: float, reason: str) -> None:
        matches.append(MatchedPair(old_step=old_step, new_step=new_step, score=score, reason=reason))
        pool_old.remove(old_step)
        pool_new.remove(new_step)
        matched_by_tier[reason] += 1

    # Tier 1: unique GlobalId identity.
    old_by_guid = _bucket(pool_old, old_signatures, key=lambda s: s.guid)
    new_by_guid = _bucket(pool_new, new_signatures, key=lambda s: s.guid)
    duplicate_guids = {
        "old": sum(1 for guid, ids in old_by_guid.items() if guid and len(ids) > 1),
        "new": sum(1 for guid, ids in new_by_guid.items() if guid and len(ids) > 1),
    }
    for guid, old_ids in old_by_guid.items():
        if not guid or len(old_ids) != 1:
            continue
        new_ids = new_by_guid.get(guid, ())
        if len(new_ids) != 1:
            continue
        old_step, new_step = old_ids[0], new_ids[0]
        old_sig, new_sig = old_signatures[old_step], new_signatures[new_step]
        if old_sig.canonical_class != new_sig.canonical_class:
            continue
        score = 1.0 if _same_vector(old_sig, new_sig) else 0.9
        _match(old_step, new_step, score, "guid")

    # Tier 2: full signature-vector equality (interchangeable entities).
    old_by_vector = _bucket(pool_old, old_signatures, key=_vector_key)
    new_by_vector = _bucket(pool_new, new_signatures, key=_vector_key)
    for key, old_ids in old_by_vector.items():
        if key is None:
            continue
        for old_step, new_step in zip(old_ids, new_by_vector.get(key, ())):
            _match(old_step, new_step, 0.8, "geometry_hash")

    matches.sort(key=lambda m: (m.old_step, m.new_step))
    unmatched_old = sorted(pool_old)
    unmatched_new = sorted(pool_new)
    diagnostics = {
        "matched_by_tier": matched_by_tier,
        "pools": {"old": len(old_signatures), "new": len(new_signatures)},
        "unmatched": {"old": len(unmatched_old), "new": len(unmatched_new)},
        "duplicate_guids": duplicate_guids,
    }
    return matches, unmatched_old, unmatched_new, diagnostics


def _bucket(pool: set[int], signatures: dict[int, SignatureVector], key) -> dict:
    """Bucket pool members by key(sig); None keys mean 'not eligible for this tier'."""
    out: dict = defaultdict(list)
    for step_id in sorted(pool):
        out[key(signatures[step_id])].append(step_id)
    return out


def _vector_key(sig: SignatureVector) -> tuple | None:
    if not (sig.vh_geometry and sig.vh_data and sig.vh_topology):
        return None
    return (sig.canonical_class, sig.vh_geometry, sig.vh_data, sig.vh_topology, sig.placement)


def _same_vector(a: SignatureVector, b: SignatureVector) -> bool:
    return (
        a.vh_geometry == b.vh_geometry
        and a.vh_data == b.vh_data
        and a.vh_topology == b.vh_topology
        and a.placement == b.placement
    )
