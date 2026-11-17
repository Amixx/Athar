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
                        tier, and so do the two entities of a GlobalId whose
                        class differs.
2. ``geometry_hash``    same canonical class + full signature-vector equality
                        (geometry/data/topology hashes + quantized placement
                        or, for an unresolved chain, its placement digest).
                        The vector does not cover relation targets, so the
                        members of one bucket can differ in what they are
                        related to: members naming the same targets, through
                        the matches made so far, pair first, and the rest
                        pair 1:1 in step order. Score 0.8.
3. ``guid_reclassified`` unique GlobalId on both sides, different canonical
                        class, both entities left unmatched by the tiers
                        above: the entity was given another class (a proxy
                        corrected to a wall). Score 0.7. Classes never pair
                        without the same unique GlobalId.

Anything weaker is reported as added+deleted rather than guessed. The 2026-06
corpus survey (docs/corpus/2026-06-10-corpus-survey.md) showed that the former
topology-unique and spatial-nearest fallback tiers never fired on a real
revision pair and only produced cross-model container matches, so they were
removed.
"""

from __future__ import annotations

import heapq
from collections import defaultdict

from athar.bottom.types import RelatedEntity, SignatureVector

from .types import MatchedPair


def match_signatures(
    old_signatures: dict[int, SignatureVector],
    new_signatures: dict[int, SignatureVector],
    old_related: dict[int, RelatedEntity],
    new_related: dict[int, RelatedEntity],
) -> tuple[list[MatchedPair], list[int], list[int], dict]:
    """Match old/new signatures 1:1; returns (matches, unmatched_old, unmatched_new, diagnostics).

    ``old_related`` and ``new_related`` hold the relation targets without a
    signature (``SignatureBundle.related_entities``).
    """
    pool_old = set(old_signatures)
    pool_new = set(new_signatures)
    matches: list[MatchedPair] = []
    matched_by_tier = {"guid": 0, "geometry_hash": 0, "guid_reclassified": 0}

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

    # Tier 2: full signature-vector equality.
    old_by_vector = _bucket(pool_old, old_signatures, key=_vector_key)
    new_by_vector = _bucket(pool_new, new_signatures, key=_vector_key)
    ambiguous: list[tuple[list[int], list[int]]] = []
    for key, old_ids in old_by_vector.items():
        new_ids = new_by_vector.get(key)
        if key is None or new_ids is None:
            continue
        if len(old_ids) == 1 and len(new_ids) == 1:
            _match(old_ids[0], new_ids[0], 0.8, "geometry_hash")
        else:
            ambiguous.append((old_ids, new_ids))
    if ambiguous:
        new_step_of = {pair.old_step: pair.new_step for pair in matches}
        for old_step, new_step in _pair_by_relation_targets(
            ambiguous, old_signatures, new_signatures, old_related, new_related, new_step_of
        ):
            _match(old_step, new_step, 0.8, "geometry_hash")

    # Tier 3: unique GlobalId kept across a class change.
    for guid, old_ids in old_by_guid.items():
        new_ids = new_by_guid.get(guid, ())
        if not guid or len(old_ids) != 1 or len(new_ids) != 1:
            continue
        if old_ids[0] in pool_old and new_ids[0] in pool_new:
            _match(old_ids[0], new_ids[0], 0.7, "guid_reclassified")

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


def _pair_by_relation_targets(
    buckets: list[tuple[list[int], list[int]]],
    old_signatures: dict[int, SignatureVector],
    new_signatures: dict[int, SignatureVector],
    old_related: dict[int, RelatedEntity],
    new_related: dict[int, RelatedEntity],
    new_step_of: dict[int, int],
):
    """Yield the pairs of equal-vector buckets with several members on a side.

    A bucket is resolved once no member names a target in an unresolved
    bucket; its members then pair by the set of their targets, each target
    identified by the new-side entity it is matched to. Buckets that wait on
    each other in a cycle are released lowest old step first, a target in an
    unresolved bucket standing for that bucket. Members that targets do not
    tell apart, and members left without a counterpart, pair in step order.

    ``new_step_of`` holds the matches made before this call and receives the
    yielded ones.
    """
    old_bucket_of = {step: index for index, (old_ids, _) in enumerate(buckets) for step in old_ids}
    new_bucket_of = {step: index for index, (_, new_ids) in enumerate(buckets) for step in new_ids}
    matched_new = set(new_step_of.values())

    waits_on: list[set[int]] = []
    waited_by: dict[int, list[int]] = defaultdict(list)
    for index, (old_ids, new_ids) in enumerate(buckets):
        needed: set[int] = set()
        for ids, signatures, bucket_of in (
            (old_ids, old_signatures, old_bucket_of),
            (new_ids, new_signatures, new_bucket_of),
        ):
            for step in ids:
                for target in signatures[step].relation_targets:
                    other = bucket_of.get(target.step_id)
                    if other is not None:
                        needed.add(other)
        waits_on.append(needed)
        for other in needed:
            waited_by[other].append(index)

    def targets_key(
        sig: SignatureVector,
        signatures: dict[int, SignatureVector],
        related: dict[int, RelatedEntity],
        bucket_of: dict[int, int],
        resolved_step,
    ) -> frozenset:
        key = set()
        for target in sig.relation_targets:
            step = target.step_id
            if step not in signatures:
                entity = related[step]
                identity = ("related", entity.canonical_class, entity.content)
            elif (matched := resolved_step(step)) is not None:
                identity = ("signature", matched)
            elif step in bucket_of and not resolved[bucket_of[step]]:
                identity = ("bucket", bucket_of[step])
            else:
                identity = ("unmatched",)
            key.add((target.relation, target.role, identity))
        return frozenset(key)

    resolved = [False] * len(buckets)
    ready = [index for index, needed in enumerate(waits_on) if not needed]
    heapq.heapify(ready)
    next_stalled = 0
    while True:
        if ready:
            index = heapq.heappop(ready)
        else:
            while next_stalled < len(buckets) and resolved[next_stalled]:
                next_stalled += 1
            if next_stalled == len(buckets):
                return
            index = next_stalled
        if resolved[index]:
            continue
        old_ids, new_ids = buckets[index]
        new_by_key: dict[frozenset, list[int]] = defaultdict(list)
        for step in new_ids:
            key = targets_key(
                new_signatures[step],
                new_signatures,
                new_related,
                new_bucket_of,
                lambda target: target if target in matched_new else None,
            )
            new_by_key[key].append(step)
        for members in new_by_key.values():
            members.reverse()
        pairs: list[tuple[int, int]] = []
        left_old: list[int] = []
        for step in old_ids:
            key = targets_key(old_signatures[step], old_signatures, old_related, old_bucket_of, new_step_of.get)
            candidates = new_by_key.get(key)
            if candidates:
                pairs.append((step, candidates.pop()))
            else:
                left_old.append(step)
        left_new = sorted(step for members in new_by_key.values() for step in members)
        pairs.extend(zip(left_old, left_new))
        for old_step, new_step in pairs:
            new_step_of[old_step] = new_step
            matched_new.add(new_step)
            yield old_step, new_step
        resolved[index] = True
        for waiting in waited_by[index]:
            waits_on[waiting].discard(index)
            if not waits_on[waiting] and not resolved[waiting]:
                heapq.heappush(ready, waiting)


def _vector_key(sig: SignatureVector) -> tuple | None:
    if not (sig.vh_geometry and sig.vh_data and sig.vh_topology):
        return None
    return (
        sig.canonical_class,
        sig.vh_geometry,
        sig.vh_data,
        sig.vh_topology,
        sig.placement,
        sig.placement_unresolved,
    )


def _same_vector(a: SignatureVector, b: SignatureVector) -> bool:
    return (
        a.vh_geometry == b.vh_geometry
        and a.vh_data == b.vh_data
        and a.vh_topology == b.vh_topology
        and a.placement == b.placement
        and a.placement_unresolved == b.placement_unresolved
    )
