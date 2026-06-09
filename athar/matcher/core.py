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
                        evidence; their entities fall through to structural
                        tiers. A GlobalId reused across classes falls through
                        too instead of poisoning both entities.
2. ``geometry_hash``    same canonical class + full signature-vector equality
                        (geometry/data/topology hashes + quantized placement),
                        zipped 1:1 in step order per bucket. Equal placement
                        implies co-location, so no proximity check is needed;
                        bucket members are semantically interchangeable. Class
                        is in the key so this tier never depends on the WL
                        seed happening to encode it. Score 0.8.
3. ``tier2_signature``  globally unique (class, topology-hash) bucket,
                        sanity-checked by proximity when centroids exist.
                        Score 0.7.
4. ``spatial_fallback`` same class and either the exact same world centroid
                        (zip) or nearest neighbour within the match radius
                        via a uniform grid. Score 0.5.
"""

from __future__ import annotations

import math
from collections import defaultdict

from athar.bottom.constants import DEFAULT_MATCH_RADIUS_M
from athar.bottom.types import SignatureVector

from .types import MatchedPair

# Safety valve for pathological inputs only (thousands of same-class entities
# packed inside one radius cell). Caps how many grid candidates one entity
# examines; a capped scan cannot prove nearestness, so the entity is left
# unmatched rather than matched approximately. Trips are reported in
# diagnostics["spatial"]["probe_capped"].
DEFAULT_SPATIAL_PROBE_LIMIT = 128

_NEIGHBOR_OFFSETS = tuple(
    (dx, dy, dz) for dx in (-1, 0, 1) for dy in (-1, 0, 1) for dz in (-1, 0, 1)
)


def match_signatures(
    old_signatures: dict[int, SignatureVector],
    new_signatures: dict[int, SignatureVector],
    *,
    radius_m: float = DEFAULT_MATCH_RADIUS_M,
    probe_limit: int = DEFAULT_SPATIAL_PROBE_LIMIT,
) -> tuple[list[MatchedPair], list[int], list[int], dict]:
    """Match old/new signatures 1:1; returns (matches, unmatched_old, unmatched_new, diagnostics)."""
    if not (isinstance(radius_m, (int, float)) and not isinstance(radius_m, bool) and radius_m > 0):
        raise ValueError(f"radius_m must be a positive number, got {radius_m!r}")
    if not (isinstance(probe_limit, int) and not isinstance(probe_limit, bool) and probe_limit > 0):
        raise ValueError(f"probe_limit must be a positive int, got {probe_limit!r}")
    radius_sq = float(radius_m) * float(radius_m)

    pool_old = set(old_signatures)
    pool_new = set(new_signatures)
    matches: list[MatchedPair] = []
    matched_by_tier = {"guid": 0, "geometry_hash": 0, "tier2_signature": 0, "spatial_fallback": 0}
    spatial_stats = {"position_zip": 0, "nearest": 0, "probe_capped": 0}

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

    # Tier 3: globally unique (class, topology) bucket with proximity sanity.
    old_by_tier2 = _bucket(pool_old, old_signatures, key=_tier2_key)
    new_by_tier2 = _bucket(pool_new, new_signatures, key=_tier2_key)
    for key, old_ids in old_by_tier2.items():
        if key is None or len(old_ids) != 1:
            continue
        new_ids = new_by_tier2.get(key, ())
        if len(new_ids) != 1:
            continue
        old_centroid = old_signatures[old_ids[0]].centroid
        new_centroid = new_signatures[new_ids[0]].centroid
        if old_centroid is None and new_centroid is None:
            pass  # no spatial evidence either way; bucket uniqueness carries the match
        elif old_centroid is None or new_centroid is None:
            continue  # asymmetric spatial evidence is suspicious; stay conservative
        elif _dist_sq(old_centroid, new_centroid) > radius_sq:
            continue
        _match(old_ids[0], new_ids[0], 0.7, "tier2_signature")

    # Tier 4a: same class at the exact same world centroid.
    old_by_position = _bucket(pool_old, old_signatures, key=_position_key)
    new_by_position = _bucket(pool_new, new_signatures, key=_position_key)
    for key, old_ids in old_by_position.items():
        if key is None:
            continue
        for old_step, new_step in zip(old_ids, new_by_position.get(key, ())):
            _match(old_step, new_step, 0.5, "spatial_fallback")
            spatial_stats["position_zip"] += 1

    # Tier 4b: same class, nearest neighbour within the radius. Single round:
    # each old prefers its nearest new; preferences resolve globally by
    # ascending distance, losers stay unmatched (conservative).
    old_by_class = _bucket(pool_old, old_signatures, key=_spatial_class_key)
    new_by_class = _bucket(pool_new, new_signatures, key=_spatial_class_key)
    preferences: list[tuple[float, int, int]] = []
    for klass, old_ids in old_by_class.items():
        if klass is None:
            continue
        new_ids = new_by_class.get(klass)
        if not new_ids:
            continue
        grid: dict[tuple[int, int, int], list[int]] = defaultdict(list)
        for new_step in new_ids:
            grid[_cell(new_signatures[new_step].centroid, radius_m)].append(new_step)
        for old_step in old_ids:
            nearest = _nearest_in_grid(
                old_signatures[old_step].centroid,
                grid,
                new_signatures,
                radius_m=radius_m,
                radius_sq=radius_sq,
                probe_limit=probe_limit,
                stats=spatial_stats,
            )
            if nearest is not None:
                preferences.append((nearest[0], old_step, nearest[1]))
    preferences.sort()
    for _dist, old_step, new_step in preferences:
        if old_step in pool_old and new_step in pool_new:
            _match(old_step, new_step, 0.5, "spatial_fallback")
            spatial_stats["nearest"] += 1

    matches.sort(key=lambda m: (m.old_step, m.new_step))
    unmatched_old = sorted(pool_old)
    unmatched_new = sorted(pool_new)
    diagnostics = {
        "matched_by_tier": matched_by_tier,
        "pools": {"old": len(old_signatures), "new": len(new_signatures)},
        "unmatched": {"old": len(unmatched_old), "new": len(unmatched_new)},
        "duplicate_guids": duplicate_guids,
        "spatial": spatial_stats,
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


def _tier2_key(sig: SignatureVector) -> tuple | None:
    if not sig.vh_topology:
        return None
    return (sig.canonical_class, sig.vh_topology)


def _position_key(sig: SignatureVector) -> tuple | None:
    if sig.centroid is None:
        return None
    return (sig.canonical_class, sig.centroid)


def _spatial_class_key(sig: SignatureVector) -> str | None:
    if sig.centroid is None:
        return None
    return sig.canonical_class


def _same_vector(a: SignatureVector, b: SignatureVector) -> bool:
    return (
        a.vh_geometry == b.vh_geometry
        and a.vh_data == b.vh_data
        and a.vh_topology == b.vh_topology
        and a.placement == b.placement
    )


def _nearest_in_grid(
    centroid: tuple[float, float, float],
    grid: dict[tuple[int, int, int], list[int]],
    signatures: dict[int, SignatureVector],
    *,
    radius_m: float,
    radius_sq: float,
    probe_limit: int,
    stats: dict[str, int],
) -> tuple[float, int] | None:
    cx, cy, cz = _cell(centroid, radius_m)
    best: tuple[float, int] | None = None
    examined = 0
    capped = False
    for dx, dy, dz in _NEIGHBOR_OFFSETS:
        for new_step in grid.get((cx + dx, cy + dy, cz + dz), ()):
            if examined >= probe_limit:
                capped = True
                break
            examined += 1
            dist_sq = _dist_sq(centroid, signatures[new_step].centroid)
            if dist_sq > radius_sq:
                continue
            candidate = (dist_sq, new_step)
            if best is None or candidate < best:
                best = candidate
        if capped:
            break
    if capped:
        # The scan stopped early, so `best` is unproven as nearest; declining
        # the match (added+deleted downstream) beats an approximate identity.
        stats["probe_capped"] += 1
        return None
    return best


def _cell(centroid: tuple[float, float, float], cell_size: float) -> tuple[int, int, int]:
    return (
        math.floor(centroid[0] / cell_size),
        math.floor(centroid[1] / cell_size),
        math.floor(centroid[2] / cell_size),
    )


def _dist_sq(a: tuple[float, float, float], b: tuple[float, float, float]) -> float:
    dx = a[0] - b[0]
    dy = a[1] - b[1]
    dz = a[2] - b[2]
    return dx * dx + dy * dy + dz * dz
