"""Match entities across two parsed IFC models.

Primary matching is by GlobalId. When GUID overlap is low (suggesting
regenerated GUIDs), falls back to content-based matching using entity
signatures built from ifc_class, name, container, type_name, properties,
and placement.

The matcher is conservative: ambiguous cases are left unmatched (appearing
as add+delete) rather than risking a wrong pairing.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections import defaultdict

# If fewer than this fraction of old GUIDs appear in the new model,
# we assume GUIDs were regenerated and activate content-based fallback.
_GUID_OVERLAP_THRESHOLD = 0.30

# Minimum number of non-empty discriminators (beyond ifc_class) required
# for a content signature to be considered reliable for exact matching.
_MIN_DISCRIMINATORS = 2

# Placement quantization cell size in mm for positional disambiguation.
_POS_CELL_MM = 50.0

# Fuzzy matching: minimum score to accept a match.
_FUZZY_SCORE_THRESHOLD = 0.55

# Fuzzy matching: minimum margin between best and second-best candidate.
_FUZZY_MARGIN = 0.10


def match_entities(old: dict, new: dict) -> dict:
    """Build an alignment between entities in two parsed IFC models.

    Returns:
        {
            "old_to_new": {old_guid: new_guid, ...},
            "method": "guid" | "content_fallback",
            "guid_overlap": float,
        }
    """
    old_entities = old["entities"]
    new_entities = new["entities"]
    old_ids = set(old_entities.keys())
    new_ids = set(new_entities.keys())

    common = old_ids & new_ids
    if old_ids:
        overlap = len(common) / len(old_ids)
    else:
        overlap = 1.0

    if overlap >= _GUID_OVERLAP_THRESHOLD:
        # High overlap — standard GUID matching (identity map for common IDs)
        old_to_new = {guid: guid for guid in common}
        return {
            "old_to_new": old_to_new,
            "method": "guid",
            "guid_overlap": overlap,
        }

    # Low overlap — content-based fallback
    old_to_new = {}
    used_old: set[str] = set()
    used_new: set[str] = set()

    # Stage 0: match any GUIDs that do overlap (partial regeneration)
    for guid in common:
        old_to_new[guid] = guid
        used_old.add(guid)
        used_new.add(guid)

    # Build normalized features
    old_features = {g: _build_features(e) for g, e in old_entities.items()}
    new_features = {g: _build_features(e) for g, e in new_entities.items()}

    # Stage 1: exact unique base signature matches
    _match_by_signature(
        old_features, new_features,
        used_old, used_new, old_to_new,
        use_position=False,
    )

    # Stage 2: resolve duplicate buckets with positional disambiguation
    _match_by_signature(
        old_features, new_features,
        used_old, used_new, old_to_new,
        use_position=True,
    )

    # Stage 3: fuzzy scoring for remaining candidates
    _match_fuzzy(
        old_features, new_features,
        used_old, used_new, old_to_new,
    )

    return {
        "old_to_new": old_to_new,
        "method": "content_fallback",
        "guid_overlap": overlap,
    }


# ---------------------------------------------------------------------------
# Feature extraction
# ---------------------------------------------------------------------------

def _build_features(entity: dict) -> dict:
    """Extract normalized matching features from a parsed entity."""
    ifc_class = entity["ifc_class"]
    name = _norm_str(entity.get("name"))
    container = _norm_str(entity.get("container"))
    type_name = _norm_str(entity.get("type_name"))
    groups = tuple(sorted(_norm_str(g) for g in entity.get("groups", []) if g))

    # Stable props: flattened scalar (key, value) pairs from attributes + psets,
    # excluding entity references and volatile keys.
    stable_props = _extract_stable_props(entity)
    props_digest = _hash_props(stable_props)

    # Placement translation
    placement = entity.get("placement")
    if placement and len(placement) >= 3:
        tx, ty, tz = placement[0][3], placement[1][3], placement[2][3]
    else:
        tx = ty = tz = None

    # Count non-empty discriminators beyond ifc_class
    discriminators = sum(1 for v in (name, type_name, container) if v)
    if groups:
        discriminators += 1
    if stable_props:
        discriminators += 1

    return {
        "ifc_class": ifc_class,
        "name": name,
        "type_name": type_name,
        "container": container,
        "groups": groups,
        "stable_props": stable_props,
        "props_digest": props_digest,
        "tx": tx, "ty": ty, "tz": tz,
        "discriminators": discriminators,
    }


_VOLATILE_KEYS = frozenset({
    "id", "guid", "globalid", "ownerhistory", "history",
    "created", "modified", "timestamp", "application",
    "creationdate", "lastmodifieddate", "changeaction",
})


def _extract_stable_props(entity: dict) -> frozenset:
    """Flatten attributes + psets into a set of (key, normalized_value) pairs."""
    pairs = set()

    for k, v in entity.get("attributes", {}).items():
        if k.lower() in _VOLATILE_KEYS:
            continue
        nv = _norm_value(v)
        if nv is not None:
            pairs.add((k, nv))

    for pset_name, props in entity.get("property_sets", {}).items():
        for k, v in props.items():
            if k.lower() in _VOLATILE_KEYS:
                continue
            nv = _norm_value(v)
            if nv is not None:
                pairs.add((f"{pset_name}.{k}", nv))

    return frozenset(pairs)


def _norm_str(s) -> str:
    """Normalize a string for comparison."""
    if not s:
        return ""
    return " ".join(str(s).strip().split()).casefold()


def _norm_value(v):
    """Normalize an attribute value for stable comparison. Returns None for unstable values."""
    if v is None:
        return None
    if isinstance(v, dict):
        # Entity references contain GUIDs — skip them
        if "ref" in v or "entity" in v:
            return None
        return None
    if isinstance(v, (list, tuple)):
        normed = tuple(_norm_value(x) for x in v)
        if all(x is None for x in normed):
            return None
        return normed
    if isinstance(v, float):
        return round(v, 4)
    if isinstance(v, str):
        s = v.strip()
        return s if s else None
    return v


def _hash_props(props: frozenset) -> str:
    """Deterministic hash of a property set for fast comparison."""
    canonical = json.dumps(sorted((k, str(v)) for k, v in props), sort_keys=True)
    return hashlib.md5(canonical.encode()).hexdigest()


# ---------------------------------------------------------------------------
# Stage 1 & 2: Signature-based matching
# ---------------------------------------------------------------------------

def _base_signature(feat: dict) -> tuple:
    return (
        feat["ifc_class"],
        feat["name"],
        feat["type_name"],
        feat["container"],
        feat["groups"],
        feat["props_digest"],
    )


def _pos_signature(feat: dict) -> tuple:
    base = _base_signature(feat)
    if feat["tx"] is not None:
        qx = round(feat["tx"] / _POS_CELL_MM)
        qy = round(feat["ty"] / _POS_CELL_MM)
        qz = round(feat["tz"] / _POS_CELL_MM)
        return base + (qx, qy, qz)
    return base + (None, None, None)


def _match_by_signature(
    old_features: dict[str, dict],
    new_features: dict[str, dict],
    used_old: set[str],
    used_new: set[str],
    old_to_new: dict[str, str],
    use_position: bool,
):
    """Match unmatched entities by exact signature (unique 1:1 only)."""
    sig_fn = _pos_signature if use_position else _base_signature

    old_buckets: dict[tuple, list[str]] = defaultdict(list)
    new_buckets: dict[tuple, list[str]] = defaultdict(list)

    for guid, feat in old_features.items():
        if guid in used_old:
            continue
        if feat["discriminators"] < _MIN_DISCRIMINATORS:
            continue
        sig = sig_fn(feat)
        old_buckets[sig].append(guid)

    for guid, feat in new_features.items():
        if guid in used_new:
            continue
        if feat["discriminators"] < _MIN_DISCRIMINATORS:
            continue
        sig = sig_fn(feat)
        new_buckets[sig].append(guid)

    for sig in old_buckets:
        if sig not in new_buckets:
            continue
        old_guids = old_buckets[sig]
        new_guids = new_buckets[sig]
        if len(old_guids) == 1 and len(new_guids) == 1:
            og, ng = old_guids[0], new_guids[0]
            old_to_new[og] = ng
            used_old.add(og)
            used_new.add(ng)


# ---------------------------------------------------------------------------
# Stage 3: Fuzzy scoring
# ---------------------------------------------------------------------------

def _match_fuzzy(
    old_features: dict[str, dict],
    new_features: dict[str, dict],
    used_old: set[str],
    used_new: set[str],
    old_to_new: dict[str, str],
):
    """Score remaining unmatched entities and greedily accept high-confidence pairs."""
    remaining_old = {g: f for g, f in old_features.items() if g not in used_old}
    remaining_new = {g: f for g, f in new_features.items() if g not in used_new}

    if not remaining_old or not remaining_new:
        return

    # Block by ifc_class for efficiency
    old_by_class: dict[str, list[tuple[str, dict]]] = defaultdict(list)
    for g, f in remaining_old.items():
        old_by_class[f["ifc_class"]].append((g, f))

    new_by_class: dict[str, list[tuple[str, dict]]] = defaultdict(list)
    for g, f in remaining_new.items():
        new_by_class[f["ifc_class"]].append((g, f))

    # Score all same-class pairs
    candidates: list[tuple[float, str, str]] = []  # (score, old_guid, new_guid)

    for cls in old_by_class:
        if cls not in new_by_class:
            continue
        for og, of in old_by_class[cls]:
            scores_for_og = []
            for ng, nf in new_by_class[cls]:
                s = _score_pair(of, nf)
                if s >= _FUZZY_SCORE_THRESHOLD:
                    scores_for_og.append((s, ng))

            if not scores_for_og:
                continue

            scores_for_og.sort(reverse=True)
            best_score, best_ng = scores_for_og[0]

            # Check margin over second-best
            if len(scores_for_og) >= 2:
                second_score = scores_for_og[1][0]
                if best_score - second_score < _FUZZY_MARGIN:
                    continue  # ambiguous — skip

            candidates.append((best_score, og, best_ng))

    # Greedy assignment by descending score
    candidates.sort(reverse=True)
    for score, og, ng in candidates:
        if og in used_old or ng in used_new:
            continue
        old_to_new[og] = ng
        used_old.add(og)
        used_new.add(ng)


def _score_pair(of: dict, nf: dict) -> float:
    """Score the similarity of two entity feature dicts (same ifc_class assumed)."""
    score = 0.0

    # Name match (0.15)
    if of["name"] and nf["name"] and of["name"] == nf["name"]:
        score += 0.15

    # Type name match (0.15)
    if of["type_name"] and nf["type_name"] and of["type_name"] == nf["type_name"]:
        score += 0.15

    # Container match (0.10)
    if of["container"] and nf["container"] and of["container"] == nf["container"]:
        score += 0.10

    # Group overlap (0.05)
    if of["groups"] or nf["groups"]:
        og = set(of["groups"])
        ng = set(nf["groups"])
        union = og | ng
        if union:
            score += 0.05 * len(og & ng) / len(union)

    # Stable props overlap (0.25)
    if of["stable_props"] or nf["stable_props"]:
        union = of["stable_props"] | nf["stable_props"]
        if union:
            inter = of["stable_props"] & nf["stable_props"]
            score += 0.25 * len(inter) / len(union)

    # Placement proximity (0.30)
    if of["tx"] is not None and nf["tx"] is not None:
        dist = math.sqrt(
            (of["tx"] - nf["tx"]) ** 2
            + (of["ty"] - nf["ty"]) ** 2
            + (of["tz"] - nf["tz"]) ** 2
        )
        # Distances in mm: <100mm=1.0, <1000mm=0.5, >5000mm=0.0
        if dist < 100:
            score += 0.30
        elif dist < 1000:
            score += 0.15
        elif dist < 5000:
            score += 0.05

    return score
