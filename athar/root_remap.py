"""Staged rooted remap scaffolding for low GUID-overlap comparisons.

Phase 2.5 (plan-aligned): build root candidates independent of GUID value,
run deterministic unique-signature matching, and keep ambiguous buckets
explicitly unresolved.
"""

from __future__ import annotations

import hashlib
import json
from collections import Counter, defaultdict
from typing import Any

from .canonical_ids import wl_refine_colors

GUID_OVERLAP_THRESHOLD = 0.30
_VOLATILE_ROOT_ATTRS = frozenset({"GlobalId", "OwnerHistory"})
_VOLATILE_ROOT_REF_PATHS = frozenset({"/OwnerHistory"})


def plan_root_remap(
    old_graph: dict,
    new_graph: dict,
    *,
    guid_overlap_threshold: float = GUID_OVERLAP_THRESHOLD,
) -> dict[str, Any]:
    """Build a deterministic old-root -> new-root remap table.

    This is intentionally conservative scaffolding:
    - only unique-signature root buckets are mapped
    - ambiguous buckets are reported and left unmatched
    """
    old_roots = _collect_unique_roots(old_graph)
    new_roots = _collect_unique_roots(new_graph)
    overlap = guid_overlap(old_roots, new_roots)

    if overlap >= guid_overlap_threshold:
        return {
            "enabled": False,
            "method": "disabled_guid_overlap",
            "guid_overlap": overlap,
            "old_to_new": {},
            "ambiguous": 0,
        }

    old_by_sig = _group_roots_by_signature(old_roots)
    new_by_sig = _group_roots_by_signature(new_roots)
    old_colors = wl_refine_colors(old_graph)
    new_colors = wl_refine_colors(new_graph)

    old_to_new: dict[str, str] = {}
    ambiguous = 0

    for signature in sorted(set(old_by_sig) | set(new_by_sig)):
        old_ids = old_by_sig.get(signature, [])
        new_ids = new_by_sig.get(signature, [])
        if len(old_ids) == 1 and len(new_ids) == 1:
            old_gid = old_ids[0]
            new_gid = new_ids[0]
            if old_gid != new_gid:
                old_to_new[old_gid] = new_gid
        elif old_ids and new_ids:
            bucket_matches, bucket_ambiguous = _disambiguate_ambiguous_bucket(
                old_roots=old_roots,
                new_roots=new_roots,
                old_colors=old_colors,
                new_colors=new_colors,
                old_ids=old_ids,
                new_ids=new_ids,
            )
            old_to_new.update(bucket_matches)
            ambiguous += bucket_ambiguous

    return {
        "enabled": True,
        "method": "signature_unique",
        "guid_overlap": overlap,
        "old_to_new": old_to_new,
        "ambiguous": ambiguous,
    }


def guid_overlap(old_roots: dict[str, dict], new_roots: dict[str, dict]) -> float:
    """Compute GUID overlap ratio with old roots as denominator."""
    old_ids = set(old_roots)
    if not old_ids:
        return 1.0
    return len(old_ids & set(new_roots)) / len(old_ids)


def root_signature(entity: dict) -> str:
    """GUID-independent signature for root-candidate matching."""
    payload = {
        "entity_type": entity.get("entity_type"),
        "attributes": _root_attributes(entity.get("attributes", {})),
        "edges": _edge_signature(entity.get("refs", [])),
    }
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _collect_unique_roots(graph: dict) -> dict[str, dict]:
    entities = graph.get("entities", {})
    guid_counts: Counter[str] = Counter()
    for entity in entities.values():
        gid = entity.get("global_id")
        if gid:
            guid_counts[gid] += 1

    roots: dict[str, dict] = {}
    for entity in entities.values():
        gid = entity.get("global_id")
        if gid and guid_counts[gid] == 1:
            roots[gid] = entity
    return roots


def _group_roots_by_signature(roots: dict[str, dict]) -> dict[str, list[str]]:
    by_sig: defaultdict[str, list[str]] = defaultdict(list)
    for gid, entity in roots.items():
        by_sig[root_signature(entity)].append(gid)
    for gids in by_sig.values():
        gids.sort()
    return dict(by_sig)


def _root_attributes(attributes: dict[str, Any]) -> dict[str, Any]:
    cleaned: dict[str, Any] = {}
    for name, value in attributes.items():
        if name in _VOLATILE_ROOT_ATTRS:
            continue
        cleaned[name] = _strip_ref_ids(value)
    return cleaned


def _strip_ref_ids(value: Any) -> Any:
    if isinstance(value, dict):
        if value.get("kind") == "ref":
            return {"kind": "ref"}
        return {k: _strip_ref_ids(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_strip_ref_ids(v) for v in value]
    return value


def _edge_signature(refs: list[dict]) -> list[dict[str, Any]]:
    counts: Counter[tuple[str, str | None]] = Counter()
    for ref in refs:
        path = ref.get("path", "")
        if path in _VOLATILE_ROOT_REF_PATHS:
            continue
        counts[(path, ref.get("target_type"))] += 1

    edges = [
        {"path": path, "target_type": target_type, "count": count}
        for (path, target_type), count in counts.items()
    ]
    edges.sort(key=lambda item: (item["path"], item["target_type"] or "", item["count"]))
    return edges


def _disambiguate_ambiguous_bucket(
    *,
    old_roots: dict[str, dict],
    new_roots: dict[str, dict],
    old_colors: dict[int, str],
    new_colors: dict[int, str],
    old_ids: list[str],
    new_ids: list[str],
) -> tuple[dict[str, str], int]:
    old_by_sig: defaultdict[str, list[str]] = defaultdict(list)
    new_by_sig: defaultdict[str, list[str]] = defaultdict(list)
    for gid in old_ids:
        old_by_sig[_neighbor_signature(old_roots[gid], old_colors)].append(gid)
    for gid in new_ids:
        new_by_sig[_neighbor_signature(new_roots[gid], new_colors)].append(gid)

    matches: dict[str, str] = {}
    matched_old: set[str] = set()
    matched_new: set[str] = set()
    for signature in sorted(set(old_by_sig) & set(new_by_sig)):
        old_group = sorted(old_by_sig[signature])
        new_group = sorted(new_by_sig[signature])
        if len(old_group) == 1 and len(new_group) == 1:
            old_gid = old_group[0]
            new_gid = new_group[0]
            matched_old.add(old_gid)
            matched_new.add(new_gid)
            if old_gid != new_gid:
                matches[old_gid] = new_gid

    unresolved_old = [gid for gid in old_ids if gid not in matched_old]
    unresolved_new = [gid for gid in new_ids if gid not in matched_new]
    ambiguous = min(len(unresolved_old), len(unresolved_new))
    return matches, ambiguous


def _neighbor_signature(entity: dict, colors: dict[int, str]) -> str:
    counts: Counter[tuple[str, str | None, str]] = Counter()
    for ref in entity.get("refs", []):
        path = ref.get("path", "")
        if path in _VOLATILE_ROOT_REF_PATHS:
            continue
        target = ref.get("target")
        target_color = colors.get(target, "MISSING") if isinstance(target, int) else "MISSING"
        counts[(path, ref.get("target_type"), target_color)] += 1

    payload = [
        {"path": path, "target_type": target_type, "target_color": target_color, "count": count}
        for (path, target_type, target_color), count in counts.items()
    ]
    payload.sort(key=lambda item: (item["path"], item["target_type"] or "", item["target_color"], item["count"]))
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()
