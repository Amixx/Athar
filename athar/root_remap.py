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
            ambiguous += min(len(old_ids), len(new_ids))

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
