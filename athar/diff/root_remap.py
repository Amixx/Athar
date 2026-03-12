"""Staged rooted remap for low GUID-overlap comparisons."""

from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any

from .wl_refinement import wl_refine_colors
from ..graph.graph_utils import (
    collect_attribute_paths,
    collect_literal_tokens,
    degree_similarity,
    edge_signature,
    jaccard,
    sha256_json,
    strip_ref_ids,
)

GUID_OVERLAP_THRESHOLD = 0.30
ROOT_REMAP_SCORE_THRESHOLD = 0.72
ROOT_REMAP_SCORE_MARGIN = 0.08
ROOT_REMAP_ASSIGNMENT_MAX = 8
_VOLATILE_ROOT_ATTRS = frozenset({"GlobalId", "OwnerHistory"})
_VOLATILE_ROOT_REF_PATHS = frozenset({"/OwnerHistory"})


def plan_root_remap(
    old_graph: dict,
    new_graph: dict,
    *,
    guid_overlap_threshold: float = GUID_OVERLAP_THRESHOLD,
    score_threshold: float = ROOT_REMAP_SCORE_THRESHOLD,
    score_margin: float = ROOT_REMAP_SCORE_MARGIN,
    assignment_max: int = ROOT_REMAP_ASSIGNMENT_MAX,
    old_colors: dict[int, str] | None = None,
    new_colors: dict[int, str] | None = None,
) -> dict[str, Any]:
    """Build a deterministic old-root -> new-root remap table."""
    old_roots = _collect_unique_roots(old_graph)
    new_roots = _collect_unique_roots(new_graph)
    overlap = guid_overlap(old_roots, new_roots)

    if overlap >= guid_overlap_threshold:
        return {
            "enabled": False,
            "method": "disabled_guid_overlap",
            "guid_overlap": overlap,
            "old_to_new": {},
            "diagnostics": {},
            "ambiguous": 0,
        }

    old_by_sig = _group_roots_by_signature(old_roots)
    new_by_sig = _group_roots_by_signature(new_roots)
    old_colors = old_colors if old_colors is not None else wl_refine_colors(old_graph)
    new_colors = new_colors if new_colors is not None else wl_refine_colors(new_graph)

    old_to_new: dict[str, str] = {}
    diagnostics: dict[str, dict[str, Any]] = {}
    ambiguous = 0

    for signature in sorted(set(old_by_sig) | set(new_by_sig)):
        old_ids = old_by_sig.get(signature, [])
        new_ids = new_by_sig.get(signature, [])
        if len(old_ids) == 1 and len(new_ids) == 1:
            old_gid = old_ids[0]
            new_gid = new_ids[0]
            if old_gid != new_gid:
                old_to_new[old_gid] = new_gid
                diagnostics[old_gid] = {"stage": "signature_unique"}
        elif old_ids and new_ids:
            bucket_matches, bucket_diagnostics, bucket_ambiguous = _disambiguate_ambiguous_bucket(
                old_roots=old_roots,
                new_roots=new_roots,
                old_colors=old_colors,
                new_colors=new_colors,
                old_ids=old_ids,
                new_ids=new_ids,
                score_threshold=score_threshold,
                score_margin=score_margin,
                assignment_max=assignment_max,
            )
            old_to_new.update(bucket_matches)
            diagnostics.update(bucket_diagnostics)
            ambiguous += bucket_ambiguous

    return {
        "enabled": True,
        "method": "staged",
        "guid_overlap": overlap,
        "old_to_new": old_to_new,
        "diagnostics": diagnostics,
        "ambiguous": ambiguous,
    }


def guid_overlap(old_roots: dict[str, dict], new_roots: dict[str, dict]) -> float:
    old_ids = set(old_roots)
    if not old_ids:
        return 1.0
    return len(old_ids & set(new_roots)) / len(old_ids)


def root_signature(entity: dict) -> str:
    payload = {
        "entity_type": entity.get("entity_type"),
        "attributes": _root_attributes(entity.get("attributes", {})),
        "edges": edge_signature(entity.get("refs", []), skip_paths=_VOLATILE_ROOT_REF_PATHS),
    }
    return sha256_json(payload)


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
        cleaned[name] = strip_ref_ids(value)
    return cleaned


def _disambiguate_ambiguous_bucket(
    *,
    old_roots: dict[str, dict],
    new_roots: dict[str, dict],
    old_colors: dict[int, str],
    new_colors: dict[int, str],
    old_ids: list[str],
    new_ids: list[str],
    score_threshold: float,
    score_margin: float,
    assignment_max: int,
) -> tuple[dict[str, str], dict[str, dict[str, Any]], int]:
    old_by_sig: defaultdict[str, list[str]] = defaultdict(list)
    new_by_sig: defaultdict[str, list[str]] = defaultdict(list)
    for gid in old_ids:
        old_by_sig[_neighbor_signature(old_roots[gid], old_colors)].append(gid)
    for gid in new_ids:
        new_by_sig[_neighbor_signature(new_roots[gid], new_colors)].append(gid)

    matches: dict[str, str] = {}
    diagnostics: dict[str, dict[str, Any]] = {}
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
                diagnostics[old_gid] = {"stage": "neighbor_signature"}

    unresolved_old = [gid for gid in sorted(old_ids) if gid not in matched_old]
    unresolved_new = [gid for gid in sorted(new_ids) if gid not in matched_new]
    if not unresolved_old or not unresolved_new:
        return matches, diagnostics, 0

    if len(unresolved_old) > assignment_max or len(unresolved_new) > assignment_max:
        return matches, diagnostics, min(len(unresolved_old), len(unresolved_new))

    old_features = {
        gid: _root_feature(old_roots[gid], old_colors)
        for gid in unresolved_old
    }
    new_features = {
        gid: _root_feature(new_roots[gid], new_colors)
        for gid in unresolved_new
    }
    score_map = _score_map(
        unresolved_old,
        unresolved_new,
        old_features,
        new_features,
        score_threshold=score_threshold,
    )
    if not score_map:
        return matches, diagnostics, min(len(unresolved_old), len(unresolved_new))

    best, second, tie_for_best = _enumerate_assignments(unresolved_old, unresolved_new, score_map)
    if best is None or best["match_count"] == 0:
        return matches, diagnostics, min(len(unresolved_old), len(unresolved_new))
    if tie_for_best:
        return matches, diagnostics, min(len(unresolved_old), len(unresolved_new))
    if second is not None and best["match_count"] == second["match_count"]:
        if (best["total_score"] - second["total_score"]) < score_margin:
            return matches, diagnostics, min(len(unresolved_old), len(unresolved_new))

    matched_by_score = 0
    for old_gid, new_gid in best["pairs"]:
        matched_old.add(old_gid)
        matched_new.add(new_gid)
        matched_by_score += 1
        if old_gid != new_gid:
            matches[old_gid] = new_gid
            diagnostics[old_gid] = {
                "stage": "scored_assignment",
                "score": score_map[(old_gid, new_gid)],
            }

    remaining_old = [gid for gid in unresolved_old if gid not in matched_old]
    remaining_new = [gid for gid in unresolved_new if gid not in matched_new]
    ambiguous = min(len(remaining_old), len(remaining_new))
    if matched_by_score == 0:
        ambiguous = min(len(unresolved_old), len(unresolved_new))
    return matches, diagnostics, ambiguous


def _root_feature(entity: dict, colors: dict[int, str]) -> dict[str, Any]:
    attrs = _root_attributes(entity.get("attributes", {}))
    refs = [ref for ref in entity.get("refs", []) if ref.get("path", "") not in _VOLATILE_ROOT_REF_PATHS]
    return {
        "entity_type": entity.get("entity_type"),
        "attribute_paths": collect_attribute_paths(attrs, base_path=""),
        "literal_tokens": collect_literal_tokens(attrs),
        "edge_labels": {(ref.get("path", ""), ref.get("target_type")) for ref in refs},
        "neighbor_tokens": _neighbor_token_set(entity, colors),
        "neighbor_signature": _neighbor_signature(entity, colors),
        "degree": len(refs),
    }


def _score_map(
    old_ids: list[str],
    new_ids: list[str],
    old_features: dict[str, dict[str, Any]],
    new_features: dict[str, dict[str, Any]],
    *,
    score_threshold: float,
) -> dict[tuple[str, str], float]:
    out: dict[tuple[str, str], float] = {}
    for old_gid in old_ids:
        for new_gid in new_ids:
            score = _similarity(old_features[old_gid], new_features[new_gid])
            if score >= score_threshold:
                out[(old_gid, new_gid)] = score
    return out


def _similarity(old_f: dict[str, Any], new_f: dict[str, Any]) -> float:
    type_score = 1.0 if old_f["entity_type"] == new_f["entity_type"] else 0.0
    attr_paths = jaccard(old_f["attribute_paths"], new_f["attribute_paths"])
    literals = jaccard(old_f["literal_tokens"], new_f["literal_tokens"])
    edges = jaccard(old_f["edge_labels"], new_f["edge_labels"])
    neighbor_overlap = jaccard(old_f["neighbor_tokens"], new_f["neighbor_tokens"])
    neighbor_eq = 1.0 if old_f["neighbor_signature"] == new_f["neighbor_signature"] else 0.0
    degree = degree_similarity(old_f["degree"], new_f["degree"])
    return (
        0.15 * type_score
        + 0.15 * attr_paths
        + 0.20 * literals
        + 0.10 * edges
        + 0.25 * neighbor_overlap
        + 0.10 * neighbor_eq
        + 0.05 * degree
    )


def _enumerate_assignments(
    old_ids: list[str],
    new_ids: list[str],
    score_map: dict[tuple[str, str], float],
) -> tuple[dict[str, Any] | None, dict[str, Any] | None, bool]:
    old_ids = sorted(old_ids)
    new_ids = sorted(new_ids)
    best: dict[str, Any] | None = None
    second: dict[str, Any] | None = None
    tie_for_best = False

    def consider(candidate: dict[str, Any]) -> None:
        nonlocal best, second, tie_for_best
        if best is None:
            best = candidate
            return
        cmp = _compare_candidate(candidate, best)
        if cmp > 0:
            second = best
            best = candidate
            tie_for_best = False
            return
        if cmp == 0:
            tie_for_best = True
            if second is None or _compare_candidate(candidate, second) > 0:
                second = candidate
            return
        if second is None or _compare_candidate(candidate, second) > 0:
            second = candidate

    def walk(idx: int, used_new: set[str], pairs: list[tuple[str, str]], total_score: float) -> None:
        if idx >= len(old_ids):
            consider({
                "pairs": tuple(sorted(pairs)),
                "match_count": len(pairs),
                "total_score": total_score,
            })
            return

        old_gid = old_ids[idx]
        walk(idx + 1, used_new, pairs, total_score)

        options = [
            (new_gid, score_map[(old_gid, new_gid)])
            for new_gid in new_ids
            if new_gid not in used_new and (old_gid, new_gid) in score_map
        ]
        options.sort(key=lambda item: (-item[1], item[0]))
        for new_gid, score in options:
            used_new.add(new_gid)
            pairs.append((old_gid, new_gid))
            walk(idx + 1, used_new, pairs, total_score + score)
            pairs.pop()
            used_new.remove(new_gid)

    walk(0, set(), [], 0.0)
    return best, second, tie_for_best


def _compare_candidate(left: dict[str, Any], right: dict[str, Any]) -> int:
    left_key = (left["match_count"], round(left["total_score"], 12), left["pairs"])
    right_key = (right["match_count"], round(right["total_score"], 12), right["pairs"])
    if left_key > right_key:
        return 1
    if left_key < right_key:
        return -1
    return 0


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
        {
            "path": path,
            "target_type": target_type,
            "target_color": target_color,
            "count": count,
        }
        for (path, target_type, target_color), count in counts.items()
    ]
    payload.sort(key=lambda item: (item["path"], item["target_type"] or "", item["target_color"], item["count"]))
    return sha256_json(payload)


def _neighbor_token_set(entity: dict, colors: dict[int, str]) -> set[tuple[str, str | None, str]]:
    tokens: set[tuple[str, str | None, str]] = set()
    for ref in entity.get("refs", []):
        path = ref.get("path", "")
        if path in _VOLATILE_ROOT_REF_PATHS:
            continue
        target = ref.get("target")
        target_color = colors.get(target, "MISSING") if isinstance(target, int) else "MISSING"
        tokens.add((path, ref.get("target_type"), target_color))
    return tokens
