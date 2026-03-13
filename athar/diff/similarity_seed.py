"""Cross-file similarity seed builders for early identity alignment."""

from __future__ import annotations

import hashlib
from typing import Any

from .._native.required import native_entity_fingerprint
from ..graph.profile_policy import entity_for_profile
from .types import GraphIR

_TEXT_BUCKET_REFINEMENT_MAX = 16


def unique_guid_pairs(
    old_graph: GraphIR,
    new_graph: GraphIR,
) -> tuple[dict[int, int], dict[str, Any]]:
    old_entities = old_graph.get("entities", {})
    new_entities = new_graph.get("entities", {})
    old_counts = _guid_counts(old_entities)
    new_counts = _guid_counts(new_entities)
    old_unique = {
        gid: step_id
        for step_id, entity in old_entities.items()
        for gid in [entity.get("global_id")]
        if isinstance(gid, str) and old_counts.get(gid, 0) == 1
    }
    new_unique = {
        gid: step_id
        for step_id, entity in new_entities.items()
        for gid in [entity.get("global_id")]
        if isinstance(gid, str) and new_counts.get(gid, 0) == 1
    }

    pairs: dict[int, int] = {}
    for gid in sorted(set(old_unique) & set(new_unique)):
        pairs[old_unique[gid]] = new_unique[gid]

    denominator = max(1, min(len(old_entities), len(new_entities)))
    unique_denominator = max(1, min(len(old_unique), len(new_unique)))
    return pairs, {
        "matched": len(pairs),
        "coverage": len(pairs) / denominator,
        "unique_guid_overlap": len(pairs) / unique_denominator,
        "old_unique": len(old_unique),
        "new_unique": len(new_unique),
    }


def text_fingerprint_pairs(
    old_graph: GraphIR,
    new_graph: GraphIR,
    *,
    profile: str,
    exclude_old: set[int] | None = None,
    exclude_new: set[int] | None = None,
) -> dict[str, Any]:
    old_collection = collect_text_fingerprint_side(
        old_graph,
        profile=profile,
        exclude_steps=exclude_old,
    )
    new_collection = collect_text_fingerprint_side(
        new_graph,
        profile=profile,
        exclude_steps=exclude_new,
    )
    return match_text_fingerprint_collections(
        old_graph,
        new_graph,
        old_collection=old_collection,
        new_collection=new_collection,
    )


def collect_text_fingerprint_side(
    graph: GraphIR,
    *,
    profile: str,
    exclude_steps: set[int] | None = None,
) -> dict[str, Any]:
    entities = graph.get("entities", {})
    excluded = exclude_steps or set()
    match_buckets: dict[str, list[int]] = {}
    all_fingerprints: dict[int, str] = {}
    profile_entities: dict[int, dict[str, Any]] = {}

    for step_id, entity in entities.items():
        profile_entity = entity_for_profile(entity, profile=profile)
        fingerprint = native_entity_fingerprint(profile_entity)
        all_fingerprints[step_id] = fingerprint
        profile_entities[step_id] = profile_entity
        if step_id not in excluded and not entity.get("global_id"):
            match_buckets.setdefault(fingerprint, []).append(step_id)

    return {
        "match_buckets": match_buckets,
        "all_fingerprints": all_fingerprints,
        "profile_entities": profile_entities,
    }


def match_text_fingerprint_collections(
    old_graph: GraphIR,
    new_graph: GraphIR,
    *,
    old_collection: dict[str, Any],
    new_collection: dict[str, Any],
) -> dict[str, Any]:
    old_entities = old_graph.get("entities", {})
    new_entities = new_graph.get("entities", {})
    old_buckets: dict[str, list[int]] = old_collection["match_buckets"]
    new_buckets: dict[str, list[int]] = new_collection["match_buckets"]
    old_all_fingerprints: dict[int, str] = old_collection["all_fingerprints"]
    new_all_fingerprints: dict[int, str] = new_collection["all_fingerprints"]

    old_to_new: dict[int, int] = {}
    diagnostics: dict[int, dict[str, Any]] = {}
    fingerprints: dict[int, str] = {}
    ambiguous = 0
    refined = 0

    for fp in sorted(set(old_buckets) & set(new_buckets)):
        old_steps = sorted(old_buckets[fp])
        new_steps = sorted(new_buckets[fp])
        if len(old_steps) == 1 and len(new_steps) == 1:
            old_step = old_steps[0]
            new_step = new_steps[0]
            old_to_new[old_step] = new_step
            fingerprints[old_step] = fp
            diagnostics[old_step] = {
                "match_confidence": 0.99,
                "matched_on": {
                    "stage": "text_fingerprint",
                    "fingerprint": fp,
                },
            }
            continue
        if len(old_steps) == len(new_steps) and len(old_steps) <= _TEXT_BUCKET_REFINEMENT_MAX:
            refined_pairs = _refine_ambiguous_bucket(
                old_steps=old_steps,
                new_steps=new_steps,
                old_entities=old_entities,
                new_entities=new_entities,
                old_step_fingerprint=old_all_fingerprints,
                new_step_fingerprint=new_all_fingerprints,
            )
            if len(refined_pairs) == len(old_steps):
                refined += 1
                for old_step, new_step in refined_pairs.items():
                    old_to_new[old_step] = new_step
                    fingerprints[old_step] = fp
                    diagnostics[old_step] = {
                        "match_confidence": 0.97,
                        "matched_on": {
                            "stage": "text_fingerprint_refined",
                            "fingerprint": fp,
                        },
                    }
                continue
        ambiguous += 1

    return {
        "old_to_new": old_to_new,
        "diagnostics": diagnostics,
        "old_all_fingerprints": old_all_fingerprints,
        "new_all_fingerprints": new_all_fingerprints,
        "old_profile_entities": old_collection["profile_entities"],
        "new_profile_entities": new_collection["profile_entities"],
        "ambiguous_buckets": ambiguous,
        "refined_buckets": refined,
        "fingerprints": fingerprints,
    }


def build_seed_color_maps(
    *,
    old_graph: GraphIR,
    new_graph: GraphIR,
    guid_pairs: dict[int, int],
    path_pairs: dict[int, int],
    text_pairs: dict[int, int],
    text_fingerprints: dict[int, str],
) -> tuple[dict[int, str], dict[int, str]]:
    old_entities = old_graph.get("entities", {})
    new_entities = new_graph.get("entities", {})
    old_seed: dict[int, str] = {}
    new_seed: dict[int, str] = {}

    for old_step, new_step in sorted(guid_pairs.items()):
        gid = old_entities.get(old_step, {}).get("global_id")
        if not isinstance(gid, str):
            gid = new_entities.get(new_step, {}).get("global_id")
        token = f"guid:{gid or f'{old_step}->{new_step}'}"
        _set_seed_pair(old_seed, new_seed, old_step, new_step, token)

    for old_step, new_step in sorted(path_pairs.items()):
        if old_step in old_seed or new_step in new_seed:
            continue
        token = f"path:{old_step}->{new_step}"
        _set_seed_pair(old_seed, new_seed, old_step, new_step, token)

    for old_step, new_step in sorted(text_pairs.items()):
        if old_step in old_seed or new_step in new_seed:
            continue
        token = f"text:{text_fingerprints.get(old_step, f'{old_step}->{new_step}')}"
        _set_seed_pair(old_seed, new_seed, old_step, new_step, token)

    return old_seed, new_seed


def _set_seed_pair(
    old_seed: dict[int, str],
    new_seed: dict[int, str],
    old_step: int,
    new_step: int,
    token: str,
) -> None:
    color = hashlib.sha256(token.encode("utf-8")).hexdigest()
    old_seed[old_step] = color
    new_seed[new_step] = color


def _guid_counts(entities: dict[int, dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for entity in entities.values():
        gid = entity.get("global_id")
        if isinstance(gid, str):
            counts[gid] = counts.get(gid, 0) + 1
    return counts


def _refine_ambiguous_bucket(
    *,
    old_steps: list[int],
    new_steps: list[int],
    old_entities: dict[int, dict[str, Any]],
    new_entities: dict[int, dict[str, Any]],
    old_step_fingerprint: dict[int, str],
    new_step_fingerprint: dict[int, str],
) -> dict[int, int]:
    old_signatures = {
        step_id: _neighbor_fingerprint_signature(
            old_entities.get(step_id, {}),
            step_fingerprint=old_step_fingerprint,
        )
        for step_id in old_steps
    }
    new_signatures = {
        step_id: _neighbor_fingerprint_signature(
            new_entities.get(step_id, {}),
            step_fingerprint=new_step_fingerprint,
        )
        for step_id in new_steps
    }
    old_by_sig: dict[tuple[Any, ...], list[int]] = {}
    new_by_sig: dict[tuple[Any, ...], list[int]] = {}
    for step_id, signature in old_signatures.items():
        old_by_sig.setdefault(signature, []).append(step_id)
    for step_id, signature in new_signatures.items():
        new_by_sig.setdefault(signature, []).append(step_id)
    if set(old_by_sig) != set(new_by_sig):
        return {}

    pairs: dict[int, int] = {}
    for signature in sorted(old_by_sig):
        old_group = sorted(old_by_sig[signature])
        new_group = sorted(new_by_sig[signature])
        if len(old_group) != 1 or len(new_group) != 1:
            return {}
        pairs[old_group[0]] = new_group[0]
    return pairs


def _neighbor_fingerprint_signature(
    entity: dict[str, Any],
    *,
    step_fingerprint: dict[int, str],
) -> tuple[Any, ...]:
    counts: dict[tuple[str, str | None, str], int] = {}
    for ref in entity.get("refs", []):
        target = ref.get("target")
        target_fp = step_fingerprint.get(target, "MISSING")
        key = (ref.get("path", ""), ref.get("target_type"), target_fp)
        counts[key] = counts.get(key, 0) + 1
    return tuple(sorted((path, target_type, target_fp, count) for (path, target_type, target_fp), count in counts.items()))
