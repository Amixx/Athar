"""Matcher stages for the graph diff engine."""

from __future__ import annotations

from collections import defaultdict, deque
from typing import Any

from .graph_utils import build_adjacency, build_reverse_adjacency
from .matcher_graph_scoring import (
    SECONDARY_ASSIGNMENT_MAX,
    SECONDARY_ASSIGNMENT_AMBIGUITY_MARGIN,
    SECONDARY_DEEPENING_DEPTH2_MAX,
    SECONDARY_DEEPENING_DEPTH3_MAX,
    SECONDARY_SCORE_THRESHOLD,
    blocking_key,
    build_feature_vector,
    fallback_signature_block_match,
    iterative_assignment_block_match,
    entity_type_family,
)


def propagate_matches_by_typed_path(
    old_graph: dict,
    new_graph: dict,
    root_pairs: dict[int, int],
    *,
    pre_matched_old: set[int] | None = None,
    pre_matched_new: set[int] | None = None,
) -> dict[str, Any]:
    """Match unique non-root targets by typed path propagation."""
    old_entities = old_graph.get("entities", {})
    new_entities = new_graph.get("entities", {})

    used_old = set(pre_matched_old or set())
    used_new = set(pre_matched_new or set())

    queue: deque[tuple[int, int]] = deque(sorted(root_pairs.items(), key=lambda p: p[0]))
    matches: dict[int, int] = {}
    diagnostics: dict[int, dict[str, Any]] = {}
    ambiguous = 0

    while queue:
        old_parent, new_parent = queue.popleft()
        old_buckets = _edge_buckets(old_entities, old_parent, used_old)
        new_buckets = _edge_buckets(new_entities, new_parent, used_new)

        for edge_key in sorted(set(old_buckets) & set(new_buckets)):
            old_targets = old_buckets[edge_key]
            new_targets = new_buckets[edge_key]
            if len(old_targets) == 1 and len(new_targets) == 1:
                old_target = old_targets[0]
                new_target = new_targets[0]
                if _is_root(old_entities.get(old_target)) or _is_root(new_entities.get(new_target)):
                    continue
                matches[old_target] = new_target
                diagnostics[old_target] = {
                    "match_confidence": 1.0,
                    "matched_on": {
                        "stage": "typed_path",
                        "path": edge_key[0],
                        "target_type": edge_key[1],
                    },
                }
                used_old.add(old_target)
                used_new.add(new_target)
                queue.append((old_target, new_target))
            elif old_targets and new_targets:
                ambiguous += min(len(old_targets), len(new_targets))

    return {
        "method": "typed_path_propagation",
        "old_to_new": matches,
        "diagnostics": diagnostics,
        "ambiguous": ambiguous,
    }


def secondary_match_unresolved(
    old_graph: dict,
    new_graph: dict,
    *,
    pre_matched_old: set[int] | None = None,
    pre_matched_new: set[int] | None = None,
    score_threshold: float = SECONDARY_SCORE_THRESHOLD,
    score_margin: float = SECONDARY_ASSIGNMENT_AMBIGUITY_MARGIN,
    assignment_max: int = SECONDARY_ASSIGNMENT_MAX,
    depth2_max: int = SECONDARY_DEEPENING_DEPTH2_MAX,
    depth3_max: int = SECONDARY_DEEPENING_DEPTH3_MAX,
) -> dict[str, Any]:
    """Deterministic secondary matcher for unmatched non-root entities."""
    old_entities = old_graph.get("entities", {})
    new_entities = new_graph.get("entities", {})
    used_old = set(pre_matched_old or set())
    used_new = set(pre_matched_new or set())

    old_blocks: defaultdict[str | None, list[int]] = defaultdict(list)
    new_blocks: defaultdict[str | None, list[int]] = defaultdict(list)
    old_features: dict[int, dict[str, Any]] = {}
    new_features: dict[int, dict[str, Any]] = {}
    old_adjacency = build_adjacency(old_entities)
    new_adjacency = build_adjacency(new_entities)
    old_reverse = build_reverse_adjacency(old_entities, old_adjacency)
    new_reverse = build_reverse_adjacency(new_entities, new_adjacency)

    for step_id, entity in old_entities.items():
        if step_id in used_old or _is_root(entity):
            continue
        family = entity_type_family(entity.get("entity_type"))
        old_blocks[family].append(step_id)
        old_features[step_id] = build_feature_vector(
            entity,
            step_id=step_id,
            entities=old_entities,
            adjacency=old_adjacency,
            reverse_adjacency=old_reverse,
        )

    for step_id, entity in new_entities.items():
        if step_id in used_new or _is_root(entity):
            continue
        family = entity_type_family(entity.get("entity_type"))
        new_blocks[family].append(step_id)
        new_features[step_id] = build_feature_vector(
            entity,
            step_id=step_id,
            entities=new_entities,
            adjacency=new_adjacency,
            reverse_adjacency=new_reverse,
        )

    matches: dict[int, int] = {}
    diagnostics: dict[int, dict[str, Any]] = {}
    ambiguous_partitions: list[dict[str, Any]] = []
    ambiguous = 0
    for key in sorted(set(old_blocks) & set(new_blocks)):
        old_steps = sorted(old_blocks[key])
        new_steps = sorted(new_blocks[key])
        if not old_steps or not new_steps:
            continue

        block_matches, block_diagnostics, block_ambiguous, block_partitions = _match_entity_type_block(
            key,
            old_steps,
            new_steps,
            old_features,
            new_features,
            score_threshold=score_threshold,
            score_margin=score_margin,
            assignment_max=assignment_max,
            depth2_max=depth2_max,
            depth3_max=depth3_max,
        )

        for old_step, new_step in block_matches.items():
            matches[old_step] = new_step
            if old_step in block_diagnostics:
                diagnostics[old_step] = block_diagnostics[old_step]
            used_old.add(old_step)
            used_new.add(new_step)
        ambiguous += block_ambiguous
        ambiguous_partitions.extend(block_partitions)

    return {
        "method": "secondary_match",
        "old_to_new": matches,
        "diagnostics": diagnostics,
        "ambiguous": ambiguous,
        "ambiguous_partitions": ambiguous_partitions,
    }


def _edge_buckets(
    entities: dict[int, dict],
    parent_step: int,
    used_steps: set[int],
) -> dict[tuple[str, str | None], list[int]]:
    parent = entities.get(parent_step)
    buckets: defaultdict[tuple[str, str | None], list[int]] = defaultdict(list)
    if not parent:
        return {}
    for ref in parent.get("refs", []):
        target = ref.get("target")
        if target is None or target in used_steps:
            continue
        if target not in entities:
            continue
        edge_key = (ref.get("path", ""), ref.get("target_type"))
        buckets[edge_key].append(target)
    for targets in buckets.values():
        targets.sort()
    return dict(buckets)


def _is_root(entity: dict | None) -> bool:
    if not entity:
        return False
    return bool(entity.get("global_id"))


def _match_entity_type_block(
    entity_family: str | None,
    old_steps: list[int],
    new_steps: list[int],
    old_features: dict[int, dict[str, Any]],
    new_features: dict[int, dict[str, Any]],
    *,
    score_threshold: float,
    score_margin: float,
    assignment_max: int,
    depth2_max: int,
    depth3_max: int,
) -> tuple[dict[int, int], dict[int, dict[str, Any]], int, list[dict[str, Any]]]:
    old_by_block: defaultdict[tuple[str | None, int, int, int, int, int, int], list[int]] = defaultdict(list)
    new_by_block: defaultdict[tuple[str | None, int, int, int, int, int, int], list[int]] = defaultdict(list)
    for step in old_steps:
        old_by_block[blocking_key(entity_family, old_features[step])].append(step)
    for step in new_steps:
        new_by_block[blocking_key(entity_family, new_features[step])].append(step)

    matches: dict[int, int] = {}
    diagnostics: dict[int, dict[str, Any]] = {}
    ambiguous_partitions: list[dict[str, Any]] = []
    ambiguous = 0
    matched_old: set[int] = set()
    matched_new: set[int] = set()
    considered_old: set[int] = set()
    considered_new: set[int] = set()

    for key in sorted(set(old_by_block) & set(new_by_block)):
        block_old = sorted(old_by_block[key])
        block_new = sorted(new_by_block[key])
        considered_old.update(block_old)
        considered_new.update(block_new)
        block_matches, block_diagnostics, block_ambiguous = _run_block_match(
            block_old,
            block_new,
            old_features,
            new_features,
            score_threshold=score_threshold,
            score_margin=score_margin,
            assignment_max=assignment_max,
            depth2_max=depth2_max,
            depth3_max=depth3_max,
        )
        matches.update(block_matches)
        diagnostics.update(_with_block_diagnostics(block_diagnostics, block_stage="coarse_block", block_key=key))
        ambiguous += block_ambiguous
        unresolved_old = [step for step in block_old if step not in block_matches]
        unresolved_new = [step for step in block_new if step not in block_matches.values()]
        if block_ambiguous and unresolved_old and unresolved_new:
            ambiguous_partitions.append({
                "entity_type": entity_family,
                "stage": "coarse_block",
                "reason": "ambiguous_assignment",
                "old_steps": sorted(unresolved_old),
                "new_steps": sorted(unresolved_new),
                "blocking_key": {
                    "entity_type": key[0],
                    "degree_bucket": key[1],
                    "edge_bucket": key[2],
                    "attribute_bucket": key[3],
                    "literal_bucket": key[4],
                    "ancestry_bucket": key[5],
                    "neighborhood_bucket": key[6],
                },
            })
        matched_old.update(block_matches)
        matched_new.update(block_matches.values())

    residual_old = [step for step in old_steps if step not in considered_old and step not in matched_old]
    residual_new = [step for step in new_steps if step not in considered_new and step not in matched_new]
    if residual_old and residual_new:
        residual_matches, residual_diagnostics, residual_ambiguous = _run_block_match(
            sorted(residual_old),
            sorted(residual_new),
            old_features,
            new_features,
            score_threshold=score_threshold,
            score_margin=score_margin,
            assignment_max=assignment_max,
            depth2_max=depth2_max,
            depth3_max=depth3_max,
        )
        matches.update(residual_matches)
        diagnostics.update(_with_block_diagnostics(residual_diagnostics, block_stage="residual", block_key=None))
        ambiguous += residual_ambiguous
        unresolved_old = [step for step in residual_old if step not in residual_matches]
        unresolved_new = [step for step in residual_new if step not in residual_matches.values()]
        if residual_ambiguous and unresolved_old and unresolved_new:
            ambiguous_partitions.append({
                "entity_type": entity_family,
                "stage": "residual",
                "reason": "ambiguous_assignment",
                "old_steps": sorted(unresolved_old),
                "new_steps": sorted(unresolved_new),
            })

    return matches, diagnostics, ambiguous, ambiguous_partitions


def _run_block_match(
    old_steps: list[int],
    new_steps: list[int],
    old_features: dict[int, dict[str, Any]],
    new_features: dict[int, dict[str, Any]],
    *,
    score_threshold: float,
    score_margin: float,
    assignment_max: int,
    depth2_max: int,
    depth3_max: int,
) -> tuple[dict[int, int], dict[int, dict[str, Any]], int]:
    if len(old_steps) > assignment_max or len(new_steps) > assignment_max:
        return fallback_signature_block_match(old_steps, new_steps, old_features, new_features)
    return iterative_assignment_block_match(
        old_steps,
        new_steps,
        old_features,
        new_features,
        score_threshold=score_threshold,
        score_margin=score_margin,
        depth2_max=depth2_max,
        depth3_max=depth3_max,
    )


def _with_block_diagnostics(
    diagnostics: dict[int, dict[str, Any]],
    *,
    block_stage: str,
    block_key: tuple[str | None, int, int, int, int, int, int] | None,
) -> dict[int, dict[str, Any]]:
    out: dict[int, dict[str, Any]] = {}
    for step_id, diag in diagnostics.items():
        matched_on = dict(diag.get("matched_on") or {})
        matched_on["block_stage"] = block_stage
        if block_key is not None:
            matched_on["blocking_key"] = {
                "entity_type": block_key[0],
                "degree_bucket": block_key[1],
                "edge_bucket": block_key[2],
                "attribute_bucket": block_key[3],
                "literal_bucket": block_key[4],
                "ancestry_bucket": block_key[5],
                "neighborhood_bucket": block_key[6],
            }
        out[step_id] = {
            **diag,
            "matched_on": matched_on,
        }
    return out



