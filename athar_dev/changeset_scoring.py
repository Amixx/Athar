"""Precision/recall scoring of a reported changeset against ground truth.

A changeset is the set of products a diff tool reports as added, deleted, or
modified, identified by GlobalId. Ground truth is the same shape, established
independently of the tool under test (an authoring-tool revision log, or a
manual review of a real v1->v2 pair).

The comparison unit is the (category, guid) pair, so reporting a product as
`added` when ground truth says `modified` is scored as both a false positive
(in `added`) and a false negative (in `modified`).
"""

from __future__ import annotations

from typing import Iterable, Mapping

CATEGORIES = ("added", "deleted", "modified")

Changeset = Mapping[str, Iterable[str]]


def _ratio(numerator: int, denominator: int) -> float | None:
    if denominator == 0:
        return None
    return round(numerator / denominator, 4)


def _f1(precision: float | None, recall: float | None) -> float | None:
    if precision is None or recall is None or (precision + recall) == 0:
        return None
    return round(2 * precision * recall / (precision + recall), 4)


def _category_score(reported: set[str], truth: set[str]) -> dict:
    tp = len(reported & truth)
    fp = len(reported - truth)
    fn = len(truth - reported)
    precision = _ratio(tp, tp + fp)
    recall = _ratio(tp, tp + fn)
    return {
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "precision": precision,
        "recall": recall,
        "f1": _f1(precision, recall),
        "false_positives": sorted(reported - truth),
        "false_negatives": sorted(truth - reported),
    }


def score_changeset(reported: Changeset, truth: Changeset) -> dict:
    """Score a reported changeset against ground truth.

    Both inputs map each category in ``CATEGORIES`` to an iterable of GlobalIds.
    Returns overall and per-category precision/recall/F1 plus the offending
    GlobalIds, so a human can inspect what the tool missed or invented.
    """
    per_category: dict[str, dict] = {}
    tp = fp = fn = 0
    for category in CATEGORIES:
        reported_set = {guid for guid in reported.get(category, ()) if guid}
        truth_set = {guid for guid in truth.get(category, ()) if guid}
        score = _category_score(reported_set, truth_set)
        per_category[category] = score
        tp += score["tp"]
        fp += score["fp"]
        fn += score["fn"]
    precision = _ratio(tp, tp + fp)
    recall = _ratio(tp, tp + fn)
    return {
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "precision": precision,
        "recall": recall,
        "f1": _f1(precision, recall),
        "per_category": per_category,
    }
