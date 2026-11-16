"""Precision/recall scoring of a reported changeset against ground truth.

A changeset is the set of products a diff tool reports as added, deleted, or
modified, identified by GlobalId. Ground truth is the same shape, established
independently of the tool under test (an authoring-tool revision log, or a
manual review of a real v1->v2 pair).

The comparison unit is the (category, guid) pair, so reporting a product as
`added` when ground truth says `modified` is scored as both a false positive
(in `added`) and a false negative (in `modified`).

Ground truth may also carry ``optional_<category>`` sets: entities whose
membership in the changeset is legitimately reportable but not required —
canonically the direct spatial containers of an added/deleted element, whose
child set factually changed. Reporting an optional entity is not a false
positive; omitting it is not a false negative. Optional sets are derived from
the edit itself (which containers' membership changed), never from any tool's
output.
"""

from __future__ import annotations

from typing import Iterable, Mapping

CATEGORIES = ("added", "deleted", "modified")

Changeset = Mapping[str, Iterable[str]]


def prf(tp: int, fp: int, fn: int) -> tuple[float, float, float]:
    """Precision, recall and F1 from counts.

    Nothing reported has precision 1 (no false claim); nothing to find has
    recall 1 (nothing missed). So an empty report on an empty truth scores 1
    everywhere, and any false positive on an empty truth scores precision 0
    and F1 0.
    """
    precision = 1.0 if tp + fp == 0 else tp / (tp + fp)
    recall = 1.0 if tp + fn == 0 else tp / (tp + fn)
    f1 = 0.0 if precision + recall == 0 else 2 * precision * recall / (precision + recall)
    return round(precision, 4), round(recall, 4), round(f1, 4)


def _category_score(reported: set[str], truth: set[str], optional: set[str]) -> dict:
    tp = len(reported & truth)
    fp = len(reported - truth - optional)
    fn = len(truth - reported)
    precision, recall, f1 = prf(tp, fp, fn)
    score = {
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "false_positives": sorted(reported - truth - optional),
        "false_negatives": sorted(truth - reported),
    }
    if optional:
        score["optional_reported"] = sorted(reported & optional)
    return score


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
        optional_set = {guid for guid in truth.get(f"optional_{category}", ()) if guid}
        score = _category_score(reported_set, truth_set, optional_set)
        per_category[category] = score
        tp += score["tp"]
        fp += score["fp"]
        fn += score["fn"]
    precision, recall, f1 = prf(tp, fp, fn)
    return {
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "per_category": per_category,
    }
