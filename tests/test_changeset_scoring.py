from __future__ import annotations

from athar_dev.changeset_scoring import score_changeset


def test_perfect_match_scores_one():
    truth = {"added": ["A"], "deleted": ["D"], "modified": ["M1", "M2"]}
    score = score_changeset(truth, truth)
    assert score["precision"] == 1.0
    assert score["recall"] == 1.0
    assert score["f1"] == 1.0
    assert (score["tp"], score["fp"], score["fn"]) == (4, 0, 0)


def test_false_positive_lowers_precision_only():
    truth = {"added": [], "deleted": [], "modified": ["M"]}
    reported = {"added": [], "deleted": [], "modified": ["M", "PHANTOM"]}
    score = score_changeset(reported, truth)
    assert score["recall"] == 1.0
    assert score["precision"] == 0.5
    assert score["per_category"]["modified"]["false_positives"] == ["PHANTOM"]


def test_missed_change_lowers_recall_only():
    truth = {"added": [], "deleted": [], "modified": ["M1", "M2"]}
    reported = {"added": [], "deleted": [], "modified": ["M1"]}
    score = score_changeset(reported, truth)
    assert score["precision"] == 1.0
    assert score["recall"] == 0.5
    assert score["per_category"]["modified"]["false_negatives"] == ["M2"]


def test_misclassified_category_penalized_both_sides():
    truth = {"added": [], "deleted": [], "modified": ["X"]}
    reported = {"added": ["X"], "deleted": [], "modified": []}
    score = score_changeset(reported, truth)
    assert (score["tp"], score["fp"], score["fn"]) == (0, 1, 1)
    assert score["precision"] == 0.0
    assert score["recall"] == 0.0


def test_empty_reported_and_truth_leaves_ratios_undefined():
    score = score_changeset({}, {})
    assert score["precision"] is None
    assert score["recall"] is None
    assert score["f1"] is None
    assert (score["tp"], score["fp"], score["fn"]) == (0, 0, 0)


def test_falsy_guids_are_ignored():
    truth = {"added": ["A", None, ""], "deleted": [], "modified": []}
    reported = {"added": ["A"], "deleted": [], "modified": []}
    score = score_changeset(reported, truth)
    assert score["precision"] == 1.0
    assert score["recall"] == 1.0
