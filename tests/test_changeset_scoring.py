from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

from athar_dev.changeset_scoring import score_changeset

_BENCHMARK_PATH = Path(__file__).resolve().parent.parent / "scripts" / "explore" / "benchmark_competitors.py"


def _load_benchmark():
    spec = importlib.util.spec_from_file_location("benchmark_competitors", _BENCHMARK_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


bench = _load_benchmark()


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


def test_empty_report_on_empty_truth_scores_one():
    truth = {"added": [], "deleted": [], "modified": [], "optional_modified": ["STOREY"]}
    for reported in ({}, {"modified": ["STOREY"]}):
        score = score_changeset(reported, truth)
        assert (score["precision"], score["recall"], score["f1"]) == (1.0, 1.0, 1.0)
        assert (score["tp"], score["fp"], score["fn"]) == (0, 0, 0)


def test_any_report_on_empty_truth_is_a_false_positive_with_f1_zero():
    score = score_changeset({"modified": ["PHANTOM"]}, {"added": [], "deleted": [], "modified": []})
    assert (score["tp"], score["fp"], score["fn"]) == (0, 1, 0)
    assert score["precision"] == 0.0
    assert score["f1"] == 0.0


def test_empty_report_on_real_changes_scores_f1_zero():
    score = score_changeset({}, {"modified": ["M1", "M2"]})
    assert score["recall"] == 0.0
    assert score["f1"] == 0.0


def test_falsy_guids_are_ignored():
    truth = {"added": ["A", None, ""], "deleted": [], "modified": []}
    reported = {"added": ["A"], "deleted": [], "modified": []}
    score = score_changeset(reported, truth)
    assert score["precision"] == 1.0
    assert score["recall"] == 1.0


def test_optional_members_are_neither_fp_nor_fn():
    truth = {"deleted": ["D"], "modified": [], "optional_modified": ["STOREY"]}
    reported_with = {"deleted": ["D"], "modified": ["STOREY"]}
    reported_without = {"deleted": ["D"], "modified": []}
    for reported in (reported_with, reported_without):
        score = score_changeset(reported, truth)
        assert (score["tp"], score["fp"], score["fn"]) == (1, 0, 0), reported
        assert score["precision"] == 1.0
        assert score["recall"] == 1.0
    assert score_changeset(reported_with, truth)["per_category"]["modified"]["optional_reported"] == ["STOREY"]


def test_optional_membership_is_category_scoped():
    truth = {"modified": [], "optional_modified": ["STOREY"], "added": []}
    reported = {"added": ["STOREY"], "modified": []}
    score = score_changeset(reported, truth)
    assert score["fp"] == 1


def test_optional_does_not_shield_unrelated_extras():
    truth = {"modified": ["A"], "optional_modified": ["STOREY"]}
    reported = {"modified": ["A", "STOREY", "NOISE1", "NOISE2"]}
    score = score_changeset(reported, truth)
    assert (score["tp"], score["fp"], score["fn"]) == (1, 2, 0)
    assert score["per_category"]["modified"]["false_positives"] == ["NOISE1", "NOISE2"]


def _ok_run(**reported):
    return {"status": "ok", "reported_guids": {"added": [], "deleted": [], "modified": [], **reported}}


def test_benchmark_scores_pairs_whose_truth_is_empty():
    reserialize = next(pair for pair in bench.BENCHMARK_PAIRS if pair.name == "reserialize_gni_190")
    entry = {"runs": {"quiet": _ok_run(), "noisy": _ok_run(modified=["A", "B"])}}
    scores = bench._score_pair(entry, reserialize.expected)
    assert scores["quiet"]["f1"] == 1.0
    assert (scores["noisy"]["f1"], scores["noisy"]["fp"]) == (0.0, 2)
