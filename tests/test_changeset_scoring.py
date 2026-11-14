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
    guids = {"added": [], "deleted": [], "modified": [], **reported}
    return {"status": "ok", "reported_guids": guids, "counts": {key: len(value) for key, value in guids.items()}}


def test_benchmark_scores_pairs_whose_truth_is_empty():
    reserialize = next(pair for pair in bench.BENCHMARK_PAIRS if pair.name == "reserialize_gni_190")
    entry = {"runs": {"quiet": _ok_run(), "noisy": _ok_run(modified=["A", "B"])}}
    scores = bench.score_pair(entry, reserialize.expected)
    assert scores["quiet"]["f1"] == 1.0
    assert (scores["noisy"]["f1"], scores["noisy"]["fp"]) == (0.0, 2)


def _scored_pair(name, runs, expected, *, dataset="synthetic"):
    entry = {"name": name, "kind": "test", "set": dataset, "expected": bench._expected_json(expected), "runs": runs}
    return bench.finish_pair(entry, expected)


def test_failed_runs_count_as_misses_and_lower_coverage():
    expected = bench.Expected(counts={"added": 0, "deleted": 0, "modified": 1}, changed_guids={"modified": ["M"]})
    rows = [
        _scored_pair("easy", {"steady": _ok_run(modified=["M"]), "crashy": _ok_run(modified=["M"])}, expected),
        _scored_pair(
            "hard",
            {"steady": _ok_run(modified=["M"]), "crashy": {"status": "error", "error": "Traceback"}},
            expected,
        ),
        _scored_pair(
            "slow",
            {"steady": _ok_run(modified=["M"]), "crashy": {"status": "timeout"}, "absent": {"status": "not_installed"}},
            expected,
        ),
    ]
    assert rows[1]["assessment"]["crashy"] == "error"
    assert rows[2]["assessment"]["crashy"] == "timeout"
    aggregates = bench.aggregate_tools(rows)["synthetic"]
    crashy = aggregates["crashy"]
    assert (crashy["completed"], crashy["attempted"]) == (1, 3)
    assert (crashy["judged"], crashy["expected"], crashy["unexpected"]) == (3, 1, 2)
    assert crashy["changeset"]["pairs"] == 3
    assert crashy["changeset"]["macro"]["recall"] == round(1 / 3, 4)
    assert aggregates["steady"]["changeset"]["macro"]["f1"] == 1.0
    assert "absent" not in aggregates


def test_dashboard_shows_coverage_and_failed_pairs():
    from athar_bench import ui

    expected = bench.Expected(counts={"added": 0, "deleted": 0, "modified": 1}, changed_guids={"modified": ["M"]})
    rows = [
        _scored_pair("easy", {"athar": _ok_run(modified=["M"])}, expected),
        _scored_pair("hard", {"athar": {"status": "timeout"}}, expected),
    ]
    data = {"summary": rows, "aggregates": bench.aggregate_tools(rows), "tools": {}, "settings": {}}
    page = ui._page(data, Path("bench.json"), ui._dashboard_tabs(data))
    assert "1 / 2" in page
    assert "0.50 / 0.50 / 0.50 (2p)" in page
    assert "Timeout" in page
    assert "Changeset P / R / F1 (macro)" in page
    assert "Changeset P / R / F1 (micro)" in page


def test_micro_average_weighs_changes_macro_weighs_pairs():
    small = bench.Expected(changed_guids={"modified": ["M"]})
    large = bench.Expected(changed_guids={"modified": [f"L{index}" for index in range(9)]})
    rows = [
        _scored_pair("small", {"athar": _ok_run(modified=["M"])}, small),
        _scored_pair("large", {"athar": _ok_run()}, large),
    ]
    changeset = bench.aggregate_tools(rows)["synthetic"]["athar"]["changeset"]
    assert changeset["macro"]["recall"] == 0.5
    assert changeset["micro"]["recall"] == 0.1
    assert (changeset["tp"], changeset["fp"], changeset["fn"]) == (1, 0, 9)


def test_time_summary_is_geometric_mean_of_ratios_to_athar():
    def timed(seconds):
        return {**_ok_run(), "seconds_median": seconds}

    expected = bench.Expected(changed_guids={})
    rows = [
        _scored_pair("tiny", {"athar": timed(1.0), "other": timed(2.0)}, expected),
        _scored_pair("huge", {"athar": timed(100.0), "other": timed(50.0)}, expected),
    ]
    ratio = bench.aggregate_tools(rows)["synthetic"]["other"]["vs_reference"]["seconds_median"]
    assert ratio == {"geomean_ratio": 1.0, "pairs": 2}


def test_repeats_that_disagree_are_flagged_nondeterministic():
    agreeing = bench.combine_repeats([_ok_run(modified=["A", "B"]), _ok_run(modified=["B", "A"])])
    assert "nondeterministic" not in agreeing
    wobbling = bench.combine_repeats([_ok_run(modified=["A"]), _ok_run(modified=["A", "B"]), _ok_run(modified=["A"])])
    assert len(wobbling["nondeterministic"]) == 2
    expected = bench.Expected(changed_guids={"modified": ["A"]})
    row = _scored_pair("wobble", {"athar": wobbling}, expected)
    assert bench.aggregate_tools([row])["synthetic"]["athar"]["nondeterministic"] == 1


def test_peak_rss_covers_descendant_processes():
    allocate = "b = b'x' * (200 * 2**20)"
    spawn = f"import subprocess, sys; subprocess.run([sys.executable, '-c', {allocate!r}], check=True)"
    completed = bench._run_subprocess([sys.executable, "-c", spawn], timeout_s=60)
    assert completed["returncode"] == 0
    assert completed["peak_rss_mb"] >= 200


def test_same_file_pairs_diff_a_copy_at_a_second_path(tmp_path):
    pair = next(pair for pair in bench.BENCHMARK_PAIRS if pair.kind == "same_file")
    old, new = pair.builder(tmp_path)
    assert old != new
    assert Path(old).read_bytes() == Path(new).read_bytes()
