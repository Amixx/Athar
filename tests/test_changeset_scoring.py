from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

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


def test_benchmark_provenance_is_stamped_and_rendered():
    from athar_bench import ui

    provenance = bench._provenance({"athar": {"version": "0.1.1"}, "ifcdiff": {"version": "0.8.5"}})
    assert provenance["canon_version"].startswith("athar-canon-")
    assert provenance["tools"]["ifcdiff"] == "0.8.5"
    data = {"summary": [], "aggregates": {}, "tools": {}, "settings": {}, "provenance": provenance}
    page = ui._page(data, Path("bench.json"), ui._dashboard_tabs(data))
    assert provenance["athar_commit"] in page
    assert provenance["canon_version"] in page
    assert provenance["run_date"] in page


def test_dashboard_shows_coverage_and_failed_pairs():
    from athar_bench import ui

    expected = bench.Expected(counts={"added": 0, "deleted": 0, "modified": 1}, changed_guids={"modified": ["M"]})
    rows = [
        _scored_pair("easy", {"athar": _ok_run(modified=["M"])}, expected),
        _scored_pair("hard", {"athar": {"status": "timeout"}}, expected),
    ]
    data = {"summary": rows, "aggregates": bench.aggregate_tools(rows), "tools": {}, "settings": {},
            "provenance": bench._provenance({})}
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


def test_gb_seconds_cover_descendant_processes():
    def run(child_body: str) -> float:
        spawn = (
            "import subprocess, sys\n"
            f"subprocess.run([sys.executable, '-c', {child_body!r}], check=True)\n"
        )
        completed = bench._run_subprocess([sys.executable, "-c", spawn], timeout_s=60)
        assert completed["returncode"] == 0
        return completed["gb_seconds"]

    idle = run("import time; time.sleep(1.5)")
    allocating = run("import time; b = b'x' * (400 * 2**20); time.sleep(1.5)")
    assert allocating > idle + 0.2


def _pid_alive(pid: int) -> bool:
    state = subprocess.run(["ps", "-o", "stat=", "-p", str(pid)], capture_output=True, text=True).stdout.strip()
    return bool(state) and not state.startswith("Z")


def test_timeout_kills_the_tools_descendants(tmp_path):
    pid_file = tmp_path / "grandchild.pid"
    script = (
        "import subprocess, sys, time\n"
        "child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(300)'])\n"
        f"open({str(pid_file)!r}, 'w').write(str(child.pid))\n"
        "time.sleep(300)\n"
    )
    completed = bench._run_subprocess([sys.executable, "-c", script], timeout_s=2)
    assert completed["status"] == "timeout"
    grandchild = int(pid_file.read_text())
    try:
        deadline = time.monotonic() + 5
        while _pid_alive(grandchild) and time.monotonic() < deadline:
            time.sleep(0.05)
        assert not _pid_alive(grandchild)
    finally:
        if _pid_alive(grandchild):
            os.kill(grandchild, 9)


def test_same_file_pairs_diff_a_copy_at_a_second_path(tmp_path):
    pair = next(pair for pair in bench.BENCHMARK_PAIRS if pair.kind == "same_file")
    old, new, _ = pair.builder(tmp_path)
    assert old != new
    assert Path(old).read_bytes() == Path(new).read_bytes()


def test_mutation_truth_uses_edited_type_occurrences_and_container(tmp_path):
    type_pair = next(pair for pair in bench.BENCHMARK_PAIRS if pair.kind == "synthetic_inherited_data")
    old, new, expected = type_pair.builder(tmp_path / "type")

    import ifcopenshell
    import ifcopenshell.util.element

    changed = expected.changed_guids["modified"]
    model = ifcopenshell.open(new)
    type_guid, = expected.changed_guids["optional_modified"]
    edited_type = model.by_guid(type_guid)
    inheritors = sorted(p.GlobalId for p in model.by_type("IfcProduct")
                        if ifcopenshell.util.element.get_type(p) == edited_type)
    assert changed == inheritors
    assert len(changed) > 1
    assert bench.score_pair({"runs": {"tool": _ok_run(modified=changed + ["UNRELATED"])}}, expected)["tool"]["fp"] == 1

    add_pair = next(pair for pair in bench.BENCHMARK_PAIRS if pair.kind == "synthetic_add")
    old, new, expected = add_pair.builder(tmp_path / "add")
    import ifcopenshell
    before, after = ifcopenshell.open(old), ifcopenshell.open(new)
    added = {p.GlobalId for p in after.by_type("IfcProduct")} - {p.GlobalId for p in before.by_type("IfcProduct")}
    assert expected.changed_guids["added"] == sorted(added)
    product = after.by_guid(next(iter(added)))
    container = product.ContainedInStructure[0].RelatingStructure.GlobalId
    assert expected.changed_guids["optional_modified"] == [container]
    runs = {"tool": _ok_run(added=list(added), modified=[container, "UNRELATED"])}
    assert bench.score_pair({"runs": runs}, expected)["tool"]["fp"] == 1


def test_benchmark_cli_scores_derived_mutation_truth(tmp_path):
    import json
    import subprocess

    output = tmp_path / "benchmark.json"
    python = Path(__file__).resolve().parent.parent / ".venv" / "bin" / "python"
    subprocess.run(
        [str(python), str(_BENCHMARK_PATH), "--only", "mutation_rename", "--repeats", "1",
         "--no-speckle", "--no-ifcfast", "--no-ifcgit", "--out", str(output), "--workdir", str(tmp_path / "work")],
        check=True, capture_output=True, text=True,
    )
    pair, = json.loads(output.read_text())["pairs"]
    assert "error" not in pair
    assert len(pair["expected"]["changed_guids"]["modified"]) == 1
    assert "athar" in pair["changeset_scores"]


def test_completed_run_without_reported_guids_is_rejected():
    expected = bench.Expected(changed_guids={"added": [], "deleted": [], "modified": ["M"]})
    with pytest.raises(ValueError, match="reported_guids"):
        bench.score_pair({"runs": {"athar": {"status": "ok"}}}, expected)


def test_provenance_commit_names_head_and_marks_tracked_edits():
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=bench.REPO_ROOT, check=True, capture_output=True, text=True
    ).stdout.strip()
    dirty = subprocess.run(
        ["git", "status", "--porcelain", "--untracked-files=no"],
        cwd=bench.REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert bench._provenance({})["athar_commit"] == (f"{head}-dirty" if dirty else head)
