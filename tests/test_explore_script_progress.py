import json

from scripts.explore import evaluate_matcher_quality, stress_determinism


def test_stress_determinism_reports_round_progress(monkeypatch, capsys, tmp_path):
    out = tmp_path / "determinism.json"
    monkeypatch.setattr(
        "sys.argv",
        [
            "stress_determinism",
            "--rounds",
            "2",
            "--progress-every",
            "1",
            "--out",
            str(out),
        ],
    )

    stress_determinism.main()
    captured = capsys.readouterr()

    assert "[determinism] round 1/2" in captured.err
    assert "[determinism] round 2/2" in captured.err
    assert "[determinism] completed rounds=2" in captured.err
    payload = json.loads(out.read_text(encoding="utf-8"))
    assert payload["rounds"] == 2


def test_evaluate_matcher_quality_reports_scenario_progress(monkeypatch, capsys, tmp_path):
    out = tmp_path / "matcher_quality.json"
    monkeypatch.setattr(
        "sys.argv",
        [
            "evaluate_matcher_quality",
            "--out",
            str(out),
        ],
    )

    evaluate_matcher_quality.main()
    captured = capsys.readouterr()

    assert "[matcher-quality] scenario 1/4 start" in captured.err
    assert "[matcher-quality] scenario 4/4 done" in captured.err
    assert "[matcher-quality] completed scenarios=4" in captured.err
    payload = json.loads(out.read_text(encoding="utf-8"))
    assert payload["scenario_count"] == 4
