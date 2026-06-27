from __future__ import annotations

from athar_qa import render_check_report, render_history


def test_render_check_report_pass():
    result = {"ok": True, "violations": [], "summary": {"added": 0, "deleted": 0, "modified": 0, "unchanged": 5}}
    text = render_check_report(result)
    assert text.startswith("Athar QA gate: PASS")
    assert "unchanged 5" in text


def test_render_check_report_fail_shows_property_change():
    result = {
        "ok": False,
        "summary": {"added": 0, "deleted": 0, "modified": 1, "unchanged": 0},
        "violations": [
            {
                "code": "property_value_changed",
                "message": "Protected property FireRating changed on 1 entity(s)",
                "property": "FireRating",
                "count": 1,
                "affected": [
                    {"class": "IfcWall", "step_id": 21, "guid": "WALL", "name": "Wall", "property": "FireRating", "old_value": "90", "new_value": "60"}
                ],
            }
        ],
    }
    text = render_check_report(result)
    assert "FAIL (1 violation)" in text
    assert "FireRating 90 -> 60" in text
    assert 'IfcWall "Wall" #21 WALL' in text


def test_render_check_report_schema_and_truncation():
    result = {
        "ok": False,
        "summary": {},
        "violations": [
            {"code": "schema_changed", "message": "Schema changed", "old": "IFC2X3", "new": "IFC4"},
            {
                "code": "property_removed",
                "message": "Properties removed from 3 entity(s)",
                "count": 3,
                "affected": [{"class": "IfcWall", "step_id": 1, "removed_properties": ["A", "B"]}],
            },
        ],
    }
    text = render_check_report(result)
    assert "schema: IFC2X3 -> IFC4" in text
    assert "removed A, B" in text
    assert "... and 2 more" in text


def test_render_history_timeline():
    events = [
        {"id": "evt-000001", "event": "baseline_set", "at": "2026-01-01T00:00:00Z", "actor": "ann", "model_key": "arch", "artifact": {"schema": "IFC4"}},
        {
            "id": "evt-000002",
            "event": "review",
            "at": "2026-01-02T00:00:00Z",
            "actor": "bob",
            "model_key": "arch",
            "verdict": "fail",
            "has_policy": True,
            "diff": {"stats": {"added": 1, "deleted": 0, "modified": 2, "unchanged": 9}},
        },
        {"id": "evt-000003", "event": "approval", "at": "2026-01-03T00:00:00Z", "actor": "ann", "model_key": "arch", "review_id": "evt-000002"},
    ]
    text = render_history(events)
    assert "review FAIL [policy]" in text
    assert "+1 -0 ~2 =9" in text
    assert "baseline_set" in text
    assert "Reviews: 0 passed, 1 failed (3 events total)" in text


def test_render_history_empty():
    assert "(no events)" in render_history([])
