from __future__ import annotations

import pytest

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
                    {
                        "class": "IfcWall",
                        "step_id": 21,
                        "guid": "WALL",
                        "name": "Wall",
                        "property": "FireRating",
                        "changes": [
                            {"kind": "changed", "pset": "Pset_WallCommon", "inherited": False, "old_value": "90", "new_value": "60"},
                            {"kind": "changed", "pset": "Pset_WallCommon", "inherited": True, "old_value": "90", "new_value": "30"},
                            {"kind": "removed", "pset": "Old", "inherited": False, "old_value": "90"},
                            {"kind": "added", "pset": "New", "inherited": True, "new_value": "30"},
                            {"kind": "count_changed", "pset": "Dup", "inherited": False, "old_count": 2, "new_count": 1},
                        ],
                    }
                ],
            }
        ],
    }
    text = render_check_report(result)
    assert "FAIL (1 violation)" in text
    assert (
        "Pset_WallCommon.FireRating 90 -> 60; Pset_WallCommon.FireRating (type) 90 -> 30; "
        "Old.FireRating 90 removed; New.FireRating (type) 30 added; Dup.FireRating occurs 2 -> 1 times"
    ) in text
    assert 'IfcWall "Wall" #21 WALL' in text


def test_render_check_report_property_added():
    result = {
        "ok": False,
        "violations": [
            {
                "code": "property_added",
                "message": "Protected property FireRating added to 1 entity(s)",
                "count": 1,
                "affected": [
                    {
                        "class": "IfcWall",
                        "step_id": 21,
                        "guid": "WALL",
                        "name": "Wall",
                        "property": "FireRating",
                        "added": [{"pset": "Pset_WallCommon", "inherited": False, "new_value": "REI60"}],
                    }
                ],
            }
        ],
    }
    assert 'IfcWall "Wall" #21 WALL: added Pset_WallCommon.FireRating REI60' in render_check_report(result)


def test_render_check_report_schema_and_truncation():
    result = {
        "ok": False,
        "summary": {},
        "violations": [
            {"code": "schema_mismatch", "message": "Schema changed", "old": "IFC2X3", "new": "IFC4"},
            {
                "code": "property_removed",
                "message": "Properties removed from 3 entity(s)",
                "count": 3,
                "affected": [{"class": "IfcWall", "step_id": 1, "removed_properties": [
                    {"pset": "P", "name": "A", "inherited": False},
                    {"pset": "P", "name": "B", "inherited": True},
                ]}],
            },
        ],
    }
    text = render_check_report(result)
    assert "schema: IFC2X3 -> IFC4" in text
    assert "removed P.A, P.B (type)" in text
    assert "... and 2 more" in text


def test_render_check_report_lists_ten_affected_entities():
    affected = [{"class": "IfcWall", "step_id": i, "guid": f"G{i}", "name": f"W{i}"} for i in range(50)]
    result = {
        "ok": False,
        "violations": [
            {
                "code": "deleted_limit",
                "message": "max_deleted exceeded: 60 > 0",
                "count": 60,
                "affected": affected,
                "truncated": 10,
            }
        ],
    }

    text = render_check_report(result)

    assert 'IfcWall "W9" #9 G9' in text
    assert "W10" not in text
    assert "... and 50 more" in text


def test_render_check_report_without_ok_is_an_error():
    with pytest.raises(ValueError, match="no boolean 'ok'"):
        render_check_report({"violations": []})


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
