from __future__ import annotations

import pytest

from athar.check import evaluate_report


def test_check_passes_when_report_stays_within_policy():
    result = evaluate_report(
        _report(),
        {
            "max_deleted": 1,
            "max_modified": 2,
            "max_deleted_by_class": {"IfcWall": 1},
            "max_modified_change_scope": {"intrinsic": 1, "transitive": 1},
        },
    )

    assert result["ok"] is True
    assert result["violations"] == []
    assert result["summary"] == {"added": 0, "deleted": 1, "modified": 2, "unchanged": 3}


def test_check_reports_count_and_class_budget_violations():
    result = evaluate_report(
        _report(),
        {
            "max_deleted": 0,
            "max_modified_by_class": {"IfcWall": 0},
        },
    )

    assert result["ok"] is False
    assert [item["code"] for item in result["violations"]] == [
        "deleted_limit",
        "modified_class_limit",
    ]
    assert result["violations"][1]["class_name"] == "IfcWall"


def test_check_reports_site_and_placement_policy_violations():
    result = evaluate_report(
        _report(),
        {
            "forbid_site_placement_change": True,
            "max_placement_delta_mm": 100,
        },
    )

    assert result["ok"] is False
    assert [item["code"] for item in result["violations"]] == [
        "site_placement_changed",
        "placement_delta_limit",
    ]
    assert result["violations"][0]["affected"][0]["class"] == "IfcSite"
    assert result["violations"][1]["affected"][0]["placement_delta_mm"] == 250.0


def test_check_reports_forbidden_aspect_changes():
    result = evaluate_report(
        _report(),
        {
            "forbid_aspect_changes": {
                "data": ["IfcWall"],
                "topology": "*",
            }
        },
    )

    assert result["ok"] is False
    assert [item["aspect"] for item in result["violations"]] == ["data", "topology"]
    assert result["violations"][0]["affected"][0]["class"] == "IfcWall"


def test_check_reports_schema_change_in_existing_report():
    report = _report()
    report["schemas"] = {"old": "IFC2X3", "new": "IFC4"}

    result = evaluate_report(report, {})

    assert result["ok"] is False
    assert result["violations"][0]["code"] == "schema_changed"


def test_check_rejects_invalid_policy_shape():
    with pytest.raises(ValueError, match="max_deleted must be a non-negative integer"):
        evaluate_report(_report(), {"max_deleted": -1})

    with pytest.raises(ValueError, match="Unsupported aspect"):
        evaluate_report(_report(), {"forbid_aspect_changes": {"owner": "*"}})


def _report() -> dict:
    return {
        "engine": "athar",
        "schemas": {"old": "IFC4", "new": "IFC4"},
        "stats": {"added": 0, "deleted": 1, "modified": 2, "unchanged": 3},
        "added": [],
        "deleted": [
            {"class": "IfcWall", "step_id": 10, "guid": "WALL_DELETED", "name": "Deleted wall"},
        ],
        "modified": [
            {
                "old": {"class": "IfcWall", "step_id": 11, "guid": "WALL", "name": "Wall"},
                "new": {"class": "IfcWall", "step_id": 21, "guid": "WALL", "name": "Wall"},
                "aspects": {
                    "geometry": "unchanged",
                    "data": "changed",
                    "topology": "changed",
                    "placement": "unchanged",
                    "placement_delta_mm": [0.0, 0.0, 0.0],
                },
                "change_scope": "mixed",
            },
            {
                "old": {"class": "IfcSite", "step_id": 12, "guid": "SITE", "name": "Site"},
                "new": {"class": "IfcSite", "step_id": 22, "guid": "SITE", "name": "Site"},
                "aspects": {
                    "geometry": "unchanged",
                    "data": "unchanged",
                    "topology": "unchanged",
                    "placement": "changed",
                    "placement_delta_mm": [150.0, 200.0, 0.0],
                },
                "change_scope": "intrinsic",
            },
        ],
        "unchanged": [{}, {}, {}],
    }
