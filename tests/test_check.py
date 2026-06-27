from __future__ import annotations

import pytest

from athar.check import builtin_policy_packs, evaluate_report, resolve_policy


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


def _report_with_property_deltas() -> dict:
    return {
        "schemas": {"old": "IFC4", "new": "IFC4"},
        "stats": {"added": 0, "deleted": 0, "modified": 2, "unchanged": 0},
        "added": [],
        "deleted": [],
        "modified": [
            {
                "old": {"class": "IfcWall", "step_id": 11, "guid": "WALL", "name": "Wall"},
                "new": {"class": "IfcWall", "step_id": 21, "guid": "WALL", "name": "Wall"},
                "aspects": {"data": "changed"},
                "change_scope": "intrinsic",
                "property_deltas": {
                    "changed": [{"name": "FireRating", "old_value": "90", "new_value": "60"}],
                    "removed": [{"name": "AcousticRating", "old_value": "52"}],
                },
            },
            {
                "old": {"class": "IfcDoor", "step_id": 12, "guid": "DOOR", "name": "Door"},
                "new": {"class": "IfcDoor", "step_id": 22, "guid": "DOOR", "name": "Door"},
                "aspects": {"data": "changed"},
                "change_scope": "intrinsic",
                "property_deltas": {
                    "changed": [{"name": "Width", "old_value": 900, "new_value": 1000}],
                },
            },
        ],
        "unchanged": [],
    }


def test_check_flags_removed_properties():
    result = evaluate_report(_report_with_property_deltas(), {"forbid_property_removal": "*"})

    assert result["ok"] is False
    violation = next(v for v in result["violations"] if v["code"] == "property_removed")
    assert violation["count"] == 1
    assert violation["affected"][0]["class"] == "IfcWall"
    assert violation["affected"][0]["removed_properties"] == ["AcousticRating"]


def test_check_property_removal_respects_class_filter():
    result = evaluate_report(_report_with_property_deltas(), {"forbid_property_removal": ["IfcDoor"]})

    assert result["ok"] is True
    assert result["violations"] == []


def test_check_flags_protected_property_value_change():
    result = evaluate_report(
        _report_with_property_deltas(),
        {"forbid_property_value_change": {"FireRating": "*"}},
    )

    assert result["ok"] is False
    violation = next(v for v in result["violations"] if v["code"] == "property_value_changed")
    assert violation["property"] == "FireRating"
    assert violation["affected"][0]["old_value"] == "90"
    assert violation["affected"][0]["new_value"] == "60"


def test_check_protected_property_change_ignores_other_properties():
    result = evaluate_report(
        _report_with_property_deltas(),
        {"forbid_property_value_change": {"FireRating": ["IfcDoor"]}},
    )

    assert result["ok"] is True


def test_check_rejects_invalid_property_value_change_shape():
    with pytest.raises(ValueError, match="forbid_property_value_change must be an object"):
        evaluate_report(_report_with_property_deltas(), {"forbid_property_value_change": ["FireRating"]})


def test_builtin_policy_packs_are_loadable():
    packs = builtin_policy_packs()
    assert {"data-integrity", "safety-critical", "no-deletions", "georeferencing"} <= set(packs)
    for name in packs:
        policy = resolve_policy(name)
        assert isinstance(policy, dict)
        # Each shipped pack is a valid, evaluable policy.
        assert evaluate_report(_report(), policy)["summary"]


def test_resolve_policy_prefers_file_path(tmp_path):
    policy_file = tmp_path / "p.json"
    policy_file.write_text('{"max_deleted": 0}', encoding="utf-8")
    assert resolve_policy(str(policy_file)) == {"max_deleted": 0}


def test_resolve_policy_unknown_name_lists_packs():
    with pytest.raises(ValueError, match="not a file and not a builtin pack"):
        resolve_policy("does-not-exist")


def test_safety_critical_pack_catches_fire_rating_change():
    result = evaluate_report(_report_with_property_deltas(), resolve_policy("safety-critical"))

    assert result["ok"] is False
    codes = {v["code"] for v in result["violations"]}
    assert {"property_value_changed", "property_removed"} <= codes
