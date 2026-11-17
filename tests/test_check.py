from __future__ import annotations

import copy
import json
import uuid

import ifcopenshell
import ifcopenshell.guid
import pytest

import athar.__main__ as main_mod
from athar.engine import diff_files
from athar.check import builtin_policy_packs, evaluate_report, normalize_policy, resolve_policy
from tests.test_report_semantics import diff, reclassify


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


def test_report_comparing_two_schemas_is_an_error():
    report = _report()
    report["schemas"] = {"old": "IFC2X3", "new": "IFC4"}

    with pytest.raises(ValueError, match="compares IFC2X3 with IFC4"):
        evaluate_report(report, {})


def test_schema_change_rule_is_not_a_policy_key():
    with pytest.raises(ValueError, match="Unknown policy key.*forbid_schema_change"):
        normalize_policy({"forbid_schema_change": True})


@pytest.mark.parametrize("output", ["json", "text"])
def test_cli_schema_mismatch_fails_the_gate(tmp_path, capsys, output):
    old = _write_ifc(tmp_path / "old.ifc", walls=_WALLS, schema="IFC2X3")
    new = _write_ifc(tmp_path / "new.ifc", walls=_WALLS)

    code = main_mod._run_check([old, new, "--policy", "no-deletions", "--format", output])

    out = capsys.readouterr().out
    assert code == 2
    if output == "json":
        verdict = json.loads(out)
        assert verdict["ok"] is False
        assert verdict["violations"] == [
            {
                "code": "schema_mismatch",
                "message": "Schema changed from IFC2X3 to IFC4; the files cannot be diffed",
                "old": "IFC2X3",
                "new": "IFC4",
            }
        ]
    else:
        assert "FAIL (1 violation)" in out
        assert "schema: IFC2X3 -> IFC4" in out


def test_check_rejects_invalid_policy_shape():
    with pytest.raises(ValueError, match="max_deleted must be a non-negative integer"):
        evaluate_report(_report(), {"max_deleted": -1})

    with pytest.raises(ValueError, match="Unknown aspect.*forbid_aspect_changes: owner"):
        evaluate_report(_report(), {"forbid_aspect_changes": {"owner": "*"}})


@pytest.mark.parametrize(
    ("policy", "message"),
    [
        ({"max_delete": 0}, "Unknown policy key.*max_delete"),
        ({"max_deleted": "0"}, "max_deleted must be a non-negative integer"),
        ({"max_deleted": True}, "max_deleted must be a non-negative integer"),
        ({"max_deleted_by_class": {"IfcWall": -1}}, "max_deleted_by_class.IfcWall must be a non-negative integer"),
        ({"max_modified_change_scope": {"intrinisc": 0}}, "Unknown change scope.*intrinisc"),
        ({"forbid_aspect_changes": {"geometry": []}}, "forbid_aspect_changes.geometry must be"),
        ({"forbid_site_placement_change": "yes"}, "forbid_site_placement_change must be true or false"),
        ({"forbid_property_value_change": ["FireRating"]}, "forbid_property_value_change must be an object"),
        ({"description": 3}, "description must be a string"),
    ],
)
def test_policy_validation_names_the_offending_key(policy, message):
    with pytest.raises(ValueError, match=message):
        normalize_policy(policy)


def test_placement_limit_is_validated_without_a_placement_change():
    report = _report()
    report["modified"] = report["modified"][:1]
    report["stats"]["modified"] = 1

    with pytest.raises(ValueError, match="max_placement_delta_mm_by_class.IfcSite must be a non-negative number"):
        evaluate_report(report, {"max_placement_delta_mm_by_class": {"IfcSite": -5}})


def test_cli_rejects_a_policy_typo_before_diffing(tmp_path, capsys):
    policy_path = tmp_path / "policy.json"
    policy_path.write_text(json.dumps({"max_delete": 0}), encoding="utf-8")

    code = main_mod._run_check([str(tmp_path / "missing-old.ifc"), str(tmp_path / "missing-new.ifc"), "--policy", str(policy_path)])

    assert code == 1
    err = capsys.readouterr().err
    assert "max_delete" in err
    assert "missing-old.ifc" not in err


def test_normalized_policy_is_a_fixed_point():
    policy = normalize_policy(
        {
            "description": "d",
            "forbid_aspect_changes": {"placement": "*", "data": "IfcWall"},
            "forbid_property_value_change": {"FireRating": ["IfcWall", "IfcDoor"]},
            "max_placement_delta_mm": 5,
        }
    )

    assert policy == {
        "description": "d",
        "max_placement_delta_mm": 5.0,
        "forbid_aspect_changes": {"data": ["IfcWall"], "placement": "*"},
        "forbid_property_value_change": {"FireRating": ["IfcDoor", "IfcWall"]},
    }
    assert normalize_policy(policy) == policy


_HEADER = {
    "engine": "athar",
    "canon_version": "athar-canon-v9",
    "audit": {"report_schema_version": 1},
    "schemas": {"old": "IFC4", "new": "IFC4"},
}
_UNCHANGED_ASPECTS = {
    "geometry": "unchanged",
    "data": "unchanged",
    "topology": "unchanged",
    "relations": "unchanged",
    "class": "unchanged",
    "placement": "unchanged",
    "placement_delta_mm": None,
}


def _report() -> dict:
    return {
        **copy.deepcopy(_HEADER),
        "stats": {"added": 0, "deleted": 1, "modified": 2, "unchanged": 3, "placement_cohorts": []},
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
                    "relations": "unchanged",
                    "class": "unchanged",
                    "placement": "unchanged",
                    "placement_delta_mm": [0.0, 0.0, 0.0],
                },
                "change_scope": "mixed",
                "data_delta": [{"path": "IfcWall.Description", "old": "a", "new": "b"}],
                "property_deltas": {},
            },
            {
                "old": {"class": "IfcSite", "step_id": 12, "guid": "SITE", "name": "Site"},
                "new": {"class": "IfcSite", "step_id": 22, "guid": "SITE", "name": "Site"},
                "aspects": {
                    "geometry": "unchanged",
                    "data": "unchanged",
                    "topology": "unchanged",
                    "relations": "unchanged",
                    "class": "unchanged",
                    "placement": "changed",
                    "placement_delta_mm": [150.0, 200.0, 0.0],
                },
                "change_scope": "intrinsic",
            },
        ],
        "unchanged": [
            {
                "old": {"class": "IfcWall", "step_id": i, "guid": f"STABLE_{i}", "name": "Stable wall"},
                "new": {"class": "IfcWall", "step_id": i, "guid": f"STABLE_{i}", "name": "Stable wall"},
                "aspects": dict(_UNCHANGED_ASPECTS),
            }
            for i in range(30, 33)
        ],
    }


def _report_with_property_deltas() -> dict:
    return {
        **copy.deepcopy(_HEADER),
        "stats": {"added": 0, "deleted": 0, "modified": 2, "unchanged": 0},
        "added": [],
        "deleted": [],
        "modified": [
            {
                "old": {"class": "IfcWall", "step_id": 11, "guid": "WALL", "name": "Wall"},
                "new": {"class": "IfcWall", "step_id": 21, "guid": "WALL", "name": "Wall"},
                "aspects": {**_UNCHANGED_ASPECTS, "data": "changed"},
                "change_scope": "intrinsic",
                "data_delta": [],
                "property_deltas": {
                    "changed": [
                        {"pset": "Custom", "name": "FireRating", "inherited": False, "old_value": "REI90", "new_value": "REI60"},
                        {"pset": "Pset_WallCommon", "name": "FireRating", "inherited": False, "old_value": "90", "new_value": "60"},
                        {"pset": "Pset_WallCommon", "name": "FireRating", "inherited": True, "old_value": "90", "new_value": "30"},
                    ],
                    "removed": [{"pset": "Pset_WallCommon", "name": "AcousticRating", "inherited": False, "old_value": "52"}],
                },
            },
            {
                "old": {"class": "IfcDoor", "step_id": 12, "guid": "DOOR", "name": "Door"},
                "new": {"class": "IfcDoor", "step_id": 22, "guid": "DOOR", "name": "Door"},
                "aspects": {**_UNCHANGED_ASPECTS, "data": "changed"},
                "change_scope": "intrinsic",
                "data_delta": [],
                "property_deltas": {
                    "changed": [{"pset": "Pset_DoorCommon", "name": "Width", "inherited": False, "old_value": 900, "new_value": 1000}],
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
    assert violation["affected"][0]["removed_properties"] == [
        {"pset": "Pset_WallCommon", "name": "AcousticRating", "inherited": False}
    ]


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
    assert violation["count"] == 1
    assert violation["affected"][0]["changes"] == [
        {"kind": "changed", "pset": "Custom", "inherited": False, "old_value": "REI90", "new_value": "REI60"},
        {"kind": "changed", "pset": "Pset_WallCommon", "inherited": False, "old_value": "90", "new_value": "60"},
        {"kind": "changed", "pset": "Pset_WallCommon", "inherited": True, "old_value": "90", "new_value": "30"},
    ]


def test_check_protected_property_change_ignores_other_properties():
    result = evaluate_report(
        _report_with_property_deltas(),
        {"forbid_property_value_change": {"FireRating": ["IfcDoor"]}},
    )

    assert result["ok"] is True


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


def test_misspelled_class_name_is_an_error():
    with pytest.raises(ValueError, match="max_deleted_by_class: 'IfcWal' is not an IFC entity"):
        evaluate_report(_report(), {"max_deleted_by_class": {"IfcWal": 0}})
    with pytest.raises(ValueError, match="forbid_aspect_changes.data: 'IfcLabel' is not an IFC entity"):
        evaluate_report(_report(), {"forbid_aspect_changes": {"data": "IfcLabel"}})


def test_class_names_match_case_insensitively():
    result = evaluate_report(_report(), {"max_deleted_by_class": {"IFCWALL": 0}})

    assert [(v["code"], v["class_name"]) for v in result["violations"]] == [("deleted_class_limit", "IfcWall")]


@pytest.mark.parametrize("schema", ["IFC4", "IFC2X3"])
@pytest.mark.parametrize("supertype", ["IfcBuildingElement", "IfcElement", "IfcProduct"])
def test_class_rule_on_a_supertype_applies_to_subtypes(schema, supertype):
    report = _report()
    report["schemas"] = {"old": schema, "new": schema}

    deleted = evaluate_report(report, {"max_deleted_by_class": {supertype: 0}})
    aspect = evaluate_report(report, {"forbid_aspect_changes": {"data": supertype}})

    assert [v["actual"] for v in deleted["violations"]] == [1]
    assert [a["class"] for v in aspect["violations"] for a in v["affected"]] == ["IfcWall"]


def test_class_rule_on_a_subtype_does_not_apply_to_its_supertype():
    result = evaluate_report(_report(), {"max_deleted_by_class": {"IfcCurtainWall": 0, "IfcSite": 0}})

    assert result["ok"] is True


def test_class_rule_on_a_collapsed_class_applies_to_its_canonical_class():
    policy = normalize_policy({"max_deleted_by_class": {"IfcWallStandardCase": 0}})

    assert policy == {"max_deleted_by_class": {"IfcWall": 0}}
    assert evaluate_report(_report(), policy)["violations"][0]["actual"] == 1
    with pytest.raises(ValueError, match="names IfcWall more than once"):
        normalize_policy({"max_deleted_by_class": {"IfcWallStandardCase": 0, "IfcWall": 1}})


def test_nearest_class_placement_limit_wins():
    report = _report()
    policy = {"max_placement_delta_mm_by_class": {"IfcSite": 1000, "IfcProduct": 10}}

    assert evaluate_report(report, policy)["ok"] is True
    policy["max_placement_delta_mm_by_class"]["IfcSite"] = 100
    assert evaluate_report(report, policy)["ok"] is False


def _guid(label: str) -> str:
    return ifcopenshell.guid.compress(uuid.uuid5(uuid.NAMESPACE_URL, label).hex)


def _write_ifc(path, *, walls, site="site", site_x=0.0, schema="IFC4") -> str:
    """Write a site holding walls; each wall is ``{"id", "x", "psets", "type_psets"}``.

    A pset's properties are a dict, or a list of (name, value) pairs when a name repeats.

    Every GlobalId derives from a label, so two calls with the same arguments
    write semantically identical files.
    """
    f = ifcopenshell.file(schema=schema)

    def placement(x):
        point = f.create_entity("IfcCartesianPoint", Coordinates=(float(x), 0.0, 0.0))
        return f.create_entity(
            "IfcLocalPlacement", RelativePlacement=f.create_entity("IfcAxis2Placement3D", Location=point)
        )

    def pset(label, name, props):
        values = [
            f.create_entity("IfcPropertySingleValue", Name=key, NominalValue=f.create_entity("IfcLabel", value))
            for key, value in (props.items() if isinstance(props, dict) else props)
        ]
        return f.create_entity("IfcPropertySet", GlobalId=_guid(label), Name=name, HasProperties=values)

    units = f.create_entity(
        "IfcUnitAssignment", Units=[f.create_entity("IfcSIUnit", UnitType="LENGTHUNIT", Name="METRE")]
    )
    project = f.create_entity("IfcProject", GlobalId=_guid("project"), Name="P", UnitsInContext=units)
    site_entity = f.create_entity("IfcSite", GlobalId=_guid(site), Name="Site", ObjectPlacement=placement(site_x))
    f.create_entity("IfcRelAggregates", GlobalId=_guid("agg"), RelatingObject=project, RelatedObjects=[site_entity])
    elements = []
    for wall in walls:
        wid = wall["id"]
        element = f.create_entity("IfcWall", GlobalId=_guid(wid), Name=wid, ObjectPlacement=placement(wall.get("x", 0.0)))
        elements.append(element)
        for name, props in wall.get("psets", {}).items():
            f.create_entity(
                "IfcRelDefinesByProperties",
                GlobalId=_guid(f"{wid}/{name}/rel"),
                RelatedObjects=[element],
                RelatingPropertyDefinition=pset(f"{wid}/{name}", name, props),
            )
        if "type_psets" in wall:
            wall_type = f.create_entity(
                "IfcWallType",
                GlobalId=_guid(f"{wid}/type"),
                Name=f"{wid}-type",
                PredefinedType="STANDARD",
                HasPropertySets=[pset(f"{wid}/type/{name}", name, props) for name, props in wall["type_psets"].items()],
            )
            f.create_entity(
                "IfcRelDefinesByType", GlobalId=_guid(f"{wid}/type/rel"), RelatedObjects=[element], RelatingType=wall_type
            )
    f.create_entity(
        "IfcRelContainedInSpatialStructure",
        GlobalId=_guid("contained"),
        RelatingStructure=site_entity,
        RelatedElements=elements,
    )
    f.write(str(path))
    return str(path)


def _diff(tmp_path, old_kwargs, new_kwargs) -> dict:
    return diff_files(_write_ifc(tmp_path / "old.ifc", **old_kwargs), _write_ifc(tmp_path / "new.ifc", **new_kwargs))


def _write_json(path, value) -> str:
    path.write_text(json.dumps(value), encoding="utf-8")
    return str(path)


_WALLS = [{"id": "w1", "x": 0.0}, {"id": "w2", "x": 1.0}, {"id": "w3", "x": 2.0}]


def test_empty_report_is_an_error():
    with pytest.raises(ValueError, match="not an Athar report"):
        evaluate_report({}, {})


def test_cli_empty_report_is_an_error_not_a_pass(tmp_path, capsys):
    report = _write_json(tmp_path / "report.json", {})
    policy = _write_json(tmp_path / "policy.json", {"max_deleted": 0})

    code = main_mod._run_check(["--report", report, "--policy", policy])

    assert code == 1
    assert "not an Athar report" in capsys.readouterr().err


@pytest.mark.parametrize(
    ("edit", "message"),
    [
        (lambda r: r.pop("canon_version"), "canon_version"),
        (lambda r: r.pop("audit"), "report_schema_version"),
        (lambda r: r["schemas"].pop("new"), "schemas"),
        (lambda r: r.pop("deleted"), "'deleted' section"),
        (lambda r: r["stats"].update(deleted=0), "stats.deleted"),
        (lambda r: r["deleted"][0].pop("guid"), "deleted\\[0\\]"),
        (lambda r: r["modified"][0]["aspects"].pop("topology"), "modified\\[0\\].aspects"),
        (lambda r: r["modified"][0]["aspects"].pop("placement_delta_mm"), "placement_delta_mm"),
    ],
)
def test_incomplete_report_is_an_error(edit, message):
    report = _report()
    edit(report)

    with pytest.raises(ValueError, match=message):
        evaluate_report(report, {})


def test_report_without_detail_a_rule_needs_names_the_rule():
    report = _report()
    del report["modified"][0]["data_delta"]
    assert evaluate_report(report, {"max_deleted": 5})["ok"] is True

    with pytest.raises(ValueError, match="forbid_property_value_change needs .*'modified\\[0\\]' has no 'data_delta'"):
        evaluate_report(report, {"forbid_property_value_change": {"FireRating": "*"}})

    report = _report()
    del report["modified"][1]["change_scope"]
    with pytest.raises(ValueError, match="max_modified_change_scope needs 'modified\\[1\\].change_scope'"):
        evaluate_report(report, {"max_modified_change_scope": {"mixed": 0}})


@pytest.mark.parametrize("rule", ["forbid_property_removal", "forbid_property_value_change", "forbid_property_addition"])
def test_property_rule_rejects_missing_property_detail(rule):
    report = _report_with_property_deltas()
    del report["modified"][0]["property_deltas"]
    spec = "*" if rule == "forbid_property_removal" else {"FireRating": "*"}
    with pytest.raises(ValueError, match=rf"{rule}.*property_deltas"):
        evaluate_report(report, {rule: spec})


@pytest.mark.parametrize("row", [None, {}, 1])
@pytest.mark.parametrize("policy", [{"max_placement_cohort_share": 0.5}, {"forbid_site_placement_change": True}])
def test_unchanged_rows_must_be_matched_entities(row, policy):
    report = _report()
    report["unchanged"][0] = row
    with pytest.raises(ValueError, match=r"unchanged\[0\]"):
        evaluate_report(report, policy)


@pytest.mark.parametrize("number", ["NaN", "Infinity", "-Infinity", "1e400"])
def test_nonfinite_placement_report_is_an_error(tmp_path, capsys, number):
    report = _report()
    report["modified"][1]["aspects"]["placement_delta_mm"] = [123, 0, 0]
    report_path = tmp_path / "report.json"
    report_path.write_text(json.dumps(report).replace("[123, 0, 0]", f"[{number}, 0, 0]"))
    policy = _write_json(tmp_path / "policy.json", {"max_placement_delta_mm": 1})
    assert main_mod._run_check(["--report", str(report_path), "--policy", policy]) == 1
    assert "finite numbers" in capsys.readouterr().err


def test_cli_explain_prints_normalized_policy(tmp_path, capsys):
    policy = _write_json(
        tmp_path / "policy.json", {"max_deleted": 0, "max_deleted_by_class": {"ifcwallstandardcase": 0}}
    )

    code = main_mod._run_check(["--policy", policy, "--explain"])

    assert code == 0
    assert json.loads(capsys.readouterr().out) == {"max_deleted": 0, "max_deleted_by_class": {"IfcWall": 0}}


def test_cli_check_existing_engine_report(tmp_path, capsys):
    report = _diff(tmp_path, {"walls": _WALLS}, {"walls": _WALLS[:2]})
    report_path = _write_json(tmp_path / "report.json", report)

    passing = main_mod._run_check(["--report", report_path, "--policy", _write_json(tmp_path / "p1.json", {"max_deleted": 1})])
    assert passing == 0
    assert json.loads(capsys.readouterr().out)["ok"] is True

    failing = main_mod._run_check(["--report", report_path, "--policy", _write_json(tmp_path / "p2.json", {"max_deleted": 0})])
    out = json.loads(capsys.readouterr().out)
    assert failing == 2
    assert [v["code"] for v in out["violations"]] == ["deleted_limit"]


def test_cli_check_diffs_ifc_files(tmp_path, capsys):
    old = _write_ifc(tmp_path / "old.ifc", walls=_WALLS)
    new = _write_ifc(tmp_path / "new.ifc", walls=_WALLS[:2])
    policy = _write_json(tmp_path / "policy.json", {"max_deleted_by_class": {"IfcBuildingElement": 0}})

    code = main_mod._run_check([old, new, "--policy", policy])

    out = json.loads(capsys.readouterr().out)
    assert code == 2
    assert out["violations"][0]["actual"] == 1
    assert out["summary"]["deleted"] == 1


def _fire(value):
    return {"Pset_WallCommon": {"FireRating": value}}


def _property_verdict(tmp_path, old_wall, new_wall, policy):
    report = _diff(tmp_path, {"walls": [{"id": "w1", **old_wall}]}, {"walls": [{"id": "w1", **new_wall}]})
    return evaluate_report(report, policy)


_PROTECT_FIRE = {"forbid_property_value_change": {"FireRating": "*"}}


def test_property_edit_expressed_as_remove_and_add_is_a_value_change(tmp_path):
    verdict = _property_verdict(
        tmp_path, {"psets": _fire("REI90")}, {"psets": {"Custom_Fire": {"FireRating": "REI60"}}}, _PROTECT_FIRE
    )

    [violation] = verdict["violations"]
    assert violation["code"] == "property_value_changed"
    assert violation["affected"][0]["changes"] == [
        {"kind": "removed", "pset": "Pset_WallCommon", "inherited": False, "old_value": "REI90"},
        {"kind": "added", "pset": "Custom_Fire", "inherited": False, "new_value": "REI60"},
    ]


def test_property_moved_from_occurrence_to_type_with_a_new_value_is_a_value_change(tmp_path):
    verdict = _property_verdict(tmp_path, {"psets": _fire("REI90")}, {"type_psets": _fire("REI60")}, _PROTECT_FIRE)

    [violation] = verdict["violations"]
    assert [(c["kind"], c["inherited"]) for c in violation["affected"][0]["changes"]] == [("changed", True)]


def test_property_moved_with_its_value_kept_is_not_a_value_change(tmp_path):
    verdict = _property_verdict(
        tmp_path,
        {"psets": {**_fire("REI90"), "Other": {"Note": "a"}}},
        {"type_psets": _fire("REI90"), "psets": {"Other": {"Note": "b"}}},
        {**_PROTECT_FIRE, "forbid_property_addition": {"FireRating": "*"}},
    )

    assert verdict["violations"] == []


def test_protected_property_removed_without_replacement_is_a_value_change(tmp_path):
    verdict = _property_verdict(
        tmp_path, {"psets": {"Pset_WallCommon": {"FireRating": "REI90", "Note": "a"}}}, {"psets": {"Pset_WallCommon": {"Note": "a"}}}, _PROTECT_FIRE
    )

    [violation] = verdict["violations"]
    assert violation["affected"][0]["changes"] == [
        {"kind": "removed", "pset": "Pset_WallCommon", "inherited": False, "old_value": "REI90"}
    ]


def test_property_addition_fails_only_when_the_policy_asks(tmp_path):
    old, new = {"psets": {"Other": {"Note": "a"}}}, {"psets": {"Other": {"Note": "a"}, **_fire("REI60")}}

    assert _property_verdict(tmp_path, old, new, _PROTECT_FIRE)["ok"] is True
    verdict = _property_verdict(tmp_path, old, new, {"forbid_property_addition": {"FireRating": ["IfcWall"]}})
    [violation] = verdict["violations"]
    assert violation["code"] == "property_added"
    assert violation["affected"][0]["added"] == [{"pset": "Pset_WallCommon", "inherited": False, "new_value": "REI60"}]


def test_occurrence_override_with_a_different_value_is_a_value_change(tmp_path):
    verdict = _property_verdict(
        tmp_path, {"type_psets": _fire("REI90")}, {"type_psets": _fire("REI90"), "psets": _fire("REI60")}, _PROTECT_FIRE
    )

    [violation] = verdict["violations"]
    assert [c["kind"] for c in violation["affected"][0]["changes"]] == ["changed"]


def test_occurrence_override_with_the_type_value_is_not_a_value_change(tmp_path):
    verdict = _property_verdict(
        tmp_path, {"type_psets": _fire("REI90")}, {"type_psets": _fire("REI90"), "psets": _fire("REI90")}, _PROTECT_FIRE
    )

    assert verdict["violations"] == []


def test_type_value_changed_under_an_occurrence_override_is_not_a_value_change(tmp_path):
    verdict = _property_verdict(
        tmp_path,
        {"type_psets": _fire("REI90"), "psets": _fire("REI60")},
        {"type_psets": _fire("REI30"), "psets": _fire("REI60")},
        _PROTECT_FIRE,
    )

    assert verdict["violations"] == []


def test_duplicated_property_losing_a_copy_is_a_value_change(tmp_path):
    twice = {"Pset_WallCommon": [("FireRating", "REI90"), ("FireRating", "REI60")]}
    verdict = _property_verdict(tmp_path, {"psets": twice}, {"psets": _fire("REI90")}, _PROTECT_FIRE)

    [violation] = verdict["violations"]
    assert {c["kind"] for c in violation["affected"][0]["changes"]} == {"changed", "count_changed"}


def test_duplicate_property_count_change_fails_closed():
    report = _report_with_property_deltas()
    report["modified"][1]["property_deltas"] = {
        "count_changed": [
            {"pset": "Pset_DoorCommon", "name": "FireRating", "inherited": False, "old_count": 2, "new_count": 1}
        ]
    }

    verdict = evaluate_report(report, {"forbid_property_value_change": {"FireRating": ["IfcDoor"]}})

    [violation] = verdict["violations"]
    assert violation["affected"][0]["changes"] == [
        {"kind": "count_changed", "pset": "Pset_DoorCommon", "inherited": False, "old_count": 2, "new_count": 1}
    ]


def test_count_limit_violations_name_the_entities(tmp_path):
    walls = [{"id": f"w{i}", "x": float(i)} for i in range(60)]
    report = _diff(tmp_path, {"walls": walls}, {"walls": walls[:2]})

    verdict = evaluate_report(report, {"max_deleted": 0, "max_deleted_by_class": {"IfcWall": 57}})

    section, by_class = verdict["violations"]
    assert section["count"] == 58
    assert len(section["affected"]) == 50
    assert section["truncated"] == 8
    assert {"class", "step_id", "guid", "name"} <= set(section["affected"][0])
    assert section["affected"][0]["class"] == "IfcWall"
    assert {ref["name"] for ref in section["affected"]} <= {f"w{i}" for i in range(2, 60)}
    assert by_class["count"] == 58
    assert by_class["truncated"] == 8


def test_georeferencing_pack_catches_recreated_site(tmp_path):
    report = _diff(tmp_path, {"walls": _WALLS, "site": "old-site"}, {"walls": _WALLS, "site": "new-site"})

    verdict = evaluate_report(report, resolve_policy("georeferencing"))

    assert verdict["ok"] is False
    assert [violation["code"] for violation in verdict["violations"]] == ["site_recreated"]


def test_bulk_shift_fails_even_when_site_does_not_move(tmp_path):
    moved = [{**wall, "x": wall["x"] + 10} for wall in _WALLS]
    report = _diff(tmp_path, {"walls": _WALLS}, {"walls": moved})

    assert evaluate_report(report, {"forbid_site_placement_change": True})["ok"] is True
    verdict = evaluate_report(report, {"max_placement_cohort_share": 0.5})
    [violation] = verdict["violations"]
    assert violation["code"] == "placement_cohort_share"
    assert violation["delta_mm"] == [10000.0, 0.0, 0.0]
    assert violation["count"] == 3
    assert violation["matched"] == 4
    assert violation["share"] == 0.75
    assert {ref["guid"] for ref in violation["affected"]} == {_guid(w["id"]) for w in _WALLS}
    assert evaluate_report(report, {"max_placement_cohort_share": 0.75})["ok"] is True


@pytest.mark.parametrize("limit", [-0.1, 1.1, True, "0.5", float("nan"), float("inf")])
def test_cohort_share_requires_a_fraction(limit):
    with pytest.raises(ValueError, match="max_placement_cohort_share"):
        evaluate_report(_report(), {"max_placement_cohort_share": limit})


@pytest.mark.parametrize("angle", [None, True, -1, 181, float("nan"), float("inf"), "1.2"])
def test_cohort_rule_rejects_invalid_rotation(angle):
    report = _report()
    steps = [item["new"]["step_id"] for item in report["modified"]]
    report["stats"]["placement_cohorts"] = [
        {"delta_mm": [0, 0, 0], "rotation_deg": angle, "count": len(steps), "members": steps}
    ]
    with pytest.raises(ValueError, match="rotation_deg"):
        evaluate_report(report, {"max_placement_cohort_share": 0.5})


def test_cohort_rule_requires_detail():
    report = _report()
    del report["stats"]["placement_cohorts"]
    with pytest.raises(ValueError, match="max_placement_cohort_share.*placement_cohorts"):
        evaluate_report(report, {"max_placement_cohort_share": 0.5})


def test_scope_limit_violation_names_the_entities():
    verdict = evaluate_report(_report(), {"max_modified_change_scope": {"mixed": 0}})

    [violation] = verdict["violations"]
    assert [ref["guid"] for ref in violation["affected"]] == ["WALL"]
    assert violation["truncated"] == 0


def test_policy_forbidding_relations_fails_on_a_recontained_wall(tmp_path):
    def move(model):
        w1 = model["W1"]
        model.containment["L1"].RelatedElements = [model["W2"]]
        model.containment["L2"].RelatedElements = [model["W3"], w1]

    report = diff(tmp_path, move)

    result = evaluate_report(report, {"forbid_aspect_changes": {"relations": ["IfcWall"]}})

    assert result["ok"] is False
    [violation] = result["violations"]
    assert violation["aspect"] == "relations"
    assert [ref["class"] for ref in violation["affected"]] == ["IfcWall"]
    assert evaluate_report(report, {"forbid_aspect_changes": {"class": "*"}})["ok"] is True


def test_policy_forbidding_class_fails_on_a_reclassified_element(tmp_path):
    report = diff(tmp_path, lambda model: reclassify(model, "W1", "IfcBuildingElementProxy"))

    result = evaluate_report(report, {"forbid_aspect_changes": {"class": "*"}})

    assert result["ok"] is False
    [violation] = result["violations"]
    assert violation["aspect"] == "class"
    assert [ref["class"] for ref in violation["affected"]] == ["IfcBuildingElementProxy"]
    assert evaluate_report(report, {"forbid_aspect_changes": {"relations": "*"}})["ok"] is True


def _building_with_address(model):
    f = model.file
    model.named["address"] = f.create_entity("IfcPostalAddress", AddressLines=["Old Street 1"], Town="Munich")
    model["B"].BuildingAddress = model["address"]
    model.named["fire pset"] = model.pset("Pset_BuildingCommon", {"FireProtection": model.text("F90")}, tag="building")
    model.define(model["B"], model["fire pset"])


def _change_address(model):
    model["address"].AddressLines = ["New Street 2"]


def _remove_building_property(model):
    model["fire pset"].HasProperties = []


def test_data_change_through_a_non_property_attribute_passes_safety_critical(tmp_path):
    report = diff(tmp_path, _change_address, setup=_building_with_address)

    [item] = [item for item in report["modified"] if item["new"]["name"] == "B"]
    assert item["aspects"]["data"] == "changed"
    assert item["property_deltas"] == {}
    result = evaluate_report(report, resolve_policy("safety-critical"))
    assert result["ok"] is True and result["violations"] == []


def test_property_removal_beside_a_non_property_data_change_still_violates(tmp_path):
    def edit(model):
        _change_address(model)
        _remove_building_property(model)

    report = diff(tmp_path, edit, setup=_building_with_address)

    result = evaluate_report(report, resolve_policy("safety-critical"))
    [violation] = result["violations"]
    assert violation["code"] == "property_removed"
    assert [ref["class"] for ref in violation["affected"]] == ["IfcBuilding"]


@pytest.mark.parametrize("rule", ["forbid_property_removal", "forbid_property_value_change", "forbid_property_addition"])
def test_report_without_property_deltas_fails_closed_on_a_data_changed_entity(tmp_path, rule):
    report = diff(tmp_path, _change_address, setup=_building_with_address)
    [item] = [item for item in report["modified"] if item["new"]["name"] == "B"]
    del item["property_deltas"]
    spec = "*" if rule == "forbid_property_removal" else {"FireRating": "*"}

    with pytest.raises(ValueError, match=rf"{rule}.*property comparison is unavailable"):
        evaluate_report(report, {rule: spec})


@pytest.mark.parametrize(
    "policy",
    [
        {"forbid_property_removal": ["IfcDoor"]},
        {"forbid_property_value_change": {"FireRating": ["IfcDoor"]}},
        {"forbid_property_addition": {"FireRating": ["IfcDoor"]}},
        {"forbid_property_removal": ["IfcDoor"], "forbid_property_value_change": {"FireRating": ["IfcSlab"]}},
    ],
)
def test_property_rule_does_not_need_deltas_on_entities_outside_its_classes(policy):
    report = _report_with_property_deltas()
    del report["modified"][0]["property_deltas"]

    assert evaluate_report(report, policy)["ok"] is True


def test_property_rule_needs_deltas_on_entities_a_sibling_rule_covers():
    report = _report_with_property_deltas()
    del report["modified"][0]["property_deltas"]
    policy = {"forbid_property_removal": ["IfcDoor"], "forbid_property_value_change": {"FireRating": ["IfcWall"]}}

    with pytest.raises(ValueError, match="forbid_property_value_change.*modified\\[0\\].property_deltas"):
        evaluate_report(report, policy)
