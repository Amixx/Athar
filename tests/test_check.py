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
    "placement": "unchanged",
    "placement_delta_mm": None,
}


def _report() -> dict:
    return {
        **copy.deepcopy(_HEADER),
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
                "data_delta": [{"path": "IfcWall.Description", "old": "a", "new": "b"}],
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
        {"pset": "Custom", "inherited": False, "old_value": "REI90", "new_value": "REI60"},
        {"pset": "Pset_WallCommon", "inherited": False, "old_value": "90", "new_value": "60"},
        {"pset": "Pset_WallCommon", "inherited": True, "old_value": "90", "new_value": "30"},
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
            for key, value in props.items()
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
