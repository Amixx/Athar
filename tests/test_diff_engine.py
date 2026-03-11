from athar.diff_engine import diff_graphs, stream_diff_graphs
from athar.diff_engine_markers import RootedOwnerProjector


def _graph_with_entities(entities: dict[int, dict]) -> dict:
    return {"metadata": {"schema": "IFC4"}, "entities": entities}


def test_diff_engine_add_remove_modify():
    old_graph = _graph_with_entities({
        1: {
            "entity_type": "IfcWall",
            "global_id": "AAA",
            "attributes": {"Name": {"kind": "string", "value": "Wall A"}},
            "refs": [],
        },
        2: {
            "entity_type": "IfcWall",
            "global_id": "BBB",
            "attributes": {"Name": {"kind": "string", "value": "Wall B"}},
            "refs": [],
        },
    })
    new_graph = _graph_with_entities({
        1: {
            "entity_type": "IfcWall",
            "global_id": "AAA",
            "attributes": {"Name": {"kind": "string", "value": "Wall A v2"}},
            "refs": [],
        },
        3: {
            "entity_type": "IfcWall",
            "global_id": "CCC",
            "attributes": {"Name": {"kind": "string", "value": "Wall C"}},
            "refs": [],
        },
    })

    diff = diff_graphs(old_graph, new_graph)
    ops = sorted([change["op"] for change in diff["base_changes"]])
    assert ops == ["ADD", "MODIFY", "REMOVE"]
    modify = next(change for change in diff["base_changes"] if change["op"] == "MODIFY")
    assert modify["field_ops"] == [{
        "path": "/attributes/Name/value",
        "op": "replace",
        "old": "Wall A",
        "new": "Wall A v2",
    }]


def test_diff_engine_uses_root_remap_on_low_guid_overlap():
    old_graph = _graph_with_entities({
        1: {
            "entity_type": "IfcWall",
            "global_id": "AAA",
            "attributes": {
                "Name": {"kind": "string", "value": "Wall A"},
                "ObjectPlacement": {"kind": "ref", "id": 10},
            },
            "refs": [
                {"path": "/ObjectPlacement", "target": 10, "target_type": "IfcLocalPlacement"}
            ],
        },
        10: {
            "entity_type": "IfcLocalPlacement",
            "attributes": {"RelativePlacement": {"kind": "null"}},
            "refs": [],
        },
    })
    new_graph = _graph_with_entities({
        2: {
            "entity_type": "IfcWall",
            "global_id": "ZZZ",
            "attributes": {
                "Name": {"kind": "string", "value": "Wall A"},
                "ObjectPlacement": {"kind": "ref", "id": 11},
            },
            "refs": [
                {"path": "/ObjectPlacement", "target": 11, "target_type": "IfcLocalPlacement"}
            ],
        },
        11: {
            "entity_type": "IfcLocalPlacement",
            "attributes": {"RelativePlacement": {"kind": "null"}},
            "refs": [],
        },
    })

    diff = diff_graphs(old_graph, new_graph)
    assert len(diff["base_changes"]) == 1
    change = diff["base_changes"][0]
    assert change["op"] == "MODIFY"
    assert change["old_entity_id"] == "G:ZZZ"
    assert change["new_entity_id"] == "G:ZZZ"
    assert change["identity"]["match_method"] == "root_remap"
    assert change["identity"]["matched_on"]["stage"] == "root_remap"


def test_diff_engine_applies_typed_path_propagation_for_non_root():
    old_graph = _graph_with_entities({
        1: {
            "entity_type": "IfcWall",
            "global_id": "AAA",
            "attributes": {
                "Name": {"kind": "string", "value": "Wall A"},
                "ObjectPlacement": {"kind": "ref", "id": 10},
            },
            "refs": [
                {"path": "/ObjectPlacement", "target": 10, "target_type": "IfcLocalPlacement"}
            ],
        },
        10: {
            "entity_type": "IfcLocalPlacement",
            "attributes": {"RelativePlacement": {"kind": "ref", "id": 20}},
            "refs": [
                {"path": "/RelativePlacement", "target": 20, "target_type": "IfcAxis2Placement3D"}
            ],
        },
        20: {
            "entity_type": "IfcAxis2Placement3D",
            "attributes": {"Name": {"kind": "string", "value": "P-old"}},
            "refs": [],
        },
    })
    new_graph = _graph_with_entities({
        2: {
            "entity_type": "IfcWall",
            "global_id": "AAA",
            "attributes": {
                "Name": {"kind": "string", "value": "Wall A"},
                "ObjectPlacement": {"kind": "ref", "id": 11},
            },
            "refs": [
                {"path": "/ObjectPlacement", "target": 11, "target_type": "IfcLocalPlacement"}
            ],
        },
        11: {
            "entity_type": "IfcLocalPlacement",
            "attributes": {"RelativePlacement": {"kind": "ref", "id": 21}},
            "refs": [
                {"path": "/RelativePlacement", "target": 21, "target_type": "IfcAxis2Placement3D"}
            ],
        },
        21: {
            "entity_type": "IfcAxis2Placement3D",
            "attributes": {"Name": {"kind": "string", "value": "P-new"}},
            "refs": [],
        },
    })

    diff = diff_graphs(old_graph, new_graph)
    ops = {change["op"] for change in diff["base_changes"]}
    methods = {change["identity"]["match_method"] for change in diff["base_changes"]}
    assert "path_propagation" in methods
    assert "ADD" not in ops
    assert "REMOVE" not in ops
    assert any(
        change["identity"]["match_method"] == "path_propagation"
        and change["identity"]["matched_on"]["stage"] == "typed_path"
        for change in diff["base_changes"]
    )


def test_diff_engine_applies_secondary_match_for_unmatched_non_root():
    old_graph = _graph_with_entities({
        1: {
            "entity_type": "IfcWall",
            "global_id": "AAA",
            "attributes": {"Name": {"kind": "string", "value": "Wall A"}},
            "refs": [],
        },
        20: {
            "entity_type": "IfcCartesianPoint",
            "attributes": {
                "Coordinates": {
                    "kind": "list",
                    "items": [
                        {"kind": "real", "value": "1"},
                        {"kind": "real", "value": "2"},
                        {"kind": "real", "value": "3"},
                    ],
                }
            },
            "refs": [],
        },
    })
    new_graph = _graph_with_entities({
        2: {
            "entity_type": "IfcWall",
            "global_id": "AAA",
            "attributes": {"Name": {"kind": "string", "value": "Wall A"}},
            "refs": [],
        },
        21: {
            "entity_type": "IfcCartesianPoint",
            "attributes": {
                "Coordinates": {
                    "kind": "list",
                    "items": [
                        {"kind": "real", "value": "1"},
                        {"kind": "real", "value": "2"},
                        {"kind": "real", "value": "4"},
                    ],
                }
            },
            "refs": [],
        },
    })

    diff = diff_graphs(old_graph, new_graph)
    ops = {change["op"] for change in diff["base_changes"]}
    methods = {change["identity"]["match_method"] for change in diff["base_changes"]}
    assert "secondary_match" in methods
    assert "ADD" not in ops
    assert "REMOVE" not in ops
    assert any(
        change["identity"]["match_method"] == "secondary_match"
        and change["identity"]["matched_on"]["stage"] in {"scored_assignment", "signature_unique"}
        for change in diff["base_changes"]
    )


def test_diff_engine_preserves_occurrence_multiplicity():
    old_graph = _graph_with_entities({
        1: {
            "entity_type": "IfcCartesianPoint",
            "attributes": {"Coordinates": {"kind": "list", "items": [{"kind": "real", "value": "1"}]}},
            "refs": [],
        },
        2: {
            "entity_type": "IfcCartesianPoint",
            "attributes": {"Coordinates": {"kind": "list", "items": [{"kind": "real", "value": "1"}]}},
            "refs": [],
        },
    })
    new_graph = _graph_with_entities({
        10: {
            "entity_type": "IfcCartesianPoint",
            "attributes": {"Coordinates": {"kind": "list", "items": [{"kind": "real", "value": "1"}]}},
            "refs": [],
        },
    })

    diff = diff_graphs(old_graph, new_graph)
    assert len(diff["base_changes"]) == 1
    change = diff["base_changes"][0]
    assert change["op"] == "CLASS_DELTA"
    assert change["identity"]["match_method"] == "equivalence_class"
    assert change["equivalence_class"]["old_count"] == 2
    assert change["equivalence_class"]["new_count"] == 1
    assert change["equivalence_class"]["id"].startswith("C:")


def test_diff_engine_ignores_step_id_churn_inside_h_matched_entity():
    old_graph = _graph_with_entities({
        1: {
            "entity_type": "IfcLocalPlacement",
            "attributes": {"RelativePlacement": {"kind": "ref", "id": 2}},
            "refs": [{"path": "/RelativePlacement", "target": 2, "target_type": "IfcAxis2Placement3D"}],
        },
        2: {
            "entity_type": "IfcAxis2Placement3D",
            "attributes": {"Name": {"kind": "string", "value": "P"}},
            "refs": [],
        },
    })
    new_graph = _graph_with_entities({
        10: {
            "entity_type": "IfcLocalPlacement",
            "attributes": {"RelativePlacement": {"kind": "ref", "id": 20}},
            "refs": [{"path": "/RelativePlacement", "target": 20, "target_type": "IfcAxis2Placement3D"}],
        },
        20: {
            "entity_type": "IfcAxis2Placement3D",
            "attributes": {"Name": {"kind": "string", "value": "P"}},
            "refs": [],
        },
    })

    diff = diff_graphs(old_graph, new_graph)
    assert diff["base_changes"] == []


def test_diff_engine_emits_reparent_marker_for_qualified_relation_type():
    old_graph = _graph_with_entities({
        1: {"entity_type": "IfcWall", "global_id": "WALL", "attributes": {}, "refs": []},
        2: {"entity_type": "IfcBuildingStorey", "global_id": "OLD_PARENT", "attributes": {}, "refs": []},
        3: {"entity_type": "IfcBuildingStorey", "global_id": "NEW_PARENT", "attributes": {}, "refs": []},
        100: {
            "entity_type": "IfcRelContainedInSpatialStructure",
            "attributes": {},
            "refs": [
                {"path": "/RelatingStructure", "target": 2, "target_type": "IfcBuildingStorey"},
                {"path": "/RelatedElements/0", "target": 1, "target_type": "IfcWall"},
            ],
        },
    })
    new_graph = _graph_with_entities({
        11: {"entity_type": "IfcWall", "global_id": "WALL", "attributes": {}, "refs": []},
        12: {"entity_type": "IfcBuildingStorey", "global_id": "OLD_PARENT", "attributes": {}, "refs": []},
        13: {"entity_type": "IfcBuildingStorey", "global_id": "NEW_PARENT", "attributes": {}, "refs": []},
        101: {
            "entity_type": "IfcRelContainedInSpatialStructure",
            "attributes": {},
            "refs": [
                {"path": "/RelatingStructure", "target": 13, "target_type": "IfcBuildingStorey"},
                {"path": "/RelatedElements/0", "target": 11, "target_type": "IfcWall"},
            ],
        },
    })

    diff = diff_graphs(old_graph, new_graph)
    assert len(diff["derived_markers"]) == 1
    marker = diff["derived_markers"][0]
    assert marker["marker_type"] == "REPARENT"
    assert marker["relation_type"] == "IfcRelContainedInSpatialStructure"
    assert marker["child_id"] == "G:WALL"
    assert marker["old_parent_id"] == "G:OLD_PARENT"
    assert marker["new_parent_id"] == "G:NEW_PARENT"


def test_diff_engine_does_not_emit_reparent_for_non_qualified_relation_type():
    old_graph = _graph_with_entities({
        1: {"entity_type": "IfcWall", "global_id": "WALL", "attributes": {}, "refs": []},
        2: {"entity_type": "IfcGroup", "global_id": "OLD_PARENT", "attributes": {}, "refs": []},
        3: {"entity_type": "IfcGroup", "global_id": "NEW_PARENT", "attributes": {}, "refs": []},
        100: {
            "entity_type": "IfcRelAssignsToGroup",
            "attributes": {},
            "refs": [
                {"path": "/RelatingGroup", "target": 2, "target_type": "IfcGroup"},
                {"path": "/RelatedObjects/0", "target": 1, "target_type": "IfcWall"},
            ],
        },
    })
    new_graph = _graph_with_entities({
        11: {"entity_type": "IfcWall", "global_id": "WALL", "attributes": {}, "refs": []},
        12: {"entity_type": "IfcGroup", "global_id": "OLD_PARENT", "attributes": {}, "refs": []},
        13: {"entity_type": "IfcGroup", "global_id": "NEW_PARENT", "attributes": {}, "refs": []},
        101: {
            "entity_type": "IfcRelAssignsToGroup",
            "attributes": {},
            "refs": [
                {"path": "/RelatingGroup", "target": 13, "target_type": "IfcGroup"},
                {"path": "/RelatedObjects/0", "target": 11, "target_type": "IfcWall"},
            ],
        },
    })

    diff = diff_graphs(old_graph, new_graph)
    assert diff["derived_markers"] == []


def test_diff_engine_sets_rooted_owners_for_shared_removed_node_with_sampling_cap():
    old_entities = {}
    for idx in range(1, 7):
        old_entities[idx] = {
            "entity_type": "IfcWall",
            "global_id": f"ROOT_{idx}",
            "attributes": {},
            "refs": [{"path": "/Representation", "target": 100, "target_type": "IfcCartesianPoint"}],
        }
    old_entities[100] = {
        "entity_type": "IfcCartesianPoint",
        "attributes": {"Coordinates": {"kind": "list", "items": [{"kind": "real", "value": "1"}]}},
        "refs": [],
    }
    new_entities = {}
    for idx in range(1, 7):
        new_entities[idx + 200] = {
            "entity_type": "IfcWall",
            "global_id": f"ROOT_{idx}",
            "attributes": {},
            "refs": [],
        }

    diff = diff_graphs(_graph_with_entities(old_entities), _graph_with_entities(new_entities))
    removed = [
        change for change in diff["base_changes"]
        if change["op"] == "REMOVE" and (change["old_entity_id"] or "").startswith("H:")
    ]
    assert len(removed) == 1
    owners = removed[0]["rooted_owners"]
    assert owners["total"] == 6
    assert owners["sample"] == [
        "G:ROOT_1",
        "G:ROOT_2",
        "G:ROOT_3",
        "G:ROOT_4",
        "G:ROOT_5",
    ]


def test_diff_engine_stats_include_dangling_ref_counts():
    old_graph = {
        "metadata": {"schema": "IFC4"},
        "entities": {
            1: {
                "entity_type": "IfcWall",
                "attributes": {},
                "refs": [{"path": "/ObjectPlacement", "target": 999, "target_type": "IfcLocalPlacement"}],
            },
        },
    }
    new_graph = {
        "metadata": {"schema": "IFC4"},
        "entities": {
            2: {
                "entity_type": "IfcWall",
                "attributes": {},
                "refs": [],
            },
        },
    }
    diff = diff_graphs(old_graph, new_graph)
    assert diff["stats"]["old_dangling_refs"] == 1
    assert diff["stats"]["new_dangling_refs"] == 0


def test_diff_engine_semantic_stable_ignores_owner_history_churn():
    old_graph = _graph_with_entities({
        1: {
            "entity_type": "IfcWall",
            "global_id": "AAA",
            "attributes": {
                "Name": {"kind": "string", "value": "Wall A"},
                "OwnerHistory": {"kind": "ref", "id": 10},
            },
            "refs": [{"path": "/OwnerHistory", "target": 10, "target_type": "IfcOwnerHistory"}],
        },
        10: {
            "entity_type": "IfcOwnerHistory",
            "attributes": {"ChangeAction": {"kind": "string", "value": "MODIFIED"}},
            "refs": [],
        },
    })
    new_graph = _graph_with_entities({
        2: {
            "entity_type": "IfcWall",
            "global_id": "AAA",
            "attributes": {
                "Name": {"kind": "string", "value": "Wall A"},
                "OwnerHistory": {"kind": "ref", "id": 11},
            },
            "refs": [{"path": "/OwnerHistory", "target": 11, "target_type": "IfcOwnerHistory"}],
        },
        11: {
            "entity_type": "IfcOwnerHistory",
            "attributes": {"ChangeAction": {"kind": "string", "value": "MODIFIED"}},
            "refs": [],
        },
    })
    diff = diff_graphs(old_graph, new_graph, profile="semantic_stable")
    assert diff["base_changes"] == []


def test_diff_engine_raw_exact_keeps_owner_history_churn():
    old_graph = _graph_with_entities({
        1: {
            "entity_type": "IfcWall",
            "global_id": "AAA",
            "attributes": {
                "Name": {"kind": "string", "value": "Wall A"},
                "OwnerHistory": {"kind": "ref", "id": 10},
            },
            "refs": [{"path": "/OwnerHistory", "target": 10, "target_type": "IfcOwnerHistory"}],
        },
        10: {
            "entity_type": "IfcOwnerHistory",
            "attributes": {"ChangeAction": {"kind": "string", "value": "MODIFIED"}},
            "refs": [],
        },
    })
    new_graph = _graph_with_entities({
        2: {
            "entity_type": "IfcWall",
            "global_id": "AAA",
            "attributes": {
                "Name": {"kind": "string", "value": "Wall A"},
                "OwnerHistory": {"kind": "ref", "id": 11},
            },
            "refs": [{"path": "/OwnerHistory", "target": 11, "target_type": "IfcOwnerHistory"}],
        },
        11: {
            "entity_type": "IfcOwnerHistory",
            "attributes": {"ChangeAction": {"kind": "string", "value": "MODIFIED"}},
            "refs": [],
        },
    })
    diff = diff_graphs(old_graph, new_graph, profile="raw_exact")
    assert len(diff["base_changes"]) == 1
    assert diff["base_changes"][0]["op"] == "MODIFY"
    assert diff["base_changes"][0]["identity"]["match_method"] == "exact_guid"


def test_diff_engine_semantic_stable_ignores_owner_history_entity_value_churn():
    old_graph = _graph_with_entities({
        10: {
            "entity_type": "IfcOwnerHistory",
            "attributes": {"ChangeAction": {"kind": "string", "value": "ADDED"}},
            "refs": [],
        },
    })
    new_graph = _graph_with_entities({
        11: {
            "entity_type": "IfcOwnerHistory",
            "attributes": {"ChangeAction": {"kind": "string", "value": "MODIFIED"}},
            "refs": [],
        },
    })
    diff = diff_graphs(old_graph, new_graph, profile="semantic_stable")
    assert diff["base_changes"] == []


def test_diff_engine_raw_exact_reports_owner_history_entity_value_churn():
    old_graph = _graph_with_entities({
        10: {
            "entity_type": "IfcOwnerHistory",
            "attributes": {"ChangeAction": {"kind": "string", "value": "ADDED"}},
            "refs": [],
        },
    })
    new_graph = _graph_with_entities({
        11: {
            "entity_type": "IfcOwnerHistory",
            "attributes": {"ChangeAction": {"kind": "string", "value": "MODIFIED"}},
            "refs": [],
        },
    })
    diff = diff_graphs(old_graph, new_graph, profile="raw_exact")
    assert len(diff["base_changes"]) == 1
    change = diff["base_changes"][0]
    assert change["op"] == "MODIFY"
    assert any(
        op["path"] == "/attributes/ChangeAction/value" and op["old"] == "ADDED" and op["new"] == "MODIFIED"
        for op in change["field_ops"]
    )


def test_diff_engine_lazy_owner_projection_skips_materialization_on_zero_diff(monkeypatch):
    old_graph = _graph_with_entities({
        1: {
            "entity_type": "IfcWall",
            "global_id": "AAA",
            "attributes": {"Name": {"kind": "string", "value": "Wall A"}},
            "refs": [],
        },
    })
    new_graph = _graph_with_entities({
        2: {
            "entity_type": "IfcWall",
            "global_id": "AAA",
            "attributes": {"Name": {"kind": "string", "value": "Wall A"}},
            "refs": [],
        },
    })

    calls = {"count": 0}
    original = RootedOwnerProjector._materialize

    def wrapped(self):
        calls["count"] += 1
        return original(self)

    monkeypatch.setattr(RootedOwnerProjector, "_materialize", wrapped)
    diff = diff_graphs(old_graph, new_graph)
    assert diff["base_changes"] == []
    assert calls["count"] == 0


def test_diff_engine_rejects_cross_schema_pairs():
    old_graph = {"metadata": {"schema": "IFC4"}, "entities": {}}
    new_graph = {"metadata": {"schema": "IFC2X3"}, "entities": {}}

    try:
        diff_graphs(old_graph, new_graph)
        assert False, "expected ValueError for schema mismatch"
    except ValueError as exc:
        assert "Schema mismatch" in str(exc)


def test_stream_diff_graphs_rejects_cross_schema_pairs():
    old_graph = {"metadata": {"schema": "IFC4"}, "entities": {}}
    new_graph = {"metadata": {"schema": "IFC2X3"}, "entities": {}}

    try:
        list(stream_diff_graphs(old_graph, new_graph))
        assert False, "expected ValueError for schema mismatch"
    except ValueError as exc:
        assert "Schema mismatch" in str(exc)
