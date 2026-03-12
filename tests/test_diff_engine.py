from athar.determinism import canonical_json
import json
import athar.diff_engine_markers as diff_engine_markers
import athar.diff_engine as diff_engine_mod
import athar.diff_engine_context as diff_engine_context_mod
from athar.diff_engine import diff_files, diff_graphs, stream_diff_graphs
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
    assert "ATTRIBUTES" in modify["change_categories"]


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
    assert diff["base_changes"] == []
    assert diff["stats"]["matched_by_method"].get("root_remap") == 1
    assert diff["stats"]["stage_match_counts"]["root_remap"] == 1


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


def test_diff_engine_geometry_policy_invariant_probe_suppresses_form_swap_modify():
    old_graph = _graph_with_entities({
        1: {
            "entity_type": "IfcWall",
            "global_id": "AAA",
            "attributes": {"Representation": {"kind": "ref", "id": 10}},
            "refs": [{"path": "/Representation", "target": 10, "target_type": "IfcShapeRepresentation"}],
        },
        10: {
            "entity_type": "IfcShapeRepresentation",
            "attributes": {},
            "refs": [{"path": "/Items/0", "target": 20, "target_type": "IfcPolyline"}],
        },
        20: {
            "entity_type": "IfcPolyline",
            "attributes": {},
            "refs": [
                {"path": "/Points/0", "target": 30, "target_type": "IfcCartesianPoint"},
                {"path": "/Points/1", "target": 31, "target_type": "IfcCartesianPoint"},
            ],
        },
        30: {
            "entity_type": "IfcCartesianPoint",
            "attributes": {"Coordinates": {"kind": "list", "items": [{"kind": "real", "value": "0"}, {"kind": "real", "value": "0"}, {"kind": "real", "value": "0"}]}},
            "refs": [],
        },
        31: {
            "entity_type": "IfcCartesianPoint",
            "attributes": {"Coordinates": {"kind": "list", "items": [{"kind": "real", "value": "1"}, {"kind": "real", "value": "0"}, {"kind": "real", "value": "0"}]}},
            "refs": [],
        },
    })
    new_graph = _graph_with_entities({
        2: {
            "entity_type": "IfcWall",
            "global_id": "AAA",
            "attributes": {"Representation": {"kind": "ref", "id": 110}},
            "refs": [{"path": "/Representation", "target": 110, "target_type": "IfcAdvancedBrep"}],
        },
        110: {
            "entity_type": "IfcAdvancedBrep",
            "attributes": {},
            "refs": [{"path": "/Outer", "target": 120, "target_type": "IfcPolyline"}],
        },
        120: {
            "entity_type": "IfcPolyline",
            "attributes": {},
            "refs": [
                {"path": "/Points/0", "target": 130, "target_type": "IfcCartesianPoint"},
                {"path": "/Points/1", "target": 131, "target_type": "IfcCartesianPoint"},
            ],
        },
        130: {
            "entity_type": "IfcCartesianPoint",
            "attributes": {"Coordinates": {"kind": "list", "items": [{"kind": "real", "value": "0"}, {"kind": "real", "value": "0"}, {"kind": "real", "value": "0"}]}},
            "refs": [],
        },
        131: {
            "entity_type": "IfcCartesianPoint",
            "attributes": {"Coordinates": {"kind": "list", "items": [{"kind": "real", "value": "1"}, {"kind": "real", "value": "0"}, {"kind": "real", "value": "0"}]}},
            "refs": [],
        },
    })

    strict = diff_graphs(old_graph, new_graph, geometry_policy="strict_syntax")
    assert any(
        change["op"] == "MODIFY"
        and change["old_entity_id"] == "G:AAA"
        and change["new_entity_id"] == "G:AAA"
        for change in strict["base_changes"]
    )

    probe = diff_graphs(old_graph, new_graph, geometry_policy="invariant_probe")
    assert not any(
        change["op"] == "MODIFY"
        and change["old_entity_id"] == "G:AAA"
        and change["new_entity_id"] == "G:AAA"
        for change in probe["base_changes"]
    )
    assert probe["geometry_policy"] == "invariant_probe"


def test_diff_engine_emits_class_delta_for_unresolved_ambiguous_secondary_partition():
    old_graph = _graph_with_entities({
        1: {
            "entity_type": "IfcWall",
            "global_id": "AAA",
            "attributes": {
                "HasPoints": {
                    "kind": "list",
                    "items": [
                        {"kind": "ref", "id": 20},
                        {"kind": "ref", "id": 21},
                    ],
                },
            },
            "refs": [
                {"path": "/HasPoints", "target": 20, "target_type": "IfcCartesianPoint"},
                {"path": "/HasPoints", "target": 21, "target_type": "IfcCartesianPoint"},
            ],
        },
        20: {
            "entity_type": "IfcCartesianPoint",
            "attributes": {"Coordinates": {"kind": "list", "items": [{"kind": "real", "value": "1"}]}},
            "refs": [],
        },
        21: {
            "entity_type": "IfcCartesianPoint",
            "attributes": {"Coordinates": {"kind": "list", "items": [{"kind": "real", "value": "2"}]}},
            "refs": [],
        },
    })
    new_graph = _graph_with_entities({
        2: {
            "entity_type": "IfcWall",
            "global_id": "AAA",
            "attributes": {
                "HasPoints": {
                    "kind": "list",
                    "items": [{"kind": "ref", "id": 30}],
                },
            },
            "refs": [
                {"path": "/HasPoints", "target": 30, "target_type": "IfcCartesianPoint"},
            ],
        },
        30: {
            "entity_type": "IfcCartesianPoint",
            "attributes": {"Coordinates": {"kind": "list", "items": [{"kind": "real", "value": "5"}]}},
            "refs": [],
        },
    })

    diff = diff_graphs(old_graph, new_graph)
    class_delta = next(
        change for change in diff["base_changes"]
        if change["op"] == "CLASS_DELTA" and (change["old_entity_id"] or "").startswith("C:")
    )
    assert class_delta["identity"]["match_method"] == "equivalence_class"
    assert class_delta["equivalence_class"]["old_count"] == 2
    assert class_delta["equivalence_class"]["new_count"] == 1
    assert class_delta["equivalence_class"]["id"].startswith("C:")

    assert not any(
        change["op"] in {"ADD", "REMOVE"}
        and (
            (change.get("old_entity_id") or "").startswith("H:")
            or (change.get("new_entity_id") or "").startswith("H:")
        )
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


def test_diff_engine_emits_class_delta_for_scc_symmetric_partition_count_change():
    old_graph = _graph_with_entities({
        1: {
            "entity_type": "IfcWall",
            "global_id": "AAA",
            "attributes": {},
            "refs": [
                {"path": "/Members", "target": 10, "target_type": "IfcProxy"},
                {"path": "/Members", "target": 11, "target_type": "IfcProxy"},
            ],
        },
        10: {
            "entity_type": "IfcProxy",
            "attributes": {},
            "refs": [{"path": "/Peer", "target": 11, "target_type": "IfcProxy"}],
        },
        11: {
            "entity_type": "IfcProxy",
            "attributes": {},
            "refs": [{"path": "/Peer", "target": 10, "target_type": "IfcProxy"}],
        },
    })
    new_graph = _graph_with_entities({
        2: {
            "entity_type": "IfcWall",
            "global_id": "AAA",
            "attributes": {},
            "refs": [
                {"path": "/Members", "target": 20, "target_type": "IfcProxy"},
                {"path": "/Members", "target": 21, "target_type": "IfcProxy"},
                {"path": "/Members", "target": 22, "target_type": "IfcProxy"},
            ],
        },
        20: {
            "entity_type": "IfcProxy",
            "attributes": {},
            "refs": [{"path": "/Peer", "target": 21, "target_type": "IfcProxy"}],
        },
        21: {
            "entity_type": "IfcProxy",
            "attributes": {},
            "refs": [{"path": "/Peer", "target": 22, "target_type": "IfcProxy"}],
        },
        22: {
            "entity_type": "IfcProxy",
            "attributes": {},
            "refs": [{"path": "/Peer", "target": 20, "target_type": "IfcProxy"}],
        },
    })

    diff = diff_graphs(old_graph, new_graph)
    class_delta = next(
        change for change in diff["base_changes"]
        if change["op"] == "CLASS_DELTA" and (change["old_entity_id"] or "").startswith("C:")
    )
    assert class_delta["equivalence_class"]["old_count"] == 2
    assert class_delta["equivalence_class"]["new_count"] == 3
    assert class_delta["identity"]["match_method"] == "equivalence_class"


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
            "attributes": {"ChangeAction": {"kind": "string", "value": "ADDED"}},
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
            "attributes": {"ChangeAction": {"kind": "string", "value": "ADDED"}},
            "refs": [],
        },
    })
    diff = diff_graphs(old_graph, new_graph, profile="raw_exact")
    assert len(diff["base_changes"]) == 1
    change = diff["base_changes"][0]
    assert change["op"] == "MODIFY"
    assert change["identity"]["match_method"] == "path_propagation"
    assert change["field_ops"] == [{
        "path": "/attributes/ChangeAction/value",
        "op": "replace",
        "old": "MODIFIED",
        "new": "ADDED",
    }]


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


def test_rooted_owner_projector_spills_to_disk_when_threshold_exceeded(monkeypatch):
    graph = _graph_with_entities({
        1: {
            "entity_type": "IfcWall",
            "global_id": "ROOT",
            "attributes": {},
            "refs": [{"path": "/Contains/0", "target": 2, "target_type": "IfcDoor"}],
        },
        2: {
            "entity_type": "IfcDoor",
            "attributes": {},
            "refs": [],
        },
    })
    ids = {1: "G:ROOT", 2: "H:CHILD"}

    monkeypatch.setenv("ATHAR_OWNER_INDEX_DISK_THRESHOLD", "1")
    monkeypatch.setattr(
        diff_engine_markers,
        "compute_rooted_owner_index",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("memory owner index should not be used")),
    )

    projector = RootedOwnerProjector(graph, ids)
    assert projector.owners_for_step(2) == {"G:ROOT"}
    assert projector.owners_for_steps([1, 2]) == {"G:ROOT"}
    projector.close()


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


def test_diff_engine_stats_include_ambiguity_breakdown_by_stage():
    old_graph = _graph_with_entities({
        1: {
            "entity_type": "IfcWall",
            "global_id": "AAA",
            "attributes": {},
            "refs": [
                {"path": "/HasParts", "target": 2, "target_type": "IfcBuildingElementPart"},
                {"path": "/HasParts", "target": 3, "target_type": "IfcBuildingElementPart"},
            ],
        },
        2: {"entity_type": "IfcBuildingElementPart", "attributes": {}, "refs": []},
        3: {"entity_type": "IfcBuildingElementPart", "attributes": {}, "refs": []},
    })
    new_graph = _graph_with_entities({
        11: {
            "entity_type": "IfcWall",
            "global_id": "AAA",
            "attributes": {},
            "refs": [
                {"path": "/HasParts", "target": 12, "target_type": "IfcBuildingElementPart"},
                {"path": "/HasParts", "target": 13, "target_type": "IfcBuildingElementPart"},
            ],
        },
        12: {"entity_type": "IfcBuildingElementPart", "attributes": {}, "refs": []},
        13: {"entity_type": "IfcBuildingElementPart", "attributes": {}, "refs": []},
    })

    diff = diff_graphs(old_graph, new_graph)
    by_stage = diff["stats"]["ambiguous_by_stage"]
    assert set(by_stage) == {"root_remap", "path_propagation", "secondary_match"}
    assert diff["stats"]["ambiguous"] == (
        by_stage["root_remap"] + by_stage["path_propagation"] + by_stage["secondary_match"]
    )


def test_diff_engine_stats_include_matched_by_method_breakdown():
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
            "attributes": {"Name": {"kind": "string", "value": "Wall A v2"}},
            "refs": [],
        },
    })

    diff = diff_graphs(old_graph, new_graph)
    by_method = diff["stats"]["matched_by_method"]
    assert by_method["exact_guid"] == 1
    assert sum(by_method.values()) == diff["stats"]["matched"]


def test_diff_engine_stats_include_root_guid_quality():
    old_graph = _graph_with_entities({
        1: {"entity_type": "IfcWall", "global_id": "DUP", "attributes": {}, "refs": []},
        2: {"entity_type": "IfcWall", "global_id": "DUP", "attributes": {}, "refs": []},
        3: {"entity_type": "IfcWall", "global_id": "", "attributes": {}, "refs": []},
    })
    new_graph = _graph_with_entities({
        10: {"entity_type": "IfcWall", "global_id": "UNIQ", "attributes": {}, "refs": []},
    })

    diff = diff_graphs(old_graph, new_graph, guid_policy="disambiguate")
    old_quality = diff["stats"]["root_guid_quality"]["old"]
    new_quality = diff["stats"]["root_guid_quality"]["new"]
    assert old_quality["duplicate_ids"] == 1
    assert old_quality["duplicate_occurrences"] == 2
    assert old_quality["invalid"] == 1
    assert new_quality["unique_valid"] == 1


def test_diff_engine_fail_fast_rejects_duplicate_global_ids():
    old_graph = _graph_with_entities({
        1: {"entity_type": "IfcWall", "global_id": "DUP", "attributes": {}, "refs": []},
        2: {"entity_type": "IfcWall", "global_id": "DUP", "attributes": {}, "refs": []},
    })
    new_graph = _graph_with_entities({})

    try:
        diff_graphs(old_graph, new_graph)
        assert False, "expected ValueError for duplicate GlobalId under fail_fast"
    except ValueError as exc:
        assert "GUID policy violation (old)" in str(exc)
        assert "duplicate GlobalId count=1" in str(exc)


def test_diff_engine_disambiguates_duplicate_global_ids_with_g_bang_ids():
    old_graph = _graph_with_entities({
        1: {"entity_type": "IfcWall", "global_id": "DUP", "attributes": {}, "refs": []},
        2: {"entity_type": "IfcWall", "global_id": "DUP", "attributes": {}, "refs": []},
    })
    new_graph = _graph_with_entities({
        10: {"entity_type": "IfcWall", "global_id": "DUP", "attributes": {}, "refs": []},
        11: {"entity_type": "IfcWall", "global_id": "DUP", "attributes": {}, "refs": []},
    })

    diff = diff_graphs(old_graph, new_graph, guid_policy="disambiguate")
    assert diff["base_changes"] == []
    assert diff["identity_policy"]["guid_policy"] == "disambiguate"


def test_diff_engine_stats_include_stage_match_counts():
    old_graph = _graph_with_entities({
        1: {
            "entity_type": "IfcWall",
            "global_id": "AAA",
            "attributes": {},
            "refs": [{"path": "/ObjectPlacement", "target": 10, "target_type": "IfcLocalPlacement"}],
        },
        10: {"entity_type": "IfcLocalPlacement", "attributes": {}, "refs": []},
    })
    new_graph = _graph_with_entities({
        2: {
            "entity_type": "IfcWall",
            "global_id": "ZZZ",
            "attributes": {},
            "refs": [{"path": "/ObjectPlacement", "target": 11, "target_type": "IfcLocalPlacement"}],
        },
        11: {"entity_type": "IfcLocalPlacement", "attributes": {}, "refs": []},
    })

    diff = diff_graphs(old_graph, new_graph)
    counts = diff["stats"]["stage_match_counts"]
    assert set(counts) == {"root_remap", "path_propagation", "secondary_match"}
    assert counts["root_remap"] >= 1
    assert counts["path_propagation"] >= 0
    assert counts["secondary_match"] >= 0


def test_diff_engine_relationship_change_category_for_rel_entities():
    old_graph = _graph_with_entities({})
    new_graph = _graph_with_entities({
        1: {
            "entity_type": "IfcRelAggregates",
            "attributes": {},
            "refs": [],
        },
    })

    diff = diff_graphs(old_graph, new_graph)
    add = diff["base_changes"][0]
    assert add["op"] == "ADD"
    assert "RELATIONSHIP" in add["change_categories"]
    assert "ENTITY" in add["change_categories"]


def test_diff_engine_handles_cycle_heavy_graph_without_recursion_failure():
    old_graph = _graph_with_entities({
        1: {
            "entity_type": "IfcWall",
            "global_id": "AAA",
            "attributes": {"Name": {"kind": "string", "value": "Wall A"}},
            "refs": [{"path": "/Representation", "target": 10, "target_type": "IfcShapeRepresentation"}],
        },
        10: {
            "entity_type": "IfcShapeRepresentation",
            "attributes": {},
            "refs": [{"path": "/Items/0", "target": 11, "target_type": "IfcProxy"}],
        },
        11: {
            "entity_type": "IfcProxy",
            "attributes": {},
            "refs": [{"path": "/BackRef", "target": 10, "target_type": "IfcShapeRepresentation"}],
        },
    })
    new_graph = _graph_with_entities({
        2: {
            "entity_type": "IfcWall",
            "global_id": "AAA",
            "attributes": {"Name": {"kind": "string", "value": "Wall A"}},
            "refs": [{"path": "/Representation", "target": 20, "target_type": "IfcShapeRepresentation"}],
        },
        20: {
            "entity_type": "IfcShapeRepresentation",
            "attributes": {},
            "refs": [{"path": "/Items/0", "target": 21, "target_type": "IfcProxy"}],
        },
        21: {
            "entity_type": "IfcProxy",
            "attributes": {},
            "refs": [{"path": "/BackRef", "target": 20, "target_type": "IfcShapeRepresentation"}],
        },
    })

    first = diff_graphs(old_graph, new_graph)
    second = diff_graphs(old_graph, new_graph)
    assert canonical_json(first) == canonical_json(second)
    assert first["base_changes"] == []
    assert first["derived_markers"] == []


def test_diff_engine_treats_pure_step_renumbered_ref_targets_as_equal():
    old_graph = _graph_with_entities({
        1: {
            "entity_type": "IfcWall",
            "global_id": "AAA",
            "attributes": {"ObjectPlacement": {"kind": "ref", "id": 10}},
            "refs": [{"path": "/ObjectPlacement", "target": 10, "target_type": "IfcLocalPlacement"}],
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
            "global_id": "AAA",
            "attributes": {"ObjectPlacement": {"kind": "ref", "id": 110}},
            "refs": [{"path": "/ObjectPlacement", "target": 110, "target_type": "IfcLocalPlacement"}],
        },
        110: {
            "entity_type": "IfcLocalPlacement",
            "attributes": {"RelativePlacement": {"kind": "null"}},
            "refs": [],
        },
    })

    diff = diff_graphs(old_graph, new_graph)
    assert diff["base_changes"] == []


def test_diff_graphs_timings_are_opt_in():
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
            "attributes": {"Name": {"kind": "string", "value": "Wall A v2"}},
            "refs": [],
        },
    })

    default_result = diff_graphs(old_graph, new_graph)
    assert "timings_ms" not in default_result["stats"]

    timed = diff_graphs(old_graph, new_graph, timings=True)
    timings = timed["stats"].get("timings_ms")
    assert isinstance(timings, dict)
    assert "prepare_context" in timings
    assert "emit_base_changes" in timings
    assert "emit_derived_markers" in timings
    assert "total" in timings
    assert "context.assign_old_ids.wl_total_ms" in timings
    assert "context.assign_new_ids.wl_total_ms" in timings
    assert "context.assign_old_ids.wl_rounds" in timings
    assert "context.assign_new_ids.wl_rounds" in timings


def test_diff_graphs_progress_callback_reports_stage_progress():
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
            "attributes": {"Name": {"kind": "string", "value": "Wall A v2"}},
            "refs": [],
        },
    })
    events: list[dict] = []

    diff_graphs(old_graph, new_graph, progress_callback=lambda event: events.append(dict(event)))

    assert events
    assert events[0]["stage"] == "prepare_context"
    assert events[0]["status"] == "start"
    assert any(
        event.get("stage") == "prepare_context"
        and event.get("status") == "running"
        and event.get("step") == "root_remap"
        for event in events
    )
    assert any(
        event.get("stage") == "emit_base_changes" and event.get("status") == "running"
        for event in events
    )
    assert events[-1]["stage"] == "done"
    assert events[-1]["status"] == "done"
    assert events[-1]["overall_progress"] >= 0.99


def test_diff_files_timings_include_parse_stages(monkeypatch):
    graph = _graph_with_entities({
        1: {
            "entity_type": "IfcWall",
            "global_id": "AAA",
            "attributes": {},
            "refs": [],
        },
    })

    calls = {"count": 0}

    def fake_parse_graph(_path: str, *, profile: str):
        calls["count"] += 1
        return graph

    monkeypatch.setattr(diff_engine_mod, "parse_graph", fake_parse_graph)
    result = diff_files("old.ifc", "new.ifc", timings=True)
    timings = result["stats"].get("timings_ms", {})
    assert calls["count"] == 2
    assert "parse_old_graph" in timings
    assert "parse_new_graph" in timings


def test_diff_graphs_skips_entities_equal_for_exact_hash_pairs(monkeypatch):
    old_graph = _graph_with_entities({
        1: {
            "entity_type": "IfcProxy",
            "attributes": {"Name": {"kind": "string", "value": "A"}},
            "refs": [],
        },
    })
    new_graph = _graph_with_entities({
        2: {
            "entity_type": "IfcProxy",
            "attributes": {"Name": {"kind": "string", "value": "A"}},
            "refs": [],
        },
    })

    def fail_entities_equal(*_args, **_kwargs):
        raise AssertionError("entities_equal should not be called for exact_hash H: pairs")

    monkeypatch.setattr(diff_engine_mod, "entities_equal", fail_entities_equal)
    diff = diff_graphs(old_graph, new_graph)
    assert diff["base_changes"] == []


def test_prepare_context_reuse_hook_not_called_when_same_graph_fast_path_applies(monkeypatch):
    graph = _graph_with_entities({
        1: {
            "entity_type": "IfcProxy",
            "attributes": {"Name": {"kind": "string", "value": "A"}},
            "refs": [],
        },
    })

    calls = {"count": 0}
    real_precompute = diff_engine_context_mod._precompute_identity_state

    def wrapped_precompute(*args, **kwargs):
        calls["count"] += 1
        return real_precompute(*args, **kwargs)

    monkeypatch.setattr(diff_engine_context_mod, "_precompute_identity_state", wrapped_precompute)
    diff = diff_graphs(graph, graph)
    assert diff["base_changes"] == []
    assert calls["count"] == 0


def test_diff_graphs_same_graph_fast_path_skips_context_build(monkeypatch):
    graph = _graph_with_entities({
        1: {
            "entity_type": "IfcProxy",
            "attributes": {},
            "refs": [],
        },
    })

    def fail_prepare(*_args, **_kwargs):
        raise AssertionError("prepare_diff_context should not run on same-graph fast path")

    monkeypatch.setattr(diff_engine_mod, "prepare_diff_context", fail_prepare)
    diff = diff_graphs(graph, graph, timings=True)
    assert diff["base_changes"] == []
    assert diff["derived_markers"] == []
    assert diff["stats"]["timings_ms"]["prepare_context"] == 0.0
    assert diff["stats"]["matched"] == 1


def test_stream_diff_graphs_same_graph_fast_path_emits_header_and_end_only():
    graph = _graph_with_entities({
        1: {
            "entity_type": "IfcProxy",
            "attributes": {},
            "refs": [],
        },
    })
    lines = list(stream_diff_graphs(graph, graph, mode="ndjson"))
    assert len(lines) == 2
    header = json.loads(lines[0])
    end = json.loads(lines[1])
    assert header["record_type"] == "header"
    assert end["record_type"] == "end"
    assert end["base_change_count"] == 0
    assert end["derived_marker_count"] == 0


def test_diff_graphs_same_graph_still_enforces_guid_fail_fast():
    graph = _graph_with_entities({
        1: {
            "entity_type": "IfcWall",
            "global_id": "DUP",
            "attributes": {},
            "refs": [],
        },
        2: {
            "entity_type": "IfcDoor",
            "global_id": "DUP",
            "attributes": {},
            "refs": [],
        },
    })
    try:
        diff_graphs(graph, graph)
    except ValueError as exc:
        assert "GUID policy violation" in str(exc)
    else:
        raise AssertionError("expected GUID policy violation under fail_fast")
