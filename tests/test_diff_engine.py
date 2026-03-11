from athar.diff_engine import diff_graphs


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
    ops = [change["op"] for change in diff["base_changes"]]
    assert ops == ["REMOVE"]
