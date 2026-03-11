from athar.matcher_graph import propagate_matches_by_typed_path, secondary_match_unresolved


def _graph(entities: dict[int, dict]) -> dict:
    return {"metadata": {"schema": "IFC4"}, "entities": entities}


def test_typed_path_propagation_matches_unique_chain():
    old_graph = _graph({
        1: {
            "entity_type": "IfcWall",
            "global_id": "ROOT_A",
            "attributes": {},
            "refs": [{"path": "/ObjectPlacement", "target": 2, "target_type": "IfcLocalPlacement"}],
        },
        2: {
            "entity_type": "IfcLocalPlacement",
            "attributes": {},
            "refs": [{"path": "/RelativePlacement", "target": 3, "target_type": "IfcAxis2Placement3D"}],
        },
        3: {"entity_type": "IfcAxis2Placement3D", "attributes": {}, "refs": []},
    })
    new_graph = _graph({
        101: {
            "entity_type": "IfcWall",
            "global_id": "ROOT_B",
            "attributes": {},
            "refs": [{"path": "/ObjectPlacement", "target": 102, "target_type": "IfcLocalPlacement"}],
        },
        102: {
            "entity_type": "IfcLocalPlacement",
            "attributes": {},
            "refs": [{"path": "/RelativePlacement", "target": 103, "target_type": "IfcAxis2Placement3D"}],
        },
        103: {"entity_type": "IfcAxis2Placement3D", "attributes": {}, "refs": []},
    })

    result = propagate_matches_by_typed_path(old_graph, new_graph, {1: 101})
    assert result["old_to_new"] == {2: 102, 3: 103}
    assert result["ambiguous"] == 0
    assert result["diagnostics"][2]["matched_on"]["stage"] == "typed_path"
    assert result["diagnostics"][2]["matched_on"]["path"] == "/ObjectPlacement"


def test_typed_path_propagation_rejects_ambiguous_buckets():
    old_graph = _graph({
        1: {
            "entity_type": "IfcWall",
            "global_id": "ROOT_A",
            "attributes": {},
            "refs": [
                {"path": "/HasParts", "target": 2, "target_type": "IfcBuildingElementPart"},
                {"path": "/HasParts", "target": 3, "target_type": "IfcBuildingElementPart"},
            ],
        },
        2: {"entity_type": "IfcBuildingElementPart", "attributes": {}, "refs": []},
        3: {"entity_type": "IfcBuildingElementPart", "attributes": {}, "refs": []},
    })
    new_graph = _graph({
        101: {
            "entity_type": "IfcWall",
            "global_id": "ROOT_B",
            "attributes": {},
            "refs": [
                {"path": "/HasParts", "target": 102, "target_type": "IfcBuildingElementPart"},
                {"path": "/HasParts", "target": 103, "target_type": "IfcBuildingElementPart"},
            ],
        },
        102: {"entity_type": "IfcBuildingElementPart", "attributes": {}, "refs": []},
        103: {"entity_type": "IfcBuildingElementPart", "attributes": {}, "refs": []},
    })

    result = propagate_matches_by_typed_path(old_graph, new_graph, {1: 101})
    assert result["old_to_new"] == {}
    assert result["ambiguous"] == 2


def test_secondary_match_unresolved_matches_unique_block():
    old_graph = _graph({
        1: {
            "entity_type": "IfcWall",
            "global_id": "ROOT_A",
            "attributes": {},
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
    new_graph = _graph({
        101: {
            "entity_type": "IfcWall",
            "global_id": "ROOT_A",
            "attributes": {},
            "refs": [],
        },
        220: {
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

    result = secondary_match_unresolved(old_graph, new_graph, pre_matched_old={1}, pre_matched_new={101})
    assert result["old_to_new"] == {20: 220}
    assert result["ambiguous"] == 0
    assert result["diagnostics"][20]["matched_on"]["stage"] in {"scored_assignment", "signature_unique"}
    assert result["diagnostics"][20]["match_confidence"] > 0.0
    assert result["diagnostics"][20]["matched_on"]["block_stage"] in {"coarse_block", "residual"}
    if result["diagnostics"][20]["matched_on"]["block_stage"] == "coarse_block":
        assert "blocking_key" in result["diagnostics"][20]["matched_on"]


def test_secondary_match_unresolved_rejects_ambiguous_block():
    old_graph = _graph({
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
    new_graph = _graph({
        220: {
            "entity_type": "IfcCartesianPoint",
            "attributes": {"Coordinates": {"kind": "list", "items": [{"kind": "real", "value": "5"}]}},
            "refs": [],
        },
        221: {
            "entity_type": "IfcCartesianPoint",
            "attributes": {"Coordinates": {"kind": "list", "items": [{"kind": "real", "value": "6"}]}},
            "refs": [],
        },
    })

    result = secondary_match_unresolved(old_graph, new_graph)
    assert result["old_to_new"] == {}
    assert result["ambiguous"] == 2


def test_secondary_match_unresolved_uses_scored_assignment_for_swapped_pairs():
    old_graph = _graph({
        20: {
            "entity_type": "IfcCartesianPoint",
            "attributes": {
                "Coordinates": {"kind": "list", "items": [{"kind": "real", "value": "1"}]},
                "Tag": {"kind": "string", "value": "A"},
            },
            "refs": [],
        },
        21: {
            "entity_type": "IfcCartesianPoint",
            "attributes": {
                "Coordinates": {"kind": "list", "items": [{"kind": "real", "value": "9"}]},
                "Tag": {"kind": "string", "value": "B"},
            },
            "refs": [],
        },
    })
    new_graph = _graph({
        220: {
            "entity_type": "IfcCartesianPoint",
            "attributes": {
                "Coordinates": {"kind": "list", "items": [{"kind": "real", "value": "9"}]},
                "Tag": {"kind": "string", "value": "B"},
            },
            "refs": [],
        },
        221: {
            "entity_type": "IfcCartesianPoint",
            "attributes": {
                "Coordinates": {"kind": "list", "items": [{"kind": "real", "value": "1"}]},
                "Tag": {"kind": "string", "value": "A"},
            },
            "refs": [],
        },
    })

    result = secondary_match_unresolved(old_graph, new_graph)
    assert result["old_to_new"] == {20: 221, 21: 220}
    assert result["ambiguous"] == 0
    assert result["diagnostics"][20]["matched_on"]["stage"] == "scored_assignment"
    assert result["diagnostics"][21]["matched_on"]["stage"] == "scored_assignment"
