import athar.diff.matcher_graph as matcher_graph_mod
from athar.diff.matcher_graph import propagate_matches_by_typed_path, secondary_match_unresolved


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


def test_typed_path_propagation_can_skip_full_diagnostics():
    old_graph = _graph({
        1: {
            "entity_type": "IfcWall",
            "global_id": "GUID_A",
            "attributes": {},
            "refs": [{"path": "/ObjectPlacement", "target": 100, "target_type": "IfcLocalPlacement"}],
        },
        100: {"entity_type": "IfcLocalPlacement", "attributes": {}, "refs": []},
    })
    new_graph = _graph({
        2: {
            "entity_type": "IfcWall",
            "global_id": "GUID_A",
            "attributes": {},
            "refs": [{"path": "/ObjectPlacement", "target": 200, "target_type": "IfcLocalPlacement"}],
        },
        200: {"entity_type": "IfcLocalPlacement", "attributes": {}, "refs": []},
    })

    result = propagate_matches_by_typed_path(
        old_graph,
        new_graph,
        {1: 2},
        pre_matched_old={1},
        pre_matched_new={2},
        collect_diagnostics=False,
    )

    assert result["old_to_new"] == {100: 200}
    assert result["diagnostics"] == {}
    assert result["match_info"] == {100: ("/ObjectPlacement", "IfcLocalPlacement")}


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
        key = result["diagnostics"][20]["matched_on"]["blocking_key"]
        assert key["neighborhood_bucket"] >= 0


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
    assert len(result["ambiguous_partitions"]) == 1
    partition = result["ambiguous_partitions"][0]
    assert partition["entity_type"] == "IfcCartesianPoint"
    assert partition["old_steps"] == [20, 21]
    assert partition["new_steps"] == [220, 221]


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


def test_secondary_match_unresolved_supports_compatible_type_family_matching():
    old_graph = _graph({
        20: {
            "entity_type": "IfcWallStandardCase",
            "attributes": {"Tag": {"kind": "string", "value": "A"}},
            "refs": [],
        },
    })
    new_graph = _graph({
        220: {
            "entity_type": "IfcWall",
            "attributes": {"Tag": {"kind": "string", "value": "A"}},
            "refs": [],
        },
    })

    result = secondary_match_unresolved(old_graph, new_graph)
    assert result["old_to_new"] == {20: 220}
    assert result["diagnostics"][20]["matched_on"]["stage"] == "scored_assignment"


def test_secondary_match_unresolved_uses_iterative_deepening_for_ambiguous_block():
    old_graph = _graph({
        1: {
            "entity_type": "IfcWall",
            "global_id": "ROOT_WALL",
            "attributes": {},
            "refs": [{"path": "/Assignments", "target": 30, "target_type": "IfcRelAssignsToGroup"}],
        },
        2: {
            "entity_type": "IfcDoor",
            "global_id": "ROOT_DOOR",
            "attributes": {},
            "refs": [{"path": "/Assignments", "target": 31, "target_type": "IfcRelAssignsToGroup"}],
        },
        30: {
            "entity_type": "IfcRelAssignsToGroup",
            "attributes": {},
            "refs": [{"path": "/RelatedObjects/0", "target": 20, "target_type": "IfcProxy"}],
        },
        31: {
            "entity_type": "IfcRelAssignsToGroup",
            "attributes": {},
            "refs": [{"path": "/RelatedObjects/0", "target": 21, "target_type": "IfcProxy"}],
        },
        20: {"entity_type": "IfcProxy", "attributes": {"Name": {"kind": "string", "value": "X"}}, "refs": []},
        21: {"entity_type": "IfcProxy", "attributes": {"Name": {"kind": "string", "value": "X"}}, "refs": []},
    })
    new_graph = _graph({
        11: {
            "entity_type": "IfcWall",
            "global_id": "ROOT_WALL",
            "attributes": {},
            "refs": [{"path": "/Assignments", "target": 131, "target_type": "IfcRelAssignsToGroup"}],
        },
        12: {
            "entity_type": "IfcDoor",
            "global_id": "ROOT_DOOR",
            "attributes": {},
            "refs": [{"path": "/Assignments", "target": 130, "target_type": "IfcRelAssignsToGroup"}],
        },
        130: {
            "entity_type": "IfcRelAssignsToGroup",
            "attributes": {},
            "refs": [{"path": "/RelatedObjects/0", "target": 220, "target_type": "IfcProxy"}],
        },
        131: {
            "entity_type": "IfcRelAssignsToGroup",
            "attributes": {},
            "refs": [{"path": "/RelatedObjects/0", "target": 221, "target_type": "IfcProxy"}],
        },
        220: {"entity_type": "IfcProxy", "attributes": {"Name": {"kind": "string", "value": "X"}}, "refs": []},
        221: {"entity_type": "IfcProxy", "attributes": {"Name": {"kind": "string", "value": "X"}}, "refs": []},
    })

    result = secondary_match_unresolved(old_graph, new_graph, pre_matched_old={1, 2}, pre_matched_new={11, 12})
    assert result["old_to_new"][20] == 221
    assert result["old_to_new"][21] == 220
    assert result["diagnostics"][20]["matched_on"]["depth"] >= 2
    assert result["diagnostics"][21]["matched_on"]["depth"] >= 2


def test_secondary_match_unresolved_assignment_cap_forces_fallback_ambiguity():
    old_graph = _graph({
        1: {
            "entity_type": "IfcWall",
            "global_id": "ROOT_WALL",
            "attributes": {},
            "refs": [{"path": "/Assignments", "target": 30, "target_type": "IfcRelAssignsToGroup"}],
        },
        2: {
            "entity_type": "IfcDoor",
            "global_id": "ROOT_DOOR",
            "attributes": {},
            "refs": [{"path": "/Assignments", "target": 31, "target_type": "IfcRelAssignsToGroup"}],
        },
        30: {
            "entity_type": "IfcRelAssignsToGroup",
            "attributes": {},
            "refs": [{"path": "/RelatedObjects/0", "target": 20, "target_type": "IfcProxy"}],
        },
        31: {
            "entity_type": "IfcRelAssignsToGroup",
            "attributes": {},
            "refs": [{"path": "/RelatedObjects/0", "target": 21, "target_type": "IfcProxy"}],
        },
        20: {"entity_type": "IfcProxy", "attributes": {"Name": {"kind": "string", "value": "X"}}, "refs": []},
        21: {"entity_type": "IfcProxy", "attributes": {"Name": {"kind": "string", "value": "X"}}, "refs": []},
    })
    new_graph = _graph({
        11: {
            "entity_type": "IfcWall",
            "global_id": "ROOT_WALL",
            "attributes": {},
            "refs": [{"path": "/Assignments", "target": 131, "target_type": "IfcRelAssignsToGroup"}],
        },
        12: {
            "entity_type": "IfcDoor",
            "global_id": "ROOT_DOOR",
            "attributes": {},
            "refs": [{"path": "/Assignments", "target": 130, "target_type": "IfcRelAssignsToGroup"}],
        },
        130: {
            "entity_type": "IfcRelAssignsToGroup",
            "attributes": {},
            "refs": [{"path": "/RelatedObjects/0", "target": 220, "target_type": "IfcProxy"}],
        },
        131: {
            "entity_type": "IfcRelAssignsToGroup",
            "attributes": {},
            "refs": [{"path": "/RelatedObjects/0", "target": 221, "target_type": "IfcProxy"}],
        },
        220: {"entity_type": "IfcProxy", "attributes": {"Name": {"kind": "string", "value": "X"}}, "refs": []},
        221: {"entity_type": "IfcProxy", "attributes": {"Name": {"kind": "string", "value": "X"}}, "refs": []},
    })

    result = secondary_match_unresolved(
        old_graph,
        new_graph,
        pre_matched_old={1, 2},
        pre_matched_new={11, 12},
        assignment_max=1,
    )
    assert result["old_to_new"] == {}
    assert result["ambiguous"] == 4


def test_secondary_match_unresolved_depth_limit_can_disable_deepening_resolution():
    old_graph = _graph({
        1: {
            "entity_type": "IfcWall",
            "global_id": "ROOT_WALL",
            "attributes": {},
            "refs": [{"path": "/Assignments", "target": 30, "target_type": "IfcRelAssignsToGroup"}],
        },
        2: {
            "entity_type": "IfcDoor",
            "global_id": "ROOT_DOOR",
            "attributes": {},
            "refs": [{"path": "/Assignments", "target": 31, "target_type": "IfcRelAssignsToGroup"}],
        },
        30: {
            "entity_type": "IfcRelAssignsToGroup",
            "attributes": {},
            "refs": [{"path": "/RelatedObjects/0", "target": 20, "target_type": "IfcProxy"}],
        },
        31: {
            "entity_type": "IfcRelAssignsToGroup",
            "attributes": {},
            "refs": [{"path": "/RelatedObjects/0", "target": 21, "target_type": "IfcProxy"}],
        },
        20: {"entity_type": "IfcProxy", "attributes": {"Name": {"kind": "string", "value": "X"}}, "refs": []},
        21: {"entity_type": "IfcProxy", "attributes": {"Name": {"kind": "string", "value": "X"}}, "refs": []},
    })
    new_graph = _graph({
        11: {
            "entity_type": "IfcWall",
            "global_id": "ROOT_WALL",
            "attributes": {},
            "refs": [{"path": "/Assignments", "target": 131, "target_type": "IfcRelAssignsToGroup"}],
        },
        12: {
            "entity_type": "IfcDoor",
            "global_id": "ROOT_DOOR",
            "attributes": {},
            "refs": [{"path": "/Assignments", "target": 130, "target_type": "IfcRelAssignsToGroup"}],
        },
        130: {
            "entity_type": "IfcRelAssignsToGroup",
            "attributes": {},
            "refs": [{"path": "/RelatedObjects/0", "target": 220, "target_type": "IfcProxy"}],
        },
        131: {
            "entity_type": "IfcRelAssignsToGroup",
            "attributes": {},
            "refs": [{"path": "/RelatedObjects/0", "target": 221, "target_type": "IfcProxy"}],
        },
        220: {"entity_type": "IfcProxy", "attributes": {"Name": {"kind": "string", "value": "X"}}, "refs": []},
        221: {"entity_type": "IfcProxy", "attributes": {"Name": {"kind": "string", "value": "X"}}, "refs": []},
    })

    result = secondary_match_unresolved(
        old_graph,
        new_graph,
        pre_matched_old={1, 2},
        pre_matched_new={11, 12},
        depth2_max=1,
        depth3_max=1,
    )
    assert result["old_to_new"] == {}
    assert result["ambiguous"] == 4


def test_secondary_match_unresolved_can_gate_large_unresolved_sets():
    old_graph = _graph({
        1: {"entity_type": "IfcProxy", "attributes": {}, "refs": []},
        2: {"entity_type": "IfcProxy", "attributes": {}, "refs": []},
    })
    new_graph = _graph({
        11: {"entity_type": "IfcProxy", "attributes": {}, "refs": []},
        12: {"entity_type": "IfcProxy", "attributes": {}, "refs": []},
    })

    result = secondary_match_unresolved(
        old_graph,
        new_graph,
        unresolved_limit=1,
    )
    assert result["old_to_new"] == {}
    assert result["diagnostics"] == {}
    assert result["ambiguous"] == 2
    assert result["ambiguous_partitions"] == []


def test_secondary_match_unresolved_can_gate_large_unresolved_pair_product():
    old_graph = _graph({
        1: {"entity_type": "IfcProxy", "attributes": {}, "refs": []},
        2: {"entity_type": "IfcProxy", "attributes": {}, "refs": []},
    })
    new_graph = _graph({
        11: {"entity_type": "IfcProxy", "attributes": {}, "refs": []},
        12: {"entity_type": "IfcProxy", "attributes": {}, "refs": []},
    })

    result = secondary_match_unresolved(
        old_graph,
        new_graph,
        unresolved_pair_limit=3,
    )
    assert result["old_to_new"] == {}
    assert result["diagnostics"] == {}
    assert result["ambiguous"] == 2
    assert result["ambiguous_partitions"] == []


def test_secondary_match_large_family_can_use_signature_fallback(monkeypatch):
    monkeypatch.setattr(matcher_graph_mod, "_SECONDARY_LARGE_FAMILY_FALLBACK_MIN", 2)

    old_graph = _graph({
        1: {
            "entity_type": "IfcCartesianPoint",
            "attributes": {"Coordinates": {"kind": "list", "items": [{"kind": "real", "value": "1"}]}},
            "refs": [],
        },
        2: {
            "entity_type": "IfcCartesianPoint",
            "attributes": {"Coordinates": {"kind": "list", "items": [{"kind": "real", "value": "2"}]}},
            "refs": [],
        },
        3: {
            "entity_type": "IfcCartesianPoint",
            "attributes": {"Coordinates": {"kind": "list", "items": [{"kind": "real", "value": "3"}]}},
            "refs": [],
        },
    })
    new_graph = _graph({
        10: {
            "entity_type": "IfcCartesianPoint",
            "attributes": {"Coordinates": {"kind": "list", "items": [{"kind": "real", "value": "1"}]}},
            "refs": [],
        },
        11: {
            "entity_type": "IfcCartesianPoint",
            "attributes": {"Coordinates": {"kind": "list", "items": [{"kind": "real", "value": "2"}]}},
            "refs": [],
        },
        12: {
            "entity_type": "IfcCartesianPoint",
            "attributes": {"Coordinates": {"kind": "list", "items": [{"kind": "real", "value": "4"}]}},
            "refs": [],
        },
    })

    result = secondary_match_unresolved(old_graph, new_graph)
    assert result["old_to_new"] == {}
    assert result["ambiguous"] == 3
    assert len(result["ambiguous_partitions"]) == 1
    assert result["ambiguous_partitions"][0]["stage"] == "large_family_fallback"


def test_secondary_match_can_reuse_prebuilt_adjacency(monkeypatch):
    old_graph = _graph({
        1: {
            "entity_type": "IfcWall",
            "global_id": "ROOT",
            "attributes": {},
            "refs": [{"path": "/Children", "target": 2, "target_type": "IfcProxy"}],
        },
        2: {"entity_type": "IfcProxy", "attributes": {"Name": {"kind": "string", "value": "A"}}, "refs": []},
    })
    new_graph = _graph({
        11: {
            "entity_type": "IfcWall",
            "global_id": "ROOT",
            "attributes": {},
            "refs": [{"path": "/Children", "target": 12, "target_type": "IfcProxy"}],
        },
        12: {"entity_type": "IfcProxy", "attributes": {"Name": {"kind": "string", "value": "A"}}, "refs": []},
    })
    old_adj = {1: [("/Children", "IfcProxy", 2)], 2: []}
    new_adj = {11: [("/Children", "IfcProxy", 12)], 12: []}
    old_rev = {1: [], 2: [("/Children", "IfcWall", 1)]}
    new_rev = {11: [], 12: [("/Children", "IfcWall", 11)]}

    monkeypatch.setattr(
        matcher_graph_mod,
        "build_adjacency",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("build_adjacency should not be called")),
    )
    monkeypatch.setattr(
        matcher_graph_mod,
        "build_reverse_adjacency",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("build_reverse_adjacency should not be called")),
    )

    result = secondary_match_unresolved(
        old_graph,
        new_graph,
        pre_matched_old={1},
        pre_matched_new={11},
        old_adjacency=old_adj,
        new_adjacency=new_adj,
        old_reverse_adjacency=old_rev,
        new_reverse_adjacency=new_rev,
    )
    assert result["old_to_new"] == {2: 12}
