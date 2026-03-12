from athar.root_remap import plan_root_remap


def _graph(entities: dict[int, dict]) -> dict:
    return {"metadata": {"schema": "IFC4"}, "entities": entities}


def _root(step_id: int, gid: str, name: str) -> tuple[int, dict]:
    return (
        step_id,
        {
            "entity_type": "IfcWall",
            "global_id": gid,
            "attributes": {
                "GlobalId": {"kind": "string", "value": gid},
                "Name": {"kind": "string", "value": name},
                "OwnerHistory": {"kind": "ref", "id": 500},
            },
            "refs": [
                {"path": "/OwnerHistory", "target": 500, "target_type": "IfcOwnerHistory"}
            ],
        },
    )


def _placement(step_id: int, tag: str) -> tuple[int, dict]:
    return (
        step_id,
        {
            "entity_type": "IfcLocalPlacement",
            "attributes": {"Tag": {"kind": "string", "value": tag}},
            "refs": [],
        },
    )


def test_root_remap_unique_signature_matching():
    old_graph = _graph(dict([
        _root(1, "OLD_A", "Wall A"),
        _root(2, "OLD_B", "Wall B"),
    ]))
    new_graph = _graph(dict([
        _root(3, "NEW_A", "Wall A"),
        _root(4, "NEW_B", "Wall B"),
    ]))

    plan = plan_root_remap(old_graph, new_graph)
    assert plan["enabled"] is True
    assert plan["guid_overlap"] == 0.0
    assert plan["old_to_new"] == {"OLD_A": "NEW_A", "OLD_B": "NEW_B"}
    assert plan["diagnostics"] == {
        "OLD_A": {"stage": "signature_unique"},
        "OLD_B": {"stage": "signature_unique"},
    }
    assert plan["ambiguous"] == 0


def test_root_remap_ambiguous_buckets_left_unmatched():
    old_graph = _graph(dict([
        _root(1, "OLD_A", "Wall Same"),
        _root(2, "OLD_B", "Wall Same"),
    ]))
    new_graph = _graph(dict([
        _root(3, "NEW_A", "Wall Same"),
        _root(4, "NEW_B", "Wall Same"),
    ]))

    plan = plan_root_remap(old_graph, new_graph)
    assert plan["old_to_new"] == {}
    assert plan["diagnostics"] == {}
    assert plan["ambiguous"] == 2


def test_root_remap_skips_when_guid_overlap_is_high():
    old_graph = _graph(dict([
        _root(1, "SHARED", "Wall A"),
        _root(2, "OLD_B", "Wall B"),
    ]))
    new_graph = _graph(dict([
        _root(3, "SHARED", "Wall A"),
        _root(4, "NEW_B", "Wall B"),
    ]))

    plan = plan_root_remap(old_graph, new_graph)
    assert plan["enabled"] is False
    assert plan["method"] == "disabled_guid_overlap"
    assert plan["old_to_new"] == {}
    assert plan["diagnostics"] == {}


def test_root_remap_disambiguates_duplicate_root_signatures_by_neighbor_colors():
    old_graph = _graph({
        1: {
            "entity_type": "IfcWall",
            "global_id": "OLD_A",
            "attributes": {
                "GlobalId": {"kind": "string", "value": "OLD_A"},
                "Name": {"kind": "string", "value": "Wall Same"},
                "ObjectPlacement": {"kind": "ref", "id": 10},
            },
            "refs": [{"path": "/ObjectPlacement", "target": 10, "target_type": "IfcLocalPlacement"}],
        },
        2: {
            "entity_type": "IfcWall",
            "global_id": "OLD_B",
            "attributes": {
                "GlobalId": {"kind": "string", "value": "OLD_B"},
                "Name": {"kind": "string", "value": "Wall Same"},
                "ObjectPlacement": {"kind": "ref", "id": 11},
            },
            "refs": [{"path": "/ObjectPlacement", "target": 11, "target_type": "IfcLocalPlacement"}],
        },
        10: {
            "entity_type": "IfcLocalPlacement",
            "attributes": {"Tag": {"kind": "string", "value": "P1"}},
            "refs": [],
        },
        11: {
            "entity_type": "IfcLocalPlacement",
            "attributes": {"Tag": {"kind": "string", "value": "P2"}},
            "refs": [],
        },
    })
    new_graph = _graph({
        3: {
            "entity_type": "IfcWall",
            "global_id": "NEW_A",
            "attributes": {
                "GlobalId": {"kind": "string", "value": "NEW_A"},
                "Name": {"kind": "string", "value": "Wall Same"},
                "ObjectPlacement": {"kind": "ref", "id": 20},
            },
            "refs": [{"path": "/ObjectPlacement", "target": 20, "target_type": "IfcLocalPlacement"}],
        },
        4: {
            "entity_type": "IfcWall",
            "global_id": "NEW_B",
            "attributes": {
                "GlobalId": {"kind": "string", "value": "NEW_B"},
                "Name": {"kind": "string", "value": "Wall Same"},
                "ObjectPlacement": {"kind": "ref", "id": 21},
            },
            "refs": [{"path": "/ObjectPlacement", "target": 21, "target_type": "IfcLocalPlacement"}],
        },
        20: {
            "entity_type": "IfcLocalPlacement",
            "attributes": {"Tag": {"kind": "string", "value": "P2"}},
            "refs": [],
        },
        21: {
            "entity_type": "IfcLocalPlacement",
            "attributes": {"Tag": {"kind": "string", "value": "P1"}},
            "refs": [],
        },
    })

    plan = plan_root_remap(old_graph, new_graph)
    assert plan["enabled"] is True
    assert plan["guid_overlap"] == 0.0
    assert plan["old_to_new"] == {"OLD_A": "NEW_B", "OLD_B": "NEW_A"}
    assert plan["diagnostics"] == {
        "OLD_A": {"stage": "neighbor_signature"},
        "OLD_B": {"stage": "neighbor_signature"},
    }
    assert plan["ambiguous"] == 0


def test_root_remap_scored_assignment_matches_remaining_pair():
    old_graph = _graph(dict([
        (
            1,
            {
                "entity_type": "IfcWall",
                "global_id": "OLD_A",
                "attributes": {
                    "GlobalId": {"kind": "string", "value": "OLD_A"},
                    "Name": {"kind": "string", "value": "Wall Same"},
                    "ObjectPlacement": {"kind": "ref", "id": 10},
                },
                "refs": [{"path": "/ObjectPlacement", "target": 10, "target_type": "IfcLocalPlacement"}],
            },
        ),
        (
            2,
            {
                "entity_type": "IfcWall",
                "global_id": "OLD_B",
                "attributes": {
                    "GlobalId": {"kind": "string", "value": "OLD_B"},
                    "Name": {"kind": "string", "value": "Wall Same"},
                    "ObjectPlacement": {"kind": "ref", "id": 11},
                },
                "refs": [{"path": "/ObjectPlacement", "target": 11, "target_type": "IfcLocalPlacement"}],
            },
        ),
        _placement(10, "P_SHARED"),
        _placement(11, "P_OLD_ONLY"),
    ]))
    new_graph = _graph(dict([
        (
            3,
            {
                "entity_type": "IfcWall",
                "global_id": "NEW_A",
                "attributes": {
                    "GlobalId": {"kind": "string", "value": "NEW_A"},
                    "Name": {"kind": "string", "value": "Wall Same"},
                    "ObjectPlacement": {"kind": "ref", "id": 20},
                },
                "refs": [{"path": "/ObjectPlacement", "target": 20, "target_type": "IfcLocalPlacement"}],
            },
        ),
        (
            4,
            {
                "entity_type": "IfcWall",
                "global_id": "NEW_B",
                "attributes": {
                    "GlobalId": {"kind": "string", "value": "NEW_B"},
                    "Name": {"kind": "string", "value": "Wall Same"},
                    "ObjectPlacement": {"kind": "ref", "id": 21},
                },
                "refs": [{"path": "/ObjectPlacement", "target": 21, "target_type": "IfcLocalPlacement"}],
            },
        ),
        _placement(20, "P_SHARED"),
        _placement(21, "P_NEW_ONLY"),
    ]))

    plan = plan_root_remap(old_graph, new_graph, score_threshold=0.60)
    assert plan["old_to_new"] == {"OLD_A": "NEW_A", "OLD_B": "NEW_B"}
    assert plan["diagnostics"]["OLD_A"] == {"stage": "neighbor_signature"}
    assert plan["diagnostics"]["OLD_B"]["stage"] == "scored_assignment"
    assert plan["diagnostics"]["OLD_B"]["score"] == 0.65
    assert plan["ambiguous"] == 0


def test_root_remap_scored_assignment_rejects_tied_best_assignments():
    old_graph = _graph(dict([
        (
            1,
            {
                "entity_type": "IfcWall",
                "global_id": "OLD_A",
                "attributes": {
                    "GlobalId": {"kind": "string", "value": "OLD_A"},
                    "Name": {"kind": "string", "value": "Wall Same"},
                    "ObjectPlacement": {"kind": "ref", "id": 10},
                },
                "refs": [{"path": "/ObjectPlacement", "target": 10, "target_type": "IfcLocalPlacement"}],
            },
        ),
        (
            2,
            {
                "entity_type": "IfcWall",
                "global_id": "OLD_B",
                "attributes": {
                    "GlobalId": {"kind": "string", "value": "OLD_B"},
                    "Name": {"kind": "string", "value": "Wall Same"},
                    "ObjectPlacement": {"kind": "ref", "id": 11},
                },
                "refs": [{"path": "/ObjectPlacement", "target": 11, "target_type": "IfcLocalPlacement"}],
            },
        ),
        _placement(10, "P1"),
        _placement(11, "P2"),
    ]))
    new_graph = _graph(dict([
        (
            3,
            {
                "entity_type": "IfcWall",
                "global_id": "NEW_A",
                "attributes": {
                    "GlobalId": {"kind": "string", "value": "NEW_A"},
                    "Name": {"kind": "string", "value": "Wall Same"},
                    "ObjectPlacement": {"kind": "ref", "id": 20},
                },
                "refs": [{"path": "/ObjectPlacement", "target": 20, "target_type": "IfcLocalPlacement"}],
            },
        ),
        (
            4,
            {
                "entity_type": "IfcWall",
                "global_id": "NEW_B",
                "attributes": {
                    "GlobalId": {"kind": "string", "value": "NEW_B"},
                    "Name": {"kind": "string", "value": "Wall Same"},
                    "ObjectPlacement": {"kind": "ref", "id": 21},
                },
                "refs": [{"path": "/ObjectPlacement", "target": 21, "target_type": "IfcLocalPlacement"}],
            },
        ),
        _placement(20, "P3"),
        _placement(21, "P4"),
    ]))

    plan = plan_root_remap(old_graph, new_graph, score_threshold=0.60, score_margin=0.20)
    assert plan["old_to_new"] == {}
    assert plan["diagnostics"] == {}
    assert plan["ambiguous"] == 2


def test_root_remap_scored_assignment_respects_assignment_cap():
    old_graph = _graph(dict([
        (
            1,
            {
                "entity_type": "IfcWall",
                "global_id": "OLD_A",
                "attributes": {
                    "GlobalId": {"kind": "string", "value": "OLD_A"},
                    "Name": {"kind": "string", "value": "Wall Same"},
                    "ObjectPlacement": {"kind": "ref", "id": 10},
                },
                "refs": [{"path": "/ObjectPlacement", "target": 10, "target_type": "IfcLocalPlacement"}],
            },
        ),
        (
            2,
            {
                "entity_type": "IfcWall",
                "global_id": "OLD_B",
                "attributes": {
                    "GlobalId": {"kind": "string", "value": "OLD_B"},
                    "Name": {"kind": "string", "value": "Wall Same"},
                    "ObjectPlacement": {"kind": "ref", "id": 11},
                },
                "refs": [{"path": "/ObjectPlacement", "target": 11, "target_type": "IfcLocalPlacement"}],
            },
        ),
        (
            3,
            {
                "entity_type": "IfcWall",
                "global_id": "OLD_C",
                "attributes": {
                    "GlobalId": {"kind": "string", "value": "OLD_C"},
                    "Name": {"kind": "string", "value": "Wall Same"},
                    "ObjectPlacement": {"kind": "ref", "id": 12},
                },
                "refs": [{"path": "/ObjectPlacement", "target": 12, "target_type": "IfcLocalPlacement"}],
            },
        ),
        _placement(10, "PO1"),
        _placement(11, "PO2"),
        _placement(12, "PO3"),
    ]))
    new_graph = _graph(dict([
        (
            4,
            {
                "entity_type": "IfcWall",
                "global_id": "NEW_A",
                "attributes": {
                    "GlobalId": {"kind": "string", "value": "NEW_A"},
                    "Name": {"kind": "string", "value": "Wall Same"},
                    "ObjectPlacement": {"kind": "ref", "id": 20},
                },
                "refs": [{"path": "/ObjectPlacement", "target": 20, "target_type": "IfcLocalPlacement"}],
            },
        ),
        (
            5,
            {
                "entity_type": "IfcWall",
                "global_id": "NEW_B",
                "attributes": {
                    "GlobalId": {"kind": "string", "value": "NEW_B"},
                    "Name": {"kind": "string", "value": "Wall Same"},
                    "ObjectPlacement": {"kind": "ref", "id": 21},
                },
                "refs": [{"path": "/ObjectPlacement", "target": 21, "target_type": "IfcLocalPlacement"}],
            },
        ),
        (
            6,
            {
                "entity_type": "IfcWall",
                "global_id": "NEW_C",
                "attributes": {
                    "GlobalId": {"kind": "string", "value": "NEW_C"},
                    "Name": {"kind": "string", "value": "Wall Same"},
                    "ObjectPlacement": {"kind": "ref", "id": 22},
                },
                "refs": [{"path": "/ObjectPlacement", "target": 22, "target_type": "IfcLocalPlacement"}],
            },
        ),
        _placement(20, "PN1"),
        _placement(21, "PN2"),
        _placement(22, "PN3"),
    ]))

    plan = plan_root_remap(
        old_graph,
        new_graph,
        score_threshold=0.60,
        assignment_max=2,
    )
    assert plan["old_to_new"] == {}
    assert plan["diagnostics"] == {}
    assert plan["ambiguous"] == 3
