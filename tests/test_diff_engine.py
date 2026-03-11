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
