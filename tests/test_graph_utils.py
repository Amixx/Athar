import athar.graph.graph_utils as graph_utils_mod


def _sample_entities() -> dict[int, dict]:
    return {
        1: {
            "entity_type": "IfcWall",
            "refs": [
                {"path": "/A", "target": 2, "target_type": "IfcSlab"},
                {"path": "/A", "target": 2, "target_type": "IfcSlab"},
                {"path": "/B", "target": 99, "target_type": "IfcDoor"},
            ],
        },
        2: {
            "entity_type": "IfcSlab",
            "refs": [
                {"path": "/Parent", "target": 3, "target_type": None},
            ],
        },
        3: {
            "entity_type": "IfcBuildingStorey",
            "refs": [],
        },
    }


def test_build_adjacency_maps_matches_python_reference_builder():
    entities = _sample_entities()
    adjacency, reverse = graph_utils_mod.build_adjacency_maps(entities)
    python_adjacency = graph_utils_mod.build_adjacency(entities)
    python_reverse = graph_utils_mod.build_reverse_adjacency(entities, python_adjacency)

    assert adjacency == python_adjacency
    assert reverse == python_reverse
