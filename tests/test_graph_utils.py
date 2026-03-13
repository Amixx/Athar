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


def test_build_adjacency_maps_falls_back_to_python_when_native_unavailable(monkeypatch):
    entities = _sample_entities()
    monkeypatch.setattr(graph_utils_mod, "_NATIVE_BUILD_ADJACENCY_MAPS", None)

    adjacency, reverse_adjacency = graph_utils_mod.build_adjacency_maps(entities)

    assert adjacency == graph_utils_mod.build_adjacency(entities)
    assert reverse_adjacency == graph_utils_mod.build_reverse_adjacency(entities, adjacency)


def test_build_adjacency_maps_uses_native_when_available(monkeypatch):
    entities = _sample_entities()
    calls: dict[str, object] = {}
    native_result = ({1: [("/Native", "IfcProxy", 2)]}, {2: [("/Native", "IfcWall", 1)]})

    def fake_native(value: dict[int, dict]) -> tuple[dict[int, list[tuple[str, str | None, int]]], dict[int, list[tuple[str, str | None, int]]]]:
        calls["entities"] = value
        return native_result

    monkeypatch.setattr(graph_utils_mod, "_NATIVE_BUILD_ADJACENCY_MAPS", fake_native)

    assert graph_utils_mod.build_adjacency_maps(entities) == native_result
    assert calls["entities"] is entities


def test_native_build_adjacency_maps_matches_python_when_extension_available():
    if graph_utils_mod._NATIVE_BUILD_ADJACENCY_MAPS is None:
        return

    entities = _sample_entities()
    native_adjacency, native_reverse = graph_utils_mod.build_adjacency_maps(entities)
    python_adjacency = graph_utils_mod.build_adjacency(entities)
    python_reverse = graph_utils_mod.build_reverse_adjacency(entities, python_adjacency)

    assert native_adjacency == python_adjacency
    assert native_reverse == python_reverse
