from __future__ import annotations

from athar.graph.canonical_values import canonical_string
from athar.index.query import query_entities, query_step_ids
from athar.index.store import EdgeEntry, GraphIndex


def _sample_graph() -> dict:
    return {
        "metadata": {"schema": "IFC4"},
        "entities": {
            1: {
                "entity_type": "IfcWall",
                "global_id": "GUID-1",
                "attributes": {
                    "Name": {"kind": "string", "value": "Wall A"},
                    "Description": {"kind": "null"},
                    "ObjectPlacement": {"kind": "ref", "id": 2},
                    "LoadBearing": {"kind": "bool", "value": True},
                },
                "refs": [
                    {"path": "/ObjectPlacement", "target": 2, "target_type": "IfcLocalPlacement"},
                    {"path": "/HasOpenings/0", "target": 3, "target_type": "IfcRelVoidsElement"},
                    {"path": "/Dangling", "target": 99, "target_type": "IfcDoor"},
                ],
            },
            2: {
                "entity_type": "IfcLocalPlacement",
                "attributes": {
                    "RelativePlacement": {"kind": "string", "value": "Axis2Placement3D"},
                },
                "refs": [],
            },
            3: {
                "entity_type": "IfcRelVoidsElement",
                "attributes": {
                    "Name": {"kind": "string", "value": "OpeningRel"},
                    "RelatedOpeningElement": {"kind": "ref", "id": 4},
                    "RelatedBuildingElement": {"kind": "ref", "id": 1},
                },
                "refs": [
                    {"path": "/RelatedOpeningElement", "target": 4, "target_type": "IfcOpeningElement"},
                    {"path": "/RelatedBuildingElement", "target": 1, "target_type": "IfcWall"},
                ],
            },
            4: {
                "entity_type": "IfcOpeningElement",
                "global_id": "GUID-OPENING",
                "attributes": {
                    "Name": {"kind": "string", "value": "Opening"},
                    "RepresentationTypes": {
                        "kind": "set",
                        "items": [
                            {"kind": "string", "value": "Body"},
                            {"kind": "string", "value": "Axis"},
                        ],
                    },
                },
                "refs": [
                    {
                        "path": "/Representation@IfcProductDefinitionShape/Representations/0",
                        "target": 2,
                        "target_type": "IfcShapeRepresentation",
                    }
                ],
            },
        },
    }


def _edge_provenance(path: str) -> tuple[str, bool]:
    stripped = path.lstrip("/")
    if not stripped:
        return "", False
    segments = stripped.split("/")
    return segments[0].split("@", 1)[0], len(segments) > 1


def _scan_attribute_steps(graph: dict, attr_name: str, *, require_nonnull: bool = False) -> list[int]:
    steps = []
    for step_id, entity in graph["entities"].items():
        value = entity.get("attributes", {}).get(attr_name)
        if value is None:
            continue
        if require_nonnull and value.get("kind") == "null":
            continue
        steps.append(step_id)
    return sorted(steps)


def _scan_attribute_value_steps(graph: dict, attr_name: str, value: dict) -> list[int]:
    target = canonical_string(value)
    steps = []
    for step_id, entity in graph["entities"].items():
        attr_value = entity.get("attributes", {}).get(attr_name)
        if attr_value is None:
            continue
        if canonical_string(attr_value) == target:
            steps.append(step_id)
    return sorted(steps)


def _scan_edges(graph: dict) -> tuple[dict[int, list[EdgeEntry]], dict[int, list[EdgeEntry]]]:
    entities = graph["entities"]
    forward: dict[int, list[EdgeEntry]] = {step_id: [] for step_id in entities}
    reverse: dict[int, list[EdgeEntry]] = {step_id: [] for step_id in entities}
    for source_step, entity in entities.items():
        for ref in entity.get("refs", []):
            target_step = ref.get("target")
            if target_step not in entities:
                continue
            attr_name, in_aggregate = _edge_provenance(ref.get("path", ""))
            forward[source_step].append(
                EdgeEntry(step_id=target_step, attr_name=attr_name, in_aggregate=in_aggregate)
            )
            reverse[target_step].append(
                EdgeEntry(step_id=source_step, attr_name=attr_name, in_aggregate=in_aggregate)
            )
    for edge_map in (forward, reverse):
        for step_id, entries in edge_map.items():
            entries.sort(key=lambda item: (item.attr_name, item.step_id, item.in_aggregate))
    return forward, reverse


def test_graph_index_matches_bruteforce_attribute_queries():
    graph = _sample_graph()
    index = GraphIndex.from_graph(graph)

    assert index.entities_with_attribute("Name") == _scan_attribute_steps(graph, "Name")
    assert index.entities_with_attribute("Description") == _scan_attribute_steps(graph, "Description")
    assert index.entities_with_nonempty_attribute("Description") == _scan_attribute_steps(
        graph,
        "Description",
        require_nonnull=True,
    )
    assert index.entities_with_nonempty_attribute("ObjectPlacement") == _scan_attribute_steps(
        graph,
        "ObjectPlacement",
        require_nonnull=True,
    )


def test_graph_index_matches_bruteforce_attribute_value_queries():
    graph = _sample_graph()
    index = GraphIndex.from_graph(graph)
    expected = {"kind": "string", "value": "Wall A"}
    aggregate_value = graph["entities"][4]["attributes"]["RepresentationTypes"]

    assert index.entities_with_attribute_value("Name", expected) == _scan_attribute_value_steps(
        graph,
        "Name",
        expected,
    )
    assert index.entities_with_attribute_value(
        "RepresentationTypes",
        aggregate_value,
    ) == _scan_attribute_value_steps(graph, "RepresentationTypes", aggregate_value)


def test_graph_index_matches_bruteforce_labeled_edges():
    graph = _sample_graph()
    index = GraphIndex.from_graph(graph)
    forward, reverse = _scan_edges(graph)

    for step_id in graph["entities"]:
        assert index.targets_of(step_id) == forward[step_id]
        assert index.sources_of(step_id) == reverse[step_id]


def test_graph_index_exposes_entity_access_and_counts():
    graph = _sample_graph()
    index = GraphIndex.from_graph(graph)

    assert index.total_entities == 4
    assert index.entity(1) == graph["entities"][1]
    assert index.entity(999) is None
    assert index.entities([4, 999, 1]) == [graph["entities"][4], graph["entities"][1]]
    assert index.type_counts() == {
        "IfcLocalPlacement": 1,
        "IfcOpeningElement": 1,
        "IfcRelVoidsElement": 1,
        "IfcWall": 1,
    }


def test_query_helpers_compose_common_filters():
    graph = _sample_graph()
    index = GraphIndex.from_graph(graph)

    assert query_step_ids(index, attr_name="Name") == [1, 3, 4]
    assert query_step_ids(
        index,
        entity_type="IfcOpeningElement",
        guid="GUID-OPENING",
        attr_name="Name",
        require_nonnull=True,
    ) == [4]
    assert query_entities(index, guid="GUID-1") == [graph["entities"][1]]
