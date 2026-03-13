from __future__ import annotations

from athar.index.store import GraphIndex


def test_graph_index_guid_lookup_is_ambiguity_aware():
    graph = {
        "metadata": {"schema": "IFC4"},
        "entities": {
            1: {"entity_type": "IfcWall", "global_id": "GUID-1", "attributes": {}, "refs": []},
            2: {"entity_type": "IfcDoor", "global_id": "GUID-2", "attributes": {}, "refs": []},
            3: {"entity_type": "IfcWall", "global_id": "GUID-2", "attributes": {}, "refs": []},
            4: {"entity_type": "IfcSpace", "attributes": {}, "refs": []},
        },
    }

    index = GraphIndex.from_graph(graph)

    assert index.entity_by_guid("GUID-1") == 1
    assert index.unique_entity_by_guid("GUID-1") == 1
    assert index.entities_by_guid("GUID-1") == [1]
    assert index.entity_by_guid("GUID-2") is None
    assert index.unique_entity_by_guid("GUID-2") is None
    assert index.entities_by_guid("GUID-2") == [2, 3]
    assert index.entities_by_guid("missing") == []
    assert index.all_guids() == {"GUID-1": [1], "GUID-2": [2, 3]}
