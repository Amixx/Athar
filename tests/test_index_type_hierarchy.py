from __future__ import annotations

import pytest

from athar.index.store import GraphIndex


def test_graph_index_supports_exact_and_subtype_type_queries():
    graph = {
        "metadata": {"schema": "IFC4"},
        "entities": {
            1: {"entity_type": "IfcWall", "attributes": {}, "refs": []},
            2: {"entity_type": "IfcWallStandardCase", "attributes": {}, "refs": []},
            3: {"entity_type": "IfcSlab", "attributes": {}, "refs": []},
        },
    }

    index = GraphIndex.from_graph(graph)

    assert index.entities_of_type("IfcWall") == [1]
    assert index.entities_of_type("IfcWall", include_subtypes=True) == [1, 2]
    assert index.entities_of_type("IfcBuildingElement", include_subtypes=True) == [1, 2, 3]
    assert index.entities_of_type("IfcDistributionElement", include_subtypes=True) == []


def test_graph_index_subtype_queries_require_schema():
    graph = {
        "metadata": {},
        "entities": {
            1: {"entity_type": "IfcWall", "attributes": {}, "refs": []},
        },
    }

    index = GraphIndex.from_graph(graph)

    with pytest.raises(ValueError):
        index.entities_of_type("IfcWall", include_subtypes=True)
