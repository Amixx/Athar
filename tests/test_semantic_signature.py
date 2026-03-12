from athar.diff.semantic_signature import semantic_signature


def test_semantic_signature_ignores_literal_value_changes():
    entity_a = {
        "entity_type": "IfcWall",
        "attributes": {"Name": {"kind": "string", "value": "Wall A"}},
        "refs": [],
    }
    entity_b = {
        "entity_type": "IfcWall",
        "attributes": {"Name": {"kind": "string", "value": "Wall B"}},
        "refs": [],
    }
    assert semantic_signature(entity_a) == semantic_signature(entity_b)


def test_semantic_signature_detects_kind_changes():
    entity_a = {
        "entity_type": "IfcWall",
        "attributes": {"Name": {"kind": "string", "value": "Wall A"}},
        "refs": [],
    }
    entity_b = {
        "entity_type": "IfcWall",
        "attributes": {"Name": {"kind": "int", "value": 42}},
        "refs": [],
    }
    assert semantic_signature(entity_a) != semantic_signature(entity_b)


def test_semantic_signature_detects_edge_target_type_changes():
    entity_a = {
        "entity_type": "IfcWall",
        "attributes": {},
        "refs": [{"path": "/ObjectPlacement", "target": 10, "target_type": "IfcLocalPlacement"}],
    }
    entity_b = {
        "entity_type": "IfcWall",
        "attributes": {},
        "refs": [{"path": "/ObjectPlacement", "target": 10, "target_type": "IfcGridPlacement"}],
    }
    assert semantic_signature(entity_a) != semantic_signature(entity_b)
