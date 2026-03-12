from athar.canonical_ids import structural_hash


def test_structural_hash_ignores_step_ids():
    entity_a = {
        "entity_type": "IfcWall",
        "attributes": {"ObjectPlacement": {"kind": "ref", "id": 10}},
        "refs": [{"path": "/ObjectPlacement", "target": 10, "target_type": "IfcLocalPlacement"}],
    }
    entity_b = {
        "entity_type": "IfcWall",
        "attributes": {"ObjectPlacement": {"kind": "ref", "id": 99}},
        "refs": [{"path": "/ObjectPlacement", "target": 99, "target_type": "IfcLocalPlacement"}],
    }
    assert structural_hash(entity_a) == structural_hash(entity_b)


def test_structural_hash_detects_target_type_changes():
    entity_a = {
        "entity_type": "IfcWall",
        "attributes": {"ObjectPlacement": {"kind": "ref", "id": 10}},
        "refs": [{"path": "/ObjectPlacement", "target": 10, "target_type": "IfcLocalPlacement"}],
    }
    entity_b = {
        "entity_type": "IfcWall",
        "attributes": {"ObjectPlacement": {"kind": "ref", "id": 10}},
        "refs": [{"path": "/ObjectPlacement", "target": 10, "target_type": "IfcGridPlacement"}],
    }
    assert structural_hash(entity_a) != structural_hash(entity_b)


def test_structural_hash_stays_sha256_wire_width():
    entity = {
        "entity_type": "IfcWall",
        "attributes": {"Name": {"kind": "string", "value": "A"}},
        "refs": [],
    }
    digest = structural_hash(entity)
    assert len(digest) == 64
    assert all(ch in "0123456789abcdef" for ch in digest)
