from athar._native.required import native_entity_fingerprint


def test_native_entity_text_fingerprint_ignores_ref_ids_but_preserves_edge_shape():
    entities = [
        {
            "entity_type": "IfcWall",
            "attributes": {
                "Name": {"kind": "string", "value": "Wall A"},
                "ObjectPlacement": {"kind": "ref", "id": 42},
                "SomeList": {"kind": "list", "items": [{"kind": "int", "value": 1}]},
            },
            "refs": [
                {"path": "/ObjectPlacement", "target": 42, "target_type": "IfcLocalPlacement"}
            ],
        },
        {
            "entity_type": "IfcProxy",
            "attributes": {
                "Flags": {"kind": "list", "items": [True, False, None, 1.25, "abc"]},
                "Bag": {
                    "kind": "bag",
                    "items": [{"kind": "ref", "id": 9}, {"kind": "string", "value": "x"}],
                },
            },
            "refs": [
                {"path": "/Relating", "target": 9, "target_type": None},
                {"path": "/Relating", "target": 10, "target_type": None},
            ],
        },
    ]

    entity_a = {
        "entity_type": "IfcWall",
        "attributes": {"Placement": {"kind": "ref", "id": 10}},
        "refs": [{"path": "/Placement", "target": 10, "target_type": "IfcLocalPlacement"}],
    }
    entity_b = {
        "entity_type": "IfcWall",
        "attributes": {"Placement": {"kind": "ref", "id": 20}},
        "refs": [{"path": "/Placement", "target": 20, "target_type": "IfcLocalPlacement"}],
    }
    entity_c = {
        "entity_type": "IfcWall",
        "attributes": {"Placement": {"kind": "ref", "id": 20}},
        "refs": [{"path": "/OtherPlacement", "target": 20, "target_type": "IfcLocalPlacement"}],
    }

    for entity in entities:
        assert native_entity_fingerprint(entity)

    assert (
        native_entity_fingerprint(entity_a)
        == native_entity_fingerprint(entity_b)
    )
    assert (
        native_entity_fingerprint(entity_a)
        != native_entity_fingerprint(entity_c)
    )
