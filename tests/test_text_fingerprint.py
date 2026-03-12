import athar.diff.text_fingerprint as text_fingerprint_mod


def test_entity_text_fingerprint_falls_back_to_python_when_native_unavailable(monkeypatch):
    entity = {
        "entity_type": "IfcWall",
        "attributes": {
            "Name": {"kind": "string", "value": "Wall A"},
            "ObjectPlacement": {"kind": "ref", "id": 42},
        },
        "refs": [{"path": "/ObjectPlacement", "target": 42, "target_type": "IfcLocalPlacement"}],
    }
    monkeypatch.setattr(text_fingerprint_mod, "_NATIVE_ENTITY_FINGERPRINT", None)

    assert (
        text_fingerprint_mod.entity_text_fingerprint(entity)
        == text_fingerprint_mod.python_entity_text_fingerprint(entity)
    )


def test_entity_text_fingerprint_uses_native_when_available(monkeypatch):
    entity = {"entity_type": "IfcWall", "attributes": {}, "refs": []}
    calls: dict[str, object] = {}

    def fake_native(value: dict) -> str:
        calls["entity"] = value
        return "native-fingerprint"

    monkeypatch.setattr(text_fingerprint_mod, "_NATIVE_ENTITY_FINGERPRINT", fake_native)

    assert text_fingerprint_mod.entity_text_fingerprint(entity) == "native-fingerprint"
    assert calls["entity"] is entity


def test_native_entity_text_fingerprint_matches_python_when_extension_available():
    if text_fingerprint_mod._NATIVE_ENTITY_FINGERPRINT is None:
        return

    entity = {
        "entity_type": "IfcWall",
        "attributes": {
            "Name": {"kind": "string", "value": "Wall A"},
            "ObjectPlacement": {"kind": "ref", "id": 42},
            "SomeList": {"kind": "list", "items": [{"kind": "int", "value": 1}]},
        },
        "refs": [{"path": "/ObjectPlacement", "target": 42, "target_type": "IfcLocalPlacement"}],
    }

    assert (
        text_fingerprint_mod.entity_text_fingerprint(entity)
        == text_fingerprint_mod.python_entity_text_fingerprint(entity)
    )


def test_python_entity_text_fingerprint_ignores_ref_ids_but_preserves_edge_shape():
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

    assert (
        text_fingerprint_mod.python_entity_text_fingerprint(entity_a)
        == text_fingerprint_mod.python_entity_text_fingerprint(entity_b)
    )
    assert (
        text_fingerprint_mod.python_entity_text_fingerprint(entity_a)
        != text_fingerprint_mod.python_entity_text_fingerprint(entity_c)
    )
