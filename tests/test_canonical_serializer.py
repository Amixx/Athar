from athar.canonical_serializer import build_class_record, build_entity_record, serialize_records


def test_serializer_orders_by_id():
    rec_a = build_entity_record(entity_id="H:2", entity_type="IfcWall", attributes={})
    rec_b = build_entity_record(entity_id="G:1", entity_type="IfcWall", attributes={})
    rec_c = build_class_record(
        class_id="C:9",
        entity_type="IfcCartesianPoint",
        old_count=2,
        new_count=1,
        exemplar={"Coordinates": {"kind": "list", "items": []}},
    )

    output = serialize_records([rec_a, rec_b, rec_c]).splitlines()
    ids = [line.split("\"id\":\"", 1)[1].split("\"", 1)[0] for line in output]
    assert ids == ["G:1", "H:2", "C:9"]
