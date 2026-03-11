from athar.root_remap import plan_root_remap


def _graph(entities: dict[int, dict]) -> dict:
    return {"metadata": {"schema": "IFC4"}, "entities": entities}


def _root(step_id: int, gid: str, name: str) -> tuple[int, dict]:
    return (
        step_id,
        {
            "entity_type": "IfcWall",
            "global_id": gid,
            "attributes": {
                "GlobalId": {"kind": "string", "value": gid},
                "Name": {"kind": "string", "value": name},
                "OwnerHistory": {"kind": "ref", "id": 500},
            },
            "refs": [
                {"path": "/OwnerHistory", "target": 500, "target_type": "IfcOwnerHistory"}
            ],
        },
    )


def test_root_remap_unique_signature_matching():
    old_graph = _graph(dict([
        _root(1, "OLD_A", "Wall A"),
        _root(2, "OLD_B", "Wall B"),
    ]))
    new_graph = _graph(dict([
        _root(3, "NEW_A", "Wall A"),
        _root(4, "NEW_B", "Wall B"),
    ]))

    plan = plan_root_remap(old_graph, new_graph)
    assert plan["enabled"] is True
    assert plan["guid_overlap"] == 0.0
    assert plan["old_to_new"] == {"OLD_A": "NEW_A", "OLD_B": "NEW_B"}
    assert plan["ambiguous"] == 0


def test_root_remap_ambiguous_buckets_left_unmatched():
    old_graph = _graph(dict([
        _root(1, "OLD_A", "Wall Same"),
        _root(2, "OLD_B", "Wall Same"),
    ]))
    new_graph = _graph(dict([
        _root(3, "NEW_A", "Wall Same"),
        _root(4, "NEW_B", "Wall Same"),
    ]))

    plan = plan_root_remap(old_graph, new_graph)
    assert plan["old_to_new"] == {}
    assert plan["ambiguous"] == 2


def test_root_remap_skips_when_guid_overlap_is_high():
    old_graph = _graph(dict([
        _root(1, "SHARED", "Wall A"),
        _root(2, "OLD_B", "Wall B"),
    ]))
    new_graph = _graph(dict([
        _root(3, "SHARED", "Wall A"),
        _root(4, "NEW_B", "Wall B"),
    ]))

    plan = plan_root_remap(old_graph, new_graph)
    assert plan["enabled"] is False
    assert plan["method"] == "disabled_guid_overlap"
    assert plan["old_to_new"] == {}
