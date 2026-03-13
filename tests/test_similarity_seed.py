import athar.diff.identity_pipeline as identity_pipeline_mod
import athar.diff.context as diff_engine_context_mod
from athar.diff.context import prepare_diff_context
from athar.diff.similarity_seed import text_fingerprint_pairs, unique_guid_pairs


def _graph_with_entities(entities: dict[int, dict]) -> dict:
    return {"metadata": {"schema": "IFC4"}, "entities": entities}


def test_unique_guid_pairs_matches_only_unique_overlap():
    old_graph = _graph_with_entities({
        1: {"entity_type": "IfcWall", "global_id": "A", "attributes": {}, "refs": []},
        2: {"entity_type": "IfcWall", "global_id": "DUP", "attributes": {}, "refs": []},
        3: {"entity_type": "IfcWall", "global_id": "DUP", "attributes": {}, "refs": []},
    })
    new_graph = _graph_with_entities({
        10: {"entity_type": "IfcWall", "global_id": "A", "attributes": {}, "refs": []},
        20: {"entity_type": "IfcWall", "global_id": "DUP", "attributes": {}, "refs": []},
    })

    pairs, diagnostics = unique_guid_pairs(old_graph, new_graph)
    assert pairs == {1: 10}
    assert diagnostics["matched"] == 1
    assert diagnostics["unique_guid_overlap"] == 1.0


def test_text_fingerprint_pairs_skips_ambiguous_buckets():
    old_graph = _graph_with_entities({
        1: {"entity_type": "IfcCartesianPoint", "attributes": {"Coordinates": {"kind": "list", "items": [{"kind": "real", "value": "0"}]}}, "refs": []},
        2: {"entity_type": "IfcCartesianPoint", "attributes": {"Coordinates": {"kind": "list", "items": [{"kind": "real", "value": "0"}]}}, "refs": []},
    })
    new_graph = _graph_with_entities({
        10: {"entity_type": "IfcCartesianPoint", "attributes": {"Coordinates": {"kind": "list", "items": [{"kind": "real", "value": "0"}]}}, "refs": []},
        20: {"entity_type": "IfcCartesianPoint", "attributes": {"Coordinates": {"kind": "list", "items": [{"kind": "real", "value": "0"}]}}, "refs": []},
    })

    out = text_fingerprint_pairs(old_graph, new_graph, profile="semantic_stable")
    assert out["old_to_new"] == {}
    assert out["ambiguous_buckets"] == 1


def test_prepare_context_uses_text_fingerprint_seeds(monkeypatch):
    old_graph = _graph_with_entities({
        1: {"entity_type": "IfcBeam", "attributes": {"Name": {"kind": "string", "value": "Keep"}}, "refs": []},
        2: {"entity_type": "IfcBeam", "attributes": {"Name": {"kind": "string", "value": "Old"}}, "refs": []},
    })
    new_graph = _graph_with_entities({
        10: {"entity_type": "IfcBeam", "attributes": {"Name": {"kind": "string", "value": "Keep"}}, "refs": []},
        20: {"entity_type": "IfcBeam", "attributes": {"Name": {"kind": "string", "value": "New"}}, "refs": []},
    })

    real_structural_hash = identity_pipeline_mod.structural_hash
    calls = {"count": 0}

    def wrapped_structural_hash(entity):
        calls["count"] += 1
        return real_structural_hash(entity)

    monkeypatch.setattr(identity_pipeline_mod, "structural_hash", wrapped_structural_hash)
    context = prepare_diff_context(old_graph, new_graph, profile="semantic_stable")
    context["old_owner_projector"].close()
    context["new_owner_projector"].close()

    assert calls["count"] == 0
    assert context["stats"]["matched_by_method"].get("text_fingerprint") == 1


def test_text_fingerprint_pairs_refines_small_ambiguous_bucket():
    old_graph = _graph_with_entities({
        1: {
            "entity_type": "IfcLocalPlacement",
            "attributes": {"RelativePlacement": {"kind": "ref", "id": 101}},
            "refs": [{"path": "/RelativePlacement", "target": 101, "target_type": "IfcAxis2Placement3D"}],
        },
        2: {
            "entity_type": "IfcLocalPlacement",
            "attributes": {"RelativePlacement": {"kind": "ref", "id": 102}},
            "refs": [{"path": "/RelativePlacement", "target": 102, "target_type": "IfcAxis2Placement3D"}],
        },
        101: {
            "entity_type": "IfcAxis2Placement3D",
            "attributes": {"Name": {"kind": "string", "value": "A"}},
            "refs": [],
        },
        102: {
            "entity_type": "IfcAxis2Placement3D",
            "attributes": {"Name": {"kind": "string", "value": "B"}},
            "refs": [],
        },
    })
    new_graph = _graph_with_entities({
        10: {
            "entity_type": "IfcLocalPlacement",
            "attributes": {"RelativePlacement": {"kind": "ref", "id": 201}},
            "refs": [{"path": "/RelativePlacement", "target": 201, "target_type": "IfcAxis2Placement3D"}],
        },
        20: {
            "entity_type": "IfcLocalPlacement",
            "attributes": {"RelativePlacement": {"kind": "ref", "id": 202}},
            "refs": [{"path": "/RelativePlacement", "target": 202, "target_type": "IfcAxis2Placement3D"}],
        },
        201: {
            "entity_type": "IfcAxis2Placement3D",
            "attributes": {"Name": {"kind": "string", "value": "A"}},
            "refs": [],
        },
        202: {
            "entity_type": "IfcAxis2Placement3D",
            "attributes": {"Name": {"kind": "string", "value": "B"}},
            "refs": [],
        },
    })

    out = text_fingerprint_pairs(old_graph, new_graph, profile="semantic_stable")
    assert out["old_to_new"] == {1: 10, 2: 20, 101: 201, 102: 202}
    assert out["refined_buckets"] >= 1


def test_text_fingerprint_pairs_skips_guid_bearing_entities():
    old_graph = _graph_with_entities({
        1: {
            "entity_type": "IfcWall",
            "global_id": "OLD_GUID",
            "attributes": {"ObjectPlacement": {"kind": "ref", "id": 100}},
            "refs": [{"path": "/ObjectPlacement", "target": 100, "target_type": "IfcLocalPlacement"}],
        },
        100: {
            "entity_type": "IfcLocalPlacement",
            "attributes": {"RelativePlacement": {"kind": "null"}},
            "refs": [],
        },
    })
    new_graph = _graph_with_entities({
        2: {
            "entity_type": "IfcWall",
            "global_id": "NEW_GUID",
            "attributes": {"ObjectPlacement": {"kind": "ref", "id": 200}},
            "refs": [{"path": "/ObjectPlacement", "target": 200, "target_type": "IfcLocalPlacement"}],
        },
        200: {
            "entity_type": "IfcLocalPlacement",
            "attributes": {"RelativePlacement": {"kind": "null"}},
            "refs": [],
        },
    })

    out = text_fingerprint_pairs(old_graph, new_graph, profile="semantic_stable")
    assert out["old_to_new"] == {100: 200}


def test_prepare_context_reuses_early_guid_path_propagation(monkeypatch):
    old_graph = _graph_with_entities({
        1: {
            "entity_type": "IfcWall",
            "global_id": "GUID_A",
            "attributes": {"ObjectPlacement": {"kind": "ref", "id": 100}},
            "refs": [{"path": "/ObjectPlacement", "target": 100, "target_type": "IfcLocalPlacement"}],
        },
        100: {
            "entity_type": "IfcLocalPlacement",
            "attributes": {"RelativePlacement": {"kind": "null"}},
            "refs": [],
        },
    })
    new_graph = _graph_with_entities({
        2: {
            "entity_type": "IfcWall",
            "global_id": "GUID_A",
            "attributes": {"ObjectPlacement": {"kind": "ref", "id": 200}},
            "refs": [{"path": "/ObjectPlacement", "target": 200, "target_type": "IfcLocalPlacement"}],
        },
        200: {
            "entity_type": "IfcLocalPlacement",
            "attributes": {"RelativePlacement": {"kind": "null"}},
            "refs": [],
        },
    })

    real_propagate = diff_engine_context_mod.propagate_matches_by_typed_path
    calls = {"count": 0}

    def wrapped_propagate(*args, **kwargs):
        calls["count"] += 1
        return real_propagate(*args, **kwargs)

    monkeypatch.setattr(diff_engine_context_mod, "propagate_matches_by_typed_path", wrapped_propagate)
    context = prepare_diff_context(old_graph, new_graph, profile="semantic_stable")
    context["old_owner_projector"].close()
    context["new_owner_projector"].close()

    assert calls["count"] == 1


def test_prepare_context_uses_whole_graph_coverage_for_early_path(monkeypatch):
    old_entities = {
        1: {
            "entity_type": "IfcWall",
            "global_id": "GUID_A",
            "attributes": {"ObjectPlacement": {"kind": "ref", "id": 100}},
            "refs": [{"path": "/ObjectPlacement", "target": 100, "target_type": "IfcLocalPlacement"}],
        },
        100: {
            "entity_type": "IfcLocalPlacement",
            "attributes": {"RelativePlacement": {"kind": "null"}},
            "refs": [],
        },
    }
    new_entities = {
        2: {
            "entity_type": "IfcWall",
            "global_id": "GUID_A",
            "attributes": {"ObjectPlacement": {"kind": "ref", "id": 200}},
            "refs": [{"path": "/ObjectPlacement", "target": 200, "target_type": "IfcLocalPlacement"}],
        },
        200: {
            "entity_type": "IfcLocalPlacement",
            "attributes": {"RelativePlacement": {"kind": "null"}},
            "refs": [],
        },
    }
    for idx in range(1000, 1120):
        old_entities[idx] = {"entity_type": "IfcCartesianPoint", "attributes": {}, "refs": []}
    for idx in range(2000, 2120):
        new_entities[idx] = {"entity_type": "IfcCartesianPoint", "attributes": {}, "refs": []}

    calls = {"count": 0}

    def wrapped_propagate(*args, **kwargs):
        calls["count"] += 1
        return {"method": "typed_path_propagation", "old_to_new": {}, "diagnostics": {}, "ambiguous": 0}

    monkeypatch.setattr(diff_engine_context_mod, "propagate_matches_by_typed_path", wrapped_propagate)
    context = prepare_diff_context(
        _graph_with_entities(old_entities),
        _graph_with_entities(new_entities),
        profile="semantic_stable",
    )
    context["old_owner_projector"].close()
    context["new_owner_projector"].close()

    assert calls["count"] == 1


def test_prepare_context_does_not_cache_seeded_identity_state(monkeypatch):
    old_graph = _graph_with_entities({
        1: {
            "entity_type": "IfcBeam",
            "attributes": {"Name": {"kind": "string", "value": "Keep"}},
            "refs": [],
        },
    })
    new_graph = _graph_with_entities({
        10: {
            "entity_type": "IfcBeam",
            "attributes": {"Name": {"kind": "string", "value": "Keep"}},
            "refs": [],
        },
    })
    save_calls = {"count": 0}

    def fake_save_cached(*args, **kwargs):
        save_calls["count"] += 1

    monkeypatch.setattr(diff_engine_context_mod, "save_cached", fake_save_cached)
    context = prepare_diff_context(
        old_graph,
        new_graph,
        profile="semantic_stable",
        file_hashes=("oldhash", "newhash"),
    )
    context["old_owner_projector"].close()
    context["new_owner_projector"].close()

    assert save_calls["count"] == 0


def test_prepare_context_uses_parallel_seeded_side_results_when_enabled(monkeypatch):
    old_graph = _graph_with_entities({
        1: {"entity_type": "IfcBeam", "attributes": {"Name": {"kind": "string", "value": "Keep"}}, "refs": []},
        2: {"entity_type": "IfcBeam", "attributes": {"Name": {"kind": "string", "value": "Old"}}, "refs": []},
    })
    new_graph = _graph_with_entities({
        10: {"entity_type": "IfcBeam", "attributes": {"Name": {"kind": "string", "value": "Keep"}}, "refs": []},
        20: {"entity_type": "IfcBeam", "attributes": {"Name": {"kind": "string", "value": "New"}}, "refs": []},
    })
    calls = {"count": 0}

    def fake_parallel(*_args, **kwargs):
        calls["count"] += 1
        return (
            diff_engine_context_mod._prepare_seeded_side_state(
                old_graph,
                profile=kwargs["profile"],
                exclude_steps=kwargs["exclude_old"],
            ),
            diff_engine_context_mod._prepare_seeded_side_state(
                new_graph,
                profile=kwargs["profile"],
                exclude_steps=kwargs["exclude_new"],
            ),
        )

    monkeypatch.setattr(diff_engine_context_mod, "_parallel_enabled", lambda: True)
    monkeypatch.setattr(diff_engine_context_mod, "_prepare_seeded_sides_parallel", fake_parallel)

    context = prepare_diff_context(old_graph, new_graph, profile="semantic_stable")
    context["old_owner_projector"].close()
    context["new_owner_projector"].close()

    assert calls["count"] == 1
    assert context["stats"]["matched_by_method"].get("text_fingerprint") == 1


def test_prepare_context_falls_back_to_sequential_seed_collection(monkeypatch):
    old_graph = _graph_with_entities({
        1: {"entity_type": "IfcBeam", "attributes": {"Name": {"kind": "string", "value": "Keep"}}, "refs": []},
    })
    new_graph = _graph_with_entities({
        10: {"entity_type": "IfcBeam", "attributes": {"Name": {"kind": "string", "value": "Keep"}}, "refs": []},
    })
    calls = {"count": 0}
    real_text_fingerprint_pairs = diff_engine_context_mod.text_fingerprint_pairs

    def wrapped_text_fingerprint_pairs(*args, **kwargs):
        calls["count"] += 1
        return real_text_fingerprint_pairs(*args, **kwargs)

    monkeypatch.setattr(diff_engine_context_mod, "_parallel_enabled", lambda: True)
    monkeypatch.setattr(diff_engine_context_mod, "_prepare_seeded_sides_parallel", lambda *args, **kwargs: None)
    monkeypatch.setattr(diff_engine_context_mod, "text_fingerprint_pairs", wrapped_text_fingerprint_pairs)

    context = prepare_diff_context(old_graph, new_graph, profile="semantic_stable")
    context["old_owner_projector"].close()
    context["new_owner_projector"].close()

    assert calls["count"] == 1
