import importlib.util

import pytest

from athar.canonical_ids import wl_refine_colors, wl_refine_with_scc_fallback


def _make_graph(offset: int = 0, edge_path: str = "/Ref") -> dict:
    return {
        "entities": {
            1 + offset: {
                "entity_type": "IfcWall",
                "attributes": {"Name": {"kind": "string", "value": "A"}},
                "refs": [
                    {"path": edge_path, "target": 2 + offset, "target_type": "IfcLocalPlacement"}
                ],
            },
            2 + offset: {
                "entity_type": "IfcLocalPlacement",
                "attributes": {"RelativePlacement": {"kind": "null"}},
                "refs": [],
            },
        }
    }


def _symmetric_cycle(size: int, *, offset: int = 0) -> dict:
    entities: dict[int, dict] = {}
    for i in range(size):
        step = offset + i + 1
        nxt = offset + ((i + 1) % size) + 1
        entities[step] = {
            "entity_type": "IfcProxy",
            "attributes": {},
            "refs": [{"path": "/Peer", "target": nxt, "target_type": "IfcProxy"}],
        }
    return {"entities": entities}


def test_wl_refinement_deterministic_across_step_ids():
    colors_a = wl_refine_colors(_make_graph(offset=0), max_rounds=3)
    colors_b = wl_refine_colors(_make_graph(offset=10), max_rounds=3)
    assert sorted(colors_a.values()) == sorted(colors_b.values())


def test_wl_refinement_detects_edge_label_change():
    colors_a = wl_refine_colors(_make_graph(edge_path="/Ref"), max_rounds=3)
    colors_b = wl_refine_colors(_make_graph(edge_path="/OtherRef"), max_rounds=3)
    assert sorted(colors_a.values()) != sorted(colors_b.values())


def test_wl_refinement_accepts_explicit_round_hash_sha256():
    colors_a = wl_refine_colors(_make_graph(edge_path="/Ref"), max_rounds=3, round_hash="sha256")
    colors_b = wl_refine_colors(_make_graph(edge_path="/Ref"), max_rounds=3, round_hash="sha256")
    assert colors_a == colors_b


def test_wl_refinement_rejects_unknown_round_hash():
    with pytest.raises(ValueError, match="Unknown WL round hash"):
        wl_refine_colors(_make_graph(), round_hash="weird_hash")


def test_wl_refinement_rejects_unavailable_explicit_backend():
    if importlib.util.find_spec("xxhash") is not None:
        pytest.skip("xxhash available in this environment")
    with pytest.raises(ValueError, match="backend unavailable"):
        wl_refine_colors(_make_graph(), round_hash="xxh3_64")


def test_wl_refinement_auto_round_hash_is_stable():
    first = wl_refine_colors(_make_graph(edge_path="/Ref"), max_rounds=3, round_hash="auto")
    second = wl_refine_colors(_make_graph(edge_path="/Ref"), max_rounds=3, round_hash="auto")
    assert first == second


def test_wl_scc_fallback_marks_symmetric_cycle_as_ambiguous_class():
    _colors, classes = wl_refine_with_scc_fallback(_symmetric_cycle(3))
    assert set(classes) == {1, 2, 3}
    assert len(set(classes.values())) == 1
    class_id = next(iter(classes.values()))
    assert class_id.startswith("C:")


def test_wl_scc_fallback_class_id_stable_across_step_renumbering():
    _colors_a, classes_a = wl_refine_with_scc_fallback(_symmetric_cycle(4, offset=0))
    _colors_b, classes_b = wl_refine_with_scc_fallback(_symmetric_cycle(4, offset=10))
    assert len(set(classes_a.values())) == 1
    assert len(set(classes_b.values())) == 1
    assert next(iter(classes_a.values())) == next(iter(classes_b.values()))


def test_wl_scc_fallback_does_not_classify_acyclic_duplicates():
    graph = {
        "entities": {
            1: {"entity_type": "IfcProxy", "attributes": {}, "refs": []},
            2: {"entity_type": "IfcProxy", "attributes": {}, "refs": []},
        }
    }
    _colors, classes = wl_refine_with_scc_fallback(graph)
    assert classes == {}


def test_wl_scc_fallback_honors_partition_size_cap():
    _colors, classes = wl_refine_with_scc_fallback(_symmetric_cycle(4), max_partition_size=2)
    assert set(classes) == {1, 2, 3, 4}
