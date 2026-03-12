import importlib.util

import pytest

from athar.canonical_ids import wl_refine_colors


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
