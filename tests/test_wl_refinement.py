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
