import pytest

from athar.diff.engine import diff_graphs
from athar.diff.matcher_policy import default_matcher_policy, resolve_matcher_policy


def _graph(entities: dict[int, dict]) -> dict:
    return {"metadata": {"schema": "IFC4"}, "entities": entities}


def test_resolve_matcher_policy_defaults():
    policy = resolve_matcher_policy(None)
    assert policy == default_matcher_policy()


def test_resolve_matcher_policy_rejects_unknown_section():
    with pytest.raises(ValueError, match="Unknown matcher_policy section"):
        resolve_matcher_policy({"weird": {}})


def test_resolve_matcher_policy_rejects_invalid_depth_limits():
    with pytest.raises(ValueError, match="depth3_max must be <= depth2_max"):
        resolve_matcher_policy({"secondary_match": {"depth2_max": 2, "depth3_max": 3}})


def test_diff_graphs_rejects_invalid_matcher_policy():
    graph = _graph({})
    with pytest.raises(ValueError, match="depth3_max must be <= depth2_max"):
        diff_graphs(graph, graph, matcher_policy={"secondary_match": {"depth2_max": 2, "depth3_max": 3}})


def test_resolve_matcher_policy_rejects_invalid_unresolved_limit():
    with pytest.raises(ValueError, match="unresolved_limit must be >= 1"):
        resolve_matcher_policy({"secondary_match": {"unresolved_limit": 0}})


def test_resolve_matcher_policy_rejects_invalid_unresolved_pair_limit():
    with pytest.raises(ValueError, match="unresolved_pair_limit must be >= 1"):
        resolve_matcher_policy({"secondary_match": {"unresolved_pair_limit": 0}})


def test_diff_graphs_secondary_threshold_override_changes_matching():
    old_graph = _graph({
        1: {
            "entity_type": "IfcWall",
            "global_id": "AAA",
            "attributes": {"Name": {"kind": "string", "value": "Wall A"}},
            "refs": [],
        },
        20: {
            "entity_type": "IfcCartesianPoint",
            "attributes": {
                "Coordinates": {
                    "kind": "list",
                    "items": [
                        {"kind": "real", "value": "1"},
                        {"kind": "real", "value": "2"},
                        {"kind": "real", "value": "3"},
                    ],
                }
            },
            "refs": [],
        },
    })
    new_graph = _graph({
        2: {
            "entity_type": "IfcWall",
            "global_id": "AAA",
            "attributes": {"Name": {"kind": "string", "value": "Wall A"}},
            "refs": [],
        },
        21: {
            "entity_type": "IfcCartesianPoint",
            "attributes": {
                "Coordinates": {
                    "kind": "list",
                    "items": [
                        {"kind": "real", "value": "1"},
                        {"kind": "real", "value": "2"},
                        {"kind": "real", "value": "4"},
                    ],
                }
            },
            "refs": [],
        },
    })

    baseline = diff_graphs(old_graph, new_graph)
    assert {change["op"] for change in baseline["base_changes"]} == {"MODIFY"}
    assert baseline["base_changes"][0]["identity"]["match_method"] == "secondary_match"

    strict = diff_graphs(
        old_graph,
        new_graph,
        matcher_policy={"secondary_match": {"score_threshold": 0.99}},
    )
    assert {change["op"] for change in strict["base_changes"]} == {"ADD", "REMOVE"}
    assert strict["identity_policy"]["matcher_policy"]["secondary_match"]["score_threshold"] == 0.99
