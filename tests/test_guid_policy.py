import pytest
import json

from athar.diff.engine import diff_graphs, stream_diff_graphs
from athar.diff.guid_policy import (
    GUID_POLICY_DISAMBIGUATE,
    GUID_POLICY_FAIL_FAST,
    enforce_or_disambiguate_guid_policy,
)


def _graph(entities: dict[int, dict]) -> dict:
    return {"metadata": {"schema": "IFC4"}, "entities": entities}


def test_guid_policy_fail_fast_raises_on_invalid_guid():
    entities = {
        1: {"entity_type": "IfcWall", "global_id": "", "attributes": {}, "refs": []},
    }
    with pytest.raises(ValueError, match="GUID policy violation"):
        enforce_or_disambiguate_guid_policy(entities, policy=GUID_POLICY_FAIL_FAST, side="old")


def test_guid_policy_disambiguate_emits_invalid_and_duplicate_g_bang_ids():
    entities = {
        1: {"entity_type": "IfcWall", "global_id": "DUP", "attributes": {}, "refs": []},
        2: {"entity_type": "IfcWall", "global_id": "DUP", "attributes": {}, "refs": []},
        3: {"entity_type": "IfcWall", "global_id": "", "attributes": {}, "refs": []},
    }
    out = enforce_or_disambiguate_guid_policy(entities, policy=GUID_POLICY_DISAMBIGUATE, side="old")
    disambiguated = out["disambiguated"]
    assert disambiguated[1]["entity_id"].startswith("G!:DUP#")
    assert disambiguated[2]["entity_id"].startswith("G!:DUP#")
    assert disambiguated[3]["entity_id"] == "G!:INVALID#1"


def test_diff_graphs_rejects_unknown_guid_policy():
    g = _graph({})
    with pytest.raises(ValueError, match="Unknown guid policy"):
        diff_graphs(g, g, guid_policy="weird")


def test_stream_diff_graphs_header_includes_identity_policy():
    old_graph = _graph({
        1: {"entity_type": "IfcWall", "global_id": "AAA", "attributes": {}, "refs": []},
    })
    new_graph = _graph({
        2: {"entity_type": "IfcWall", "global_id": "AAA", "attributes": {}, "refs": []},
    })
    records = [line for line in stream_diff_graphs(old_graph, new_graph, guid_policy="disambiguate", mode="ndjson")]
    header = json.loads(records[0])
    assert header["identity_policy"]["guid_policy"] == "disambiguate"
    assert "matcher_policy" in header["identity_policy"]
