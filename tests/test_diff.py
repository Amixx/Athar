"""Tests for the semantic IFC diff pipeline.

Uses real IFC files from data/, modified programmatically with known changes.
"""

from athar.parser import parse
from athar.differ import diff


def test_detects_added_elements(modified_arch):
    old_path, new_path, manifest = modified_arch
    result = diff(parse(old_path), parse(new_path))

    added_guids = {e["guid"] for e in result["added"]}
    for entry in manifest["added"]:
        assert entry["guid"] in added_guids, f"Expected added element {entry['guid']} not found"


def test_detects_deleted_elements(modified_arch):
    old_path, new_path, manifest = modified_arch
    result = diff(parse(old_path), parse(new_path))

    deleted_guids = {e["guid"] for e in result["deleted"]}
    for entry in manifest["deleted"]:
        assert entry["guid"] in deleted_guids, f"Expected deleted element {entry['guid']} not found"


def test_detects_renamed_elements(modified_arch):
    old_path, new_path, manifest = modified_arch
    result = diff(parse(old_path), parse(new_path))

    changed_by_guid = {e["guid"]: e for e in result["changed"]}
    for entry in manifest["renamed"]:
        assert entry["guid"] in changed_by_guid, f"Renamed element {entry['guid']} not in changed"
        changes = changed_by_guid[entry["guid"]]["changes"]
        name_changes = [c for c in changes if c["field"] == "name"]
        assert len(name_changes) == 1
        assert name_changes[0]["old"] == entry["old_name"]
        assert name_changes[0]["new"] == entry["new_name"]


def test_detects_property_changes(modified_arch):
    old_path, new_path, manifest = modified_arch
    result = diff(parse(old_path), parse(new_path))

    changed_by_guid = {e["guid"]: e for e in result["changed"]}
    for entry in manifest["property_changed"]:
        assert entry["guid"] in changed_by_guid, f"Property-changed element {entry['guid']} not in changed"
        changes = changed_by_guid[entry["guid"]]["changes"]
        pset_field = f"pset.{entry['pset']}"
        pset_changes = [c for c in changes if c["field"].startswith(pset_field)]
        assert len(pset_changes) >= 1, f"No change found for {pset_field}"


def test_no_false_positives(modified_arch):
    """Diffing a file against itself should produce zero changes."""
    old_path, _, _ = modified_arch
    result = diff(parse(old_path), parse(old_path))
    assert result["added"] == []
    assert result["deleted"] == []
    assert result["changed"] == []


def test_summary_counts_consistent(modified_arch):
    old_path, new_path, _ = modified_arch
    result = diff(parse(old_path), parse(new_path))
    s = result["summary"]
    assert s["added"] == len(result["added"])
    assert s["deleted"] == len(result["deleted"])
    assert s["changed"] == len(result["changed"])
