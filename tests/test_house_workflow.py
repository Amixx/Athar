"""End-to-end workflow test using 3 versions of BasicHouse.

Tests the full pipeline: parse → diff → report across v1→v2→v3.
Uses pre-generated fixtures from scripts/generate_house_versions.py.

Regenerate fixtures with:
    python scripts/generate_house_versions.py
"""

import os

import pytest

from athar.parser import parse
from athar.differ import diff
from athar_layers.placement import enrich_diff

V1 = "tests/fixtures/house_v1.ifc"
V2 = "tests/fixtures/house_v2.ifc"
V3 = "tests/fixtures/house_v3.ifc"

# Skip all tests if fixtures haven't been generated yet
pytestmark = pytest.mark.skipif(
    not os.path.exists(V1),
    reason="Run 'python scripts/generate_house_versions.py' first",
)


@pytest.fixture(scope="module")
def parsed_v1():
    return parse(V1)


@pytest.fixture(scope="module")
def parsed_v2():
    return parse(V2)


@pytest.fixture(scope="module")
def parsed_v3():
    return parse(V3)


@pytest.fixture(scope="module")
def diff_v1_v2(parsed_v1, parsed_v2):
    result = diff(parsed_v1, parsed_v2)
    enrich_diff(result, parsed_v2["entities"])
    return result


@pytest.fixture(scope="module")
def diff_v2_v3(parsed_v2, parsed_v3):
    result = diff(parsed_v2, parsed_v3)
    enrich_diff(result, parsed_v3["entities"])
    return result


# --- v1 → v2: Renovation phase ---

class TestV1ToV2:
    """v1→v2: 3 windows removed, 6 furniture moved 2m east, door property changed."""

    def test_three_windows_deleted(self, diff_v1_v2):
        deleted_guids = {e["guid"] for e in diff_v1_v2["deleted"]}
        removed_windows = [
            "2DedXznHnDaeAWsrTB_q8F",
            "2DedXznHnDaeAWsrTB_q8E",
            "2DedXznHnDaeAWsrTB_q8D",
        ]
        for guid in removed_windows:
            assert guid in deleted_guids, f"Window {guid} should be deleted"

    def test_deleted_are_windows_not_openings(self, diff_v1_v2):
        deleted_classes = {e["ifc_class"] for e in diff_v1_v2["deleted"]}
        # Windows should be deleted, but their openings are filtered out
        # (filled openings are implementation details, not user-facing)
        assert "IfcWindow" in deleted_classes
        assert "IfcOpeningElement" not in deleted_classes

    def test_no_walls_deleted(self, diff_v1_v2):
        deleted_classes = [e["ifc_class"] for e in diff_v1_v2["deleted"]]
        assert "IfcWallStandardCase" not in deleted_classes

    def test_bulk_movement_detected(self, diff_v1_v2):
        """6 furniture items moved by the same vector should be detected as bulk movement.

        Bulk-moved items are pulled out of 'changed' into 'bulk_movements'.
        Each group has 'entities' (list of {guid, ifc_class, name}).
        """
        bulk = diff_v1_v2.get("bulk_movements", [])
        assert len(bulk) >= 1, "Should detect at least one bulk movement group"
        # The bulk movement should contain our 6 furniture items
        all_bulk_guids = set()
        for group in bulk:
            all_bulk_guids.update(e["guid"] for e in group["entities"])
        moved_guids = {
            "2DedXznHnDaeAWsrTB_qBW",
            "2DedXznHnDaeAWsrTB_q8V",
            "2DedXznHnDaeAWsrTB_q8U",
            "2DedXznHnDaeAWsrTB_q8T",
            "2DedXznHnDaeAWsrTB_q8S",
            "2DedXznHnDaeAWsrTB_q8R",
        }
        assert moved_guids.issubset(all_bulk_guids), (
            f"Bulk movement should contain all 6 furniture items, "
            f"missing: {moved_guids - all_bulk_guids}"
        )

    def test_bulk_movement_description(self, diff_v1_v2):
        """Bulk movement should describe direction and distance."""
        bulk = diff_v1_v2["bulk_movements"]
        # Find the group with our furniture
        for group in bulk:
            guids = {e["guid"] for e in group["entities"]}
            if "2DedXznHnDaeAWsrTB_qBW" in guids:
                assert "east" in group["description"]
                assert "2.0m" in group["description"]
                break
        else:
            pytest.fail("Furniture bulk movement group not found")

    def test_door_property_changed(self, diff_v1_v2):
        """Door fire rating should show as a property change."""
        door_guid = "2DedXznHnDaeAWsrTB_qBb"
        changed_by_guid = {e["guid"]: e for e in diff_v1_v2["changed"]}
        assert door_guid in changed_by_guid, "Door should show property change"
        changes = changed_by_guid[door_guid]["changes"]
        fire_changes = [c for c in changes if "FireRating" in c["field"]]
        assert len(fire_changes) >= 1, "Should detect FireRating change"

    def test_no_false_positives_on_untouched(self, diff_v1_v2):
        """Elements we didn't touch should not appear in changed/added/deleted."""
        # An untouched interior wall
        untouched_wall = "2DedXznHnDaeAWsrTB_qBg"
        all_affected = set()
        all_affected.update(e["guid"] for e in diff_v1_v2["added"])
        all_affected.update(e["guid"] for e in diff_v1_v2["deleted"])
        all_affected.update(e["guid"] for e in diff_v1_v2["changed"])
        assert untouched_wall not in all_affected

    def test_summary_counts(self, diff_v1_v2):
        s = diff_v1_v2["summary"]
        assert s["deleted"] >= 3, "At least 3 windows deleted"
        assert s["changed"] >= 1, "At least door property changed"


# --- v2 → v3: Further modifications ---

class TestV2ToV3:
    """v2→v3: interior wall removed, exterior door moved, 5 furniture deleted."""

    def test_wall_deleted(self, diff_v2_v3):
        deleted_guids = {e["guid"] for e in diff_v2_v3["deleted"]}
        assert "2DedXznHnDaeAWsrTB_qBg" in deleted_guids, "Interior wall should be deleted"

    def test_furniture_deleted(self, diff_v2_v3):
        deleted_guids = {e["guid"] for e in diff_v2_v3["deleted"]}
        removed_furniture = [
            "2DedXznHnDaeAWsrTB_q8M",
            "2DedXznHnDaeAWsrTB_q8L",
            "2DedXznHnDaeAWsrTB_q8K",
            "2DedXznHnDaeAWsrTB_q8J",
            "2DedXznHnDaeAWsrTB_q8I",
        ]
        for guid in removed_furniture:
            assert guid in deleted_guids, f"Furniture {guid} should be deleted"

    def test_door_moved(self, diff_v2_v3):
        """Exterior door should show a placement change."""
        door_guid = "2DedXznHnDaeAWsrTB_q8y"
        changed_by_guid = {e["guid"]: e for e in diff_v2_v3["changed"]}
        assert door_guid in changed_by_guid, "Moved door should be in changed"
        changes = changed_by_guid[door_guid]["changes"]
        placement_changes = [c for c in changes if c["field"] == "placement"]
        assert len(placement_changes) == 1, "Door should have exactly one placement change"

    def test_no_windows_affected(self, diff_v2_v3):
        """No windows should be added or deleted in v2→v3."""
        for e in diff_v2_v3["added"]:
            assert e["ifc_class"] != "IfcWindow"
        for e in diff_v2_v3["deleted"]:
            assert e["ifc_class"] != "IfcWindow"

    def test_nothing_added(self, diff_v2_v3):
        """v2→v3 only removes and moves — nothing should be added."""
        assert len(diff_v2_v3["added"]) == 0


# --- Full chain: v1 → v3 ---

class TestV1ToV3:
    """Cumulative diff: v1→v3 should show all changes combined."""

    def test_cumulative_deletions(self, parsed_v1, parsed_v3):
        result = diff(parsed_v1, parsed_v3)
        enrich_diff(result, parsed_v3["entities"])
        deleted_guids = {e["guid"] for e in result["deleted"]}

        # All 3 windows from v1→v2
        assert "2DedXznHnDaeAWsrTB_q8F" in deleted_guids
        assert "2DedXznHnDaeAWsrTB_q8E" in deleted_guids
        assert "2DedXznHnDaeAWsrTB_q8D" in deleted_guids
        # Wall from v2→v3
        assert "2DedXznHnDaeAWsrTB_qBg" in deleted_guids
        # Furniture from v2→v3
        assert "2DedXznHnDaeAWsrTB_q8M" in deleted_guids

    def test_cumulative_changes(self, parsed_v1, parsed_v3):
        result = diff(parsed_v1, parsed_v3)
        enrich_diff(result, parsed_v3["entities"])
        changed_guids = {e["guid"] for e in result["changed"]}

        # Door moved in v2→v3 should appear in changed or bulk_movements
        all_affected = set(changed_guids)
        for bm in result.get("bulk_movements", []):
            all_affected.update(e["guid"] for e in bm["entities"])
        assert "2DedXznHnDaeAWsrTB_q8y" in all_affected, "Door should show movement"

        # Furniture moved in v1→v2 should appear in bulk_movements
        bulk_guids = set()
        for bm in result.get("bulk_movements", []):
            bulk_guids.update(e["guid"] for e in bm["entities"])
        assert "2DedXznHnDaeAWsrTB_qBW" in bulk_guids, "Furniture should be in bulk movements"
