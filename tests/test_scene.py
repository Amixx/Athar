"""Tests for the scene model builder.

Uses real IFC files from data/ to verify scene model construction.
"""

from athar.parser import parse
from athar.scene import build_scene

BASIC_HOUSE = "data/BasicHouse.ifc"
ADVANCED = "data/AdvancedProject.ifc"


def _parse_and_build(path: str) -> dict:
    return build_scene(parse(path))


class TestBasicHouse:
    """Tests against BasicHouse.ifc — a simple Swedish house."""

    def setup_method(self):
        self.scene = _parse_and_build(BASIC_HOUSE)

    def test_has_storeys(self):
        names = [s["name"] for s in self.scene["storeys"]]
        assert "Floor 0" in names
        assert "Floor 1" in names

    def test_storeys_sorted_by_elevation(self):
        elevations = [s["elevation"] for s in self.scene["storeys"]]
        assert elevations == sorted(elevations)

    def test_walls_have_orientation(self):
        walls = [
            el for el in self.scene["elements"].values()
            if el["category"] == "wall"
        ]
        assert len(walls) > 0
        oriented = [w for w in walls if w["orientation"] is not None]
        assert len(oriented) == len(walls), "All walls should have orientation"
        for w in oriented:
            assert "facing" in w["orientation"]

    def test_exterior_walls_detected(self):
        walls = [
            el for el in self.scene["elements"].values()
            if el["category"] == "wall"
        ]
        exterior = [w for w in walls if w["is_external"] is True]
        interior = [w for w in walls if w["is_external"] is False]
        assert len(exterior) > 0, "Should detect exterior walls"
        assert len(interior) > 0, "Should detect interior walls"

    def test_doors_hosted_in_walls(self):
        doors = [
            el for el in self.scene["elements"].values()
            if el["category"] == "door"
        ]
        assert len(doors) > 0
        hosted = [d for d in doors if d["host_guid"] is not None]
        assert len(hosted) == len(doors), "All doors should be hosted in walls"
        # Verify host is actually a wall
        for d in hosted:
            host = self.scene["elements"].get(d["host_guid"])
            assert host is not None
            assert host["category"] == "wall"

    def test_windows_hosted_in_walls(self):
        windows = [
            el for el in self.scene["elements"].values()
            if el["category"] == "window"
        ]
        assert len(windows) > 0
        hosted = [w for w in windows if w["host_guid"] is not None]
        assert len(hosted) == len(windows), "All windows should be hosted in walls"

    def test_exterior_windows_in_exterior_walls(self):
        windows = [
            el for el in self.scene["elements"].values()
            if el["category"] == "window" and el["host_guid"]
        ]
        for w in windows:
            host = self.scene["elements"][w["host_guid"]]
            if host["is_external"] is True:
                assert "exterior" in w["label"]

    def test_walls_list_hosted_elements(self):
        walls_with_hosted = [
            el for el in self.scene["elements"].values()
            if el["category"] == "wall" and el["hosted_guids"]
        ]
        assert len(walls_with_hosted) > 0
        for w in walls_with_hosted:
            for hg in w["hosted_guids"]:
                hosted = self.scene["elements"].get(hg)
                assert hosted is not None
                assert hosted["category"] in ("door", "window")

    def test_labels_are_descriptive(self):
        """Labels should contain category and storey at minimum."""
        for el in self.scene["elements"].values():
            label = el["label"]
            assert label, f"Element {el['guid']} has no label"
            # Most elements on a storey should mention it
            if el["storey"]:
                assert el["storey"] in label

    def test_revit_names_cleaned(self):
        """Revit 'Family:Type:ID' names should have the element ID stripped."""
        import re
        revit_pattern = re.compile(r":\d{4,}$")  # Revit IDs are 6+ digits
        for el in self.scene["elements"].values():
            if el["name"]:
                assert not revit_pattern.search(el["name"]), (
                    f"Name still has Revit element ID: {el['name']}"
                )

    def test_furniture_uses_name_as_label(self):
        furniture = [
            el for el in self.scene["elements"].values()
            if el["category"] == "furniture" and el["name"]
        ]
        assert len(furniture) > 0
        for f in furniture:
            # Label should contain the element's name (not just "furniture")
            assert f["name"] in f["label"] or "furniture" in f["label"]


class TestAdvancedProject:
    """Tests against AdvancedProject.ifc — a larger multi-storey building."""

    def setup_method(self):
        self.scene = _parse_and_build(ADVANCED)

    def test_has_many_storeys(self):
        assert len(self.scene["storeys"]) >= 5

    def test_roof_aggregation(self):
        """Roof should have child slabs via aggregation."""
        roofs = [
            el for el in self.scene["elements"].values()
            if el["category"] == "roof"
        ]
        assert len(roofs) > 0
        roofs_with_children = [r for r in roofs if r["child_guids"]]
        assert len(roofs_with_children) > 0

    def test_stair_aggregation(self):
        """Stairs should have child flights/members via aggregation."""
        stairs = [
            el for el in self.scene["elements"].values()
            if el["category"] == "stair"
        ]
        assert len(stairs) > 0
        stairs_with_children = [s for s in stairs if s["child_guids"]]
        assert len(stairs_with_children) > 0

    def test_no_spatial_structure_in_elements(self):
        """IfcBuilding and IfcBuildingStorey should not appear in elements."""
        for el in self.scene["elements"].values():
            assert el["ifc_class"] not in ("IfcBuilding", "IfcBuildingStorey")
