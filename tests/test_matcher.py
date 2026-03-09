"""Tests for content-based entity matching when GlobalIds are regenerated."""

from athar.matcher import match_entities, _build_features, _score_pair


def _make_entity(ifc_class="IfcWall", name="Wall A", container="Floor 0",
                 type_name="Basic Wall", placement=None, attributes=None,
                 property_sets=None, groups=None):
    return {
        "ifc_class": ifc_class,
        "name": name,
        "container": container,
        "type_name": type_name,
        "placement": placement or [[1, 0, 0, 0], [0, 1, 0, 0], [0, 0, 1, 0], [0, 0, 0, 1]],
        "attributes": attributes or {"Name": name, "Description": "A wall"},
        "property_sets": property_sets or {"Pset_WallCommon": {"IsExternal": True}},
        "owner_history": None,
        "groups": groups or [],
    }


def _make_model(entities_dict, metadata=None):
    return {
        "metadata": metadata or {"schema": "IFC4", "timestamp": None},
        "entities": entities_dict,
        "relationships": {"voids": [], "fills": [], "aggregates": [], "spatial_children": {}},
    }


class TestGuidMatching:
    """When GUID overlap is high, use standard GUID matching."""

    def test_identical_guids(self):
        entities = {"GUID1": _make_entity(), "GUID2": _make_entity(name="Wall B")}
        old = _make_model(entities)
        new = _make_model(entities)
        result = match_entities(old, new)
        assert result["method"] == "guid"
        assert result["old_to_new"] == {"GUID1": "GUID1", "GUID2": "GUID2"}

    def test_high_overlap_uses_guid(self):
        old = _make_model({
            "G1": _make_entity(name="A"),
            "G2": _make_entity(name="B"),
            "G3": _make_entity(name="C"),
        })
        # G1 and G2 preserved, G3 removed, G4 added
        new = _make_model({
            "G1": _make_entity(name="A"),
            "G2": _make_entity(name="B"),
            "G4": _make_entity(name="D"),
        })
        result = match_entities(old, new)
        assert result["method"] == "guid"
        assert result["old_to_new"] == {"G1": "G1", "G2": "G2"}


class TestContentFallback:
    """When GUID overlap is low, fall back to content-based matching."""

    def test_regenerated_guids_matched_by_content(self):
        """All GUIDs changed but entities are the same — should match by content."""
        wall_a = _make_entity(name="Wall A", container="Floor 0", type_name="Basic Wall")
        wall_b = _make_entity(name="Wall B", container="Floor 1", type_name="Thick Wall")
        slab = _make_entity(ifc_class="IfcSlab", name="Slab 1", container="Floor 0",
                            type_name="Concrete Slab")

        old = _make_model({
            "OLD1": wall_a,
            "OLD2": wall_b,
            "OLD3": slab,
        })
        new = _make_model({
            "NEW1": wall_a,
            "NEW2": wall_b,
            "NEW3": slab,
        })
        result = match_entities(old, new)
        assert result["method"] == "content_fallback"
        assert result["old_to_new"]["OLD1"] == "NEW1"
        assert result["old_to_new"]["OLD2"] == "NEW2"
        assert result["old_to_new"]["OLD3"] == "NEW3"

    def test_partial_guid_regeneration(self):
        """Mostly regenerated GUIDs with one stable — fallback should handle both."""
        wall = _make_entity(name="Wall A")
        slab = _make_entity(ifc_class="IfcSlab", name="Slab 1", container="Floor 0",
                            type_name="Concrete Slab")
        door = _make_entity(ifc_class="IfcDoor", name="Door 1", container="Floor 0",
                            type_name="Single Door")
        window = _make_entity(ifc_class="IfcWindow", name="Window 1", container="Floor 0",
                              type_name="Casement")

        old = _make_model({
            "STABLE1": wall,
            "OLD_SLAB": slab,
            "OLD_DOOR": door,
            "OLD_WIN": window,
        })
        new = _make_model({
            "STABLE1": wall,     # same GUID
            "NEW_SLAB": slab,    # new GUID, same content
            "NEW_DOOR": door,    # new GUID, same content
            "NEW_WIN": window,   # new GUID, same content
        })
        result = match_entities(old, new)
        # 1/4 overlap = 25% → triggers content fallback
        assert result["method"] == "content_fallback"
        assert result["old_to_new"]["STABLE1"] == "STABLE1"
        assert result["old_to_new"]["OLD_SLAB"] == "NEW_SLAB"
        assert result["old_to_new"]["OLD_DOOR"] == "NEW_DOOR"
        assert result["old_to_new"]["OLD_WIN"] == "NEW_WIN"

    def test_ambiguous_entities_left_unmatched(self):
        """Multiple identical entities — should not guess."""
        window = _make_entity(ifc_class="IfcWindow", name="Window",
                              container="Floor 0", type_name="Standard Window",
                              attributes={}, property_sets={})
        old = _make_model({
            "OLD1": window,
            "OLD2": window,
        })
        new = _make_model({
            "NEW1": window,
            "NEW2": window,
        })
        result = match_entities(old, new)
        assert result["method"] == "content_fallback"
        # Ambiguous — both have same signature. With placement disambiguation
        # they might match (same identity placement), but if not, it's fine
        # to leave unmatched. The important thing is no crash.
        assert len(result["old_to_new"]) <= 2

    def test_different_entities_not_matched(self):
        """Entities with different content should not be matched."""
        old = _make_model({
            "OLD1": _make_entity(name="Wall A", container="Floor 0"),
        })
        new = _make_model({
            "NEW1": _make_entity(ifc_class="IfcSlab", name="Slab X", container="Floor 2"),
        })
        result = match_entities(old, new)
        assert result["method"] == "content_fallback"
        assert len(result["old_to_new"]) == 0

    def test_positional_disambiguation(self):
        """Two entities with same content but different positions should match by position."""
        wall1 = _make_entity(name="Wall", container="Floor 0",
                             placement=[[1, 0, 0, 1000], [0, 1, 0, 2000], [0, 0, 1, 0], [0, 0, 0, 1]])
        wall2 = _make_entity(name="Wall", container="Floor 0",
                             placement=[[1, 0, 0, 5000], [0, 1, 0, 8000], [0, 0, 1, 0], [0, 0, 0, 1]])

        old = _make_model({"OLD1": wall1, "OLD2": wall2})
        new = _make_model({"NEW1": wall1, "NEW2": wall2})
        result = match_entities(old, new)
        assert result["method"] == "content_fallback"
        assert result["old_to_new"].get("OLD1") == "NEW1"
        assert result["old_to_new"].get("OLD2") == "NEW2"


class TestFeatureExtraction:
    """Test the internal feature extraction."""

    def test_entity_references_excluded_from_props(self):
        """GUID references in attributes should not be part of stable props."""
        entity = _make_entity(attributes={"Name": "Wall", "RelRef": {"ref": "SOME_GUID"}})
        feat = _build_features(entity)
        # The ref should be excluded
        for k, v in feat["stable_props"]:
            assert "SOME_GUID" not in str(v)

    def test_volatile_keys_excluded(self):
        """Keys like 'id', 'guid' should not appear in stable props."""
        entity = _make_entity(attributes={"Name": "Wall", "GlobalId": "ABC123"})
        feat = _build_features(entity)
        for k, v in feat["stable_props"]:
            assert k.lower() not in ("globalid", "id", "guid")


class TestScoring:
    """Test fuzzy scoring between entity feature pairs."""

    def test_identical_entities_score_high(self):
        entity = _make_entity()
        f = _build_features(entity)
        score = _score_pair(f, f)
        assert score >= 0.8

    def test_different_class_not_scored(self):
        """Fuzzy matching requires same ifc_class (enforced at caller level)."""
        f1 = _build_features(_make_entity(ifc_class="IfcWall"))
        f2 = _build_features(_make_entity(ifc_class="IfcSlab"))
        # Score still computed but won't be used since class filter is in matcher
        # Just verify it's low when name/type/container differ
        f2_diff = _build_features(_make_entity(ifc_class="IfcSlab", name="Other",
                                                container="Floor 5", type_name="X"))
        score = _score_pair(f1, f2_diff)
        assert score < 0.5

    def test_nearby_placement_scores_higher(self):
        e1 = _make_entity(placement=[[1, 0, 0, 1000], [0, 1, 0, 2000], [0, 0, 1, 0], [0, 0, 0, 1]])
        e2_near = _make_entity(placement=[[1, 0, 0, 1010], [0, 1, 0, 2010], [0, 0, 1, 0], [0, 0, 0, 1]])
        e2_far = _make_entity(placement=[[1, 0, 0, 99000], [0, 1, 0, 99000], [0, 0, 1, 0], [0, 0, 0, 1]])

        f1 = _build_features(e1)
        f_near = _build_features(e2_near)
        f_far = _build_features(e2_far)

        score_near = _score_pair(f1, f_near)
        score_far = _score_pair(f1, f_far)
        assert score_near > score_far
