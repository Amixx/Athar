"""Case subtypes collapse to the matcher class of their supertype."""

from __future__ import annotations

import pytest

from ifcopenshell import ifcopenshell_wrapper as wrapper

from athar.bottom.native_schema import _build_descriptors
from athar.bottom.schema_dict import canonical_class_name

SCHEMAS = ("IFC2X3", "IFC4")


def _entities(schema_name: str) -> dict:
    schema = wrapper.schema_by_name(schema_name)
    return {d.name(): d for d in schema.declarations() if hasattr(d, "all_attributes")}


@pytest.mark.parametrize("schema_name", SCHEMAS)
def test_case_subtypes_collapse_to_their_supertype(schema_name):
    entities = _entities(schema_name)
    cases = [name for name in entities if name.endswith(("StandardCase", "ElementedCase"))]
    assert "IfcWallStandardCase" in cases
    for name in cases:
        assert canonical_class_name(schema_name, name) == entities[name].supertype().name()
    assert canonical_class_name(schema_name, "IfcWall") == "IfcWall"


def test_ifc4_case_classes_share_the_matcher_class_of_their_supertype():
    descriptors = _build_descriptors("IFC4")
    assert descriptors["IFCSLABELEMENTEDCASE"]["class"] == "IfcSlab"
    assert descriptors["IFCOPENINGSTANDARDCASE"]["class"] == "IfcOpeningElement"
    assert descriptors["IFCBEAMSTANDARDCASE"]["class"] == "IfcBeam"
