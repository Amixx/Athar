"""The native policy tables are complete and consistent with both schemas."""

from __future__ import annotations

import pytest

from ifcopenshell import ifcopenshell_wrapper as wrapper

from athar.bottom._native import native
from athar.bottom.native_schema import _build_descriptors
from athar.bottom.schema_dict import canonical_class_name

SCHEMAS = ("IFC2X3", "IFC4")
ROOT_CLASSES = ("IfcProduct", "IfcProject")


def _entities(schema_name: str) -> dict:
    schema = wrapper.schema_by_name(schema_name)
    return {d.name(): d for d in schema.declarations() if hasattr(d, "all_attributes")}


def _supertypes(declaration) -> set[str]:
    names = set()
    while declaration is not None:
        names.add(declaration.name())
        declaration = declaration.supertype()
    return names


def _referenced_entities(attr_type) -> list:
    """Entity declarations an attribute of this type can reference."""
    if hasattr(attr_type, "type_of_element"):
        return _referenced_entities(attr_type.type_of_element())
    if hasattr(attr_type, "select_list"):
        return [e for item in attr_type.select_list() for e in _referenced_entities(item)]
    if hasattr(attr_type, "all_attributes"):
        return [attr_type]
    if hasattr(attr_type, "declared_type"):
        declared = attr_type.declared_type()
        return [] if isinstance(declared, str) or declared is None else _referenced_entities(declared)
    return []


def _can_reference_a_root(attr_type) -> bool:
    for entity in _referenced_entities(attr_type):
        above = _supertypes(entity)
        if any(root in above for root in ROOT_CLASSES):
            return True
        if any(entity.name() in _supertypes(entity.schema().declaration_by_name(root)) for root in ROOT_CLASSES):
            return True
    return False


def _attributes(keyword: str) -> dict[str, list]:
    """Attribute name -> its type in each schema that declares the class."""
    found: dict[str, list] = {}
    for schema_name in SCHEMAS:
        for name, declaration in _entities(schema_name).items():
            if name.upper() == keyword:
                for attribute in declaration.all_attributes():
                    found.setdefault(attribute.name(), []).append(attribute.type_of_attribute())
    return found


def _policy() -> dict[str, tuple[list, str | None]]:
    return {keyword: (links, reason) for keyword, links, reason in native().relationship_policy()}


@pytest.mark.parametrize("schema_name", SCHEMAS)
def test_every_concrete_relationship_class_has_a_policy_entry(schema_name):
    concrete = {
        name.upper()
        for name, declaration in _entities(schema_name).items()
        if "IfcRelationship" in _supertypes(declaration) and not declaration.is_abstract()
    }
    assert len(concrete) > 40
    assert concrete - _policy().keys() == set()


def test_policy_entries_name_schema_classes_and_attributes():
    for keyword, (links, reason) in _policy().items():
        attributes = _attributes(keyword)
        assert attributes, f"{keyword} is in neither schema"
        assert bool(links) != bool(reason), keyword
        for source_attr, target_attr, _kind in links:
            assert source_attr in attributes and target_attr in attributes, (keyword, source_attr, target_attr)


def test_every_root_capable_reference_of_a_projected_relationship_is_a_role():
    for keyword, (links, reason) in _policy().items():
        if reason:
            continue
        roles = {attr for source_attr, target_attr, _ in links for attr in (source_attr, target_attr)}
        for name, types in _attributes(keyword).items():
            if any(_can_reference_a_root(t) for t in types):
                assert name in roles, (keyword, name)


def test_reference_tables_name_schema_attributes():
    tables = native().reference_policy()
    assert set(tables) == {
        "unhashed_attributes",
        "denied_references",
        "reverse_attachments",
        "unordered_lists",
        "cyclic_lists",
    }
    all_attributes = {
        attribute.name()
        for schema_name in SCHEMAS
        for declaration in _entities(schema_name).values()
        for attribute in declaration.all_attributes()
    }
    for table, rows in tables.items():
        for keyword, attr, reason in rows:
            if keyword == "*":
                assert attr in all_attributes, (table, attr)
            else:
                assert attr in _attributes(keyword), (table, keyword, attr)
            if table in ("unhashed_attributes", "denied_references", "unordered_lists"):
                assert reason, (table, keyword, attr)


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


@pytest.mark.parametrize("schema_name", SCHEMAS)
def test_descriptor_flags_mark_the_project_and_relationships(schema_name):
    descriptors = _build_descriptors(schema_name)
    project = descriptors["IFCPROJECT"]
    assert project["project"] and not project["spatial"] and not project["product"]
    assert descriptors["IFCRELAGGREGATES"]["relationship"]
    assert not descriptors["IFCWALL"]["relationship"] and not descriptors["IFCWALL"]["project"]
