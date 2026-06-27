"""Tests for the Python schema/unit glue the native parser depends on.

These exercise the file-independent introspection in ``parser`` and
``native_schema`` directly (no IFC file needed): measure-type detection and the
per-class descriptor flags Rust consumes. The Rust side that *applies* these
descriptors is tested in ``athar/_native`` (``cargo test``); the end-to-end
behavior is tested through the engine suite.
"""

from __future__ import annotations

import json

import pytest

from ifcopenshell import ifcopenshell_wrapper as wrapper

from athar.bottom.native_schema import _build_descriptors, schema_descriptors_json
from athar.bottom.parser import _measure_type_from_attr_type

SCHEMAS = ("IFC2X3", "IFC4")


def _attr_type(schema_name: str, entity: str, attr: str):
    schema = wrapper.schema_by_name(schema_name)
    decl = schema.declaration_by_name(entity)
    attribute = {a.name(): a for a in decl.all_attributes()}[attr]
    return attribute.type_of_attribute()


@pytest.mark.parametrize("schema_name", SCHEMAS)
def test_measure_detection_is_deterministic_and_finds_outermost_measure(schema_name: str) -> None:
    # Regression: the type-chain walk once used id()-based cycle detection over
    # transient SWIG proxies, so recycled heap addresses produced phantom
    # cycles and measure detection flipped between runs. It must be stable and
    # read the *Measure name from the outermost defined type.
    attr_type = _attr_type(schema_name, "IfcExtrudedAreaSolid", "Depth")
    outcomes = {_measure_type_from_attr_type(attr_type) for _ in range(2000)}
    assert outcomes == {"IfcPositiveLengthMeasure"}


@pytest.mark.parametrize("schema_name", SCHEMAS)
def test_descriptor_flags_spatial_and_product_classes(schema_name: str) -> None:
    descriptors = _build_descriptors(schema_name)

    storey = descriptors["IFCBUILDINGSTOREY"]
    assert storey["spatial"] is True
    assert storey["product"] is True  # spatial structure elements are products

    site = descriptors["IFCSITE"]
    assert site["spatial"] is True

    wall = descriptors["IFCWALL"]
    assert wall["product"] is True
    assert wall["spatial"] is False


@pytest.mark.parametrize("schema_name", SCHEMAS)
def test_descriptor_records_guid_and_name_indices(schema_name: str) -> None:
    wall = _build_descriptors(schema_name)["IFCWALL"]
    # GlobalId is the first attribute on every rooted IFC entity; Name follows
    # OwnerHistory. The descriptor exposes their positions for the native parser.
    assert wall["guid"] == 0
    assert isinstance(wall["name"], int) and wall["name"] > 0
    assert wall["attrs"][wall["guid"]]["name"] == "GlobalId"
    assert wall["attrs"][wall["name"]]["name"] == "Name"


def test_schema_descriptors_json_is_cached_and_valid_json() -> None:
    first = schema_descriptors_json("IFC4")
    second = schema_descriptors_json("IFC4")
    assert first is second  # lru_cache returns the same object
    parsed = json.loads(first)
    assert "IFCWALL" in parsed
