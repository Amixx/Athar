import ifcopenshell
import pytest

from athar.graph_parser import parse_graph

BASIC_HOUSE = "data/BasicHouse.ifc"


def test_graph_parser_emits_metadata_and_entities():
    graph = parse_graph(BASIC_HOUSE)
    assert graph["metadata"]["schema"]
    assert graph["entities"]
    diagnostics = graph["metadata"]["diagnostics"]
    assert isinstance(diagnostics["dangling_refs"], int)
    assert isinstance(diagnostics["dangling_refs_sample"], list)


def test_select_wrapper_and_refs_are_canonicalized():
    model = ifcopenshell.open(BASIC_HOUSE)
    graph = parse_graph(BASIC_HOUSE)

    prop = model.by_type("IfcPropertySingleValue")[0]
    prop_rec = graph["entities"][prop.id()]
    nominal = prop_rec["attributes"]["NominalValue"]
    assert nominal["kind"] == "select"
    assert nominal["type"] == prop.NominalValue.is_a()
    assert nominal["value"]["kind"] == "simple"

    wall = model.by_type("IfcWallStandardCase")[0]
    wall_rec = graph["entities"][wall.id()]
    assert wall_rec["attributes"]["ObjectPlacement"]["kind"] == "ref"
    assert any(r["path"] == "/ObjectPlacement" for r in wall_rec["refs"])


@pytest.mark.parametrize("schema", ["IFC2X3", "IFC4", "IFC4X3"])
def test_graph_parser_supports_core_ifc_schemas(tmp_path, schema):
    filepath = tmp_path / f"minimal_{schema}.ifc"
    ifcopenshell.file(schema=schema).write(str(filepath))
    graph = parse_graph(str(filepath))
    assert graph["metadata"]["schema"] == schema
    assert graph["entities"] == {}
