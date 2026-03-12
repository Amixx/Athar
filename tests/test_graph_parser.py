import ifcopenshell
import pytest

from athar.graph.graph_parser import parse_graph

BASIC_HOUSE = "data/BasicHouse.ifc"


def test_graph_parser_emits_metadata_and_entities():
    graph = parse_graph(BASIC_HOUSE)
    assert graph["metadata"]["schema"]
    assert graph["entities"]
    diagnostics = graph["metadata"]["diagnostics"]
    assert isinstance(diagnostics["dangling_refs"], int)
    assert isinstance(diagnostics["dangling_refs_sample"], list)
    assert isinstance(graph["metadata"]["units"]["unit_factors"], dict)


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


def test_graph_parser_preserves_non_length_measure_wrapper_types(tmp_path):
    f = ifcopenshell.file(schema="IFC4")
    wrappers = [
        ("Area", f.createIfcAreaMeasure(12.5), "IfcAreaMeasure"),
        ("Volume", f.createIfcVolumeMeasure(3.75), "IfcVolumeMeasure"),
        ("Angle", f.createIfcPlaneAngleMeasure(1.2), "IfcPlaneAngleMeasure"),
        ("Derived", f.createIfcThermalTransmittanceMeasure(0.42), "IfcThermalTransmittanceMeasure"),
    ]
    step_to_type: dict[int, str] = {}
    for name, wrapped_value, wrapped_type in wrappers:
        prop = f.createIfcPropertySingleValue(name, None, wrapped_value, None)
        step_to_type[prop.id()] = wrapped_type

    filepath = tmp_path / "measures.ifc"
    f.write(str(filepath))

    graph = parse_graph(str(filepath))
    for step_id, wrapped_type in step_to_type.items():
        rec = graph["entities"][step_id]
        nominal = rec["attributes"]["NominalValue"]
        assert nominal["kind"] == "select"
        assert nominal["type"] == wrapped_type
        assert nominal["value"]["kind"] == "simple"
        assert nominal["value"]["type"] == wrapped_type


def test_graph_parser_extracts_unit_assignment_factors():
    graph = parse_graph(BASIC_HOUSE, profile="semantic_stable")
    unit_factors = graph["metadata"]["units"]["unit_factors"]
    assert unit_factors["LENGTHUNIT"] == pytest.approx(0.001)
    assert unit_factors["PLANEANGLEUNIT"] == pytest.approx(0.0174532925199433)
    assert unit_factors["AREAUNIT"] == pytest.approx(1.0)
    assert unit_factors["VOLUMEUNIT"] == pytest.approx(1.0)


def test_graph_parser_normalizes_non_length_measures_by_type(tmp_path):
    f = ifcopenshell.file(schema="IFC4")

    area_dim = f.createIfcDimensionalExponents(2, 0, 0, 0, 0, 0, 0)
    vol_dim = f.createIfcDimensionalExponents(3, 0, 0, 0, 0, 0, 0)
    angle_dim = f.createIfcDimensionalExponents(0, 0, 0, 0, 0, 0, 0)

    si_area = f.createIfcSIUnit(None, "AREAUNIT", None, "SQUARE_METRE")
    si_volume = f.createIfcSIUnit(None, "VOLUMEUNIT", None, "CUBIC_METRE")
    si_angle = f.createIfcSIUnit(None, "PLANEANGLEUNIT", None, "RADIAN")
    si_power = f.createIfcSIUnit(None, "POWERUNIT", None, "WATT")
    si_temp = f.createIfcSIUnit(None, "THERMODYNAMICTEMPERATUREUNIT", None, "KELVIN")

    conv_area = f.createIfcConversionBasedUnit(
        area_dim,
        "AREAUNIT",
        "SQFT",
        f.createIfcMeasureWithUnit(f.createIfcReal(0.09290304), si_area),
    )
    conv_volume = f.createIfcConversionBasedUnit(
        vol_dim,
        "VOLUMEUNIT",
        "CUFT",
        f.createIfcMeasureWithUnit(f.createIfcReal(0.028316846592), si_volume),
    )
    conv_angle = f.createIfcConversionBasedUnit(
        angle_dim,
        "PLANEANGLEUNIT",
        "DEGREE",
        f.createIfcMeasureWithUnit(f.createIfcReal(0.0174532925199433), si_angle),
    )

    derived_tt = f.createIfcDerivedUnit(
        [
            f.createIfcDerivedUnitElement(si_power, 1),
            f.createIfcDerivedUnitElement(conv_area, -1),
            f.createIfcDerivedUnitElement(si_temp, -1),
        ],
        "THERMALTRANSMITTANCEUNIT",
        None,
    )
    f.createIfcUnitAssignment([conv_area, conv_volume, conv_angle, derived_tt])

    area_prop = f.createIfcPropertySingleValue("Area", None, f.createIfcAreaMeasure(10.0), None)
    volume_prop = f.createIfcPropertySingleValue("Volume", None, f.createIfcVolumeMeasure(10.0), None)
    angle_prop = f.createIfcPropertySingleValue("Angle", None, f.createIfcPlaneAngleMeasure(180.0), None)
    tt_prop = f.createIfcPropertySingleValue("Thermal", None, f.createIfcThermalTransmittanceMeasure(1.0), None)

    filepath = tmp_path / "non_length_units.ifc"
    f.write(str(filepath))

    graph = parse_graph(str(filepath), profile="semantic_stable")
    values_by_step = {}
    for prop in (area_prop, volume_prop, angle_prop, tt_prop):
        values_by_step[prop.id()] = graph["entities"][prop.id()]["attributes"]["NominalValue"]["value"]["value"]["value"]

    assert values_by_step[area_prop.id()] == "0.92902999999999991"
    assert values_by_step[volume_prop.id()] == "0.28316799999999998"
    assert values_by_step[angle_prop.id()] == "3.1415926500000002"
    assert values_by_step[tt_prop.id()] == "10.763910417"
