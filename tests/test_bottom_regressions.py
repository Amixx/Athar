from __future__ import annotations

from athar.bottom.edge_policy import DOMAIN_GEOMETRY, DOMAIN_PLACEMENT, EDGE_INCLUDE
from athar.bottom.parser import _canonicalize_scalar, _is_spatial_entity
from athar.bottom.spatial import build_spatial_features
from athar.bottom.types import ClassifiedEdge, EntityRef, ParseDiagnostics, ParseResult, ParsedEntity


def test_spatial_features_use_world_space_geometry_points() -> None:
    parse_result = ParseResult(
        filepath="toy.ifc",
        schema="IFC2X3",
        index={},
        entities={
            1: ParsedEntity(
                step_id=1,
                entity_type="IfcWall",
                canonical_class="IfcWall",
                global_id="WALL-1",
                attributes={},
                refs=[
                    EntityRef(
                        source_step=1,
                        target_step=2,
                        source_type="IfcWall",
                        target_type="IfcLocalPlacement",
                        attr_name="ObjectPlacement",
                        path="/ObjectPlacement",
                    ),
                    EntityRef(
                        source_step=1,
                        target_step=5,
                        source_type="IfcWall",
                        target_type="IfcCartesianPoint",
                        attr_name="Representation",
                        path="/Representation",
                    ),
                ],
                is_product=True,
                is_spatial=False,
            ),
            2: ParsedEntity(
                step_id=2,
                entity_type="IfcLocalPlacement",
                canonical_class="IfcLocalPlacement",
                global_id=None,
                attributes={},
                refs=[
                    EntityRef(
                        source_step=2,
                        target_step=3,
                        source_type="IfcLocalPlacement",
                        target_type="IfcAxis2Placement3D",
                        attr_name="RelativePlacement",
                        path="/RelativePlacement",
                    )
                ],
                is_product=False,
                is_spatial=False,
            ),
            3: ParsedEntity(
                step_id=3,
                entity_type="IfcAxis2Placement3D",
                canonical_class="IfcAxis2Placement3D",
                global_id=None,
                attributes={},
                refs=[
                    EntityRef(
                        source_step=3,
                        target_step=4,
                        source_type="IfcAxis2Placement3D",
                        target_type="IfcCartesianPoint",
                        attr_name="Location",
                        path="/Location",
                    )
                ],
                is_product=False,
                is_spatial=False,
            ),
            4: ParsedEntity(
                step_id=4,
                entity_type="IfcCartesianPoint",
                canonical_class="IfcCartesianPoint",
                global_id=None,
                attributes={"Coordinates": _coords(10.0, 0.0, 0.0)},
                refs=[],
                is_product=False,
                is_spatial=False,
            ),
            5: ParsedEntity(
                step_id=5,
                entity_type="IfcCartesianPoint",
                canonical_class="IfcCartesianPoint",
                global_id=None,
                attributes={"Coordinates": _coords(1.0, 2.0, 3.0)},
                refs=[],
                is_product=False,
                is_spatial=False,
            ),
        },
        incoming_refs={},
        unit_context={"unit_factors": {"LENGTHUNIT": 1.0}},
        diagnostics=ParseDiagnostics(),
    )
    edges = [
        ClassifiedEdge(
            source_step=1,
            target_step=2,
            classification=EDGE_INCLUDE,
            domain=DOMAIN_PLACEMENT,
            label="IfcWall.ObjectPlacement",
        ),
        ClassifiedEdge(
            source_step=2,
            target_step=3,
            classification=EDGE_INCLUDE,
            domain=DOMAIN_PLACEMENT,
            label="IfcLocalPlacement.RelativePlacement",
        ),
        ClassifiedEdge(
            source_step=3,
            target_step=4,
            classification=EDGE_INCLUDE,
            domain=DOMAIN_PLACEMENT,
            label="IfcAxis2Placement3D.Location",
        ),
        ClassifiedEdge(
            source_step=1,
            target_step=5,
            classification=EDGE_INCLUDE,
            domain=DOMAIN_GEOMETRY,
            label="IfcWall.Representation",
        ),
    ]

    spatial = build_spatial_features(parse_result, edges)
    feature = spatial[1]

    assert feature.centroid == (11.0, 2.0, 3.0)
    assert feature.aabb == (11.0, 2.0, 3.0, 11.0, 2.0, 3.0)
    assert feature.placement is not None
    assert feature.placement[3] == 10_000_000


def test_string_scalar_canonicalization_preserves_numeric_literals() -> None:
    assert _canonicalize_scalar("0", measure_type=None, attr_name="Name", unit_context={}) == {
        "kind": "string",
        "value": "0",
    }
    assert _canonicalize_scalar("1", measure_type=None, attr_name="Name", unit_context={}) == {
        "kind": "string",
        "value": "1",
    }
    assert _canonicalize_scalar(".TRUE.", measure_type=None, attr_name="Name", unit_context={}) == {
        "kind": "bool",
        "value": True,
    }
    assert _canonicalize_scalar("TRUE", measure_type=None, attr_name="Name", unit_context={}) == {
        "kind": "string",
        "value": "TRUE",
    }


def test_spatial_entity_detection_accepts_ifc2x3_spatial_structure_elements() -> None:
    assert _is_spatial_entity(_FakeIfcEntity({"IfcSpatialElement"}))
    assert _is_spatial_entity(_FakeIfcEntity({"IfcSpatialStructureElement"}))
    assert _is_spatial_entity(
        _FakeIfcEntity({"IfcSpatialStructureElement"}, raise_on_unknown=True),
    )
    assert not _is_spatial_entity(_FakeIfcEntity(set()))


def _coords(x: float, y: float, z: float) -> dict[str, object]:
    return {
        "kind": "list",
        "items": [
            {"kind": "real_q", "value": int(round(x * 1_000_000))},
            {"kind": "real_q", "value": int(round(y * 1_000_000))},
            {"kind": "real_q", "value": int(round(z * 1_000_000))},
        ],
    }


class _FakeIfcEntity:
    def __init__(self, hits: set[str], *, raise_on_unknown: bool = False) -> None:
        self._hits = set(hits)
        self._raise_on_unknown = raise_on_unknown

    def is_a(self, type_name: str | None = None):  # noqa: ANN201 - mirrors ifcopenshell API.
        if type_name is None:
            return "FakeEntity"
        if type_name in self._hits:
            return True
        if self._raise_on_unknown:
            raise RuntimeError(f"unknown type query {type_name}")
        return False
