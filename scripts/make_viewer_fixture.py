"""Generate the tiny IFC pair + report fixture used by the viewer tests.

Outputs into viewer/e2e/fixtures/: old.ifc, new.ifc, report.json,
manifest.json. The pair is constructed so every visual bucket the viewer
distinguishes is populated:

- WallA untouched                      -> unchanged
- WallB moved +1.0 m in X              -> modified, placement-only (line)
- WallC removed in new                 -> deleted
- WallD FireRating REI60 -> REI90      -> modified, data-only (static blue)
- WallE present only in new            -> added

GUIDs are content-stable so regeneration only changes bytes if the model
definition changes. Run from anywhere:

    .venv/bin/python scripts/make_viewer_fixture.py
"""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

import ifcopenshell
from ifcopenshell import guid as ifc_guid

from athar.engine import diff_files

FIXTURE_DIR = REPO_ROOT / "viewer" / "e2e" / "fixtures"

WALL_LENGTH = 4.0
WALL_THICKNESS = 0.3
WALL_HEIGHT = 3.0


def stable_guid(label: str) -> str:
    return ifc_guid.compress(hashlib.md5(f"athar-viewer-fixture:{label}".encode()).hexdigest())


def build_model(path: Path, walls: dict[str, tuple[float, float, str]]) -> None:
    """walls: name -> (x, y, fire_rating)."""
    f = ifcopenshell.file(schema="IFC4")

    units = f.create_entity(
        "IfcUnitAssignment",
        Units=(
            f.create_entity("IfcSIUnit", UnitType="LENGTHUNIT", Name="METRE"),
            f.create_entity("IfcSIUnit", UnitType="AREAUNIT", Name="SQUARE_METRE"),
            f.create_entity("IfcSIUnit", UnitType="VOLUMEUNIT", Name="CUBIC_METRE"),
            f.create_entity("IfcSIUnit", UnitType="PLANEANGLEUNIT", Name="RADIAN"),
        ),
    )

    def axis3d(x: float = 0.0, y: float = 0.0, z: float = 0.0):
        return f.create_entity(
            "IfcAxis2Placement3D",
            Location=f.create_entity("IfcCartesianPoint", Coordinates=(x, y, z)),
        )

    context = f.create_entity(
        "IfcGeometricRepresentationContext",
        ContextType="Model",
        CoordinateSpaceDimension=3,
        Precision=1e-5,
        WorldCoordinateSystem=axis3d(),
    )

    project = f.create_entity(
        "IfcProject",
        GlobalId=stable_guid("project"),
        Name="Athar Viewer Fixture",
        RepresentationContexts=(context,),
        UnitsInContext=units,
    )

    site_placement = f.create_entity("IfcLocalPlacement", RelativePlacement=axis3d())
    site = f.create_entity(
        "IfcSite",
        GlobalId=stable_guid("site"),
        Name="Site",
        ObjectPlacement=site_placement,
        CompositionType="ELEMENT",
    )
    building_placement = f.create_entity(
        "IfcLocalPlacement", PlacementRelTo=site_placement, RelativePlacement=axis3d()
    )
    building = f.create_entity(
        "IfcBuilding",
        GlobalId=stable_guid("building"),
        Name="Building",
        ObjectPlacement=building_placement,
        CompositionType="ELEMENT",
    )
    storey_placement = f.create_entity(
        "IfcLocalPlacement", PlacementRelTo=building_placement, RelativePlacement=axis3d()
    )
    storey = f.create_entity(
        "IfcBuildingStorey",
        GlobalId=stable_guid("storey"),
        Name="Storey",
        ObjectPlacement=storey_placement,
        CompositionType="ELEMENT",
        Elevation=0.0,
    )

    def aggregate(label: str, parent, child) -> None:
        f.create_entity(
            "IfcRelAggregates",
            GlobalId=stable_guid(f"agg:{label}"),
            RelatingObject=parent,
            RelatedObjects=(child,),
        )

    aggregate("project-site", project, site)
    aggregate("site-building", site, building)
    aggregate("building-storey", building, storey)

    wall_entities = []
    for name in sorted(walls):
        x, y, fire_rating = walls[name]
        placement = f.create_entity(
            "IfcLocalPlacement",
            PlacementRelTo=storey_placement,
            RelativePlacement=axis3d(x, y, 0.0),
        )
        profile = f.create_entity(
            "IfcRectangleProfileDef",
            ProfileType="AREA",
            XDim=WALL_LENGTH,
            YDim=WALL_THICKNESS,
            Position=f.create_entity(
                "IfcAxis2Placement2D",
                Location=f.create_entity("IfcCartesianPoint", Coordinates=(0.0, 0.0)),
            ),
        )
        solid = f.create_entity(
            "IfcExtrudedAreaSolid",
            SweptArea=profile,
            Position=axis3d(),
            ExtrudedDirection=f.create_entity("IfcDirection", DirectionRatios=(0.0, 0.0, 1.0)),
            Depth=WALL_HEIGHT,
        )
        shape = f.create_entity(
            "IfcShapeRepresentation",
            ContextOfItems=context,
            RepresentationIdentifier="Body",
            RepresentationType="SweptSolid",
            Items=(solid,),
        )
        wall = f.create_entity(
            "IfcWall",
            GlobalId=stable_guid(f"wall:{name}"),
            Name=name,
            ObjectPlacement=placement,
            Representation=f.create_entity("IfcProductDefinitionShape", Representations=(shape,)),
        )
        prop = f.create_entity(
            "IfcPropertySingleValue",
            Name="FireRating",
            NominalValue=f.create_entity("IfcLabel", fire_rating),
        )
        pset = f.create_entity(
            "IfcPropertySet",
            GlobalId=stable_guid(f"pset:{name}"),
            Name="Pset_WallCommon",
            HasProperties=(prop,),
        )
        f.create_entity(
            "IfcRelDefinesByProperties",
            GlobalId=stable_guid(f"reldef:{name}"),
            RelatedObjects=(wall,),
            RelatingPropertyDefinition=pset,
        )
        wall_entities.append(wall)

    f.create_entity(
        "IfcRelContainedInSpatialStructure",
        GlobalId=stable_guid("containment"),
        RelatedElements=tuple(wall_entities),
        RelatingStructure=storey,
    )

    f.write(str(path))


def main() -> None:
    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)
    old_path = FIXTURE_DIR / "old.ifc"
    new_path = FIXTURE_DIR / "new.ifc"

    build_model(
        old_path,
        {
            "WallA": (0.0, 0.0, "REI60"),
            "WallB": (0.0, 4.0, "REI60"),
            "WallC": (5.0, 0.0, "REI60"),
            "WallD": (5.0, 4.0, "REI60"),
        },
    )
    build_model(
        new_path,
        {
            "WallA": (0.0, 0.0, "REI60"),
            "WallB": (1.0, 4.0, "REI60"),
            "WallD": (5.0, 4.0, "REI90"),
            "WallE": (2.5, 8.0, "REI60"),
        },
    )

    report = diff_files(str(old_path), str(new_path))
    (FIXTURE_DIR / "report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")

    manifest = {
        "athar_viewer_manifest": 1,
        "schema_version": 1,
        "generator": "scripts/make_viewer_fixture.py",
        "old": {"name": "old.ifc", "url": "/old.ifc"},
        "new": {"name": "new.ifc", "url": "/new.ifc"},
        "report": {"url": "/report.json"},
    }
    (FIXTURE_DIR / "manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
    )

    stats = report["stats"]
    print(f"fixtures written to {FIXTURE_DIR}")
    print(
        f"report: +{stats['added']} -{stats['deleted']} ~{stats['modified']} ={stats['unchanged']}"
    )


if __name__ == "__main__":
    main()
