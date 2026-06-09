from athar.bottom.types import ParseDiagnostics, SignatureBundle, SignatureVector
from athar.delta.report import build_delta_report
from athar.matcher.types import MatchedPair


def _signature(
    step_id: int,
    *,
    guid: str | None = None,
    entity_type: str = "IfcWall",
    canonical_class: str = "IfcWall",
    vh_geometry: str = "geom-a",
    vh_data: str = "data-a",
    vh_topology: str = "topo-a",
    placement: tuple[int, ...] | None = None,
) -> SignatureVector:
    return SignatureVector(
        step_id=step_id,
        guid=guid,
        entity_type=entity_type,
        canonical_class=canonical_class,
        vh_geometry=vh_geometry,
        vh_data=vh_data,
        vh_topology=vh_topology,
        placement=placement,
        centroid=None,
        aabb=None,
        canon_version="athar-canon-v1",
    )


def _bundle(*, schema: str, signatures: dict[int, SignatureVector], warnings: list[str] | None = None) -> SignatureBundle:
    return SignatureBundle(
        filepath=f"/tmp/{schema}.ifc",
        schema=schema,
        canon_version="athar-canon-v1",
        signatures=signatures,
        diagnostics=ParseDiagnostics(
            dangling_refs=1,
            cycle_breaks=2,
            warnings=list(warnings or []),
        ),
        edge_stats={"semantic": 3},
    )


def test_build_delta_report_splits_modified_vs_unchanged_and_computes_placement_delta_mm():
    old_bundle = _bundle(
        schema="IFC4",
        signatures={
            1: _signature(
                1,
                guid="A",
                placement=(1_000_000, 0, 0, 0, 0, 1_000_000, 0, 0, 0, 0, 1_000_000, 0),
            ),
            2: _signature(
                2,
                guid="B",
                placement=(1_000_000, 0, 0, 100_000, 0, 1_000_000, 0, 200_000, 0, 0, 1_000_000, 300_000),
            ),
        },
    )
    new_bundle = _bundle(
        schema="IFC4",
        signatures={
            10: _signature(
                10,
                guid="A",
                vh_data="data-b",
                placement=(1_000_000, 0, 0, 0, 0, 1_000_000, 0, 0, 0, 0, 1_000_000, 0),
            ),
            20: _signature(
                20,
                guid="B",
                placement=(1_000_000, 0, 0, 103_500, 0, 1_000_000, 0, 197_500, 0, 0, 1_000_000, 301_000),
            ),
        },
    )
    matches = [
        MatchedPair(old_step=1, new_step=10, score=0.99, reason="guid"),
        MatchedPair(old_step=2, new_step=20, score=0.98, reason="guid"),
    ]

    report = build_delta_report(old_bundle, new_bundle, matches, unmatched_old=[], unmatched_new=[])

    assert report["stats"]["modified"] == 2
    assert report["stats"]["unchanged"] == 0
    by_guid = {item["new"]["guid"]: item for item in report["modified"]}
    assert by_guid["A"]["aspects"]["data"] == "changed"
    assert by_guid["A"]["data_hash"] == {"old": "data-a", "new": "data-b"}
    assert by_guid["A"]["change_scope"] == "intrinsic"
    assert by_guid["B"]["aspects"]["placement"] == "changed"
    assert by_guid["B"]["data_hash"] == {"old": "data-a", "new": "data-a"}
    assert by_guid["B"]["change_scope"] == "intrinsic"
    assert by_guid["B"]["aspects"]["placement_delta_mm"] == (3.5, -2.5, 1.0)
    assert report["stats"]["modified_change_scope"] == {
        "intrinsic": 2,
        "transitive": 0,
        "mixed": 0,
    }
    assert report["stats"]["modified_match_reasons"] == {"guid": 2}
    assert report["stats"]["modified_score_bands"] == {
        "high": 2,
        "medium": 0,
        "low": 0,
    }


def test_build_delta_report_sorts_added_deleted_and_includes_diagnostics_summary():
    old_bundle = _bundle(
        schema="IFC2X3",
        signatures={
            5: _signature(5, guid="OLD_A", canonical_class="IfcWall"),
            6: _signature(6, guid="OLD_B", canonical_class="IfcBeam"),
        },
        warnings=["old-warning"],
    )
    new_bundle = _bundle(
        schema="IFC2X3",
        signatures={
            15: _signature(15, guid="NEW_A", canonical_class="IfcColumn"),
            16: _signature(16, guid="NEW_B", canonical_class="IfcDoor"),
        },
        warnings=["new-warning"],
    )

    report = build_delta_report(
        old_bundle,
        new_bundle,
        matches=[],
        unmatched_old=[5, 6],
        unmatched_new=[16, 15],
    )

    assert [item["class"] for item in report["added"]] == ["IfcColumn", "IfcDoor"]
    assert [item["class"] for item in report["deleted"]] == ["IfcBeam", "IfcWall"]
    assert report["stats"]["old_diagnostics"] == {
        "dangling_refs": 1,
        "cycle_breaks": 2,
        "warnings": ["old-warning"],
    }
    assert report["stats"]["new_diagnostics"] == {
        "dangling_refs": 1,
        "cycle_breaks": 2,
        "warnings": ["new-warning"],
    }


def test_build_delta_report_ignores_matches_with_missing_signature_steps():
    old_bundle = _bundle(schema="IFC4", signatures={1: _signature(1, guid="A")})
    new_bundle = _bundle(schema="IFC4", signatures={10: _signature(10, guid="A")})

    # old step 999 is missing from old signatures and should be ignored.
    report = build_delta_report(
        old_bundle,
        new_bundle,
        matches=[MatchedPair(old_step=999, new_step=10, score=0.5, reason="fallback")],
        unmatched_old=[1],
        unmatched_new=[10],
    )

    assert report["modified"] == []
    assert report["unchanged"] == []
    assert len(report["added"]) == 1
    assert len(report["deleted"]) == 1


def test_build_delta_report_sets_placement_delta_none_when_matrices_are_missing_or_short():
    old_bundle = _bundle(
        schema="IFC4",
        signatures={
            1: _signature(1, guid="A", placement=None),
            2: _signature(2, guid="B", placement=(1, 2, 3)),
        },
    )
    new_bundle = _bundle(
        schema="IFC4",
        signatures={
            10: _signature(10, guid="A", placement=None),
            20: _signature(20, guid="B", placement=(1, 2, 3)),
        },
    )
    report = build_delta_report(
        old_bundle,
        new_bundle,
        matches=[
            MatchedPair(old_step=1, new_step=10, score=1.0, reason="guid"),
            MatchedPair(old_step=2, new_step=20, score=1.0, reason="guid"),
        ],
        unmatched_old=[],
        unmatched_new=[],
    )

    # both pairs are unchanged; placement_delta_mm is unavailable in both cases.
    assert len(report["unchanged"]) == 2
    assert all(item["aspects"]["placement_delta_mm"] is None for item in report["unchanged"])


def test_build_delta_report_change_scope_classifies_transitive_and_mixed():
    old_bundle = _bundle(
        schema="IFC4",
        signatures={
            1: _signature(1, guid="A", vh_geometry="g", vh_data="d", vh_topology="t-old"),
            2: _signature(2, guid="B", vh_geometry="g-old", vh_data="d-old", vh_topology="t-old"),
        },
    )
    new_bundle = _bundle(
        schema="IFC4",
        signatures={
            10: _signature(10, guid="A", vh_geometry="g", vh_data="d", vh_topology="t-new"),
            20: _signature(20, guid="B", vh_geometry="g-new", vh_data="d-old", vh_topology="t-new"),
        },
    )

    report = build_delta_report(
        old_bundle,
        new_bundle,
        matches=[
            MatchedPair(old_step=1, new_step=10, score=1.0, reason="guid"),
            MatchedPair(old_step=2, new_step=20, score=1.0, reason="guid"),
        ],
        unmatched_old=[],
        unmatched_new=[],
    )

    by_guid = {item["new"]["guid"]: item for item in report["modified"]}
    assert by_guid["A"]["change_scope"] == "transitive"
    assert by_guid["B"]["change_scope"] == "mixed"
    assert report["stats"]["modified_change_scope"] == {
        "intrinsic": 0,
        "transitive": 1,
        "mixed": 1,
    }


def test_build_delta_report_downgrades_low_confidence_fallback_for_transitive_changes():
    old_bundle = _bundle(
        schema="IFC4",
        signatures={1: _signature(1, guid="A", vh_geometry="g", vh_data="d", vh_topology="t-old")},
    )
    new_bundle = _bundle(
        schema="IFC4",
        signatures={10: _signature(10, guid="B", vh_geometry="g", vh_data="d", vh_topology="t-new")},
    )

    report = build_delta_report(
        old_bundle,
        new_bundle,
        matches=[MatchedPair(old_step=1, new_step=10, score=0.70, reason="tier2_signature")],
        unmatched_old=[],
        unmatched_new=[],
    )

    item = report["modified"][0]
    assert item["change_scope"] == "transitive"
    assert item["conflict"] == {
        "downgraded": True,
        "rule": "fallback_low_confidence_transitive_or_mixed",
    }
    assert report["stats"]["modified_conflicts"] == {"downgraded": 1, "normal": 0}
    assert report["stats"]["modified_match_reasons"] == {"tier2_signature": 1}
    assert report["stats"]["modified_score_bands"] == {
        "high": 0,
        "medium": 1,
        "low": 0,
    }


def test_build_delta_report_keeps_normal_conflict_for_high_confidence_or_intrinsic():
    old_bundle = _bundle(
        schema="IFC4",
        signatures={1: _signature(1, guid="A", vh_geometry="g-old", vh_data="d", vh_topology="t")},
    )
    new_bundle = _bundle(
        schema="IFC4",
        signatures={10: _signature(10, guid="B", vh_geometry="g-new", vh_data="d", vh_topology="t")},
    )

    report = build_delta_report(
        old_bundle,
        new_bundle,
        matches=[MatchedPair(old_step=1, new_step=10, score=0.70, reason="tier2_signature")],
        unmatched_old=[],
        unmatched_new=[],
    )

    item = report["modified"][0]
    assert item["change_scope"] == "intrinsic"
    assert item["conflict"] == {
        "downgraded": False,
        "rule": "none",
    }
    assert report["stats"]["modified_conflicts"] == {"downgraded": 0, "normal": 1}
    assert report["stats"]["modified_match_reasons"] == {"tier2_signature": 1}
    assert report["stats"]["modified_score_bands"] == {
        "high": 0,
        "medium": 1,
        "low": 0,
    }
