from athar.bottom.types import SignatureVector
from athar.matcher.candidates import generate_candidates
from athar.matcher.scoring import score_candidates


def _sig(
    step_id: int,
    *,
    guid: str | None = None,
    klass: str = "IfcWall",
    geom: str = "",
    data: str = "",
    topo: str = "",
    centroid: tuple[float, float, float] | None = None,
) -> SignatureVector:
    return SignatureVector(
        step_id=step_id,
        guid=guid,
        entity_type=klass,
        canonical_class=klass,
        vh_geometry=geom,
        vh_data=data,
        vh_topology=topo,
        placement=None,
        centroid=centroid,
        aabb=None,
        canon_version="athar-canon-v1",
    )


def test_generate_candidates_adds_tier2_signature_for_unique_class_topology_bucket():
    old_signatures = {
        1: _sig(1, klass="IfcWall", geom="g-old", data="d-old", topo="t-shared", centroid=(0.0, 0.0, 0.0)),
    }
    new_signatures = {
        10: _sig(10, klass="IfcWall", geom="g-new", data="d-new", topo="t-shared", centroid=(0.1, 0.0, 0.0)),
    }

    candidates = generate_candidates(old_signatures, new_signatures, radius_m=0.5)
    assert any(
        candidate.old_step == 1 and candidate.new_step == 10 and candidate.reason == "tier2_signature"
        for candidate in candidates
    )


def test_generate_candidates_skips_tier2_when_bucket_is_ambiguous():
    old_signatures = {
        1: _sig(1, klass="IfcWall", topo="t-shared", centroid=(0.0, 0.0, 0.0)),
        2: _sig(2, klass="IfcWall", topo="t-shared", centroid=(10.0, 0.0, 0.0)),
    }
    new_signatures = {
        10: _sig(10, klass="IfcWall", topo="t-shared", centroid=(0.1, 0.0, 0.0)),
    }

    candidates = generate_candidates(old_signatures, new_signatures, radius_m=0.5)
    assert all(candidate.reason != "tier2_signature" for candidate in candidates)


def test_score_candidates_gives_tier2_confidence_band():
    old_signatures = {
        1: _sig(1, klass="IfcWall", geom="g-old", data="d-old", topo="t-shared", centroid=(0.0, 0.0, 0.0)),
    }
    new_signatures = {
        10: _sig(10, klass="IfcWall", geom="g-new", data="d-new", topo="t-shared", centroid=(0.2, 0.0, 0.0)),
    }
    candidates = generate_candidates(old_signatures, new_signatures, radius_m=0.5)

    scored = score_candidates(candidates, old_signatures, new_signatures, radius_m=0.5)
    item = next(score for score in scored if score.old_step == 1 and score.new_step == 10)
    assert item.score == 0.7


def test_generate_candidates_prefers_tier2_over_spatial_for_same_pair():
    old_signatures = {
        1: _sig(1, klass="IfcWall", geom="old", data="old", topo="t-shared", centroid=(0.0, 0.0, 0.0)),
    }
    new_signatures = {
        10: _sig(10, klass="IfcWall", geom="new", data="new", topo="t-shared", centroid=(0.1, 0.0, 0.0)),
    }

    candidates = generate_candidates(old_signatures, new_signatures, radius_m=0.5)
    pair = next(candidate for candidate in candidates if candidate.old_step == 1 and candidate.new_step == 10)
    assert pair.reason == "tier2_signature"


def test_score_candidates_caps_confidence_when_guid_is_duplicated():
    old_signatures = {
        1: _sig(1, guid="DUP", klass="IfcWall", geom="g", data="d", topo="t", centroid=(0.0, 0.0, 0.0)),
        2: _sig(2, guid="DUP", klass="IfcWall", geom="g", data="d", topo="t", centroid=(1.0, 0.0, 0.0)),
    }
    new_signatures = {
        10: _sig(10, guid="DUP", klass="IfcWall", geom="g", data="d", topo="t", centroid=(0.0, 0.0, 0.0)),
    }

    candidates = generate_candidates(old_signatures, new_signatures, radius_m=0.5)
    scored = score_candidates(candidates, old_signatures, new_signatures, radius_m=0.5)
    assert scored
    assert all(item.score <= 0.5 for item in scored)
