from athar.bottom.types import SignatureVector
from athar.matcher.core import match_signatures


def _sig(
    step_id: int,
    *,
    guid: str | None = None,
    klass: str = "IfcWall",
    geom: str = "g",
    data: str = "d",
    topo: str = "t",
    placement: tuple[int, ...] | None = None,
    centroid: tuple[float, float, float] | None = None,
    name: str | None = None,
) -> SignatureVector:
    return SignatureVector(
        step_id=step_id,
        guid=guid,
        name=name,
        entity_type=klass,
        canonical_class=klass,
        vh_geometry=geom,
        vh_data=data,
        vh_topology=topo,
        placement=placement,
        centroid=centroid,
        aabb=None,
        canon_version="athar-canon-v1",
    )


def _by_pair(matches):
    return {(m.old_step, m.new_step): m for m in matches}


# --- tier 1: unique GlobalId identity ---


def test_unique_guid_same_vector_matches_at_full_confidence():
    old = {1: _sig(1, guid="A")}
    new = {10: _sig(10, guid="A")}

    matches, unmatched_old, unmatched_new, _diag = match_signatures(old, new)

    assert _by_pair(matches)[(1, 10)].score == 1.0
    assert _by_pair(matches)[(1, 10)].reason == "guid"
    assert unmatched_old == [] and unmatched_new == []


def test_unique_guid_name_change_is_metadata_not_vector_evidence():
    old = {1: _sig(1, guid="A", name="Old name")}
    new = {10: _sig(10, guid="A", name="New name")}

    matches, unmatched_old, unmatched_new, _diag = match_signatures(old, new)

    assert _by_pair(matches)[(1, 10)].score == 1.0
    assert unmatched_old == [] and unmatched_new == []


def test_unique_guid_changed_vector_matches_at_high_confidence():
    old = {1: _sig(1, guid="A", data="d-old")}
    new = {10: _sig(10, guid="A", data="d-new")}

    matches, _, _, _ = match_signatures(old, new)

    assert _by_pair(matches)[(1, 10)].score == 0.9
    assert _by_pair(matches)[(1, 10)].reason == "guid"


def test_duplicated_guid_is_not_identity_and_falls_through_to_vector_zip():
    old = {
        1: _sig(1, guid="DUP", data="dx"),
        2: _sig(2, guid="DUP", data="dy"),
    }
    new = {
        10: _sig(10, guid="DUP", data="dx"),
        20: _sig(20, guid="DUP", data="dy"),
    }

    matches, unmatched_old, unmatched_new, diag = match_signatures(old, new)

    pairs = _by_pair(matches)
    assert set(pairs) == {(1, 10), (2, 20)}
    assert all(m.reason == "geometry_hash" and m.score == 0.8 for m in matches)
    assert diag["matched_by_tier"]["guid"] == 0
    assert diag["duplicate_guids"] == {"old": 1, "new": 1}
    assert unmatched_old == [] and unmatched_new == []


def test_pathological_duplicated_guid_population_stays_linear():
    n = 500
    old = {i: _sig(i, guid="DUP", data=f"d{i}") for i in range(1, n + 1)}
    new = {i + 10000: _sig(i + 10000, guid="DUP", data=f"d{i}") for i in range(1, n + 1)}

    matches, unmatched_old, unmatched_new, diag = match_signatures(old, new)

    # one vector-zip pair per entity, never a 500x500 cross product
    assert len(matches) == n
    assert diag["matched_by_tier"]["geometry_hash"] == n
    assert unmatched_old == [] and unmatched_new == []


def test_guid_reused_across_classes_falls_through_without_poisoning_either_entity():
    old = {1: _sig(1, guid="A", klass="IfcWall")}
    new = {
        10: _sig(10, guid="A", klass="IfcDoor", geom="other", data="other", topo="other"),
        20: _sig(20, klass="IfcWall"),
    }

    matches, unmatched_old, unmatched_new, _ = match_signatures(old, new)

    # the wall still matches structurally; the cross-class GUID reuse is ignored
    assert _by_pair(matches)[(1, 20)].reason == "geometry_hash"
    assert unmatched_new == [10]


def test_no_guid_entities_never_match_through_the_guid_tier():
    old = {1: _sig(1, guid=None, geom="ga", data="da", topo="ta")}
    new = {10: _sig(10, guid=None, geom="gb", data="db", topo="tb")}

    matches, unmatched_old, unmatched_new, _ = match_signatures(old, new)

    assert matches == []
    assert unmatched_old == [1] and unmatched_new == [10]


# --- tier 2: full vector equality ---


def test_identical_twins_zip_one_to_one_in_step_order():
    n = 50
    shared = dict(geom="g", data="d", topo="t", centroid=(1.0, 2.0, 3.0))
    old = {i: _sig(i, **shared) for i in range(1, n + 1)}
    new = {i + 1000: _sig(i + 1000, **shared) for i in range(1, n + 1)}

    matches, _, _, diag = match_signatures(old, new)

    assert [(m.old_step, m.new_step) for m in matches] == [(i, i + 1000) for i in range(1, n + 1)]
    assert all(m.score == 0.8 and m.reason == "geometry_hash" for m in matches)
    assert diag["matched_by_tier"]["geometry_hash"] == n


def test_vector_zip_requires_all_hashes_present():
    # synthetic signatures with empty hashes must not be treated as equal vectors
    old = {1: _sig(1, geom="", data="", topo="")}
    new = {10: _sig(10, geom="", data="", topo="")}

    matches, unmatched_old, unmatched_new, _ = match_signatures(old, new)

    assert matches == []
    assert unmatched_old == [1] and unmatched_new == [10]


def test_identical_vectors_with_different_classes_never_match():
    # identical hashes, placement, and centroid — only the class differs.
    # No tier may pair them: class substitution must surface as added+deleted.
    shared = dict(
        geom="g", data="d", topo="t",
        placement=(1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1),
        centroid=(0.0, 0.0, 0.0),
    )
    old = {1: _sig(1, klass="IfcWall", **shared)}
    new = {10: _sig(10, klass="IfcBeam", **shared)}

    matches, unmatched_old, unmatched_new, _ = match_signatures(old, new)

    assert matches == []
    assert unmatched_old == [1] and unmatched_new == [10]


def test_vector_zip_leftovers_stay_unmatched():
    shared = dict(geom="g", data="d", topo="t", centroid=(0.0, 0.0, 0.0))
    old = {1: _sig(1, **shared), 2: _sig(2, **shared)}
    new = {10: _sig(10, **shared)}

    matches, unmatched_old, _, _ = match_signatures(old, new)

    assert set(_by_pair(matches)) == {(1, 10)}
    assert unmatched_old == [2]


# --- weaker evidence never matches (conservative added+deleted) ---
#
# The 2026-06 corpus survey showed topology-only and proximity-only matching
# never fired on a real revision pair and only paired entities across models;
# those tiers were removed. These scenarios must now stay unmatched.


def test_shared_topology_alone_never_matches():
    old = {1: _sig(1, geom="g-old", data="d-old", topo="t-shared", centroid=(0.0, 0.0, 0.0))}
    new = {10: _sig(10, geom="g-new", data="d-new", topo="t-shared", centroid=(0.2, 0.0, 0.0))}

    matches, unmatched_old, unmatched_new, _ = match_signatures(old, new)

    assert matches == []
    assert unmatched_old == [1] and unmatched_new == [10]


def test_shared_position_alone_never_matches():
    shared_centroid = (5.0, 5.0, 5.0)
    old = {1: _sig(1, geom="g1", data="d1", topo="t1", centroid=shared_centroid)}
    new = {10: _sig(10, geom="g2", data="d2", topo="t2", centroid=shared_centroid)}

    matches, unmatched_old, unmatched_new, _ = match_signatures(old, new)

    assert matches == []
    assert unmatched_old == [1] and unmatched_new == [10]


# --- whole-matcher invariants ---


def test_same_inputs_match_completely_with_zero_unmatched():
    old = {
        1: _sig(1, guid="A", centroid=(0.0, 0.0, 0.0)),
        2: _sig(2, guid="DUP", data="dx", centroid=(1.0, 0.0, 0.0)),
        3: _sig(3, guid="DUP", data="dy", centroid=(2.0, 0.0, 0.0)),
        4: _sig(4, geom="g4", data="d4", topo="t4", centroid=(3.0, 0.0, 0.0)),
    }
    new = {step + 100: _sig(step + 100, **kwargs) for step, kwargs in (
        (1, dict(guid="A", centroid=(0.0, 0.0, 0.0))),
        (2, dict(guid="DUP", data="dx", centroid=(1.0, 0.0, 0.0))),
        (3, dict(guid="DUP", data="dy", centroid=(2.0, 0.0, 0.0))),
        (4, dict(geom="g4", data="d4", topo="t4", centroid=(3.0, 0.0, 0.0))),
    )}

    matches, unmatched_old, unmatched_new, diag = match_signatures(old, new)

    assert len(matches) == 4
    assert unmatched_old == [] and unmatched_new == []
    assert diag["unmatched"] == {"old": 0, "new": 0}
    assert all(m.new_step == m.old_step + 100 for m in matches)


def test_match_results_are_deterministic_under_input_dict_order():
    def _old(order):
        sigs = {
            1: _sig(1, guid="A", centroid=(0.0, 0.0, 0.0)),
            2: _sig(2, geom="gx", data="dx", topo="tx", centroid=(1.0, 0.0, 0.0)),
            3: _sig(3, klass="IfcBeam", geom="gb", data="db", topo="tb", centroid=(5.0, 0.0, 0.0)),
        }
        return {k: sigs[k] for k in order}

    def _new(order):
        sigs = {
            10: _sig(10, guid="A", centroid=(0.0, 0.0, 0.0)),
            20: _sig(20, geom="gx", data="dx", topo="tx", centroid=(1.0, 0.0, 0.0)),
            30: _sig(30, klass="IfcBeam", geom="gB", data="dB", topo="tb", centroid=(5.1, 0.0, 0.0)),
        }
        return {k: sigs[k] for k in order}

    first = match_signatures(_old([1, 2, 3]), _new([10, 20, 30]))
    second = match_signatures(_old([3, 2, 1]), _new([30, 20, 10]))

    assert first[0] == second[0]
    assert first[1] == second[1]
    assert first[2] == second[2]
    # the changed beam carries no guid/vector evidence, so it stays unmatched
    assert first[1] == [3]
    assert first[2] == [30]


def test_diagnostics_account_for_every_entity():
    old = {
        1: _sig(1, guid="A"),
        2: _sig(2, geom="gx", data="dx", topo="tx"),
        3: _sig(3, geom="lone", data="lone", topo="", centroid=None),
    }
    new = {
        10: _sig(10, guid="A"),
        20: _sig(20, geom="gx", data="dx", topo="tx"),
    }

    matches, unmatched_old, unmatched_new, diag = match_signatures(old, new)

    assert sum(diag["matched_by_tier"].values()) == len(matches)
    assert diag["pools"] == {"old": 3, "new": 2}
    assert len(matches) + len(unmatched_old) == diag["pools"]["old"]
    assert len(matches) + len(unmatched_new) == diag["pools"]["new"]
