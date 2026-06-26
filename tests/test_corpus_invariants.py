"""Corpus-wide invariant tests over the small real-world IFC files.

Every assertion here is structural (accounting, class safety, conservative
matching, diagnostics visibility) — never a golden count tied to one export.
Large/slow corpus coverage lives in test_acceptance_large_ifc.py behind
ATHAR_RUN_LARGE_ACCEPTANCE=1.
"""

from __future__ import annotations

import re
import uuid

import ifcopenshell
import pytest

from athar.engine import diff_files
from tests.corpus import assert_report_invariants, assert_zero_diff, corpus_path

SMALL_FILES = (
    "tiny_no_products",
    "cube_brep",
    "sample_house_roof",
    "building_arch",
    "bl_v0",
    "bl_v1",
    "bl_v2",
    "bl_v3",
    "duplex_arch",
    "gni_190",
    "gni_9",
    "gni_50",
    "gni_62",
    "gni_149",
    "gni_18",
    "gni_p0_str",
    "gni_p3_str",
)

REVISION_PAIRS = (
    ("bl_v0", "bl_v1"),
    ("bl_v1", "bl_v2"),
    ("bl_v2", "bl_v3"),
    ("bl_v0", "bl_v3"),
)


@pytest.mark.parametrize("key", SMALL_FILES)
def test_same_file_diff_is_clean(key):
    path = corpus_path(key)
    report = diff_files(path, path)
    assert_report_invariants(report)
    assert_zero_diff(report)


@pytest.mark.parametrize("old_key,new_key", REVISION_PAIRS)
def test_revision_pair_invariants(old_key, new_key):
    report = diff_files(corpus_path(old_key), corpus_path(new_key))
    assert_report_invariants(report)
    # Successive exports of the same Archicad scene keep GlobalIds, so every
    # match must come from the guid tier — structural tiers stay silent.
    diag = report["stats"]["matcher_diagnostics"]
    assert diag["matched_by_tier"]["guid"] == sum(diag["matched_by_tier"].values())


def test_discipline_pair_is_mostly_disjoint():
    # Same sample project, different disciplines: content barely overlaps and
    # the conservative tiers must not manufacture broad cross-model matches.
    report = diff_files(corpus_path("building_arch"), corpus_path("bl_v0"))
    assert_report_invariants(report)
    stats = report["stats"]
    matched = stats["modified"] + stats["unchanged"]
    assert matched <= min(stats["old_signatures"], stats["new_signatures"]) // 2
    assert stats["deleted"] >= stats["old_signatures"] // 2
    assert stats["added"] >= stats["new_signatures"] // 2


def test_unrelated_pair_has_no_guid_matches():
    report = diff_files(corpus_path("cube_brep"), corpus_path("sample_house_roof"))
    assert_report_invariants(report)
    assert report["stats"]["matcher_diagnostics"]["matched_by_tier"]["guid"] == 0


@pytest.mark.parametrize(
    "old_key,new_key",
    (
        ("gni_190", "gni_9"),
        ("gni_50", "gni_62"),
    ),
)
def test_gni_independent_fundamentals_are_unrelated(old_key, new_key):
    report = diff_files(corpus_path(old_key), corpus_path(new_key))
    assert_report_invariants(report)
    stats = report["stats"]
    assert stats["matcher_diagnostics"]["matched_by_tier"]["guid"] == 0
    assert stats["deleted"] >= stats["old_signatures"] // 2
    assert stats["added"] >= stats["new_signatures"] // 2


@pytest.mark.parametrize(
    "old_key,new_key",
    (
        ("gni_p0_arc", "gni_p0_str"),
    ),
)
def test_gni_arch_structure_pairs_are_mostly_disjoint(old_key, new_key):
    report = diff_files(corpus_path(old_key), corpus_path(new_key))
    assert_report_invariants(report)
    stats = report["stats"]
    assert stats["deleted"] >= stats["old_signatures"] // 2
    assert stats["added"] >= stats["new_signatures"] // 2


def test_gni_arch_structure_pair_with_shared_guids_still_has_visible_discipline_churn():
    # Pair 3 has many shared GlobalIds across arch/structure files, so it is
    # not a "mostly disjoint" case. Keep it in the default tier as a different
    # real-world shape: same schema, class-safe matches, and non-vacuous
    # discipline-specific additions/deletions.
    report = diff_files(corpus_path("gni_p3_arc"), corpus_path("gni_p3_str"))
    assert_report_invariants(report)
    stats = report["stats"]
    assert stats["added"] > 0
    assert stats["deleted"] > 0
    assert stats["matcher_diagnostics"]["matched_by_tier"]["geometry_hash"] == 0


def test_cross_schema_pair_is_rejected_end_to_end():
    with pytest.raises(ValueError, match="Schema mismatch"):
        diff_files(corpus_path("bl_v1"), corpus_path("duplex_arch"))


@pytest.fixture(scope="module")
def guid_scrambled_pair(tmp_path_factory):
    """(clean, scrambled) copies of bl_v2 that differ only in GlobalId values.

    Both files go through the same ifcopenshell write path so the GUID rewrite
    is the only delta between them.
    """
    import ifcopenshell.guid

    namespace = uuid.UUID("f43a7c2b-8fd4-4d2d-9898-8891d75432b2")
    base = tmp_path_factory.mktemp("guid-scramble")
    clean = str(base / "clean.ifc")
    scrambled = str(base / "scrambled.ifc")
    model = ifcopenshell.open(corpus_path("bl_v2"))
    model.write(clean)
    for entity in model:
        if not hasattr(entity, "GlobalId"):
            continue
        guid = entity.GlobalId
        if isinstance(guid, str) and guid.strip():
            entity.GlobalId = ifcopenshell.guid.compress(uuid.uuid5(namespace, guid).hex)
    model.write(scrambled)
    return clean, scrambled


def test_guid_scramble_alone_yields_zero_diff(guid_scrambled_pair):
    # Metamorphic invariant: rewriting every GlobalId must not invent any
    # added/deleted/modified entities; identity is recovered through the
    # structural tiers without GlobalId evidence.
    clean, scrambled = guid_scrambled_pair
    report = diff_files(clean, scrambled)
    assert_report_invariants(report)
    stats = report["stats"]
    assert stats["added"] == 0
    assert stats["deleted"] == 0
    assert stats["modified"] == 0
    assert stats["unchanged"] == stats["new_signatures"] > 0
    assert stats["matcher_diagnostics"]["matched_by_tier"]["guid"] == 0


@pytest.fixture(scope="module")
def duplicated_guid_pair(tmp_path_factory):
    """(clean, duplicated) copies of bl_v2 where two products share one GlobalId.

    Both copies go through the same ifcopenshell write path so the GUID
    duplication is the only delta between them.
    """
    base = tmp_path_factory.mktemp("dup-guid")
    clean = str(base / "clean.ifc")
    duplicated = str(base / "duplicated.ifc")
    model = ifcopenshell.open(corpus_path("bl_v2"))
    model.write(clean)
    products = model.by_type("IfcProduct")
    victims = [p for p in products if p.GlobalId][:2]
    assert len(victims) == 2
    shared = victims[0].GlobalId
    victims[1].GlobalId = shared
    model.write(duplicated)
    return clean, duplicated


def test_duplicated_guid_is_surfaced_and_never_identity_evidence(duplicated_guid_pair):
    clean, duplicated = duplicated_guid_pair
    report = diff_files(clean, duplicated)
    assert_report_invariants(report)
    stats = report["stats"]
    # GlobalId never enters any hash, so duplicating one must not invent a
    # single added/deleted/modified entity ...
    assert stats["added"] == 0
    assert stats["deleted"] == 0
    assert stats["modified"] == 0
    # ... the collision is surfaced in stats ...
    assert stats["guid_collisions"] == {"old": 0, "new": 1}
    diag = stats["matcher_diagnostics"]
    assert diag["duplicate_guids"] == {"old": 0, "new": 1}
    # ... and the colliding entities are recovered by structural tiers, never
    # by the guid tier.
    assert diag["matched_by_tier"]["geometry_hash"] >= 2


def test_duplicated_guid_same_file_diff_is_clean(duplicated_guid_pair):
    _, duplicated = duplicated_guid_pair
    report = diff_files(duplicated, duplicated)
    assert_report_invariants(report)
    assert_zero_diff(report)
    assert report["stats"]["guid_collisions"] == {"old": 1, "new": 1}


@pytest.fixture(scope="module")
def dangling_ref_variant(tmp_path_factory):
    """Copy of bl_v2 with one relationship target mangled to a nonexistent id.

    Mirrors the corrupted real-world acceptance variant
    (181MB spanish "-bad"): one `#N` ref rewritten to `#29999...9`.
    ifcopenshell repairs the reference to an empty aggregate at load time.
    """
    text = open(corpus_path("bl_v2"), encoding="utf-8").read()
    match = re.search(r"(#\d+=\s*IFCRELAGGREGATES\([^;]*\(#)(\d+)(\)\);)", text)
    assert match is not None
    mangled = text[: match.start(2)] + "2" + "9" * 22 + text[match.end(2):]
    path = tmp_path_factory.mktemp("dangling") / "dangling.ifc"
    path.write_text(mangled, encoding="utf-8")
    return str(path)


def test_dangling_ref_corruption_diffs_cleanly_instead_of_crashing(dangling_ref_variant):
    report = diff_files(corpus_path("bl_v2"), dangling_ref_variant)
    assert_report_invariants(report)
    stats = report["stats"]
    # The corruption empties one aggregation; entities must stay matched and
    # the difference must surface as transitive topology change only.
    assert stats["added"] == 0
    assert stats["deleted"] == 0
    assert stats["modified_change_scope"]["intrinsic"] == 0
    assert stats["modified_change_scope"]["mixed"] == 0
