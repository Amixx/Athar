"""Opt-in acceptance coverage over the medium/large real-world IFC corpus.

Set ATHAR_RUN_LARGE_ACCEPTANCE=1 to run. Files that are absent or unfetched
LFS pointers skip individually, so partial corpora still give partial signal.
Optional per-test wall-clock bound: ATHAR_ACCEPTANCE_TIMEOUT_S (seconds).

Assertions are invariants and qualitative pair shapes (revision pairs stay
matched, disjoint pairs stay unmatched), never golden counts.
"""

from __future__ import annotations

import os
import signal
import uuid

import pytest

from athar.engine import diff_files
from tests.corpus import acceptance_path, assert_report_invariants, assert_zero_diff

_RUN_LARGE = os.getenv("ATHAR_RUN_LARGE_ACCEPTANCE", "0") == "1"
_TIMEOUT_S = int(os.getenv("ATHAR_ACCEPTANCE_TIMEOUT_S", "0"))

pytestmark = [
    pytest.mark.skipif(
        not _RUN_LARGE,
        reason="set ATHAR_RUN_LARGE_ACCEPTANCE=1 to run large IFC acceptance tests",
    ),
    pytest.mark.large_acceptance,
]

# Medium/large corpus files, ascending size. Small files run in the default
# tier (test_corpus_invariants.py).
LARGE_FILES = (
    "gni_77",
    "gni_68",
    "gni_p5_str",
    "gni_p7_arc",
    "gni_p8_arc",
    "duplex_mech",
    "revit_arc",
    "duplex_mep",
    "revit_mep",
    "gni_p5_arc",
    "advanced_project",
    "adv_changed",
    "basic_house",
    "uni_house",
    "real_world_big",
    "spanish",
)

DISCIPLINE_PAIRS = (
    ("duplex_arch", "duplex_mech"),
    ("revit_arc", "revit_mep"),
    ("gni_p5_arc", "gni_p5_str"),
    ("gni_p7_arc", "gni_p7_str"),
    ("gni_p8_arc", "gni_p8_str"),
)

UNRELATED_PAIRS = (
    ("basic_house", "advanced_project"),
    ("spanish", "uni_house"),
    ("gni_77", "gni_68"),
)


@pytest.fixture(autouse=True)
def _bounded_runtime():
    """Optional per-test wall-clock bound via ATHAR_ACCEPTANCE_TIMEOUT_S (Unix only)."""
    if _TIMEOUT_S <= 0 or not hasattr(signal, "SIGALRM"):
        yield
        return

    def _on_alarm(signum, frame):
        raise TimeoutError(f"acceptance test exceeded ATHAR_ACCEPTANCE_TIMEOUT_S={_TIMEOUT_S}s")

    previous = signal.signal(signal.SIGALRM, _on_alarm)
    signal.alarm(_TIMEOUT_S)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous)


@pytest.mark.parametrize("key", LARGE_FILES)
def test_same_file_diff_is_clean(key):
    path = acceptance_path(key)
    report = diff_files(path, path)
    assert_report_invariants(report)
    assert_zero_diff(report)
    # Non-vacuity floor: these models must yield substantial signature
    # populations, otherwise the parse silently lost the model.
    assert report["stats"]["unchanged"] > 100


def test_revision_pair_stays_matched():
    # Real authoring-tool revision of the same 44MB project: identity must be
    # carried by GlobalIds, and edits must surface as modifications rather
    # than added/deleted churn.
    report = diff_files(acceptance_path("advanced_project"), acceptance_path("adv_changed"))
    assert_report_invariants(report)
    stats = report["stats"]
    matched = stats["modified"] + stats["unchanged"]
    smaller_side = min(stats["old_signatures"], stats["new_signatures"])
    assert matched >= smaller_side // 2
    diag = stats["matcher_diagnostics"]
    assert diag["matched_by_tier"]["guid"] >= matched * 9 // 10


@pytest.mark.parametrize("old_key,new_key", DISCIPLINE_PAIRS)
def test_discipline_pair_is_mostly_disjoint(old_key, new_key):
    # Same project, different disciplines: the models describe different
    # elements, so conservative tiers must not manufacture broad matches.
    report = diff_files(acceptance_path(old_key), acceptance_path(new_key))
    assert_report_invariants(report)
    stats = report["stats"]
    assert stats["deleted"] >= stats["old_signatures"] // 2
    assert stats["added"] >= stats["new_signatures"] // 2


@pytest.mark.parametrize("old_key,new_key", UNRELATED_PAIRS)
def test_unrelated_pair_stays_unmatched(old_key, new_key):
    # Cross-project stress shape: unrelated models share no GlobalIds; the
    # guid tier must stay silent and most of both sides must stay unmatched.
    report = diff_files(acceptance_path(old_key), acceptance_path(new_key))
    assert_report_invariants(report)
    stats = report["stats"]
    assert stats["old_signatures"] > 100
    assert stats["new_signatures"] > 100
    assert stats["matcher_diagnostics"]["matched_by_tier"]["guid"] == 0
    assert stats["deleted"] >= stats["old_signatures"] // 2
    assert stats["added"] >= stats["new_signatures"] // 2


def test_guid_scramble_at_scale_recovers_identity(tmp_path_factory):
    # Metamorphic invariant at 44MB: rewriting every GlobalId must not invent
    # any added/deleted/modified entities; structural tiers recover identity.
    import ifcopenshell
    import ifcopenshell.guid

    src = acceptance_path("advanced_project")
    namespace = uuid.UUID("f43a7c2b-8fd4-4d2d-9898-8891d75432b2")
    base = tmp_path_factory.mktemp("guid-scramble-large")
    clean = str(base / "clean.ifc")
    scrambled = str(base / "scrambled.ifc")
    model = ifcopenshell.open(src)
    model.write(clean)
    for entity in model:
        if not hasattr(entity, "GlobalId"):
            continue
        guid = entity.GlobalId
        if isinstance(guid, str) and guid.strip():
            entity.GlobalId = ifcopenshell.guid.compress(uuid.uuid5(namespace, guid).hex)
    model.write(scrambled)

    report = diff_files(clean, scrambled)
    assert_report_invariants(report)
    stats = report["stats"]
    assert stats["added"] == 0
    assert stats["deleted"] == 0
    assert stats["modified"] == 0
    assert stats["unchanged"] == stats["new_signatures"] > 100
    assert stats["matcher_diagnostics"]["matched_by_tier"]["guid"] == 0
