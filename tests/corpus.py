"""Shared IFC corpus registry and report invariants for engine tests.

The corpus spans three roots:

- repo LFS files under ``real-world-test/`` and ``data/`` (required: tests fail
  loudly on unfetched LFS pointers so a broken checkout is not silently green),
- tiny checked-in fixtures under ``tests/fixtures/``,
- an optional external corpus (default ``../vscode-ifc/test-files``, override
  via ``ATHAR_EXTERNAL_CORPUS_DIR``) whose files *skip* when absent so the
  suite stays runnable on machines without that checkout.

Invariant helpers assert the engine's structural contracts — accounting,
1:1 matching, class safety, diagnostics visibility — never golden counts.
"""

from __future__ import annotations

import math
import os
from dataclasses import dataclass
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
EXTERNAL_CORPUS_DIR = Path(
    os.getenv("ATHAR_EXTERNAL_CORPUS_DIR", str(REPO_ROOT.parent / "vscode-ifc" / "test-files"))
)

_KNOWN_TIER_SCORES = {
    "guid": {0.9, 1.0},
    "geometry_hash": {0.8},
}
_SECTIONS = ("added", "deleted", "modified", "unchanged")


@dataclass(frozen=True)
class CorpusFile:
    key: str
    path: Path
    schema: str
    required: bool  # required files fail on LFS pointers; optional ones skip


def _repo(key: str, rel: str, schema: str) -> CorpusFile:
    return CorpusFile(key, REPO_ROOT / rel, schema, required=True)


def _external(key: str, name: str, schema: str) -> CorpusFile:
    return CorpusFile(key, EXTERNAL_CORPUS_DIR / name, schema, required=False)


CORPUS: dict[str, CorpusFile] = {
    f.key: f
    for f in (
        # small (default tier)
        _repo("tiny_no_products", "tests/fixtures/tiny_no_products.ifc", "IFC4"),
        _external("cube_brep", "000.010611MB__Ifc4_CubeAdvancedBrep.ifc", "IFC4"),
        _external("sample_house_roof", "000.063019MB__Ifc4_SampleHouse_1_Roof.ifc", "IFC4"),
        _external("building_arch", "000.225635MB__Building-Architecture.ifc", "IFC4"),
        _repo("bl_v0", "real-world-test/Building-Landscaping-v0-original.ifc", "IFC4"),
        _repo("bl_v1", "real-world-test/Building-Landscaping-v1.ifc", "IFC4"),
        _repo("bl_v2", "real-world-test/Building-Landscaping-v2.ifc", "IFC4"),
        _repo("bl_v3", "real-world-test/Building-Landscaping-v3.ifc", "IFC4"),
        _repo("duplex_arch", "real-world-test/Duplex-Architecture.ifc", "IFC2X3"),
        _repo("gni_190", "corpus/gni-bim-sample/2025_BIMfundamentals/model_190.ifc", "IFC4"),
        _repo("gni_9", "corpus/gni-bim-sample/2025_BIMfundamentals/model_9.ifc", "IFC4"),
        _repo("gni_50", "corpus/gni-bim-sample/2025_BIMfundamentals/model_50.ifc", "IFC4"),
        _repo("gni_62", "corpus/gni-bim-sample/2025_BIMfundamentals/model_62.ifc", "IFC4"),
        _repo("gni_149", "corpus/gni-bim-sample/2025_BIMfundamentals/model_149.ifc", "IFC4"),
        _repo("gni_18", "corpus/gni-bim-sample/2025_BIMfundamentals/model_18.ifc", "IFC2X3"),
        _repo("gni_p0_arc", "corpus/gni-bim-sample/2026_BIMprojects/model_0_arc.ifc", "IFC2X3"),
        _repo("gni_p0_str", "corpus/gni-bim-sample/2026_BIMprojects/model_0_structure.ifc", "IFC2X3"),
        _repo("gni_p3_arc", "corpus/gni-bim-sample/2026_BIMprojects/model_3_arc.ifc", "IFC2X3"),
        _repo("gni_p3_str", "corpus/gni-bim-sample/2026_BIMprojects/model_3_structure.ifc", "IFC2X3"),
        # medium/large (opt-in acceptance tier)
        _repo("gni_77", "corpus/gni-bim-sample/2025_BIMfundamentals/model_77.ifc", "IFC4"),
        _repo("gni_68", "corpus/gni-bim-sample/2025_BIMfundamentals/model_68.ifc", "IFC4"),
        _repo("gni_p5_arc", "corpus/gni-bim-sample/2026_BIMprojects/model_5_arc.ifc", "IFC2X3"),
        _repo("gni_p5_str", "corpus/gni-bim-sample/2026_BIMprojects/model_5_structure.ifc", "IFC2X3"),
        _repo("gni_p7_arc", "corpus/gni-bim-sample/2026_BIMprojects/model_7_arc.ifc", "IFC2X3"),
        _repo("gni_p7_str", "corpus/gni-bim-sample/2026_BIMprojects/model_7_structure.ifc", "IFC2X3"),
        _repo("gni_p8_arc", "corpus/gni-bim-sample/2026_BIMprojects/model_8_arc.ifc", "IFC4"),
        _repo("gni_p8_str", "corpus/gni-bim-sample/2026_BIMprojects/model_8_structure.ifc", "IFC4"),
        _external("duplex_mech", "008.781887MB__Ifc2x3_Duplex_Mechanical.ifc", "IFC2X3"),
        _external("revit_arc", "013.646137MB__Ifc4_Revit_ARC.ifc", "IFC4"),
        _external("duplex_mep", "017.871432MB__Ifc2x3_Duplex_MEP.ifc", "IFC2X3"),
        _external("revit_mep", "029.165277MB__Ifc4_Revit_MEP.ifc", "IFC4"),
        _repo("advanced_project", "real-world-test/AdvancedProject.ifc", "IFC2X3"),
        _repo("adv_changed", "real-world-test/adv proj changed.ifc", "IFC2X3"),
        _repo("basic_house", "data/BasicHouse.ifc", "IFC2X3"),
        _repo("uni_house", "real-world-test/uni-project-house-50mb.ifc", "IFC2X3"),
        _external("real_world_big", "168.047573MB__real-world-big.ifc", "IFC2X3"),
        _repo("spanish", "real-world-test/real-world-spanish-180mb.ifc", "IFC2X3"),
    )
}


def _is_lfs_pointer(path: Path) -> bool:
    with path.open("rb") as fh:
        return fh.read(64).startswith(b"version https://git-lfs")


def corpus_path(key: str) -> str:
    """Resolve a corpus key to a usable file path, or skip/fail per its policy."""
    spec = CORPUS[key]
    if not spec.path.exists():
        if spec.required:
            pytest.fail(f"missing required corpus file: {spec.path}")
        pytest.skip(f"external corpus file not available: {spec.path}")
    if _is_lfs_pointer(spec.path):
        if spec.required:
            pytest.fail(f"{spec.path} is an unfetched git-lfs pointer; run `git lfs pull`")
        pytest.skip(f"external corpus file is an unfetched git-lfs pointer: {spec.path}")
    return str(spec.path)


def acceptance_path(key: str) -> str:
    """Like corpus_path, but always skip when unavailable.

    The opt-in acceptance tier runs on machines that may hold only part of the
    large corpus (LFS not pulled, external checkout absent); partial runs skip
    rather than fail.
    """
    spec = CORPUS[key]
    if not spec.path.exists():
        pytest.skip(f"acceptance corpus file not available: {spec.path}")
    if _is_lfs_pointer(spec.path):
        pytest.skip(f"acceptance corpus file is an unfetched git-lfs pointer: {spec.path}")
    return str(spec.path)


def assert_report_invariants(report: dict) -> None:
    """Structural contracts every successful diff report must satisfy."""
    stats = report["stats"]

    # Section/stat accounting: every signature lands in exactly one section.
    for section in _SECTIONS:
        assert stats[section] == len(report[section])
    assert stats["deleted"] + stats["modified"] + stats["unchanged"] == stats["old_signatures"]
    assert stats["added"] + stats["modified"] + stats["unchanged"] == stats["new_signatures"]
    assert stats["dropped_matches"] == 0

    # Same-schema policy: a successful report never mixes schemas.
    assert report["schemas"]["old"] == report["schemas"]["new"]

    # Matcher diagnostics must be visible and account for every entity.
    diag = stats["matcher_diagnostics"]
    assert diag["pools"] == {"old": stats["old_signatures"], "new": stats["new_signatures"]}
    assert diag["unmatched"] == {"old": stats["deleted"], "new": stats["added"]}
    assert sum(diag["matched_by_tier"].values()) == stats["modified"] + stats["unchanged"]
    assert set(diag["matched_by_tier"]) == set(_KNOWN_TIER_SCORES)
    assert set(diag["duplicate_guids"]) == {"old", "new"}
    # Bundle-level GUID collisions and matcher-pool duplicate GUIDs count the
    # same population, so they must agree.
    assert stats["guid_collisions"] == diag["duplicate_guids"]

    # Matched-pair contracts.
    for section in ("modified", "unchanged"):
        for item in report[section]:
            # No tier may ever pair entities across canonical classes.
            assert item["old"]["class"] == item["new"]["class"], item
            reason = item["match"]["reason"]
            assert item["match"]["score"] in _KNOWN_TIER_SCORES[reason], item
            if reason == "guid":
                assert item["old"]["guid"], item
                assert item["old"]["guid"] == item["new"]["guid"], item
            if section == "modified":
                # geometry_hash matches equal full vectors, so any modified
                # item can only come from the guid tier with a changed vector.
                assert reason == "guid" and item["match"]["score"] == 0.9, item
            aspects = item["aspects"]
            changed = any(v == "changed" for k, v in aspects.items() if k != "placement_delta_mm")
            assert changed == (section == "modified"), item
            assert (aspects["data"] == "unchanged") == (item["data_hash"]["old"] == item["data_hash"]["new"]), item
            # A placement reported unchanged can carry no translation delta:
            # equal placement matrices have identical translation entries, so
            # the only valid delta is absent or zero. (A *changed* placement may
            # still net zero translation — e.g. a pure rotation — so the
            # converse is intentionally not asserted.)
            delta = aspects["placement_delta_mm"]
            if aspects["placement"] == "unchanged":
                assert delta is None or math.hypot(*delta) == 0, item
            intrinsic = any(aspects[name] == "changed" for name in ("geometry", "data", "placement"))
            transitive = aspects["topology"] == "changed"
            expected_scope = (
                "mixed" if intrinsic and transitive
                else "intrinsic" if intrinsic
                else "transitive" if transitive
                else "none"
            )
            assert item["change_scope"] == expected_scope, item

    # 1:1 matching: no entity may appear in two sections or twice in one.
    old_steps = [item["old"]["step_id"] for s in ("modified", "unchanged") for item in report[s]]
    old_steps += [item["step_id"] for item in report["deleted"]]
    new_steps = [item["new"]["step_id"] for s in ("modified", "unchanged") for item in report[s]]
    new_steps += [item["step_id"] for item in report["added"]]
    assert len(old_steps) == len(set(old_steps)) == stats["old_signatures"]
    assert len(new_steps) == len(set(new_steps)) == stats["new_signatures"]


def assert_zero_diff(report: dict) -> None:
    """A pair with identical content must report nothing added/deleted/modified."""
    assert report["stats"]["added"] == 0
    assert report["stats"]["deleted"] == 0
    assert report["stats"]["modified"] == 0
    assert report["stats"]["unchanged"] == report["stats"]["old_signatures"]
    assert report["stats"]["unchanged"] == report["stats"]["new_signatures"]
