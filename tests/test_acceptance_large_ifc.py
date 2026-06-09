from __future__ import annotations

import os
from pathlib import Path

import pytest

from athar.engine import diff_files


REPO_ROOT = Path(__file__).resolve().parent.parent

_RUN_LARGE = os.getenv("ATHAR_RUN_LARGE_ACCEPTANCE", "0") == "1"
_HOLY_GRAIL = Path(
    os.getenv(
        "ATHAR_ACCEPTANCE_HOLY_GRAIL_PATH",
        str(REPO_ROOT / "real-world-test" / "real-world-spanish-180mb.ifc"),
    )
)
_SIMPLIFIED = Path(
    os.getenv(
        "ATHAR_ACCEPTANCE_SIMPLIFIED_PATH",
        str(REPO_ROOT / "real-world-test" / "uni-project-house-50mb.ifc"),
    )
)


pytestmark = pytest.mark.skipif(
    not _RUN_LARGE,
    reason="set ATHAR_RUN_LARGE_ACCEPTANCE=1 to run large IFC acceptance tests",
)
pytestmark = [pytestmark, pytest.mark.large_acceptance]


def _require(path: Path) -> None:
    if not path.exists():
        pytest.skip(f"missing acceptance IFC: {path}")


def test_large_ifc_holy_grail_same_file_short_circuit_acceptance():
    _require(_HOLY_GRAIL)
    src = str(_HOLY_GRAIL)
    report = diff_files(src, src)
    assert report["stats"]["added"] == 0
    assert report["stats"]["deleted"] == 0
    assert report["stats"]["modified"] == 0
    assert report["stats"]["unchanged"] > 0


def test_large_ifc_holy_grail_vs_simplified_acceptance_shape():
    _require(_HOLY_GRAIL)
    _require(_SIMPLIFIED)
    old_path = str(_HOLY_GRAIL)
    new_path = str(_SIMPLIFIED)
    report = diff_files(old_path, new_path)

    assert report["engine"] == "athar"
    assert report["schemas"]["old"] in {"IFC4", "IFC2X3"}
    assert report["schemas"]["new"] in {"IFC4", "IFC2X3"}
    assert report["stats"]["old_signatures"] > 0
    assert report["stats"]["new_signatures"] > 0
    assert (
        report["stats"]["added"]
        + report["stats"]["deleted"]
        + report["stats"]["modified"]
        + report["stats"]["unchanged"]
    ) > 0
