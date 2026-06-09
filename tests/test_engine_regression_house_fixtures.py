from pathlib import Path

import pytest

from athar.engine import diff_files


REPO_ROOT = Path(__file__).resolve().parent.parent
FIXTURES = Path(__file__).resolve().parent / "fixtures"
REAL_WORLD = REPO_ROOT / "real-world-test"
pytestmark = pytest.mark.medium_regression


def test_regression_diff_report_invariants_small_realworld_pair():
    old_path = str(REAL_WORLD / "Building-Landscaping-v1.ifc")
    new_path = str(REAL_WORLD / "Building-Landscaping-v2.ifc")

    report = diff_files(old_path, new_path)

    assert report["engine"] == "athar"
    assert report["canon_version"] == "athar-canon-v1"
    assert report["schemas"]["old"] in {"IFC2X3", "IFC4"}
    assert report["schemas"]["new"] == report["schemas"]["old"]
    assert report["stats"]["old_signatures"] > 0
    assert report["stats"]["new_signatures"] > 0
    assert (report["stats"]["modified"] + report["stats"]["added"] + report["stats"]["deleted"]) > 0
    assert report["stats"]["added"] == len(report["added"])
    assert report["stats"]["deleted"] == len(report["deleted"])
    assert report["stats"]["modified"] == len(report["modified"])
    assert report["stats"]["unchanged"] == len(report["unchanged"])


@pytest.mark.skip(reason="large regression pair; covered by opt-in acceptance/perf tier")
def test_regression_large_house_pair_invariants():
    old_path = str(FIXTURES / "house_v1.ifc")
    new_path = str(FIXTURES / "house_v2.ifc")

    report = diff_files(old_path, new_path)

    assert report["engine"] == "athar"
    assert report["canon_version"] == "athar-canon-v1"
    assert report["schemas"] == {"old": "IFC2X3", "new": "IFC2X3"}
    assert report["stats"]["modified"] > 0
