from __future__ import annotations

import os
import signal
from pathlib import Path

import pytest

from athar.engine import diff_files


REPO_ROOT = Path(__file__).resolve().parent.parent

_RUN_LARGE = os.getenv("ATHAR_RUN_LARGE_ACCEPTANCE", "0") == "1"
_TIMEOUT_S = int(os.getenv("ATHAR_ACCEPTANCE_TIMEOUT_S", "0"))
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


pytestmark = [
    pytest.mark.skipif(
        not _RUN_LARGE,
        reason="set ATHAR_RUN_LARGE_ACCEPTANCE=1 to run large IFC acceptance tests",
    ),
    pytest.mark.large_acceptance,
]


def _require(path: Path) -> None:
    if not path.exists():
        pytest.skip(f"missing acceptance IFC: {path}")
    with path.open("rb") as fh:
        head = fh.read(64)
    if head.startswith(b"version https://git-lfs"):
        pytest.skip(f"acceptance IFC is an unfetched git-lfs pointer: {path}")


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


def _assert_stats_consistent(report: dict) -> None:
    stats = report["stats"]
    for section in ("added", "deleted", "modified", "unchanged"):
        assert stats[section] == len(report[section])
    assert stats["deleted"] + stats["modified"] + stats["unchanged"] == stats["old_signatures"]
    assert stats["added"] + stats["modified"] + stats["unchanged"] == stats["new_signatures"]
    assert stats["dropped_matches"] == 0
    diag = stats["matcher_diagnostics"]
    assert diag["pools"] == {"old": stats["old_signatures"], "new": stats["new_signatures"]}
    assert diag["unmatched"] == {"old": stats["deleted"], "new": stats["added"]}
    assert sum(diag["matched_by_tier"].values()) == stats["modified"] + stats["unchanged"]


def test_large_ifc_holy_grail_same_file_is_unchanged():
    _require(_HOLY_GRAIL)
    src = str(_HOLY_GRAIL)
    report = diff_files(src, src)
    _assert_stats_consistent(report)
    assert report["stats"]["added"] == 0
    assert report["stats"]["deleted"] == 0
    assert report["stats"]["modified"] == 0
    # Non-vacuity floor: a ~180MB model must yield a substantial signature
    # population, otherwise the parse silently lost the model.
    assert report["stats"]["unchanged"] > 100
    assert report["stats"]["matcher_diagnostics"]["spatial"]["probe_capped"] == 0


def test_large_ifc_holy_grail_vs_simplified_completes_with_consistent_stats():
    # Cross-project stress shape: two unrelated models. The value here is that
    # the diff completes within bounds, stays internally consistent, and the
    # conservative tiers do not hallucinate broad cross-project matches.
    _require(_HOLY_GRAIL)
    _require(_SIMPLIFIED)
    report = diff_files(str(_HOLY_GRAIL), str(_SIMPLIFIED))
    _assert_stats_consistent(report)

    stats = report["stats"]
    assert report["engine"] == "athar"
    assert report["schemas"]["old"] in {"IFC4", "IFC2X3"}
    assert report["schemas"]["new"] in {"IFC4", "IFC2X3"}
    assert stats["old_signatures"] > 100
    assert stats["new_signatures"] > 100
    # Unrelated projects share no GlobalIds; the guid tier must stay silent
    # and most of both sides must remain unmatched.
    assert stats["matcher_diagnostics"]["matched_by_tier"]["guid"] == 0
    assert stats["deleted"] >= stats["old_signatures"] // 2
    assert stats["added"] >= stats["new_signatures"] // 2
