"""Approval and rejection are accepted only for pending reviews on the current baseline."""

from __future__ import annotations

import json

import pytest

from athar_store.cli import build_store_parser, run_store
from tests.test_store import _GUID_A, _GUID_B, _write_model


@pytest.fixture
def cli(tmp_path, capsys):
    root = str(tmp_path / "store")
    parser = build_store_parser()

    def run(*argv: str) -> tuple[int, str, str]:
        code = run_store(parser.parse_args(["--root", root, *argv]))
        captured = capsys.readouterr()
        return code, captured.out, captured.err

    return run


@pytest.fixture
def lineage(cli, tmp_path):
    baseline = tmp_path / "baseline.ifc"
    first = tmp_path / "first.ifc"
    second = tmp_path / "second.ifc"
    _write_model(baseline, [(_GUID_A, "wall-A")])
    _write_model(first, [(_GUID_A, "wall-A"), (_GUID_B, "wall-B")])
    _write_model(second, [(_GUID_A, "wall-A-renamed")])
    assert cli("set-baseline", "acme", "arch", str(baseline), "--actor", "alice")[0] == 0
    reviews = []
    for candidate in (first, second):
        code, out, _ = cli("review", "acme", "arch", str(candidate), "--actor", "bob")
        assert code in (0, 2)
        reviews.append(json.loads(out)["id"])
    return reviews


def _baseline_event(cli) -> str:
    code, out, _ = cli("baseline", "acme", "arch")
    assert code == 0
    return json.loads(out)["established_event_id"]


def test_approving_twice_is_refused(cli, lineage):
    first, _ = lineage
    code, out, _ = cli("approve", "acme", "arch", first, "--actor", "carol")
    assert code == 0
    approval = json.loads(out)["id"]

    code, _, err = cli("approve", "acme", "arch", first, "--actor", "carol")
    assert code == 1
    assert f"Review {first} is already approved by event {approval}." in err
    assert _baseline_event(cli) == approval


def test_rejecting_an_approved_review_is_refused(cli, lineage):
    first, _ = lineage
    code, out, _ = cli("approve", "acme", "arch", first, "--actor", "carol")
    approval = json.loads(out)["id"]

    code, _, err = cli("reject", "acme", "arch", first, "--actor", "dana")
    assert code == 1
    assert f"already approved by event {approval}" in err


def test_decisions_on_a_rejected_review_are_refused(cli, lineage):
    first, _ = lineage
    code, out, _ = cli("reject", "acme", "arch", first, "--actor", "carol")
    assert code == 0
    rejection = json.loads(out)["id"]
    before = _baseline_event(cli)

    for action in ("approve", "reject"):
        code, _, err = cli(action, "acme", "arch", first, "--actor", "dana")
        assert code == 1
        assert f"Review {first} was rejected by event {rejection}." in err
    assert _baseline_event(cli) == before


def test_decisions_on_a_stale_review_are_refused(cli, lineage):
    first, second = lineage
    code, out, _ = cli("approve", "acme", "arch", first, "--actor", "carol")
    approval = json.loads(out)["id"]

    for action in ("approve", "reject"):
        code, _, err = cli(action, "acme", "arch", second, "--actor", "dana")
        assert code == 1
        assert f"Review {second} is stale: the baseline changed by event {approval}." in err
    assert _baseline_event(cli) == approval
