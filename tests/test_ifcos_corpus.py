"""Zero-diff invariants over IfcOpenShell's public test files (opt-in).

The files are not redistributed; ``fixtures/ifcos_corpus_manifest.json`` pins
each by upstream URL and sha256. Set ``ATHAR_IFCOS_CORPUS_DIR`` to a checkout
of that commit to run these tests; once set, a missing or altered file fails.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

import ifcopenshell
import pytest

from athar.engine import diff_files
from tests.corpus import assert_report_invariants, assert_zero_diff

_MANIFEST = json.loads((Path(__file__).parent / "fixtures" / "ifcos_corpus_manifest.json").read_text())
_CORPUS_DIR = os.getenv("ATHAR_IFCOS_CORPUS_DIR")

pytestmark = pytest.mark.skipif(not _CORPUS_DIR, reason="set ATHAR_IFCOS_CORPUS_DIR to run the IfcOpenShell corpus")


def _verified_path(entry: dict) -> str:
    path = Path(_CORPUS_DIR) / entry["path"]
    assert path.is_file(), f"missing {entry['path']}; fetch {_MANIFEST['raw_base_url']}/{entry['path']}"
    assert hashlib.sha256(path.read_bytes()).hexdigest() == entry["sha256"], f"sha256 mismatch: {entry['path']}"
    return str(path)


@pytest.mark.parametrize("entry", _MANIFEST["files"], ids=lambda e: e["path"])
def test_self_diff_is_zero(entry: dict) -> None:
    path = _verified_path(entry)
    report = diff_files(path, path)
    assert_report_invariants(report)
    assert_zero_diff(report)


@pytest.mark.parametrize("entry", _MANIFEST["files"], ids=lambda e: e["path"])
def test_ifcopenshell_reserialization_is_zero(entry: dict, tmp_path: Path) -> None:
    path = _verified_path(entry)
    rewritten = tmp_path / "rewritten.ifc"
    ifcopenshell.open(path).write(str(rewritten))
    report = diff_files(path, str(rewritten))
    assert_report_invariants(report)
    assert_zero_diff(report)
