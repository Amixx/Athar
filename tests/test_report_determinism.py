from __future__ import annotations

import os
import subprocess
import sys

import pytest

from tests.corpus import corpus_path

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _cli_stdout(old: str, new: str, hash_seed: int) -> bytes:
    result = subprocess.run(
        [sys.executable, "-m", "athar", old, new],
        capture_output=True,
        check=True,
        cwd=REPO_ROOT,
        env={**os.environ, "PYTHONHASHSEED": str(hash_seed)},
    )
    return result.stdout


@pytest.mark.parametrize(
    ("old_key", "new_key"),
    [("duplex_arch", "duplex_arch"), ("bl_v1", "bl_v2")],
)
def test_report_bytes_do_not_depend_on_the_process(old_key, new_key):
    old, new = corpus_path(old_key), corpus_path(new_key)
    first = _cli_stdout(old, new, hash_seed=1)
    assert first
    for seed in (2, 3):
        assert _cli_stdout(old, new, hash_seed=seed) == first
