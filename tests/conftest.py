"""Fixtures that create modified IFC files for testing."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from scripts.make_modified_ifc import make_modified

ARCH_IFC = "data/Building-Architecture.ifc"


@pytest.fixture
def modified_arch(tmp_path):
    """Create a modified version of Building-Architecture.ifc, return (old_path, new_path, manifest)."""
    output = str(tmp_path / "modified.ifc")
    manifest = make_modified(ARCH_IFC, output)
    return ARCH_IFC, output, manifest
