"""Byte-identical parity between the native accelerator and pure Python.

The Rust port of the bottom signature pipeline (Stage A: Merkle + WL gossip)
is only acceptable if it reproduces signatures exactly. These tests run the
real pipeline twice on a corpus fixture — once through ``athar_native`` and
once with it forced off — and assert equality at the hash, topology, and
whole-signature-vector levels.

They skip when the native extension is not built, so the suite stays green for
pure-Python checkouts. Build the extension with ``make native-build``.
"""

from __future__ import annotations

import hashlib
import json

import pytest

import athar.bottom.merkle as merkle_mod
import athar.bottom.wl_gossip as wl_mod
from athar.bottom._native import native_available
from athar.bottom.edge_policy import build_edge_set
from athar.bottom.merkle import compute_merkle_hashes
from athar.bottom.parser import parse_ifc
from athar.bottom.signatures import build_signature_bundle
from athar.bottom.wl_gossip import compute_topology_hashes

from .corpus import corpus_path

pytestmark = pytest.mark.skipif(
    not native_available(),
    reason="athar_native extension not built (run `make native-build`)",
)

# Small real IFC fixture (~1.2 MB); large enough to exercise geometry/data
# Merkle trees and spatial gossip, small enough for the fast suite.
_FIXTURE_KEY = "bl_v1"


def _force_python(monkeypatch) -> None:
    monkeypatch.setattr(merkle_mod, "native", lambda: None)
    monkeypatch.setattr(wl_mod, "native", lambda: None)


def test_merkle_hashes_match_pure_python(monkeypatch):
    path = corpus_path(_FIXTURE_KEY)
    parsed = parse_ifc(path)
    edges = build_edge_set(parsed)

    native_hashes = compute_merkle_hashes(parsed, edges)

    _force_python(monkeypatch)
    python_hashes = compute_merkle_hashes(parsed, edges)

    assert native_hashes == python_hashes
    assert native_hashes  # fixture actually produced hashes


def test_topology_hashes_match_pure_python(monkeypatch):
    path = corpus_path(_FIXTURE_KEY)
    parsed = parse_ifc(path)
    edges = build_edge_set(parsed)
    merkle_hashes = compute_merkle_hashes(parsed, edges)

    native_topo = compute_topology_hashes(parsed, edges, merkle_hashes)

    _force_python(monkeypatch)
    python_topo = compute_topology_hashes(parsed, edges, merkle_hashes)

    assert native_topo == python_topo
    assert native_topo


def _signature_digest(bundle) -> str:
    """Order-independent sha256 over the full signature vectors.

    Mirrors the determinism oracle described in the Rust-port task: the hash
    folds in every identity-bearing field so any drift in geometry/data/
    topology hashing surfaces as a digest mismatch.
    """
    rows = []
    for step_id, sig in sorted(bundle.signatures.items()):
        rows.append(
            [
                step_id,
                sig.guid,
                sig.name,
                sig.entity_type,
                sig.canonical_class,
                sig.vh_geometry,
                sig.vh_data,
                sig.vh_topology,
                sig.placement,
            ]
        )
    payload = json.dumps(rows, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def test_full_signature_bundle_is_byte_identical(monkeypatch):
    path = corpus_path(_FIXTURE_KEY)

    native_bundle = build_signature_bundle(path)

    _force_python(monkeypatch)
    python_bundle = build_signature_bundle(path)

    assert _signature_digest(native_bundle) == _signature_digest(python_bundle)
