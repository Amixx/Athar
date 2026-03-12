"""Tests for graph_cache module."""

import os
import pickle
from pathlib import Path

import pytest

from athar.diff.graph_cache import (
    _CACHE_ENABLED_ENV,
    _CACHE_VERSION,
    clear_cache,
    content_hash,
    load_cached,
    restore_identity_state,
    save_cached,
)


@pytest.fixture
def cache_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("ATHAR_CACHE_DIR", str(tmp_path))
    monkeypatch.setenv(_CACHE_ENABLED_ENV, "1")
    return tmp_path


@pytest.fixture
def sample_graph():
    return {
        "metadata": {"schema": "IFC4", "timestamp": None, "diagnostics": {}, "units": {}},
        "entities": {
            1: {"entity_type": "IfcWall", "global_id": "abc", "attributes": {"Name": "W1"}, "refs": []},
            2: {"entity_type": "IfcSlab", "global_id": "def", "attributes": {"Name": "S1"}, "refs": []},
        },
    }


@pytest.fixture
def sample_identity_state():
    return {
        "guid_counts": {"abc": 1, "def": 1},
        "unique_guid_steps": {"abc": 1, "def": 2},
        "graph_adjacency": {1: [(2, "/Ref", "IfcSlab")], 2: []},
        "graph_reverse_adjacency": {1: [], 2: [(1, "/Ref", "IfcSlab")]},
        "id_adjacency": {1: [(2, "/Ref", "IfcSlab")], 2: []},
        "id_reverse_adjacency": {1: [], 2: [(1, "/Ref", "IfcSlab")]},
        "profile_hashes": {1: "h1", 2: "h2"},
        "colors": {1: "c1", 2: "c2"},
        "scc_classes": {},
        "wl_diagnostics": {},
        "entities": {1: {}, 2: {}},
        "id_entities": {1: {}, 2: {}},
    }


def test_content_hash_returns_hex_string(tmp_path):
    f = tmp_path / "test.ifc"
    f.write_bytes(b"hello world")
    h = content_hash(str(f))
    assert isinstance(h, str)
    assert len(h) == 32  # xxh3_128 hex


def test_content_hash_returns_none_for_missing_file():
    assert content_hash("/nonexistent/file.ifc") is None


def test_content_hash_deterministic(tmp_path):
    f = tmp_path / "test.ifc"
    f.write_bytes(b"deterministic content")
    assert content_hash(str(f)) == content_hash(str(f))


def test_content_hash_changes_with_content(tmp_path):
    f = tmp_path / "test.ifc"
    f.write_bytes(b"version 1")
    h1 = content_hash(str(f))
    f.write_bytes(b"version 2")
    h2 = content_hash(str(f))
    assert h1 != h2


def test_save_and_load_roundtrip(cache_dir, sample_graph, sample_identity_state):
    save_cached("abc123", "raw_exact", graph=sample_graph, identity_state=sample_identity_state)
    loaded = load_cached("abc123", "raw_exact")
    assert loaded is not None
    assert loaded["graph"] == sample_graph
    assert loaded["profile"] == "raw_exact"
    assert loaded["cache_version"] == _CACHE_VERSION


def test_load_returns_none_on_miss(cache_dir):
    assert load_cached("nonexistent", "raw_exact") is None


def test_load_returns_none_when_disabled(cache_dir, sample_graph, sample_identity_state, monkeypatch):
    save_cached("abc123", "raw_exact", graph=sample_graph, identity_state=sample_identity_state)
    monkeypatch.setenv(_CACHE_ENABLED_ENV, "0")
    assert load_cached("abc123", "raw_exact") is None


def test_save_noop_when_disabled(cache_dir, sample_graph, sample_identity_state, monkeypatch):
    monkeypatch.setenv(_CACHE_ENABLED_ENV, "0")
    save_cached("abc123", "raw_exact", graph=sample_graph, identity_state=sample_identity_state)
    monkeypatch.setenv(_CACHE_ENABLED_ENV, "1")
    assert load_cached("abc123", "raw_exact") is None


def test_load_returns_none_for_none_hash(cache_dir):
    assert load_cached(None, "raw_exact") is None


def test_save_noop_for_none_hash(cache_dir, sample_graph, sample_identity_state):
    save_cached(None, "raw_exact", graph=sample_graph, identity_state=sample_identity_state)
    # No files should be created
    assert list(cache_dir.glob("*.pkl")) == []


def test_different_profiles_separate_entries(cache_dir, sample_graph, sample_identity_state):
    save_cached("abc123", "raw_exact", graph=sample_graph, identity_state=sample_identity_state)
    save_cached("abc123", "semantic_stable", graph=sample_graph, identity_state=sample_identity_state)
    assert load_cached("abc123", "raw_exact") is not None
    assert load_cached("abc123", "semantic_stable") is not None


def test_corrupted_cache_returns_none(cache_dir):
    # Write garbage to a cache file
    path = cache_dir / f"v{_CACHE_VERSION}_abc123_raw_exact.pkl"
    path.write_bytes(b"not valid pickle")
    assert load_cached("abc123", "raw_exact") is None
    # File should be cleaned up
    assert not path.exists()


def test_wrong_version_returns_none(cache_dir):
    path = cache_dir / f"v{_CACHE_VERSION}_abc123_raw_exact.pkl"
    with open(path, "wb") as f:
        pickle.dump({"cache_version": _CACHE_VERSION + 999}, f)
    assert load_cached("abc123", "raw_exact") is None
    assert not path.exists()


def test_restore_identity_state_reattaches_entities(sample_graph, sample_identity_state):
    cached_entry = {
        "identity_state": {
            k: v for k, v in sample_identity_state.items()
            if k not in ("entities", "id_entities")
        },
    }
    restored = restore_identity_state(cached_entry, sample_graph, profile="raw_exact")
    assert restored["entities"] is sample_graph["entities"]
    assert restored["id_entities"] is sample_graph["entities"]  # raw_exact → same ref
    assert restored["profile_hashes"] == sample_identity_state["profile_hashes"]
    assert restored["colors"] == sample_identity_state["colors"]


def test_clear_cache(cache_dir, sample_graph, sample_identity_state):
    save_cached("a", "raw_exact", graph=sample_graph, identity_state=sample_identity_state)
    save_cached("b", "raw_exact", graph=sample_graph, identity_state=sample_identity_state)
    n = clear_cache()
    assert n == 2
    assert load_cached("a", "raw_exact") is None
    assert load_cached("b", "raw_exact") is None


def test_identity_state_strips_entities_on_save(cache_dir, sample_graph, sample_identity_state):
    save_cached("abc123", "raw_exact", graph=sample_graph, identity_state=sample_identity_state)
    loaded = load_cached("abc123", "raw_exact")
    identity = loaded["identity_state"]
    assert "entities" not in identity
    assert "id_entities" not in identity
    assert "profile_hashes" in identity
