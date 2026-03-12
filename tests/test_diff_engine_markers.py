import athar.diff_engine_markers as markers


def test_owner_index_disk_threshold_defaults_to_zero(monkeypatch):
    monkeypatch.delenv(markers.OWNER_INDEX_DISK_THRESHOLD_ENV, raising=False)
    assert markers.owner_index_disk_threshold() == 0


def test_owner_index_disk_threshold_invalid_value_defaults_to_zero(monkeypatch):
    monkeypatch.setenv(markers.OWNER_INDEX_DISK_THRESHOLD_ENV, "abc")
    assert markers.owner_index_disk_threshold() == 0


def test_owner_index_disk_threshold_negative_value_clamps_to_zero(monkeypatch):
    monkeypatch.setenv(markers.OWNER_INDEX_DISK_THRESHOLD_ENV, "-10")
    assert markers.owner_index_disk_threshold() == 0


def test_owner_index_disk_threshold_positive_value_passes_through(monkeypatch):
    monkeypatch.setenv(markers.OWNER_INDEX_DISK_THRESHOLD_ENV, "12345")
    assert markers.owner_index_disk_threshold() == 12345


def test_rooted_owner_projector_uses_on_demand_mode_by_default(monkeypatch):
    graph = {
        "entities": {
            1: {
                "entity_type": "IfcWall",
                "refs": [{"path": "/Contains/0", "target": 2, "target_type": "IfcDoor"}],
            },
            2: {"entity_type": "IfcDoor", "refs": []},
        }
    }
    ids = {1: "G:ROOT", 2: "H:CHILD"}
    monkeypatch.delenv(markers.OWNER_INDEX_DISK_THRESHOLD_ENV, raising=False)
    monkeypatch.setattr(
        markers,
        "compute_rooted_owner_index",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("eager index should not run in default mode")),
    )

    projector = markers.RootedOwnerProjector(graph, ids)
    assert projector.owners_for_step(2) == {"G:ROOT"}
    assert projector.owners_for_steps([1, 2]) == {"G:ROOT"}


def test_rooted_owner_projector_uses_eager_index_when_threshold_set(monkeypatch):
    graph = {
        "entities": {
            1: {
                "entity_type": "IfcWall",
                "refs": [{"path": "/Contains/0", "target": 2, "target_type": "IfcDoor"}],
            },
            2: {"entity_type": "IfcDoor", "refs": []},
        }
    }
    ids = {1: "G:ROOT", 2: "H:CHILD"}
    monkeypatch.setenv(markers.OWNER_INDEX_DISK_THRESHOLD_ENV, "999999")
    called = {"count": 0}

    def _fake_compute(_graph, _ids):
        called["count"] += 1
        return {1: {"G:ROOT"}, 2: {"G:ROOT"}}

    monkeypatch.setattr(markers, "compute_rooted_owner_index", _fake_compute)
    projector = markers.RootedOwnerProjector(graph, ids)
    assert projector.owners_for_step(2) == {"G:ROOT"}
    assert called["count"] == 1


def test_rooted_owner_projector_can_use_prebuilt_reverse_adjacency(monkeypatch):
    graph = {
        "entities": {
            1: {"entity_type": "IfcWall", "refs": [{"path": "/Contains/0", "target": 2, "target_type": "IfcDoor"}]},
            2: {"entity_type": "IfcDoor", "refs": []},
        }
    }
    ids = {1: "G:ROOT", 2: "H:CHILD"}
    reverse = {
        1: [],
        2: [("/Contains/0", "IfcWall", 1)],
    }
    monkeypatch.delenv(markers.OWNER_INDEX_DISK_THRESHOLD_ENV, raising=False)
    monkeypatch.setattr(
        markers,
        "_build_reverse_sources",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("_build_reverse_sources should not be called")),
    )

    projector = markers.RootedOwnerProjector(graph, ids, reverse_adjacency=reverse)
    assert projector.owners_for_step(2) == {"G:ROOT"}
