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

