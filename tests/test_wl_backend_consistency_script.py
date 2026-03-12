from scripts.explore.check_wl_backend_consistency import _partition_summary


def test_partition_summary_is_label_invariant():
    labels_a = {1: "a", 2: "a", 3: "b", 4: "c"}
    labels_b = {1: "x", 2: "x", 3: "y", 4: "z"}

    summary_a = _partition_summary(labels_a)
    summary_b = _partition_summary(labels_b)

    assert summary_a["group_count"] == summary_b["group_count"]
    assert summary_a["largest_group_size"] == summary_b["largest_group_size"]
    assert summary_a["sha256"] == summary_b["sha256"]


def test_partition_summary_changes_when_partition_changes():
    labels_a = {1: "a", 2: "a", 3: "b", 4: "c"}
    labels_b = {1: "a", 2: "b", 3: "c", 4: "d"}

    summary_a = _partition_summary(labels_a)
    summary_b = _partition_summary(labels_b)

    assert summary_a["sha256"] != summary_b["sha256"]

