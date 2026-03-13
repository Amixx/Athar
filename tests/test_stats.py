from athar.diff.stats import build_stats


def test_build_stats_uses_index_summaries_without_changing_match_method_counts():
    old_graph = {"entities": {1: {}, 2: {}, 3: {}}}
    new_graph = {"entities": {10: {}, 20: {}, 30: {}}}

    stats = build_stats(
        old_graph=old_graph,
        new_graph=new_graph,
        old_by_id={},
        new_by_id={},
        old_index_summary={
            "count_by_id": {"G:A": 2, "H:B": 1},
            "methods_by_id": {
                "G:A": ["exact_guid", "text_fingerprint"],
                "H:B": ["secondary_match"],
            },
        },
        new_index_summary={
            "count_by_id": {"G:A": 1, "H:B": 1},
        },
        remap_ambiguous=0,
        path_ambiguous=0,
        secondary_ambiguous=0,
        remap_matches=0,
        path_matches=0,
        secondary_matches=0,
        old_dangling_refs=0,
        new_dangling_refs=0,
        old_guid_quality={"valid_total": 0, "unique_valid": 0, "duplicate_ids": 0, "duplicate_occurrences": 0, "invalid": 0},
        new_guid_quality={"valid_total": 0, "unique_valid": 0, "duplicate_ids": 0, "duplicate_occurrences": 0, "invalid": 0},
    )

    assert stats["matched"] == 2
    assert stats["matched_by_method"] == {
        "exact_guid": 1,
        "secondary_match": 1,
    }
