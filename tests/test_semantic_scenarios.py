"""Known-edit semantic scenarios over small real seeds.

Each test builds a before/after pair in a tmp dir by applying one constructed
mutation from ``athar_dev.ifc_mutations`` to a real seed file, then asserts the
report against the mutation's own manifest — the expectations are derived from
the edit, never from current engine output. Structural report contracts are
re-checked through the shared corpus invariants.
"""

from __future__ import annotations

import math

import ifcopenshell
import pytest

from athar_dev import ifc_mutations as mutations
from athar.engine import diff_files
from tests.corpus import assert_report_invariants, assert_zero_diff, corpus_path

SEEDS = ("gni_190", "gni_9")
_DELTA_TOLERANCE_MM = 0.01


@pytest.fixture(scope="module", params=SEEDS)
def scenario_seed(request, tmp_path_factory):
    """(baseline_path, mutate) for one seed.

    The baseline and every mutated copy go through the same ifcopenshell
    write path, so the constructed mutation is the only delta in each pair.
    The baseline is written once per seed; the engine's bundle cache then
    parses it once across all scenarios.
    """
    seed_path = corpus_path(request.param)
    base_dir = tmp_path_factory.mktemp(f"semantic-{request.param}")
    baseline = str(base_dir / "baseline.ifc")
    ifcopenshell.open(seed_path).write(baseline)

    def mutate(mutator):
        model = ifcopenshell.open(seed_path)
        mutation = mutator(model)
        mutated = str(base_dir / f"{mutation.operation}.ifc")
        model.write(mutated)
        return mutated, mutation

    return baseline, mutate


def test_guid_scramble_yields_zero_diff(scenario_seed):
    baseline, mutate = scenario_seed
    mutated, _ = mutate(mutations.scramble_guids)
    report = diff_files(baseline, mutated)
    assert_report_invariants(report)
    # Truth: ids are labels, not content — nothing changed semantically, and
    # the rewritten ids must not pass as identity evidence.
    assert_zero_diff(report)
    assert report["stats"]["matcher_diagnostics"]["matched_by_tier"]["guid"] == 0


def test_move_one_product_is_one_intrinsic_placement_change(scenario_seed):
    baseline, mutate = scenario_seed
    mutated, mutation = mutate(mutations.move_product)
    report = diff_files(baseline, mutated)
    assert_report_invariants(report)
    assert report["stats"]["added"] == 0
    assert report["stats"]["deleted"] == 0
    # Placement enters no hash and no gossip seed, so the move must not
    # ripple: the victim is the single modified entity.
    assert [item["old"]["guid"] for item in report["modified"]] == [mutation.victim_guid]
    item = report["modified"][0]
    assert item["match"]["reason"] == "guid"
    _assert_aspects(item, mutation.expected_victim_aspects)
    assert item["change_scope"] == "intrinsic"
    delta = item["aspects"]["placement_delta_mm"]
    assert delta is not None
    # Rigid parent frames preserve the norm of the local offset.
    assert math.hypot(*delta) == pytest.approx(mutation.expected_delta_mm, abs=_DELTA_TOLERANCE_MM)


def test_delete_one_product_deletes_exactly_that_product(scenario_seed):
    baseline, mutate = scenario_seed
    mutated, mutation = mutate(mutations.delete_product)
    report = diff_files(baseline, mutated)
    assert_report_invariants(report)
    assert report["stats"]["added"] == 0
    assert [item["guid"] for item in report["deleted"]] == [mutation.victim_guid]
    # Nothing else changed intrinsically; neighbors may only carry the
    # relational fallout of the removal.
    for item in report["modified"]:
        assert item["change_scope"] == "transitive", item


def test_pset_value_edit_is_a_data_change_on_that_product(scenario_seed):
    baseline, mutate = scenario_seed
    mutated, mutation = mutate(mutations.edit_pset_value)
    report = diff_files(baseline, mutated)
    _assert_single_product_data_edit(report, mutation)
    victim = [item for item in report["modified"] if item["old"]["guid"] == mutation.victim_guid][0]
    assert "property_deltas" in victim
    assert "changed" in victim["property_deltas"]


def test_type_pset_value_edit_is_inherited_data_change_on_occurrences(scenario_seed):
    baseline, mutate = scenario_seed
    mutated, mutation = mutate(mutations.edit_type_pset_value)
    report = diff_files(baseline, mutated)
    assert_report_invariants(report)
    assert report["stats"]["added"] == 0
    assert report["stats"]["deleted"] == 0
    # Truth: the type object has no signature of its own — the edited value is
    # inherited, so it must surface as a data change on exactly the linked
    # occurrences, each matched by GlobalId.
    affected = set(mutation.affected_guids)
    victims = [item for item in report["modified"] if item["old"]["guid"] in affected]
    assert sorted(item["old"]["guid"] for item in victims) == sorted(mutation.affected_guids)
    for item in victims:
        assert item["match"]["reason"] == "guid"
        _assert_aspects(item, mutation.expected_victim_aspects)
        assert item["data_hash"]["old"] != item["data_hash"]["new"]
        assert "property_deltas" in item
        assert "changed" in item["property_deltas"]
    for other in report["modified"]:
        if other["old"]["guid"] not in affected:
            assert other["change_scope"] == "transitive", other


def test_rename_is_a_data_change_on_that_product(scenario_seed):
    baseline, mutate = scenario_seed
    mutated, mutation = mutate(mutations.rename_product)
    report = diff_files(baseline, mutated)
    _assert_single_product_data_edit(report, mutation)
    # A scalar Name edit must not fan out through topology gossip as dozens of
    # neighbor changes. Humans expect this to read as one product data change.
    assert report["stats"]["modified"] == 1
    assert len(report["modified"][0]["data_delta"]) == 1
    data_delta = report["modified"][0]["data_delta"][0]
    assert data_delta["path"] == f"{mutation.victim_type}[GlobalId={mutation.victim_guid}].Name"
    assert data_delta["new"] == f"{data_delta['old']} [athar-renamed]"


def test_add_one_product_adds_exactly_that_product(scenario_seed):
    baseline, mutate = scenario_seed
    mutated, mutation = mutate(mutations.add_product)
    report = diff_files(baseline, mutated)
    assert_report_invariants(report)
    # Symmetric to deletion: exactly the new proxy appears, nothing is removed,
    # and nothing pre-existing changes intrinsically — only the spatial
    # container may ripple (transitively) from gaining a child.
    assert report["stats"]["deleted"] == 0
    assert [item["guid"] for item in report["added"]] == [mutation.victim_guid]
    for item in report["modified"]:
        assert item["change_scope"] == "transitive", item


def test_duplicated_guid_is_surfaced_and_never_identity_evidence(scenario_seed):
    baseline, mutate = scenario_seed
    mutated, mutation = mutate(mutations.duplicate_guid)
    report = diff_files(baseline, mutated)
    assert_report_invariants(report)
    # Truth: content untouched — duplicating an id must not invent a change —
    # but the collision must be surfaced.
    assert_zero_diff(report)
    assert report["stats"]["guid_collisions"] == {"old": 0, "new": 1}
    # The two entities now sharing an id must not be matched *by* that id.
    involved = {mutation.victim_guid, mutation.partner_guid}
    items = [item for item in report["unchanged"] if item["old"]["guid"] in involved]
    assert len(items) == 2
    for item in items:
        assert item["match"]["reason"] != "guid", item


def _assert_single_product_data_edit(report: dict, mutation: mutations.Mutation) -> None:
    """A pure data edit on one product: victim guid-matched with data changed,
    geometry/placement untouched; everything else at most transitive."""
    assert_report_invariants(report)
    assert report["stats"]["added"] == 0
    assert report["stats"]["deleted"] == 0
    victims = [item for item in report["modified"] if item["old"]["guid"] == mutation.victim_guid]
    assert len(victims) == 1
    item = victims[0]
    assert item["match"]["reason"] == "guid"
    _assert_aspects(item, mutation.expected_victim_aspects)
    assert item["data_hash"]["old"] != item["data_hash"]["new"]
    for other in report["modified"]:
        if other["old"]["guid"] != mutation.victim_guid:
            assert other["change_scope"] == "transitive", other


def _assert_aspects(item: dict, expected: dict[str, str]) -> None:
    for aspect, state in expected.items():
        assert item["aspects"][aspect] == state, (aspect, item)
