"""Duplicated property sets must surface in the report.

A merge that copies property sets leaves two shapes behind: copies that no
object references (orphans, which have no signature of their own) and an
object defined by a second copy of a set it already has (identical property
names and values, so a name-keyed property comparison sees no change).
"""

from __future__ import annotations

import ifcopenshell
import ifcopenshell.util.element
import ifcopenshell.guid
import pytest

from athar.engine import diff_files
from athar_dev.ifc_mutations import _pick_exclusive_string_property
from athar_git.render import render_text_report
from tests.corpus import assert_report_invariants, assert_zero_diff, corpus_path


@pytest.fixture(scope="module")
def seed(tmp_path_factory):
    seed_path = corpus_path("gni_9")
    out = tmp_path_factory.mktemp("dup-psets")
    baseline = str(out / "baseline.ifc")
    ifcopenshell.open(seed_path).write(baseline)

    def write(name, edit):
        model = ifcopenshell.open(seed_path)
        victim, prop = _pick_exclusive_string_property(model)
        pset = next(ref for ref in model.get_inverse(prop) if ref.is_a("IfcPropertySet"))
        copy = model.create_entity(
            "IfcPropertySet",
            GlobalId=ifcopenshell.guid.new(),
            OwnerHistory=pset.OwnerHistory,
            Name=pset.Name,
            Description=pset.Description,
            HasProperties=[ifcopenshell.util.element.copy_deep(model, prop) for prop in pset.HasProperties],
        )
        edit(model, victim, copy)
        path = str(out / f"{name}.ifc")
        model.write(path)
        return path, victim, pset

    return baseline, write


def test_orphan_property_set_copy_is_reported_added_and_removed(seed):
    baseline, write = seed
    mutated, _, pset = write("orphan", lambda model, victim, copy: None)

    report = diff_files(baseline, mutated)
    assert_report_invariants(report)
    assert_zero_diff(report)
    [added] = report["orphan_property_sets"]["added"]
    assert added["name"] == pset.Name
    assert report["orphan_property_sets"]["removed"] == []
    assert report["stats"]["orphan_property_sets"]["added"] == 1
    assert "Unattached property sets: +1 -0" in render_text_report(report)
    assert "No semantic entity changes." not in render_text_report(report)

    reverse = diff_files(mutated, baseline)
    assert reverse["orphan_property_sets"]["added"] == []
    assert [item["name"] for item in reverse["orphan_property_sets"]["removed"]] == [pset.Name]


def test_orphans_with_regenerated_guids_are_unchanged(seed):
    baseline, write = seed
    old, _, _ = write("orphan-old", lambda model, victim, copy: None)
    new, _, _ = write("orphan-new", lambda model, victim, copy: None)

    report = diff_files(old, new)
    assert report["stats"]["orphan_property_sets"] == {"old": 1, "new": 1, "added": 0, "removed": 0}
    assert "No semantic entity changes." in render_text_report(report)


def test_second_identical_property_set_on_an_object_is_a_property_count_change(seed):
    baseline, write = seed

    def attach(model, victim, copy):
        model.create_entity(
            "IfcRelDefinesByProperties",
            GlobalId=ifcopenshell.guid.new(),
            RelatedObjects=[victim],
            RelatingPropertyDefinition=copy,
        )

    mutated, victim, pset = write("attached", attach)

    report = diff_files(baseline, mutated)
    assert_report_invariants(report)
    [item] = [item for item in report["modified"] if item["new"]["guid"] == victim.GlobalId]
    assert item["aspects"]["data"] == "changed"
    count_changed = item["property_deltas"]["count_changed"]
    assert {entry["name"] for entry in count_changed} == {prop.Name for prop in pset.HasProperties}
    assert all((entry["old_count"], entry["new_count"]) == (1, 2) for entry in count_changed)
    assert report["orphan_property_sets"] == {"added": [], "removed": []}
