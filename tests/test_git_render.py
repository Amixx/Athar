import re

from athar_git.render import markdown_code, render_markdown_report, render_text_report


def _outside_code(body: str) -> str:
    """The Markdown body with fenced blocks and code spans removed."""
    without_blocks = re.sub(r"^(`{3,})text\n.*?\n\1$", "", body, flags=re.MULTILINE | re.DOTALL)
    return re.sub(r"(`+)(?!`).*?(?<!`)\1(?!`)", "", without_blocks)


def test_inline_code_fence_exceeds_content_backticks():
    assert markdown_code('`a``</details>') == '``` `a``</details> ```'


def test_markdown_keeps_hostile_names_and_values_inside_code():

    hostile = '```\n</details><script>alert(1)</script>\n````'
    report = {
        "schemas": {"old": "IFC4</details>", "new": "IFC4"},
        "stats": {"added": 1},
        "added": [{"class": "IfcWall", "step_id": 1, "name": hostile}],
        "orphan_property_sets": {"added": [{"name": "</details><script>"}]},
    }
    body = render_markdown_report(report)
    assert "`````text\n" in body
    assert "\n`````\n" in body
    assert body.count("\n</details>") == 1
    assert "`</details><script>`" in body
    assert "`IFC4</details>`" in body


def test_render_text_prioritizes_property_deltas_and_keeps_uncovered_data():
    report = {
        "schemas": {"old": "IFC4", "new": "IFC4"},
        "stats": {"modified": 1},
        "modified": [{
            "old": {"class": "IfcWall", "step_id": 1},
            "new": {"class": "IfcWall", "step_id": 2},
            "match": {"reason": "guid", "score": 0.9},
            "aspects": {"data": "changed"},
            "property_deltas": {"changed": [{"pset": "Pset_Wall", "name": "Height", "old_value": 2, "new_value": 3}]},
            "data_delta": [
                {"path": "IfcPropertySingleValue.NominalValue[10]", "old": 2, "new": 3},
                {"path": "IfcWall.Description", "old": "old", "new": "new"},
            ],
        }],
    }
    body = render_text_report(report)
    assert "property: Pset_Wall.Height: 2 -> 3" in body
    assert "data: IfcPropertySingleValue.NominalValue" not in body
    assert "data: IfcWall.Description: old -> new" in body


def test_render_text_caps_each_section_independently():
    report = {"stats": {"added": 3, "deleted": 2, "modified": 4}}
    report["deleted"] = [{"class": "IfcDoor", "step_id": n} for n in range(2)]
    report["added"] = [{"class": "IfcWindow", "step_id": n + 10} for n in range(3)]
    report["modified"] = [{"old": {}, "new": {"class": "IfcWall", "step_id": n + 20}} for n in range(4)]
    body = render_text_report(report, max_items=1)
    assert body.index("\nDeleted (") < body.index("\nAdded (") < body.index("\nModified (")
    assert "- IfcDoor #0" in body and "+ IfcWindow #10" in body and "~ IfcWall #20" in body
    assert "1 more deleted" in body and "2 more added" in body and "3 more modified" in body
    assert "IfcDoor #1" not in body and "IfcWindow #11" not in body and "IfcWall #21" not in body


def test_render_text_orders_sections_and_summarizes_placement_cohort():
    report = {
        "schemas": {"old": "IFC4", "new": "IFC4"},
        "stats": {"deleted": 1, "added": 1, "modified": 2},
        "deleted": [{"class": "IfcDoor", "step_id": 1}],
        "added": [{"class": "IfcWindow", "step_id": 2}],
        "modified": [
            {"old": {"class": "IfcWall", "step_id": 3}, "new": {"class": "IfcWall", "step_id": 13}, "aspects": {"placement": "changed", "placement_delta_mm": [100, 0, 0]}},
            {"old": {"class": "IfcWall", "step_id": 4}, "new": {"class": "IfcWall", "step_id": 14}, "aspects": {"placement": "changed", "placement_delta_mm": [100, 0, 0]}},
        ],
        "stats": {"deleted": 1, "added": 1, "modified": 2, "placement_cohorts": [{"count": 2, "members": [13, 14], "delta_mm": [100, 0, 0]}]},
    }
    body = render_text_report(report)
    assert "2 elements moved by (0.1, 0, 0) m" in body
    assert body.index("\nDeleted (") < body.index("\nAdded (")
    assert "IfcWall #13" not in body and "IfcWall #14" not in body
    report['modified'][1]['aspects']['data'] = 'changed'
    report['modified'][1]['property_deltas'] = {'changed': [{'pset': 'P', 'name': 'Rating', 'old_value': 'A', 'new_value': 'B'}]}
    report['modified'].append({'old': {}, 'new': {'class': 'IfcBeam', 'step_id': 15}, 'aspects': {'placement': 'changed', 'placement_delta_mm': [-1000, 2500, 0]}})
    body = render_text_report(report)
    assert 'IfcWall #14 [data]' in body and 'P.Rating: A -> B' in body
    assert body.count('placement vector:') == 1
    assert 'placement vector: (-1, 2.5, 0) m' in body
    member = report['modified'][0]
    for aspect in ('relations', 'class'):
        member['aspects'][aspect] = 'changed'
        assert f'IfcWall #13 [{aspect}]' in render_text_report(report)
        del member['aspects'][aspect]
    member['identity'] = {'guid_changed': True, 'old_guid': 'old', 'new_guid': 'new'}
    assert '  GlobalId: old -> new' in render_text_report(report)


def test_render_text_only_explains_non_guid_matches():
    report = {"stats": {"modified": 2}, "modified": [
        {"old": {}, "new": {"class": "IfcWall", "step_id": 1}, "match": {"reason": "guid", "score": 0.9}},
        {"old": {}, "new": {"class": "IfcWall", "step_id": 2}, "match": {"reason": "geometry_hash", "score": 0.8}},
    ]}
    body = render_text_report(report)
    assert 'match: guid' not in body
    assert 'match: geometry_hash score 0.8' in body
    assert '~ IfcWall #1' in body and '~ IfcWall #2' in body


def test_render_text_uses_human_quantity_values_or_labels_quantized_values():
    item = {
        "old": {}, "new": {"class": "IfcWall", "step_id": 1},
        "aspects": {"data": "changed"},
        "property_deltas": {"changed": [{"pset": "Qto_WallBaseQuantities", "name": "NetVolume", "old_value": 660.944217, "new_value": 660.944233}]},
        "data_delta": [{"path": "IfcQuantityVolume.VolumeValue", "old": "660944217", "new": "660944233"}],
    }
    report = {"stats": {"modified": 1}, "modified": [item]}
    body = render_text_report(report)
    assert 'Qto_WallBaseQuantities.NetVolume: 660.944217 -> 660.944233' in body
    assert 'data: IfcQuantityVolume' not in body
    del item['property_deltas']
    body = render_text_report(report)
    assert 'quantized; human value unavailable' in body
    assert '660944217 -> 660944233' in body


def test_render_text_formats_numbers_without_losing_small_edits():
    report = {
        "stats": {"modified": 1234, "unchanged": 10000},
        "modified": [{
            "old": {}, "new": {"class": "IfcWall", "step_id": 1},
            "aspects": {"data": "changed", "placement": "changed", "placement_delta_mm": [-214000, -299000, 1000]},
            "property_deltas": {"changed": [
                {"pset": "P", "name": "Tiny", "old_value": 0.000001, "new_value": 0.000002},
                {"pset": "P", "name": "Big", "old_value": 9007199254740993, "new_value": 9007199254740994},
            ]},
        }],
    }
    body = render_text_report(report)
    assert '~1,234 =10,000' in body
    assert '0.000001 -> 0.000002' in body
    assert '9007199254740993 -> 9007199254740994' in body
    assert 'placement vector: (-214, -299, 1) m' in body
    assert 'placement delta 367.693 m' in body
    assert ' mm]' not in body


HOSTILE_MARKDOWN = [
    "[click](https://evil.example/x)",
    "![pixel](https://evil.example/p.png)",
    "@octocat",
    "@acme/security-team",
    "Pset\n## Policy gate: `pass`",
]


def test_markdown_renders_ifc_names_only_inside_code():
    report = {
        "schemas": {"old": "IFC4", "new": "IFC4"},
        "stats": {"modified": 1, "added": 1},
        "added": [{"class": name, "step_id": 1, "name": name} for name in HOSTILE_MARKDOWN],
        "modified": [{"old": {}, "new": {"class": HOSTILE_MARKDOWN[0], "step_id": 2, "name": HOSTILE_MARKDOWN[4]}}],
        "orphan_property_sets": {"added": [{"name": name} for name in HOSTILE_MARKDOWN]},
    }
    body = render_markdown_report(report)
    outside = _outside_code(body)
    assert "Unattached property sets: +5 -0" in outside
    for live in ("](", "@", "Policy gate"):
        assert live not in outside
    assert not re.search(r"^#", outside, flags=re.MULTILINE)


def _property_edit_pair(tmp_path, edit):
    import ifcopenshell

    from athar_dev.ifc_mutations import _pick_exclusive_string_property
    from tests.corpus import corpus_path

    paths = []
    for side in ("old", "new"):
        model = ifcopenshell.open(corpus_path("gni_9"))
        victim, prop = _pick_exclusive_string_property(model)
        pset = next(ref for ref in model.get_inverse(prop) if ref.is_a("IfcPropertySet"))
        extra = [
            model.createIfcPropertySingleValue("", None, model.createIfcLabel("unnamed-old"), None),
            model.createIfcPropertySingleValue(prop.Name, None, model.createIfcLabel("first-old"), None),
        ]
        pset.HasProperties = [*extra, *pset.HasProperties]
        if side == "new":
            edit(prop, *extra)
        path = tmp_path / f"{side}.ifc"
        model.write(str(path))
        paths.append(str(path))
    return paths, prop.Name


def test_render_text_shows_raw_changes_the_property_deltas_do_not_name(tmp_path):
    from athar.engine import diff_files

    def edit(named, unnamed, duplicate):
        named.NominalValue.wrappedValue += "-edited"
        unnamed.NominalValue.wrappedValue = "unnamed-new"
        duplicate.NominalValue.wrappedValue = "first-new"

    (old, new), name = _property_edit_pair(tmp_path, edit)
    body = render_text_report(diff_files(old, new))

    assert f".{name}: " in body and "-edited" in body
    assert "IFCLABEL: unnamed-old" in body and "IFCLABEL: unnamed-new" in body
    assert "IFCLABEL: first-old" in body and "IFCLABEL: first-new" in body


def test_render_text_hides_raw_rows_the_property_deltas_account_for(tmp_path):
    from athar.engine import diff_files

    def edit(named, unnamed, duplicate):
        named.NominalValue.wrappedValue += "-edited"

    (old, new), name = _property_edit_pair(tmp_path, edit)
    body = render_text_report(diff_files(old, new))

    assert f".{name}: " in body and "-edited" in body
    assert "data: IfcPropertySingleValue" not in body


def test_markdown_shows_raw_boolean_row_when_a_string_property_delta_has_the_same_text():
    nominal = "IfcPropertySingleValue.NominalValue"
    report = {
        "schemas": {"old": "IFC4", "new": "IFC4"},
        "stats": {"modified": 1},
        "modified": [{
            "old": {"class": "IfcWall", "step_id": 1},
            "new": {"class": "IfcWall", "step_id": 2},
            "match": {"reason": "guid", "score": 0.9},
            "aspects": {"data": "changed"},
            "property_deltas": {"changed": [{"pset": "P", "name": "Named", "old_value": "false", "new_value": "true"}]},
            "data_delta": [
                {"path": f"{nominal}[1]", "old": "IFCLABEL: false", "new": "IFCLABEL: true"},
                {"path": f"{nominal}[2]", "old": "IFCLABEL: true", "new": "IFCLABEL: false"},
                {"path": f"{nominal}[3]", "old": "IFCBOOLEAN: false", "new": "IFCBOOLEAN: true"},
            ],
        }],
    }
    body = render_markdown_report(report)
    assert "IFCBOOLEAN: false -> IFCBOOLEAN: true" in body


def test_render_text_caps_placement_cohort_lines_largest_first():
    cohorts = [{"count": 2 + (n == 7), "members": [2 * n, 2 * n + 1], "delta_mm": [n + 1, 0, 0]} for n in range(300)]
    report = {"stats": {"modified": 600, "placement_cohorts": cohorts}}

    body = render_text_report(report, max_items=10)

    assert body.count(" elements moved by ") == 10
    assert body.index("3 elements moved by (0.008, 0, 0) m") < body.index("2 elements moved by")
    assert "... and 290 more placement cohorts (580 elements) not shown" in body

    hostile = "```\n</details><script>"
    for cohort in cohorts:
        cohort['root_cause'] = {'class': 'IfcBuilding', 'name': hostile, 'step_id': 1}
        cohort['rotation_deg'] = 90
    body = render_markdown_report(report, max_items=10)
    assert body.count('Group move:') == 10
    assert ', 90°' in body
    assert '290 more placement cohorts' in body
    assert hostile not in body
    assert '<script>' not in _outside_code(body)
    assert render_text_report(report, max_cohorts=0).count('Group move:') == 0


MIDDLE_DOTS = "\u00b7\u2219\u22c5"


def test_renderer_labels_contain_no_middle_dots():
    import athar_git.pr_bot
    import athar_git.render
    from pathlib import Path

    for module in (athar_git.render, athar_git.pr_bot):
        source = Path(module.__file__).read_text(encoding="utf-8")
        assert not any(char in source for char in MIDDLE_DOTS), module.__name__

    report = {
        "schemas": {"old": "IFC4", "new": "IFC4"},
        "stats": {
            "added": 2, "deleted": 2, "modified": 4, "unchanged": 1,
            "modified_change_scope": {"intrinsic": 1, "mixed": 1, "transitive": 1},
            "matcher_diagnostics": {"tiers": {"guid": 3, "geometry_hash": 1}},
            "placement_cohorts": [{"count": 2, "members": [40, 41], "delta_mm": [5, 0, 0]},
                                  {"count": 2, "members": [42, 43], "delta_mm": [6, 0, 0]}],
        },
        "added": [{"class": "IfcDoor", "step_id": 1, "name": "D"}, {"class": "IfcDoor", "step_id": 2}],
        "deleted": [{"class": "IfcWindow", "step_id": 3}, {"class": "IfcWindow", "step_id": 4}],
        "modified": [{
            "old": {"class": "IfcWall", "step_id": 9, "guid": "g"}, "new": {"class": "IfcWall", "step_id": 10, "name": "W"},
            "match": {"reason": "geometry_hash", "score": 0.8},
            "aspects": {"data": "changed", "geometry": "changed", "placement": "changed", "placement_delta_mm": [2000, 0, 0]},
            "property_deltas": {
                "changed": [{"pset": "P", "name": "A", "old_value": 1, "new_value": 2}],
                "added": [{"pset": "P", "name": "B", "new_value": "x"}],
                "removed": [{"pset": "P", "name": "C", "old_value": None}],
                "count_changed": [{"pset": "P", "name": "A", "old_count": 1, "new_count": 2}],
            },
            "data_delta": [{"path": f"IfcQuantityLength.LengthValue[{n}]", "old": str(n), "new": None} for n in range(7)],
        }] * 4,
        "orphan_property_sets": {"added": [{"name": "O"}], "removed": [{}]},
    }
    for body in (
        render_text_report(report, max_items=1, max_cohorts=1),
        render_text_report(report, max_items=1, details=False),
        render_markdown_report(report, max_items=1, max_cohorts=1),
        render_markdown_report({"stats": {}}),
    ):
        assert not any(char in body for char in MIDDLE_DOTS), body


def test_render_shows_model_text_with_middle_dots_unchanged():
    report = {"stats": {"modified": 1}, "modified": [{
        "old": {}, "new": {"class": "IfcWall", "step_id": 1, "name": "W\u00b71"},
        "aspects": {"data": "changed"},
        "property_deltas": {"changed": [{"pset": "P\u2219", "name": "N\u22c5", "old_value": "a\u00b7b", "new_value": "a-b"}]},
        "data_delta": [{"path": "IfcWall.Description", "old": "c\u00b7d", "new": "c-d"}],
    }]}

    for body in (render_text_report(report), render_markdown_report(report)):
        assert '"W\u00b71"' in body
        assert "P\u2219.N\u22c5: a\u00b7b -> a-b" in body
        assert "IfcWall.Description: c\u00b7d -> c-d" in body


def test_render_groups_digits_only_in_counts_and_unit_values():
    report = {"stats": {"modified": 1234, "placement_cohorts": [
        {"count": 2, "members": [1, 2], "delta_mm": [1234567, 0, 0]}]}, "modified": [{
        "old": {}, "new": {"class": "IfcWall", "step_id": 3},
        "aspects": {"data": "changed"},
        "property_deltas": {"changed": [{"pset": "P", "name": "Year", "old_value": 2024, "new_value": 2025},
                                        {"pset": "P", "name": "Ratio", "old_value": 12345.5, "new_value": 0.000001}]},
        "data_delta": [{"path": "IfcWall.Tag", "old": 100000, "new": 100001}],
    }]}

    body = render_text_report(report)

    assert "~1,234 " in body
    assert "2 elements moved by (1,234.567, 0, 0) m" in body
    assert "P.Year: 2024 -> 2025" in body
    assert "P.Ratio: 12345.5 -> 0.000001" in body
    assert "IfcWall.Tag: 100000 -> 100001" in body


def test_render_escapes_newlines_in_values_and_property_labels():
    fake = "\n~ IfcWall #99 [data]\r\n  property: P.Fake: 1 -> 2"
    report = {"stats": {"modified": 1}, "modified": [{
        "old": {"guid": "g" + fake}, "new": {"class": "IfcWall", "step_id": 1},
        "match": {"reason": "geometry_hash", "score": 0.8},
        "aspects": {"data": "changed"},
        "property_deltas": {"changed": [{"pset": "P" + fake, "name": "N" + fake, "old_value": "a" + fake, "new_value": "b"}]},
        "data_delta": [{"path": "IfcWall.Description" + fake, "old": "c" + fake, "new": "d"}],
    }]}

    for body in (render_text_report(report), render_markdown_report(report)):
        assert "\r" not in body
        assert not re.search(r"^~ IfcWall #99", body, flags=re.MULTILINE)
        assert not re.search(r"^  property: P\.Fake", body, flags=re.MULTILINE)
        assert "a\\n~ IfcWall #99 [data]\\r\\n  property: P.Fake: 1 -> 2 -> b" in body
