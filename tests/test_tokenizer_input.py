"""STEP inputs written as raw bytes, diffed through the engine.

Each file starts from a minimal IFC4 model written by ifcopenshell and is
patched at the byte level, the way exporters that bypass a STEP writer
produce them.
"""

from __future__ import annotations

import re
import subprocess
import sys

import ifcopenshell
import pytest

from athar.engine import diff_files

WALL_GUID = "3sVXttebj56AU9d_bhqJcu"
PLACEHOLDER = b"ATHAR-VALUE"


@pytest.fixture(scope="module")
def template(tmp_path_factory) -> bytes:
    model = ifcopenshell.file(schema="IFC4")
    wall = model.createIfcWall(WALL_GUID, None, "Wall")
    value = model.createIfcPropertySingleValue(
        "Unit", None, model.createIfcLabel(PLACEHOLDER.decode()), None
    )
    pset = model.createIfcPropertySet("2$fVynv551rfghMBNKUIon", None, "Pset_Test", None, [value])
    model.createIfcRelDefinesByProperties("0dasYmpGnDngHYFCDgsWWJ", None, None, None, [wall], pset)
    path = tmp_path_factory.mktemp("template") / "template.ifc"
    model.write(str(path))
    return path.read_bytes()


def write(tmp_path, name: str, content: bytes) -> str:
    path = tmp_path / name
    path.write_bytes(content)
    return str(path)


def change_counts(report) -> tuple[int, int, int]:
    return tuple(report["stats"][section] for section in ("added", "deleted", "modified"))


def test_raw_byte_edit_is_a_data_change_on_the_owning_element(tmp_path, template):
    old = write(tmp_path, "old.ifc", template.replace(PLACEHOLDER, b"m\xb2"))
    new = write(tmp_path, "new.ifc", template.replace(PLACEHOLDER, b"m\xb3"))

    report = diff_files(old, new)

    assert change_counts(report) == (0, 0, 1)
    [item] = report["modified"]
    assert item["new"]["guid"] == WALL_GUID
    assert item["aspects"]["data"] == "changed"
    [delta] = item["data_delta"]
    assert delta["old"].endswith("m²")
    assert delta["new"].endswith("m³")


@pytest.mark.parametrize(
    ("raw", "escaped", "text"),
    [
        (b"H\xf6he", b"H\\X\\F6he", "Höhe"),
        (b"m\xb2", b"m\\X\\B2", "m²"),
        (b"a\x80b", b"a\\X\\80b", "a\x80b"),
        (b"ab\xc3", b"ab\\X\\C3", "abÃ"),
        (b"H\xc3\xb6he", b"H\\X2\\00F6\\X0\\he", "Höhe"),
    ],
    ids=["latin1-lead-byte", "latin1-superscript", "cp1252-0x80", "lone-lead-byte-before-quote", "utf8"],
)
def test_raw_bytes_hash_like_their_escaped_twin(tmp_path, template, raw, escaped, text):
    raw_path = write(tmp_path, "raw.ifc", template.replace(PLACEHOLDER, raw))
    escaped_path = write(tmp_path, "escaped.ifc", template.replace(PLACEHOLDER, escaped))

    [value] = ifcopenshell.open(escaped_path).by_type("IfcPropertySingleValue")
    assert value.NominalValue.wrappedValue == text
    assert change_counts(diff_files(raw_path, escaped_path)) == (0, 0, 0)


def test_data_keyword_inside_header_does_not_start_the_data_section(tmp_path, template):
    header = template.replace(b"ViewDefinition [CoordinationView]", b"DATA; #1=IFCWALL($);")
    assert header != template
    report = diff_files(write(tmp_path, "header.ifc", header), write(tmp_path, "plain.ifc", template))

    assert change_counts(report) == (0, 0, 0)
    assert report["stats"]["unchanged"] == 1


def test_lowercase_keywords_parse_as_their_upper_case_class(tmp_path, template):
    lowercase = template.replace(b"=IFCWALL(", b"=IfcWall(").replace(b"IFCLABEL(", b"ifclabel(")
    assert lowercase != template
    report = diff_files(write(tmp_path, "lower.ifc", lowercase), write(tmp_path, "upper.ifc", template))

    assert change_counts(report) == (0, 0, 0)
    assert report["stats"]["unchanged"] == 1


def test_truncated_file_error_names_offset_and_record(tmp_path, template):
    match = re.search(rb"#(\d+)=IFCPROPERTYSET\('", template)
    quote = match.end() - 1
    truncated = write(tmp_path, "truncated.ifc", template[: quote + 5])

    with pytest.raises(ValueError) as error:
        diff_files(truncated, write(tmp_path, "full.ifc", template))
    assert str(error.value) == f"unterminated string at byte {quote} in record #{match.group(1).decode()}"
    result = subprocess.run(
        [sys.executable, "-m", "athar", truncated, str(tmp_path / "full.ifc")],
        capture_output=True, text=True,
    )
    assert result.returncode == 1
    assert result.stderr == f"Error: {error.value}\n"
