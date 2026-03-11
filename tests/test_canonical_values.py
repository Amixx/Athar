from athar.canonical_values import (
    canonical_bag,
    canonical_scalar,
    canonical_select,
    canonical_set,
    canonical_simple,
    canonical_string,
)


def test_float_canonicalization_normalizes_signed_zero():
    assert canonical_scalar(-0.0)["value"] == "0"
    assert canonical_scalar(0.0)["value"] == "0"


def test_set_ordering_is_deterministic():
    a = canonical_set([1, 2, 3])
    b = canonical_set([3, 2, 1])
    assert canonical_string(a) == canonical_string(b)


def test_bag_preserves_multiplicity():
    bag = canonical_bag([1, 2, 1])
    assert len(bag["items"]) == 3
    assert [item["value"] for item in bag["items"]] == [1, 1, 2]


def test_wrapper_and_select_are_explicit():
    wrapped = canonical_simple("IfcLabel", "Door A")
    assert wrapped["kind"] == "simple"
    assert wrapped["type"] == "IfcLabel"

    select = canonical_select("IfcBoolean", True)
    assert select["kind"] == "select"
    assert select["type"] == "IfcBoolean"
