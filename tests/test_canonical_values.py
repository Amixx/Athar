from athar.canonical_values import (
    CanonicalizationError,
    canonical_bag,
    canonical_list,
    canonical_scalar,
    canonical_select,
    canonical_set,
    canonical_simple,
    canonical_string,
    PROFILE_RAW_EXACT,
    PROFILE_SEMANTIC_STABLE,
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


def test_non_finite_floats_are_rejected():
    try:
        canonical_scalar(float("inf"))
        assert False, "expected CanonicalizationError"
    except CanonicalizationError:
        pass

    try:
        canonical_scalar(float("nan"))
        assert False, "expected CanonicalizationError"
    except CanonicalizationError:
        pass


def test_unknown_profile_is_rejected_for_float_scalars():
    try:
        canonical_scalar(1.25, profile="bad_profile")
        assert False, "expected CanonicalizationError"
    except CanonicalizationError:
        pass


def test_nested_aggregate_ordering_is_deterministic():
    a = canonical_set([
        [3, 1],
        [1, 2],
    ])
    b = canonical_set([
        [1, 2],
        [3, 1],
    ])
    assert canonical_string(a) == canonical_string(b)


def test_list_preserves_input_order():
    a = canonical_list([3, 1, 2])
    b = canonical_list([1, 2, 3])
    assert canonical_string(a) != canonical_string(b)


def test_semantic_profile_applies_quantizer_for_reals():
    value = canonical_scalar(
        1.23456,
        profile=PROFILE_SEMANTIC_STABLE,
        quantize=lambda v: round(v, 2),
    )
    assert value["kind"] == "real"
    assert value["value"] == "1.23"


def test_raw_exact_profile_ignores_quantizer_for_reals():
    value = canonical_scalar(
        1.23456,
        profile=PROFILE_RAW_EXACT,
        quantize=lambda _v: 99.0,
    )
    assert value["kind"] == "real"
    assert value["value"] == "1.2345600000000001"


def test_measure_wrapper_types_are_preserved():
    wrappers = [
        ("IfcAreaMeasure", 12.5),
        ("IfcVolumeMeasure", 3.75),
        ("IfcPlaneAngleMeasure", 1.57079632679),
    ]
    for wrapper_type, value in wrappers:
        wrapped = canonical_simple(wrapper_type, value)
        assert wrapped["kind"] == "simple"
        assert wrapped["type"] == wrapper_type


def test_semantic_stable_normalizes_measure_values_by_unit_type():
    unit_context = {
        "unit_factors": {
            "AREAUNIT": 0.09290304,  # ft2 -> m2
            "VOLUMEUNIT": 0.028316846592,  # ft3 -> m3
            "PLANEANGLEUNIT": 0.0174532925199433,  # degree -> rad
            "THERMALTRANSMITTANCEUNIT": 5.678263337,  # BTU/(h*ft2*F) -> W/(m2*K)
        }
    }
    area = canonical_simple(
        "IfcAreaMeasure",
        10.0,
        profile=PROFILE_SEMANTIC_STABLE,
        unit_context=unit_context,
    )
    volume = canonical_simple(
        "IfcVolumeMeasure",
        10.0,
        profile=PROFILE_SEMANTIC_STABLE,
        unit_context=unit_context,
    )
    angle = canonical_simple(
        "IfcPlaneAngleMeasure",
        180.0,
        profile=PROFILE_SEMANTIC_STABLE,
        unit_context=unit_context,
    )
    derived = canonical_simple(
        "IfcThermalTransmittanceMeasure",
        1.0,
        profile=PROFILE_SEMANTIC_STABLE,
        unit_context=unit_context,
    )
    assert area["value"]["value"] == "0.92902999999999991"
    assert volume["value"]["value"] == "0.28316799999999998"
    assert angle["value"]["value"] == "3.1415926500000002"
    assert derived["value"]["value"] == "5.6782633370000006"


def test_raw_exact_does_not_apply_measure_unit_normalization():
    unit_context = {"unit_factors": {"AREAUNIT": 0.09290304}}
    area = canonical_simple(
        "IfcAreaMeasure",
        10.0,
        profile=PROFILE_RAW_EXACT,
        unit_context=unit_context,
    )
    assert area["value"]["value"] == "10"
