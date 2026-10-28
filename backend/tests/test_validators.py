import pytest

from app.validators import (
    container_check_digit,
    normalize_container,
    normalize_country,
    normalize_imo,
    normalize_locode,
)


def test_container_check_digit_reference_example() -> None:
    # The worked example from ISO 6346: CSQU305438 → check digit 3.
    assert container_check_digit("CSQU305438") == 3
    assert normalize_container("csqu 305438-3") == "CSQU3054383"


@pytest.mark.parametrize("bad", ["CSQU3054384", "CSQ3054383", "CSQX3054383", "1234567890A"])
def test_container_rejects_bad(bad: str) -> None:
    with pytest.raises(ValueError):
        normalize_container(bad)


def test_imo_check_digit() -> None:
    # IMO 9074729: 9*7 + 0*6 + 7*5 + 4*4 + 7*3 + 2*2 = 139 → 9
    assert normalize_imo("IMO 9074729") == "9074729"
    with pytest.raises(ValueError):
        normalize_imo("9074728")
    with pytest.raises(ValueError):
        normalize_imo("907472")


@pytest.mark.parametrize(
    ("raw", "expected"),
    [("IN", "IN"), ("ind", "IN"), ("India", "IN"), ("netherlands", "NL"), (" ae ", "AE")],
)
def test_country(raw: str, expected: str) -> None:
    assert normalize_country(raw) == expected


def test_country_unknown() -> None:
    with pytest.raises(ValueError):
        normalize_country("Atlantis")


def test_locode() -> None:
    assert normalize_locode("in nsa") == "INNSA"
    with pytest.raises(ValueError):
        normalize_locode("IN-NSA1")
