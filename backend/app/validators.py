"""Domain validators shared by the API schemas and the CSV importer."""

import re

import pycountry

_CONTAINER_RE = re.compile(r"^[A-Z]{3}[UJZR][0-9]{7}$")
_LOCODE_RE = re.compile(r"^[A-Z]{2}[A-Z2-9]{3}$")


def _letter_values() -> dict[str, int]:
    """ISO 6346: A=10, B=12, ... counting upward and skipping multiples of 11."""
    values, v = {}, 10
    for ch in "ABCDEFGHIJKLMNOPQRSTUVWXYZ":
        if v % 11 == 0:
            v += 1
        values[ch] = v
        v += 1
    return values


_LETTER_VALUES = _letter_values()


def normalize_country(value: str) -> str:
    """Accepts ISO alpha-2, alpha-3 or an English country name; returns alpha-2."""
    v = value.strip()
    if not v:
        raise ValueError("country is required")
    match = (
        pycountry.countries.get(alpha_2=v.upper())
        if len(v) == 2
        else pycountry.countries.get(alpha_3=v.upper())
        if len(v) == 3
        else None
    )
    if match is None:
        try:
            match = pycountry.countries.lookup(v)
        except LookupError:
            match = None
    if match is None:
        raise ValueError(f"unknown country '{value}'")
    return str(match.alpha_2)


def country_name(alpha2: str) -> str:
    c = pycountry.countries.get(alpha_2=alpha2)
    return str(c.name) if c else alpha2


def normalize_locode(value: str) -> str:
    v = value.replace(" ", "").strip().upper()
    if not _LOCODE_RE.match(v):
        raise ValueError(f"'{value}' is not a UN/LOCODE (e.g. INNSA)")
    return v


def container_check_digit(code10: str) -> int:
    total = 0
    for i, ch in enumerate(code10):
        val = _LETTER_VALUES[ch] if ch.isalpha() else int(ch)
        total += val * (2**i)
    return total % 11 % 10


def normalize_container(value: str) -> str:
    """ISO 6346 container number, e.g. MSKU1234565, with check-digit validation."""
    v = re.sub(r"[\s-]", "", value).upper()
    if not _CONTAINER_RE.match(v):
        raise ValueError(f"'{value}' is not an ISO 6346 container number")
    if container_check_digit(v[:10]) != int(v[10]):
        raise ValueError(f"'{value}' has an invalid check digit")
    return v


def normalize_imo(value: str) -> str:
    """IMO ship identification number (7 digits with check digit)."""
    v = value.strip().upper().removeprefix("IMO").strip()
    if not re.fullmatch(r"[0-9]{7}", v):
        raise ValueError(f"'{value}' is not a 7-digit IMO number")
    check = sum(int(d) * w for d, w in zip(v[:6], range(7, 1, -1), strict=True)) % 10
    if check != int(v[6]):
        raise ValueError(f"'{value}' has an invalid IMO check digit")
    return v
