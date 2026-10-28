import csv
import io
import zipfile
from pathlib import Path

import pytest

from app.models.trade import CoordSource
from app.sources.ports import (
    PortRecord,
    merge_natural_earth,
    merge_wpi,
    name_variants,
    parse_dms,
    parse_unlocode_coords,
    parse_unlocode_zip,
    parse_wpi_csv,
)


def test_unlocode_coords() -> None:
    assert parse_unlocode_coords("1305N 08017E") == (13.08333, 80.28333)
    assert parse_unlocode_coords("3352S 15113E") == (-33.86667, 151.21667)
    assert parse_unlocode_coords("5155N 00430W") == (51.91667, -4.5)
    assert parse_unlocode_coords("") is None
    assert parse_unlocode_coords("9999N 00000E") is None


def test_dms() -> None:
    assert parse_dms("22°43'42\"N") == 22.72833
    assert parse_dms("123°49'00\"W") == -123.81667
    assert parse_dms("garbage") is None


def test_name_variants() -> None:
    assert name_variants("Chennai (ex Madras)") == {"chennai", "madras"}
    assert "jawaharlal nehru" in name_variants("Jawaharlal Nehru Port")
    assert "jawaharlal nehru" in name_variants("Nhava Sheva (Jawaharlal Nehru)")


def _unlocode_zip(tmp_path: Path, rows: list[list[str]]) -> Path:
    buf = io.StringIO()
    csv.writer(buf).writerows(rows)
    p = tmp_path / "u.zip"
    with zipfile.ZipFile(p, "w") as z:
        z.writestr("release/csv/UNLOCODE CodeListPart1.csv", buf.getvalue())
    return p


def test_parse_unlocode_zip_filters_and_prefers_changed_rows(tmp_path: Path) -> None:
    rows = [
        ["", "IN", "", ".INDIA", "", "", "", "", "", "", "", ""],
        [
            "",
            "IN",
            "NSA",
            "Nhava Sheva (JN)",
            "Nhava Sheva (JN)",
            "MH",
            "1-------",
            "AA",
            "0002",
            "",
            "",
            "",
        ],
        [
            "#",
            "IN",
            "NSA",
            "Jawaharlal Nehru (Nhava Sheva)",
            "Jawaharlal Nehru (Nhava Sheva)",
            "MH",
            "1-------",
            "AA",
            "2501",
            "",
            "",
            "",
        ],
        [
            "",
            "IN",
            "MAA",
            "Chennai",
            "Chennai",
            "TN",
            "12345---",
            "AI",
            "0407",
            "",
            "1305N 08017E",
            "",
        ],
        ["", "IN", "XYZ", "Inland Town", "Inland Town", "", "--3-----", "AA", "", "", "", ""],
        ["X", "IN", "OLD", "Removed Port", "Removed Port", "", "1-------", "AA", "", "", "", ""],
    ]
    recs = parse_unlocode_zip(_unlocode_zip(tmp_path, rows))
    assert set(recs) == {"INNSA", "INMAA"}
    assert recs["INNSA"].name == "Jawaharlal Nehru (Nhava Sheva)"
    assert recs["INMAA"].is_seaport and recs["INMAA"].is_airport
    assert recs["INMAA"].coords[CoordSource.unlocode] == (13.08333, 80.28333)


def test_parse_unlocode_zip_layout_change(tmp_path: Path) -> None:
    p = tmp_path / "bad.zip"
    with zipfile.ZipFile(p, "w") as z:
        z.writestr("something.csv", "a,b")
    with pytest.raises(ValueError, match="layout changed"):
        parse_unlocode_zip(p)


def test_wpi_merge_overrides_unlocode_coordinates(tmp_path: Path) -> None:
    p = tmp_path / "wpi.csv"
    p.write_text(
        "﻿portNumber,portName,latitude,longitude,harborSize,unloCode\n"
        '48617,Mundra,"22°43\'42""N","69°42\'24""E",S,IN MUN\n',
        encoding="utf-8",
    )
    wpi = parse_wpi_csv(p)
    rec = PortRecord(
        "INMUN",
        "Mundra",
        "Mundra",
        "IN",
        None,
        True,
        False,
        coords={CoordSource.unlocode: (22.7, 69.7)},
    )
    assert merge_wpi({"INMUN": rec}, wpi) == 1
    src, lat, lon = rec.best_coords()  # type: ignore[misc]
    assert src == CoordSource.wpi and (lat, lon) == (22.72833, 69.70667)
    assert rec.harbor_size == "S"


def test_natural_earth_only_fills_plausible_matches() -> None:
    located = PortRecord(
        "INBOM",
        "Mumbai",
        "Mumbai",
        "IN",
        None,
        True,
        False,
        coords={CoordSource.unlocode: (18.967, 72.817)},
    )
    jnpt = PortRecord("INNSA", "Nhava Sheva (Jawaharlal Nehru)", "", "IN", None, True, False)
    # Same name exists in a country with no nearby located port → must not be filled.
    far = PortRecord("XXABC", "Victoria", "", "SC", None, True, False)
    records = {r.unlocode: r for r in (located, jnpt, far)}
    n = merge_natural_earth(
        records, [("Jawaharlal Nehru Port", 18.95, 72.93), ("Victoria", 48.42, -123.37)]
    )
    assert n == 1
    assert jnpt.coords[CoordSource.natural_earth] == (18.95, 72.93)
    assert not far.coords
