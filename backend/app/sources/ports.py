"""Port reference data.

Primary list: UN/LOCODE (UNECE) — every seaport (function 1) and airport (function 4).
Coordinates, best first: manual admin override > NGA World Port Index > UN/LOCODE > Natural Earth.
Ports with no coordinates from any source stay selectable but are flagged in the UI, and
cannot be routed until an admin sets them.
"""

import csv
import io
import math
import re
import unicodedata
import zipfile
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import shapefile  # pyshp
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.models.trade import COORD_PRIORITY, CoordSource
from app.sources.base import SourceSpec, register

UNLOCODE = register(
    SourceSpec(
        code="ports_unlocode",
        name="UN/LOCODE (UNECE)",
        url="https://unlocode.unece.org/downloads/unlocode-latest.zip",
        licence_note=(
            "Published free of charge by UNECE as the UN standard for trade location codes. "
            "UNECE does not publish an explicit data licence; confirm with UNECE before "
            "redistributing the raw list."
        ),
        attribution="UN/LOCODE, United Nations Economic Commission for Europe (UNECE)",
        stale_after_hours=24 * 21,
    )
)
WPI = register(
    SourceSpec(
        code="ports_wpi",
        name="NGA World Port Index (Pub. 150)",
        url="https://msi.nga.mil/api/publications/world-port-index?output=csv",
        licence_note="US Government work (NGA); no copyright claimed. Free for commercial use.",
        attribution="World Port Index, US National Geospatial-Intelligence Agency",
        stale_after_hours=24 * 21,
    )
)
NATURAL_EARTH = register(
    SourceSpec(
        code="ports_natural_earth",
        name="Natural Earth 1:10m ports",
        url="https://naciscdn.org/naturalearth/10m/cultural/ne_10m_ports.zip",
        licence_note="Public domain (Natural Earth terms of use).",
        attribution="Made with Natural Earth",
        stale_after_hours=24 * 60,
    )
)

# Natural Earth points are matched by name only, so they must land near a known port of
# the same country to be trusted.
NE_MAX_KM_FROM_COUNTRY_PORT = 500


@dataclass
class PortRecord:
    unlocode: str
    name: str
    name_ascii: str
    country_iso2: str
    subdivision: str | None
    is_seaport: bool
    is_airport: bool
    coords: dict[CoordSource, tuple[float, float]] = field(default_factory=dict)  # (lat, lon)
    wpi_port_number: int | None = None
    harbor_size: str | None = None

    def best_coords(self) -> tuple[CoordSource, float, float] | None:
        if not self.coords:
            return None
        src = min(self.coords, key=lambda s: COORD_PRIORITY[s])
        lat, lon = self.coords[src]
        return src, lat, lon


# ---- parsing helpers (pure, unit-tested) ---------------------------------------------


_UNLOCODE_COORD = re.compile(r"^(\d{2})(\d{2})([NS])\s+(\d{3})(\d{2})([EW])$")
_DMS = re.compile(r"(\d+)\D+(\d+)\D+(\d+(?:\.\d+)?)\D*([NSEW])")


def parse_unlocode_coords(value: str) -> tuple[float, float] | None:
    m = _UNLOCODE_COORD.match(value.strip())
    if not m:
        return None
    lat = int(m[1]) + int(m[2]) / 60
    lon = int(m[4]) + int(m[5]) / 60
    if m[3] == "S":
        lat = -lat
    if m[6] == "W":
        lon = -lon
    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        return None
    return round(lat, 5), round(lon, 5)


def parse_dms(value: str) -> float | None:
    m = _DMS.search(value)
    if not m:
        return None
    v = int(m[1]) + int(m[2]) / 60 + float(m[3]) / 3600
    return round(-v if m[4] in "SW" else v, 5)


def ascii_name(name: str) -> str:
    return unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode().strip()


_NAME_NOISE = re.compile(r"\b(port|of|harbou?r|terminal|the)\b")


def name_variants(name: str) -> set[str]:
    """'Chennai (ex Madras)' -> {'chennai', 'madras'}; 'Jawaharlal Nehru Port' -> {...nehru}."""
    base = ascii_name(name).lower()
    parts = [re.sub(r"\(.*?\)", "", base), *re.findall(r"\((.*?)\)", base)]
    out = set()
    for p in parts:
        p = re.sub(r"^\s*ex\s+", "", p)
        p = _NAME_NOISE.sub(" ", p)
        p = re.sub(r"[^a-z0-9 ]+", " ", p)
        p = " ".join(p.split())
        if len(p) >= 3:
            out.add(p)
    return out


def haversine_km(a: tuple[float, float], b: tuple[float, float]) -> float:
    lat1, lon1, lat2, lon2 = map(math.radians, (*a, *b))
    h = (
        math.sin((lat2 - lat1) / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2
    )
    return 2 * 6371 * math.asin(math.sqrt(h))


# ---- source parsers -------------------------------------------------------------------


def parse_unlocode_zip(path: Path) -> dict[str, PortRecord]:
    """Columns: Change, Country, Location, Name, NameWoDiacritics, Subdivision, Function,
    Status, Date, IATA, Coordinates, Remarks."""
    records: dict[str, PortRecord] = {}
    preferred: dict[str, bool] = {}
    with zipfile.ZipFile(path) as z:
        names = sorted(n for n in z.namelist() if re.search(r"CodeListPart\d\.csv$", n))
        if not names:
            raise ValueError("UN/LOCODE archive layout changed: no CodeListPart*.csv files")
        for n in names:
            raw = z.read(n)
            try:
                content = raw.decode("utf-8")
            except UnicodeDecodeError:
                content = raw.decode("cp1252")
            for row in csv.reader(io.StringIO(content)):
                if len(row) < 11 or not row[2]:
                    continue
                change, country, loc, name, name_wo, subdiv, func = row[:7]
                if change in ("X", "="):  # marked for removal / reference-only entry
                    continue
                func = func.ljust(8, "-")
                is_sea, is_air = func[0] == "1", func[3] == "4"
                if not (is_sea or is_air):
                    continue
                code = f"{country}{loc}"
                # Rows flagged as changed (#, |, +) are the newer version of a duplicate code.
                is_changed = change in ("#", "|", "+")
                if code in records and preferred.get(code) and not is_changed:
                    continue
                rec = PortRecord(
                    unlocode=code,
                    name=name.strip(),
                    name_ascii=(name_wo or ascii_name(name)).strip(),
                    country_iso2=country,
                    subdivision=subdiv or None,
                    is_seaport=is_sea,
                    is_airport=is_air,
                )
                c = parse_unlocode_coords(row[10])
                if c:
                    rec.coords[CoordSource.unlocode] = c
                records[code] = rec
                preferred[code] = is_changed
    return records


def parse_wpi_csv(path: Path) -> dict[str, dict]:
    """Returns WPI rows keyed by UN/LOCODE ('IR ABD' -> 'IRABD')."""
    out: dict[str, dict] = {}
    with open(path, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames or "unloCode" not in reader.fieldnames:
            raise ValueError("WPI CSV layout changed: no unloCode column")
        for row in reader:
            code = (row.get("unloCode") or "").replace(" ", "").upper()
            if len(code) != 5:
                continue
            lat, lon = parse_dms(row.get("latitude", "")), parse_dms(row.get("longitude", ""))
            if lat is None or lon is None:
                continue
            out[code] = {
                "lat": lat,
                "lon": lon,
                "port_number": int(row["portNumber"]) if row.get("portNumber") else None,
                "harbor_size": (row.get("harborSize") or "").strip() or None,
                "name": row.get("portName"),
            }
    return out


def parse_natural_earth(path: Path) -> list[tuple[str, float, float]]:
    with zipfile.ZipFile(path) as z:
        shp = io.BytesIO(z.read("ne_10m_ports.shp"))
        dbf = io.BytesIO(z.read("ne_10m_ports.dbf"))
        shx = io.BytesIO(z.read("ne_10m_ports.shx"))
    reader = shapefile.Reader(shp=shp, dbf=dbf, shx=shx, encoding="latin-1")
    out = []
    for sr in reader.iterShapeRecords():
        name = sr.record["name"]
        if not name or not sr.shape.points:
            continue
        lon, lat = sr.shape.points[0]
        out.append((name, round(lat, 5), round(lon, 5)))
    return out


# ---- merge ----------------------------------------------------------------------------


def merge_wpi(records: dict[str, PortRecord], wpi: dict[str, dict]) -> int:
    n = 0
    for code, w in wpi.items():
        rec = records.get(code)
        if rec is None:
            continue
        rec.coords[CoordSource.wpi] = (w["lat"], w["lon"])
        rec.wpi_port_number = w["port_number"]
        rec.harbor_size = w["harbor_size"]
        rec.is_seaport = True
        n += 1
    return n


def merge_natural_earth(
    records: dict[str, PortRecord], ne_points: list[tuple[str, float, float]]
) -> int:
    """Fill coordinates for seaports that have none, via unambiguous name matches."""
    by_name: dict[str, list[tuple[float, float]]] = defaultdict(list)
    for name, lat, lon in ne_points:
        for v in name_variants(name):
            by_name[v].append((lat, lon))

    located_by_country: dict[str, list[tuple[float, float]]] = defaultdict(list)
    missing_by_name: dict[str, list[PortRecord]] = defaultdict(list)
    for rec in records.values():
        if not rec.is_seaport:
            continue
        best = rec.best_coords()
        if best:
            located_by_country[rec.country_iso2].append((best[1], best[2]))
        else:
            for v in name_variants(rec.name):
                missing_by_name[v].append(rec)

    n = 0
    for variant, recs in missing_by_name.items():
        pts = by_name.get(variant)
        if not pts or len(pts) != 1:
            continue
        candidates = [r for r in recs if not r.coords]
        pt = pts[0]
        # Keep only candidates whose country already has a located port nearby.
        plausible = [
            r
            for r in candidates
            if any(
                haversine_km(pt, k) <= NE_MAX_KM_FROM_COUNTRY_PORT
                for k in located_by_country.get(r.country_iso2, [])
            )
        ]
        if len(plausible) == 1:
            plausible[0].coords[CoordSource.natural_earth] = pt
            n += 1
    return n


UPSERT_SQL = text(
    """
    INSERT INTO ports (unlocode, name, name_ascii, country_iso2, subdivision, is_seaport,
                       is_airport, geom, coord_source, wpi_port_number, harbor_size,
                       source_as_of, created_at, updated_at)
    VALUES (:unlocode, :name, :name_ascii, :country_iso2, :subdivision, :is_seaport,
            :is_airport,
            CASE WHEN CAST(:lat AS double precision) IS NULL THEN NULL
                 ELSE ST_SetSRID(ST_MakePoint(CAST(:lon AS double precision),
                                              CAST(:lat AS double precision)), 4326)::geography
            END,
            CAST(:coord_source AS coord_source), :wpi_port_number, :harbor_size,
            :as_of, now(), now())
    ON CONFLICT (unlocode) DO UPDATE SET
        name = EXCLUDED.name,
        name_ascii = EXCLUDED.name_ascii,
        country_iso2 = EXCLUDED.country_iso2,
        subdivision = EXCLUDED.subdivision,
        is_seaport = EXCLUDED.is_seaport,
        is_airport = EXCLUDED.is_airport,
        wpi_port_number = EXCLUDED.wpi_port_number,
        harbor_size = EXCLUDED.harbor_size,
        source_as_of = EXCLUDED.source_as_of,
        updated_at = now(),
        -- Admin-entered coordinates are never overwritten; otherwise a failed source must
        -- not erase previously known coordinates.
        geom = CASE WHEN ports.coord_source = 'manual' THEN ports.geom
                    ELSE COALESCE(EXCLUDED.geom, ports.geom) END,
        coord_source = CASE WHEN ports.coord_source = 'manual' THEN ports.coord_source
                            WHEN EXCLUDED.geom IS NULL THEN ports.coord_source
                            ELSE EXCLUDED.coord_source END
    """
)


def upsert_ports(db: Session, records: dict[str, PortRecord]) -> int:
    as_of = datetime.now(UTC)
    batch = []
    for rec in records.values():
        best = rec.best_coords()
        batch.append(
            {
                "unlocode": rec.unlocode,
                "name": rec.name[:200],
                "name_ascii": rec.name_ascii[:200],
                "country_iso2": rec.country_iso2,
                "subdivision": rec.subdivision,
                "is_seaport": rec.is_seaport,
                "is_airport": rec.is_airport,
                "lat": best[1] if best else None,
                "lon": best[2] if best else None,
                "coord_source": best[0].value if best else None,
                "wpi_port_number": rec.wpi_port_number,
                "harbor_size": rec.harbor_size,
                "as_of": as_of,
            }
        )
    for i in range(0, len(batch), 2000):
        db.execute(UPSERT_SQL, batch[i : i + 2000])
    db.commit()
    return len(batch)
