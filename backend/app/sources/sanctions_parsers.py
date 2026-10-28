"""Parsers for the official sanctions list files. Pure: file in, ParsedList out.

Formats (verified against the live files, Oct 2026):
- OFAC  SDN_ENHANCED.XML / CONS_ENHANCED.XML  (Sanctions List Service "enhanced" XML)
- UN    consolidated.xml                       (Security Council Consolidated List)
- EU    xmlFullSanctionsList_1_1               (Financial Sanctions Files, FSF)
- UK    UK-Sanctions-List.xml                  (FCDO UK Sanctions List)
"""

import re
import unicodedata
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import UTC, datetime
from functools import lru_cache
from pathlib import Path

import pycountry

from app.models.sanctions import EntityType


@dataclass
class ParsedName:
    name: str
    is_primary: bool = False
    is_weak: bool = False

    @property
    def script(self) -> str | None:
        return script_of(self.name)


@dataclass
class ParsedEntry:
    uid: str
    entity_type: EntityType
    names: list[ParsedName]
    programs: list[str] = field(default_factory=list)
    countries: list[str] = field(default_factory=list)
    birth_dates: list[str] = field(default_factory=list)
    imo_numbers: list[str] = field(default_factory=list)
    listed_on: str | None = None
    remarks: str | None = None

    @property
    def primary_name(self) -> str:
        for n in self.names:
            if n.is_primary:
                return n.name
        return self.names[0].name

    def finalize(self) -> "ParsedEntry":
        seen: set[tuple[str, bool]] = set()
        names = []
        for n in self.names:
            n.name = " ".join(n.name.split())
            key = (n.name.lower(), n.is_primary)
            if n.name and key not in seen:
                seen.add(key)
                names.append(n)
        self.names = names
        self.programs = sorted({p.strip() for p in self.programs if p and p.strip()})
        self.countries = sorted({c for c in self.countries if c})
        self.birth_dates = sorted({d.strip() for d in self.birth_dates if d and d.strip()})
        self.imo_numbers = sorted({i for i in self.imo_numbers if i})
        if self.remarks:
            self.remarks = self.remarks.strip()[:2000] or None
        return self


@dataclass
class ParsedList:
    published_at: datetime | None
    entries: list[ParsedEntry]


# ---- helpers ---------------------------------------------------------------------------

_COUNTRY_ALIASES = {
    "russia": "RU",
    "iran": "IR",
    "syria": "SY",
    "north korea": "KP",
    "korea, north": "KP",
    "democratic people's republic of korea": "KP",
    "korea, democratic people's republic of": "KP",
    "south korea": "KR",
    "korea, south": "KR",
    "burma": "MM",
    "venezuela": "VE",
    "bolivia": "BO",
    "vietnam": "VN",
    "viet nam": "VN",
    "laos": "LA",
    "tanzania": "TZ",
    "moldova": "MD",
    "turkey": "TR",
    "turkiye": "TR",
    "türkiye": "TR",
    "congo, democratic republic of the": "CD",
    "democratic republic of the congo": "CD",
    "congo": "CG",
    "republic of the congo": "CG",
    "ivory coast": "CI",
    "cote d'ivoire": "CI",
    "côte d'ivoire": "CI",
    "palestinian": "PS",
    "west bank": "PS",
    "gaza": "PS",
    "palestine": "PS",
    "taiwan": "TW",
    "kosovo": "XK",
    "crimea": "UA",
    "region: crimea": "UA",
    "united kingdom": "GB",
    "uk": "GB",
    "usa": "US",
    "united states": "US",
    "united states of america": "US",
    "czech republic": "CZ",
    "macau": "MO",
    "macao": "MO",
    "hong kong": "HK",
    "brunei": "BN",
    "cape verde": "CV",
    "micronesia": "FM",
    "swaziland": "SZ",
    "the gambia": "GM",
    "gambia": "GM",
    "uae": "AE",
    "united arab emirates": "AE",
    "iran (islamic republic of)": "IR",
    "syrian arab republic": "SY",
    "russian federation": "RU",
    "lao people's democratic republic": "LA",
}


@lru_cache(maxsize=4096)
def country_code(value: str | None) -> str | None:
    """Free-text country name (as published on a list) → ISO alpha-2, or None."""
    if not value:
        return None
    v = " ".join(value.split()).strip(" .")
    if not v:
        return None
    if len(v) == 2 and v.isalpha():
        return v.upper() if pycountry.countries.get(alpha_2=v.upper()) else None
    alias = _COUNTRY_ALIASES.get(v.lower())
    if alias:
        return alias
    try:
        return str(pycountry.countries.lookup(v).alpha_2)
    except LookupError:
        return None


def script_of(text: str) -> str | None:
    for ch in text:
        if ch.isalpha():
            try:
                return unicodedata.name(ch).split(" ")[0].title()  # LATIN, CYRILLIC, ARABIC, CJK…
            except ValueError:
                return None
    return None


_IMO = re.compile(r"(?<!\d)(\d{7})(?!\d)")


def imo_from(text: str | None) -> str | None:
    if not text:
        return None
    m = _IMO.search(text)
    return m.group(1) if m else None


def _t(el: ET.Element | None) -> str:
    return (el.text or "").strip() if el is not None else ""


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    v = value.strip()
    for fmt in ("%d/%m/%Y",):
        try:
            return datetime.strptime(v, fmt).replace(tzinfo=UTC)
        except ValueError:
            pass
    try:
        dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
    except ValueError:
        return None


# ---- OFAC (enhanced XML) -------------------------------------------------------------------

_OFAC_TYPES = {
    "individual": EntityType.individual,
    "entity": EntityType.entity,
    "vessel": EntityType.vessel,
    "aircraft": EntityType.aircraft,
}
_OFAC_COUNTRY_FEATURES = {"nationality country", "citizenship country", "vessel flag"}


def parse_ofac(path: Path) -> ParsedList:
    published = None
    entries: list[ParsedEntry] = []
    for _, el in ET.iterparse(path):  # noqa: S314
        tag = _local(el.tag)
        if tag == "dataAsOf":
            published = _parse_dt(el.text)
        if tag != "entity":
            continue
        kids = {_local(c.tag): c for c in el}
        gi = kids.get("generalInfo")
        etype = ""
        if gi is not None:
            for c in gi:
                if _local(c.tag) == "entityType":
                    etype = _t(c).lower()
        e = ParsedEntry(
            uid=el.get("id", ""), entity_type=_OFAC_TYPES.get(etype, EntityType.unknown), names=[]
        )
        for sec_name, sec in kids.items():
            if sec_name == "sanctionsPrograms":
                e.programs += [_t(p) for p in sec]
            elif sec_name == "sanctionsLists":
                dates = [p.get("datePublished") for p in sec if p.get("datePublished")]
                e.listed_on = min(dates) if dates else None
            elif sec_name == "names":
                for name in sec:
                    nk = {_local(c.tag): c for c in name}
                    primary = _t(nk.get("isPrimary")) == "true"
                    weak = _t(nk.get("isLowQuality")) == "true"
                    for tr in nk.get("translations", ET.Element("x")):
                        full = next((_t(c) for c in tr if _local(c.tag) == "formattedFullName"), "")
                        if full:
                            e.names.append(ParsedName(full, is_primary=primary, is_weak=weak))
            elif sec_name == "addresses":
                for addr in sec:
                    for c in addr:
                        if _local(c.tag) == "country":
                            e.countries.append(country_code(_t(c)) or "")
            elif sec_name == "features":
                for feat in sec:
                    fk = {_local(c.tag): c for c in feat}
                    ftype = _t(fk.get("type")).lower()
                    if ftype in _OFAC_COUNTRY_FEATURES:
                        e.countries.append(country_code(_t(fk.get("value"))) or "")
                    elif ftype == "birthdate":
                        e.birth_dates.append(_t(fk.get("value")))
            elif sec_name == "identityDocuments":
                for doc in sec:
                    dk = {_local(c.tag): c for c in doc}
                    if _t(dk.get("type")) == "Vessel Registration Identification":
                        e.imo_numbers.append(imo_from(_t(dk.get("documentNumber"))) or "")
        if gi is not None:
            e.remarks = next((_t(c) for c in gi if _local(c.tag) == "remarks"), None)
        if e.names:
            entries.append(e.finalize())
        el.clear()
    return ParsedList(published, entries)


# ---- UN ---------------------------------------------------------------------------------


def _un_full_name(el: ET.Element) -> str:
    parts = [_t(el.find(k)) for k in ("FIRST_NAME", "SECOND_NAME", "THIRD_NAME", "FOURTH_NAME")]
    return " ".join(p for p in parts if p)


def parse_un(path: Path) -> ParsedList:
    root = ET.parse(path).getroot()  # noqa: S314 - 2 MB file
    published = _parse_dt(root.get("dateGenerated"))
    entries = []
    for group, kind, alias_tag, addr_tag in (
        ("INDIVIDUALS", EntityType.individual, "INDIVIDUAL_ALIAS", "INDIVIDUAL_ADDRESS"),
        ("ENTITIES", EntityType.entity, "ENTITY_ALIAS", "ENTITY_ADDRESS"),
    ):
        for el in root.findall(f"{group}/*"):
            e = ParsedEntry(
                uid=_t(el.find("DATAID")),
                entity_type=kind,
                names=[ParsedName(_un_full_name(el), is_primary=True)],
            )
            original = _t(el.find("NAME_ORIGINAL_SCRIPT"))
            if original:
                e.names.append(ParsedName(original))
            for a in el.findall(alias_tag):
                alias = _t(a.find("ALIAS_NAME"))
                if alias:
                    e.names.append(
                        ParsedName(alias, is_weak=_t(a.find("QUALITY")).lower() == "low")
                    )
            e.programs = [_t(el.find("UN_LIST_TYPE"))]
            e.countries = [country_code(_t(v)) or "" for v in el.findall("NATIONALITY/VALUE")]
            e.countries += [country_code(_t(a.find("COUNTRY"))) or "" for a in el.findall(addr_tag)]
            for dob in el.findall("INDIVIDUAL_DATE_OF_BIRTH"):
                e.birth_dates.append(_t(dob.find("DATE")) or _t(dob.find("YEAR")))
            e.listed_on = _t(el.find("LISTED_ON")) or None
            e.remarks = _t(el.find("COMMENTS1")) or None
            if e.uid and e.names[0].name:
                entries.append(e.finalize())
    return ParsedList(published, entries)


# ---- EU (FSF) -------------------------------------------------------------------------------

_EU_TYPES = {"person": EntityType.individual, "enterprise": EntityType.entity}


def parse_eu(path: Path) -> ParsedList:
    published = None
    entries = []
    for ev, el in ET.iterparse(path, events=("start", "end")):  # noqa: S314
        tag = _local(el.tag)
        if ev == "start":
            if tag == "export":
                published = _parse_dt(el.get("generationDate"))
            continue
        if tag != "sanctionEntity":
            continue
        e = ParsedEntry(uid=el.get("logicalId", ""), entity_type=EntityType.unknown, names=[])
        e.remarks = el.get("euReferenceNumber") or None
        first_alias = True
        for c in el:
            ct = _local(c.tag)
            if ct == "subjectType":
                e.entity_type = _EU_TYPES.get(c.get("code", ""), EntityType.unknown)
            elif ct == "nameAlias":
                whole = c.get("wholeName", "")
                if whole:
                    e.names.append(
                        ParsedName(
                            whole, is_primary=first_alias, is_weak=c.get("strong") == "false"
                        )
                    )
                    first_alias = False
            elif ct == "regulation":
                e.programs.append(c.get("programme", ""))
                pub = c.get("publicationDate")
                if pub and (e.listed_on is None or pub < e.listed_on):
                    e.listed_on = pub
            elif ct in ("citizenship", "address"):
                e.countries.append(country_code(c.get("countryIso2Code")) or "")
            elif ct == "birthdate":
                e.birth_dates.append(c.get("birthdate") or c.get("year") or "")
            elif ct == "identification" and c.get("identificationTypeCode") == "imo":
                e.imo_numbers.append(imo_from(c.get("number")) or "")
        # The EU file has no vessel subject type; entries with an IMO number are vessels.
        if e.imo_numbers and e.entity_type != EntityType.individual:
            e.entity_type = EntityType.vessel
        if e.uid and e.names:
            entries.append(e.finalize())
        el.clear()
    return ParsedList(published, entries)


# ---- UK (FCDO) --------------------------------------------------------------------------------

_UK_TYPES = {
    "individual": EntityType.individual,
    "entity": EntityType.entity,
    "ship": EntityType.vessel,
}


def parse_uk(path: Path) -> ParsedList:
    published = None
    entries = []
    for _, el in ET.iterparse(path):  # noqa: S314
        if el.tag == "DateGenerated":
            published = _parse_dt(el.text)
        if el.tag != "Designation":
            continue
        e = ParsedEntry(
            uid=_t(el.find("UniqueID")),
            entity_type=_UK_TYPES.get(
                _t(el.find("IndividualEntityShip")).lower(), EntityType.unknown
            ),
            names=[],
        )
        for n in el.findall("Names/Name"):
            full = " ".join(_t(n.find(f"Name{i}")) for i in range(1, 7)).strip()
            if full:
                e.names.append(
                    ParsedName(
                        full,
                        is_primary=_t(n.find("NameType")).lower().startswith("primary"),
                        is_weak=_t(n.find("AliasStrength")).lower() == "weak",
                    )
                )
        for n in el.findall("NonLatinNames/NonLatinName"):
            if _t(n.find("NameNonLatinScript")):
                e.names.append(ParsedName(_t(n.find("NameNonLatinScript"))))
        e.programs = [_t(el.find("RegimeName"))]
        e.countries = [country_code(_t(x)) or "" for x in el.iter("Nationality")]
        e.countries += [country_code(_t(x)) or "" for x in el.iter("AddressCountry")]
        e.countries += [country_code(_t(x)) or "" for x in el.iter("CurrentBelievedFlagOfShip")]
        e.birth_dates = [_t(x) for x in el.iter("DOB")]
        e.imo_numbers = [imo_from(_t(x)) or "" for x in el.iter("IMONumber")]
        designated = _parse_dt(_t(el.find("DateDesignated")))
        e.listed_on = designated.date().isoformat() if designated else None
        e.remarks = _t(el.find("OtherInformation")) or None
        if e.uid and e.names:
            entries.append(e.finalize())
        el.clear()
    return ParsedList(published, entries)
