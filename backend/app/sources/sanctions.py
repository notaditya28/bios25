"""Sanctions list ingestion: download → parse → diff-apply → report what changed.

Each list is tracked as its own data source. An unchanged file (same SHA-256 as the last
successful run) is skipped. A file that parses to far fewer entries than we already hold is
rejected rather than applied, so a truncated download can never "delist" thousands of names.
"""

import hashlib
import json
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import delete, func, insert, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from app.config import get_settings
from app.db import SessionLocal
from app.models import SanctionEntry, SanctionList, SanctionName
from app.screening.normalize import NORMALIZER_VERSION, normalize
from app.sources import sanctions_parsers as parsers
from app.sources.base import SourceSpec, fetch, register, track_run

MIN_KEEP_RATIO = 0.5  # reject a file with < 50% of the entries we currently hold


@dataclass(frozen=True)
class ListSpec:
    code: str  # sanction_lists.code
    name: str
    publisher: str
    source: SourceSpec
    parser: Callable[[Path], parsers.ParsedList]
    url: Callable[[], str]

    @property
    def cache_name(self) -> str:
        return f"sanctions-{self.code.lower()}.xml"


_OFAC_BASE = "https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports"
_EU_URL = "https://webgate.ec.europa.eu/fsd/fsf/public/files/xmlFullSanctionsList_1_1/content"


def _eu_url() -> str:
    token = get_settings().eu_fsf_token
    if not token:
        raise RuntimeError(
            "EU_FSF_TOKEN is not set (free registration at webgate.ec.europa.eu/fsd/fsf)"
        )
    return f"{_EU_URL}?token={token}"


def _src(code: str, name: str, url: str, licence: str, attribution: str) -> SourceSpec:
    # Lists are fetched every 6 hours; 36 h without a successful fetch is "stale".
    return register(SourceSpec(code, name, url, licence, attribution, stale_after_hours=36))


LISTS: dict[str, ListSpec] = {
    s.code: s
    for s in [
        ListSpec(
            "OFAC_SDN",
            "OFAC Specially Designated Nationals (SDN) List",
            "US Treasury — Office of Foreign Assets Control",
            _src(
                "sanctions_ofac_sdn",
                "OFAC SDN List",
                f"{_OFAC_BASE}/SDN_ENHANCED.XML",
                "US Government public data; free for any use.",
                "US Treasury OFAC",
            ),
            parsers.parse_ofac,
            lambda: f"{_OFAC_BASE}/SDN_ENHANCED.XML",
        ),
        ListSpec(
            "OFAC_CONS",
            "OFAC Consolidated (non-SDN) Sanctions List",
            "US Treasury — Office of Foreign Assets Control",
            _src(
                "sanctions_ofac_cons",
                "OFAC Consolidated (non-SDN) List",
                f"{_OFAC_BASE}/CONS_ENHANCED.XML",
                "US Government public data; free for any use.",
                "US Treasury OFAC",
            ),
            parsers.parse_ofac,
            lambda: f"{_OFAC_BASE}/CONS_ENHANCED.XML",
        ),
        ListSpec(
            "UN",
            "UN Security Council Consolidated List",
            "United Nations Security Council",
            _src(
                "sanctions_un",
                "UN Security Council Consolidated List",
                "https://scsanctions.un.org/resources/xml/en/consolidated.xml",
                "Public UN document.",
                "United Nations Security Council",
            ),
            parsers.parse_un,
            lambda: "https://scsanctions.un.org/resources/xml/en/consolidated.xml",
        ),
        ListSpec(
            "EU",
            "EU Consolidated Financial Sanctions List",
            "European Commission (FSF)",
            _src(
                "sanctions_eu",
                "EU Financial Sanctions Files",
                _EU_URL,
                "Public EU data; download requires an FSF token (free registration).",
                "European Commission",
            ),
            parsers.parse_eu,
            _eu_url,
        ),
        ListSpec(
            "UK",
            "UK Sanctions List",
            "UK Foreign, Commonwealth & Development Office",
            _src(
                "sanctions_uk",
                "UK Sanctions List (FCDO)",
                "https://sanctionslist.fcdo.gov.uk/docs/UK-Sanctions-List.xml",
                "Open Government Licence v3.0.",
                "Contains public sector information "
                "licensed under the Open Government Licence v3.0 (FCDO)",
            ),
            parsers.parse_uk,
            lambda: "https://sanctionslist.fcdo.gov.uk/docs/UK-Sanctions-List.xml",
        ),
    ]
}


def entry_hash(e: parsers.ParsedEntry) -> str:
    payload = {
        "t": e.entity_type.value,
        "n": sorted((n.name, n.is_primary, n.is_weak) for n in e.names),
        "p": e.programs,
        "c": e.countries,
        "b": e.birth_dates,
        "i": e.imo_numbers,
        "l": e.listed_on,
        "r": e.remarks,
        "v": NORMALIZER_VERSION,
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


def _name_rows(entry_id: uuid.UUID, e: parsers.ParsedEntry) -> list[dict]:
    rows = []
    for n in e.names:
        nn = normalize(n.name)
        if not nn.tokens:
            continue
        rows.append(
            {
                "entry_id": entry_id,
                "name": n.name,
                "name_norm": nn.norm,
                "token_keys": nn.distinctive_keys,
                "script": n.script,
                "is_primary": n.is_primary,
                "is_weak": n.is_weak,
            }
        )
    return rows


def _entry_values(e: parsers.ParsedEntry, h: str) -> dict:
    return {
        "entity_type": e.entity_type,
        "primary_name": e.primary_name,
        "programs": e.programs,
        "countries": e.countries,
        "birth_dates": e.birth_dates,
        "imo_numbers": e.imo_numbers,
        "listed_on": e.listed_on,
        "remarks": e.remarks,
        "content_hash": h,
        "active": True,
        "removed_at": None,
    }


def apply_list(
    db: Session, spec: ListSpec, parsed: parsers.ParsedList, version_hash: str
) -> dict[str, int]:
    now = datetime.now(UTC)
    db.execute(
        pg_insert(SanctionList)
        .values(code=spec.code, name=spec.name, publisher=spec.publisher)
        .on_conflict_do_nothing()
    )
    existing_active = (
        db.scalar(
            select(func.count()).where(SanctionEntry.list_code == spec.code, SanctionEntry.active)
        )
        or 0
    )
    if not parsed.entries:
        raise ValueError(f"{spec.code}: file parsed to zero entries")
    if existing_active and len(parsed.entries) < existing_active * MIN_KEEP_RATIO:
        raise ValueError(
            f"{spec.code}: only {len(parsed.entries)} entries parsed vs {existing_active} held; "
            "refusing to apply (possible truncated or changed file)"
        )

    existing = {
        uid: (eid, h, active)
        for uid, eid, h, active in db.execute(
            select(
                SanctionEntry.source_uid,
                SanctionEntry.id,
                SanctionEntry.content_hash,
                SanctionEntry.active,
            ).where(SanctionEntry.list_code == spec.code)
        )
    }
    added = updated = 0
    new_entries: list[dict] = []
    new_names: list[dict] = []
    changed_ids: list[uuid.UUID] = []
    seen: set[str] = set()
    for e in parsed.entries:
        if e.uid in seen:
            continue
        seen.add(e.uid)
        h = entry_hash(e)
        cur = existing.get(e.uid)
        if cur is None:
            eid = uuid.uuid4()
            new_entries.append(
                {"id": eid, "list_code": spec.code, "source_uid": e.uid, **_entry_values(e, h)}
            )
            new_names += _name_rows(eid, e)
            added += 1
        elif cur[1] != h or not cur[2]:
            eid = cur[0]
            db.execute(
                update(SanctionEntry)
                .where(SanctionEntry.id == eid)
                .values(**_entry_values(e, h), updated_at=now)
            )
            changed_ids.append(eid)
            new_names += _name_rows(eid, e)
            updated += 1
    if changed_ids:
        db.execute(delete(SanctionName).where(SanctionName.entry_id.in_(changed_ids)))
    for i in range(0, len(new_entries), 2000):
        db.execute(insert(SanctionEntry), new_entries[i : i + 2000])
    for i in range(0, len(new_names), 5000):
        db.execute(insert(SanctionName), new_names[i : i + 5000])

    gone = [eid for uid, (eid, _, active) in existing.items() if active and uid not in seen]
    if gone:
        db.execute(
            update(SanctionEntry)
            .where(SanctionEntry.id.in_(gone))
            .values(active=False, removed_at=now)
        )
    db.execute(
        update(SanctionList)
        .where(SanctionList.code == spec.code)
        .values(
            version_hash=version_hash,
            published_at=parsed.published_at,
            fetched_at=now,
            entry_count=len(seen),
        )
    )
    db.commit()
    return {"added": added, "updated": updated, "removed": len(gone), "total": len(seen)}


def refresh_list(code: str) -> bool:
    """Returns True when the list's content changed (so subjects need re-screening)."""
    spec = LISTS[code]
    with track_run(spec.source.code) as run:
        path, sha = fetch(spec.url(), cache_name=spec.cache_name)
        run.content_hash = sha
        # The stored version covers both the file and how we normalise it.
        version = hashlib.sha256(f"{sha}:{NORMALIZER_VERSION}".encode()).hexdigest()
        with SessionLocal() as db:
            held = db.get(SanctionList, code)
            if held and held.version_hash == version:
                run.skipped = True
                run.data_as_of = held.published_at
                return False
            parsed = spec.parser(path)
            stats = apply_list(db, spec, parsed, version)
        run.rows = stats["total"]
        run.details = stats
        run.data_as_of = parsed.published_at
        return bool(stats["added"] or stats["updated"] or stats["removed"])
