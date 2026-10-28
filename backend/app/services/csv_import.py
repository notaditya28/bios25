"""CSV bulk import for partners, lanes and shipments.

Imports are all-or-nothing: every row is validated inside a savepoint, and the file is only
committed when no row failed. `dry_run=True` validates without saving. Dates accept ISO
(2026-11-03) and Indian day-first formats (03/11/2026, 03-11-2026).
"""

import csv
import io
import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from pydantic import ValidationError
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import ClientCompany, CsvImport, Lane, Partner, TransportMode
from app.schemas.trade import ImportResult, ImportRowError, LaneBase, PartnerBase, ShipmentBase
from app.services import trade as svc

MAX_BYTES = 5 * 1024 * 1024
MAX_ROWS = 5000

TEMPLATES: dict[str, list[str]] = {
    "partners": ["kind", "name", "country", "city", "port", "address", "contact_email", "notes"],
    "lanes": ["origin_port", "dest_port", "mode", "partner_name", "label"],
    "shipments": [
        "reference",
        "origin_port",
        "dest_port",
        "mode",
        "partner_name",
        "etd",
        "eta",
        "container_refs",
        "container_count",
        "value_inr",
        "cargo_description",
        "hs_code",
        "vessel_name",
        "vessel_imo",
        "status",
    ],
}
REQUIRED: dict[str, set[str]] = {
    "partners": {"kind", "name", "country"},
    "lanes": {"origin_port", "dest_port"},
    "shipments": {"reference", "origin_port", "dest_port"},
}


class ImportFileError(ValueError):
    pass


def template_csv(kind: str) -> str:
    buf = io.StringIO()
    csv.writer(buf).writerow(TEMPLATES[kind])
    return buf.getvalue()


def _parse_date(v: str) -> date | None:
    v = v.strip()
    if not v:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%d.%m.%Y"):
        try:
            return datetime.strptime(v, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"unrecognised date '{v}' (use YYYY-MM-DD or DD/MM/YYYY)")


def _parse_decimal(v: str) -> Decimal | None:
    v = re.sub(r"[,\s₹]|INR|Rs\.?", "", v.strip(), flags=re.IGNORECASE)
    if not v:
        return None
    try:
        return Decimal(v)
    except InvalidOperation:
        raise ValueError(f"'{v}' is not a number") from None


def _partner_by_name(db: Session, company: ClientCompany, name: str) -> Partner | None:
    name = " ".join(name.split())
    if not name:
        return None
    matches = list(
        db.scalars(
            select(Partner).where(
                Partner.company_id == company.id, func.lower(Partner.name) == name.lower()
            )
        )
    )
    if not matches:
        raise svc.DomainError(f"no partner named '{name}'", "partner_name")
    if len(matches) > 1:
        raise svc.DomainError(f"several partners are named '{name}'", "partner_name")
    return matches[0]


def _mode(v: str) -> TransportMode:
    v = (v or "sea").strip().lower()
    try:
        return TransportMode(v)
    except ValueError:
        raise svc.DomainError(f"mode must be 'sea' or 'air', got '{v}'", "mode") from None


def _blank_to_none(row: dict[str, str]) -> dict[str, Any]:
    return {k: (v.strip() if v and v.strip() else None) for k, v in row.items()}


def _row_partner(db: Session, company: ClientCompany, row: dict[str, str]) -> None:
    r = _blank_to_none(row)
    data = PartnerBase(
        kind=(r["kind"] or "").lower(),
        name=r["name"] or "",
        country=r["country"] or "",
        city=r.get("city"),
        port=r.get("port"),
        address=r.get("address"),
        contact_email=r.get("contact_email"),
        notes=r.get("notes"),
    )
    svc.new_partner(db, company, data)


def _row_lane(db: Session, company: ClientCompany, row: dict[str, str]) -> None:
    r = _blank_to_none(row)
    partner = _partner_by_name(db, company, r.get("partner_name") or "")
    data = LaneBase(
        origin_port=r["origin_port"] or "",
        dest_port=r["dest_port"] or "",
        mode=_mode(r.get("mode") or "sea"),
        partner_id=partner.id if partner else None,
        label=r.get("label"),
    )
    svc.new_lane(db, company, data)


def _row_shipment(db: Session, company: ClientCompany, row: dict[str, str]) -> None:
    r = _blank_to_none(row)
    mode = _mode(r.get("mode") or "sea")
    partner = _partner_by_name(db, company, r.get("partner_name") or "")
    origin, dest = r["origin_port"] or "", r["dest_port"] or ""
    # Shipments on a lane the user has not created yet get the lane created for them.
    lane_spec = LaneBase(
        origin_port=origin, dest_port=dest, mode=mode, partner_id=partner.id if partner else None
    )
    try:
        lane: Lane = svc.find_lane(db, company, lane_spec.origin_port, lane_spec.dest_port, mode)
    except svc.DomainError:
        lane = svc.new_lane(db, company, lane_spec)
        db.flush()
    containers = [c for c in re.split(r"[;|\s]+", r.get("container_refs") or "") if c]
    count = r.get("container_count")
    data = ShipmentBase(
        lane_id=lane.id,
        partner_id=partner.id if partner else None,
        reference=r["reference"] or "",
        container_refs=containers,
        container_count=int(count) if count else None,
        etd=_parse_date(r.get("etd") or ""),
        eta=_parse_date(r.get("eta") or ""),
        value_inr=_parse_decimal(r.get("value_inr") or ""),
        cargo_description=r.get("cargo_description"),
        hs_code=r.get("hs_code"),
        vessel_name=r.get("vessel_name"),
        vessel_imo=r.get("vessel_imo"),
        status=(r.get("status") or "planned").lower(),
    )
    svc.new_shipment(db, company, data)


ROW_HANDLERS = {"partners": _row_partner, "lanes": _row_lane, "shipments": _row_shipment}


def _read(content: bytes) -> tuple[list[str], list[dict[str, str]]]:
    if len(content) > MAX_BYTES:
        raise ImportFileError("file is larger than 5 MB")
    try:
        text = content.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = content.decode("cp1252")  # Excel on Windows often saves CSV as cp1252
    reader = csv.DictReader(io.StringIO(text))
    headers = [h.strip().lower() for h in (reader.fieldnames or [])]
    reader.fieldnames = headers
    rows = list(reader)
    if len(rows) > MAX_ROWS:
        raise ImportFileError(f"file has more than {MAX_ROWS} rows; split it")
    return headers, rows


def _pydantic_errors(e: ValidationError) -> list[tuple[str | None, str]]:
    out = []
    for err in e.errors():
        loc = ".".join(str(x) for x in err["loc"]) or None
        msg = err["msg"].removeprefix("Value error, ")
        out.append((loc, msg))
    return out


def run_import(
    db: Session,
    *,
    company: ClientCompany,
    kind: str,
    filename: str,
    content: bytes,
    user_id: Any,
    dry_run: bool,
) -> ImportResult:
    headers, rows = _read(content)
    missing = REQUIRED[kind] - set(headers)
    if missing:
        raise ImportFileError(f"missing required column(s): {', '.join(sorted(missing))}")
    unknown = set(headers) - set(TEMPLATES[kind])
    if unknown:
        raise ImportFileError(f"unknown column(s): {', '.join(sorted(unknown))}")
    if not rows:
        raise ImportFileError("file has no data rows")

    handler = ROW_HANDLERS[kind]
    errors: list[ImportRowError] = []
    created = 0
    for i, row in enumerate(rows, start=1):
        sp = db.begin_nested()
        try:
            handler(db, company, {k: (v or "") for k, v in row.items() if k})
            db.flush()
            sp.commit()
            created += 1
        except ValidationError as e:
            sp.rollback()
            errors += [ImportRowError(row=i, field=f, message=m) for f, m in _pydantic_errors(e)]
        except (svc.DomainError, ValueError) as e:
            sp.rollback()
            errors.append(ImportRowError(row=i, field=getattr(e, "field", None), message=str(e)))

    if dry_run or errors:
        db.rollback()
        created_count = 0
    else:
        created_count = created

    status = "validated" if dry_run and not errors else "rejected" if errors else "committed"
    record_id = None
    if not dry_run:
        rec = CsvImport(
            organization_id=company.organization_id,
            company_id=company.id,
            created_by=user_id,
            kind=kind,
            filename=filename[:255],
            row_count=len(rows),
            created_count=created_count,
            error_count=len(errors),
            errors=[e.model_dump() for e in errors[:500]],
            status=status,
        )
        db.add(rec)
        db.commit()
        record_id = rec.id
    return ImportResult(
        id=record_id,
        kind=kind,
        dry_run=dry_run,
        row_count=len(rows),
        created_count=created_count if not dry_run else created,
        errors=errors,
        status=status,
    )
