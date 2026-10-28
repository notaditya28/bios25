from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException
from geoalchemy2.elements import WKTElement
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db import get_db
from app.deps import get_verified_user, require_platform_admin
from app.models import CoordSource, DataSource, Port, SourceRun, User
from app.schemas.auth import MessageOut
from app.schemas.trade import PortCoordsIn, PortOut
from app.services.audit import audit
from app.services.serialize import port_out
from app.tasks.ports import refresh_ports

router = APIRouter(prefix="/api", tags=["sources", "admin"])


class SourceStatus(BaseModel):
    code: str
    name: str
    enabled: bool
    attribution: str | None
    last_success_at: datetime | None
    last_data_as_of: datetime | None
    stale: bool
    stale_after_hours: int


class SourceAdminStatus(SourceStatus):
    url: str
    licence_note: str
    last_error: str | None
    recent_runs: list[dict]


def _status(src: DataSource) -> dict:
    now = datetime.now(UTC)
    stale = src.last_success_at is None or now - src.last_success_at > timedelta(
        hours=src.stale_after_hours
    )
    return {
        "code": src.code,
        "name": src.name,
        "enabled": src.enabled,
        "attribution": src.attribution,
        "last_success_at": src.last_success_at,
        "last_data_as_of": src.last_data_as_of,
        "stale": stale,
        "stale_after_hours": src.stale_after_hours,
    }


@router.get("/sources", response_model=list[SourceStatus])
def source_freshness(
    _: User = Depends(get_verified_user), db: Session = Depends(get_db)
) -> list[dict]:
    """Freshness only (no error details) — drives the staleness badges for all users."""
    return [_status(s) for s in db.scalars(select(DataSource).order_by(DataSource.code))]


@router.get("/admin/sources", response_model=list[SourceAdminStatus])
def admin_sources(
    _: User = Depends(require_platform_admin), db: Session = Depends(get_db)
) -> list[dict]:
    out = []
    for s in db.scalars(select(DataSource).order_by(DataSource.code)):
        runs = db.scalars(
            select(SourceRun)
            .where(SourceRun.source_code == s.code)
            .order_by(SourceRun.started_at.desc())
            .limit(10)
        )
        out.append(
            {
                **_status(s),
                "url": s.url,
                "licence_note": s.licence_note,
                "last_error": s.last_error,
                "recent_runs": [
                    {
                        "started_at": r.started_at,
                        "finished_at": r.finished_at,
                        "status": r.status,
                        "rows": r.rows,
                        "error": r.error,
                    }
                    for r in runs
                ],
            }
        )
    return out


RUNNABLE = {
    "ports_unlocode": refresh_ports,
    "ports_wpi": refresh_ports,
    "ports_natural_earth": refresh_ports,
}


@router.post("/admin/sources/{code}/run", response_model=MessageOut, status_code=202)
def run_source(code: str, admin: User = Depends(require_platform_admin)) -> MessageOut:
    task = RUNNABLE.get(code)
    if task is None:
        raise HTTPException(404, "Unknown or non-runnable source")
    task.delay()
    return MessageOut(message=f"Refresh of {code} queued")


@router.put("/admin/ports/{unlocode}/coordinates", response_model=PortOut)
def set_port_coordinates(
    unlocode: str,
    body: PortCoordsIn,
    admin: User = Depends(require_platform_admin),
    db: Session = Depends(get_db),
) -> PortOut:
    port = db.scalar(select(Port).where(Port.unlocode == unlocode.upper()))
    if port is None:
        raise HTTPException(404, "Port not found")
    before = port_out(port).model_dump(mode="json")
    port.geom = WKTElement(f"POINT({body.lon} {body.lat})", srid=4326)
    port.coord_source = CoordSource.manual
    audit(
        db,
        actor=admin.id,
        org=None,
        action="port.coordinates_set",
        target_type="port",
        target_id=port.unlocode,
        before=before,
        after=body.model_dump(),
        note=body.note,
    )
    db.commit()
    db.refresh(port)
    return port_out(port)
