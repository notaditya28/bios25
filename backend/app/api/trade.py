import uuid
from collections.abc import Callable
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import Select, case, func, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.db import get_db
from app.deps import OrgContext, company_in_org, get_org, get_verified_user
from app.models import (
    Lane,
    Partner,
    PartnerKind,
    Port,
    Shipment,
    ShipmentStatus,
    SubjectType,
    TransportMode,
    User,
)
from app.schemas.trade import (
    LaneIn,
    LaneOut,
    LanePatch,
    Page,
    PartnerIn,
    PartnerOut,
    PartnerPatch,
    PortOut,
    ShipmentIn,
    ShipmentOut,
    ShipmentPatch,
)
from app.screening.service import forget_subject
from app.services import trade as svc
from app.services.audit import audit
from app.services.serialize import lane_out, partner_out, port_out, shipment_out
from app.tasks.sanctions import request_screening

router = APIRouter(prefix="/api", tags=["trade"])


def _domain(fn: Callable[[], Any]) -> Any:
    try:
        return fn()
    except svc.DomainError as e:
        raise HTTPException(422, str(e)) from None


def _commit(db: Session, conflict_msg: str) -> None:
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(409, conflict_msg) from None


def _paginate(db: Session, q: Select, limit: int, offset: int) -> tuple[list, int]:
    total = db.scalar(select(func.count()).select_from(q.order_by(None).subquery())) or 0
    rows = list(db.scalars(q.limit(limit).offset(offset)).unique())
    return rows, total


# ---- ports (global reference data) ----------------------------------------------------


@router.get("/ports", response_model=list[PortOut])
def search_ports(
    q: str = Query(min_length=2, max_length=80),
    mode: TransportMode | None = None,
    country: str | None = Query(default=None, min_length=2, max_length=2),
    limit: int = Query(default=20, le=50),
    _: User = Depends(get_verified_user),
    db: Session = Depends(get_db),
) -> list[PortOut]:
    term = q.strip()
    stmt = select(Port)
    if mode == TransportMode.sea:
        stmt = stmt.where(Port.is_seaport)
    elif mode == TransportMode.air:
        stmt = stmt.where(Port.is_airport)
    if country:
        stmt = stmt.where(Port.country_iso2 == country.upper())
    compact = term.replace(" ", "").upper()
    like = term.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    size_rank = case({"L": 0, "M": 1, "S": 2, "V": 3}, value=Port.harbor_size, else_=4)
    stmt = stmt.where(
        or_(
            Port.unlocode == compact,
            Port.name_ascii.ilike(f"%{like}%", escape="\\"),
            func.similarity(Port.name_ascii, term) > 0.35,
        )
    ).order_by(
        (Port.unlocode == compact).desc(),
        # Literal matches before fuzzy ones, then locatable and larger ports first.
        Port.name_ascii.ilike(f"{like}%", escape="\\").desc(),
        Port.name_ascii.ilike(f"%{like}%", escape="\\").desc(),
        Port.geom.is_(None),
        size_rank,
        func.similarity(Port.name_ascii, term).desc(),
    )
    return [port_out(p) for p in db.scalars(stmt.limit(limit))]


@router.get("/ports/{unlocode}", response_model=PortOut)
def get_port(
    unlocode: str, _: User = Depends(get_verified_user), db: Session = Depends(get_db)
) -> PortOut:
    p = db.scalar(select(Port).where(Port.unlocode == unlocode.upper()))
    if p is None:
        raise HTTPException(404, "Port not found")
    return port_out(p)


# ---- partners -------------------------------------------------------------------------


def _partner(db: Session, ctx: OrgContext, pid: uuid.UUID) -> Partner:
    p = db.get(Partner, pid)
    if p is None or p.organization_id != ctx.org.id:
        raise HTTPException(404, "Partner not found")
    return p


@router.get("/orgs/{org_id}/partners", response_model=Page[PartnerOut])
def list_partners(
    company_id: uuid.UUID | None = None,
    kind: PartnerKind | None = None,
    q: str | None = Query(default=None, max_length=100),
    limit: int = Query(default=50, le=200),
    offset: int = Query(default=0, ge=0),
    ctx: OrgContext = Depends(get_org),
    db: Session = Depends(get_db),
) -> Page[PartnerOut]:
    stmt = select(Partner).where(Partner.organization_id == ctx.org.id)
    if company_id:
        stmt = stmt.where(Partner.company_id == company_id)
    if kind:
        stmt = stmt.where(Partner.kind == kind)
    if q:
        stmt = stmt.where(Partner.name.ilike(f"%{q}%"))
    rows, total = _paginate(db, stmt.order_by(Partner.name), limit, offset)
    return Page(items=[partner_out(p) for p in rows], total=total, limit=limit, offset=offset)


@router.post("/orgs/{org_id}/partners", response_model=PartnerOut, status_code=201)
def create_partner(
    body: PartnerIn, ctx: OrgContext = Depends(get_org), db: Session = Depends(get_db)
) -> PartnerOut:
    company = company_in_org(db, ctx, body.company_id)
    p = _domain(lambda: svc.new_partner(db, company, body))
    db.flush()
    audit(
        db,
        actor=ctx.user.id,
        org=ctx.org.id,
        action="partner.created",
        target_type="partner",
        target_id=p.id,
        after=body.model_dump(mode="json"),
    )
    _commit(db, "Partner conflicts with an existing record")
    request_screening(SubjectType.partner, p.id)
    db.refresh(p)
    return partner_out(p)


@router.get("/orgs/{org_id}/partners/{partner_id}", response_model=PartnerOut)
def get_partner(
    partner_id: uuid.UUID, ctx: OrgContext = Depends(get_org), db: Session = Depends(get_db)
) -> PartnerOut:
    return partner_out(_partner(db, ctx, partner_id))


@router.put("/orgs/{org_id}/partners/{partner_id}", response_model=PartnerOut)
def update_partner(
    partner_id: uuid.UUID,
    body: PartnerPatch,
    ctx: OrgContext = Depends(get_org),
    db: Session = Depends(get_db),
) -> PartnerOut:
    p = _partner(db, ctx, partner_id)
    before = partner_out(p).model_dump(mode="json")
    _domain(lambda: svc.apply_partner(db, p, body))
    audit(
        db,
        actor=ctx.user.id,
        org=ctx.org.id,
        action="partner.updated",
        target_type="partner",
        target_id=p.id,
        before=before,
        after=body.model_dump(mode="json"),
    )
    _commit(db, "Partner conflicts with an existing record")
    request_screening(SubjectType.partner, p.id)
    db.refresh(p)
    return partner_out(p)


@router.delete("/orgs/{org_id}/partners/{partner_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_partner(
    partner_id: uuid.UUID, ctx: OrgContext = Depends(get_org), db: Session = Depends(get_db)
) -> None:
    p = _partner(db, ctx, partner_id)
    audit(
        db,
        actor=ctx.user.id,
        org=ctx.org.id,
        action="partner.deleted",
        target_type="partner",
        target_id=p.id,
        before=partner_out(p).model_dump(mode="json"),
    )
    forget_subject(db, p.id)
    db.delete(p)
    db.commit()


# ---- lanes ----------------------------------------------------------------------------


def _lane(db: Session, ctx: OrgContext, lid: uuid.UUID) -> Lane:
    lane = db.get(Lane, lid)
    if lane is None or lane.organization_id != ctx.org.id:
        raise HTTPException(404, "Lane not found")
    return lane


@router.get("/orgs/{org_id}/lanes", response_model=Page[LaneOut])
def list_lanes(
    company_id: uuid.UUID | None = None,
    mode: TransportMode | None = None,
    active: bool | None = None,
    limit: int = Query(default=50, le=200),
    offset: int = Query(default=0, ge=0),
    ctx: OrgContext = Depends(get_org),
    db: Session = Depends(get_db),
) -> Page[LaneOut]:
    stmt = select(Lane).where(Lane.organization_id == ctx.org.id)
    if company_id:
        stmt = stmt.where(Lane.company_id == company_id)
    if mode:
        stmt = stmt.where(Lane.mode == mode)
    if active is not None:
        stmt = stmt.where(Lane.active == active)
    rows, total = _paginate(db, stmt.order_by(Lane.created_at.desc()), limit, offset)
    return Page(items=[lane_out(x) for x in rows], total=total, limit=limit, offset=offset)


@router.post("/orgs/{org_id}/lanes", response_model=LaneOut, status_code=201)
def create_lane(
    body: LaneIn, ctx: OrgContext = Depends(get_org), db: Session = Depends(get_db)
) -> LaneOut:
    company = company_in_org(db, ctx, body.company_id)
    lane = _domain(lambda: svc.new_lane(db, company, body))
    db.flush()
    audit(
        db,
        actor=ctx.user.id,
        org=ctx.org.id,
        action="lane.created",
        target_type="lane",
        target_id=lane.id,
        after=body.model_dump(mode="json"),
    )
    _commit(db, "This lane already exists")
    db.refresh(lane)
    return lane_out(lane)


@router.put("/orgs/{org_id}/lanes/{lane_id}", response_model=LaneOut)
def update_lane(
    lane_id: uuid.UUID,
    body: LanePatch,
    ctx: OrgContext = Depends(get_org),
    db: Session = Depends(get_db),
) -> LaneOut:
    lane = _lane(db, ctx, lane_id)
    company = company_in_org(db, ctx, lane.company_id)
    before = lane_out(lane).model_dump(mode="json")
    _domain(lambda: svc.apply_lane(db, company, lane, body))
    audit(
        db,
        actor=ctx.user.id,
        org=ctx.org.id,
        action="lane.updated",
        target_type="lane",
        target_id=lane.id,
        before=before,
        after=body.model_dump(mode="json"),
    )
    _commit(db, "This lane already exists")
    db.refresh(lane)
    return lane_out(lane)


@router.delete("/orgs/{org_id}/lanes/{lane_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_lane(
    lane_id: uuid.UUID, ctx: OrgContext = Depends(get_org), db: Session = Depends(get_db)
) -> None:
    lane = _lane(db, ctx, lane_id)
    in_use = db.scalar(select(func.count()).where(Shipment.lane_id == lane.id)) or 0
    if in_use:
        raise HTTPException(409, f"{in_use} shipment(s) use this lane; deactivate it instead")
    audit(
        db,
        actor=ctx.user.id,
        org=ctx.org.id,
        action="lane.deleted",
        target_type="lane",
        target_id=lane.id,
        before=lane_out(lane).model_dump(mode="json"),
    )
    db.delete(lane)
    db.commit()


# ---- shipments ------------------------------------------------------------------------


def _shipment(db: Session, ctx: OrgContext, sid: uuid.UUID) -> Shipment:
    s = db.get(Shipment, sid)
    if s is None or s.organization_id != ctx.org.id:
        raise HTTPException(404, "Shipment not found")
    return s


@router.get("/orgs/{org_id}/shipments", response_model=Page[ShipmentOut])
def list_shipments(
    company_id: uuid.UUID | None = None,
    lane_id: uuid.UUID | None = None,
    status_: list[ShipmentStatus] | None = Query(default=None, alias="status"),
    q: str | None = Query(default=None, max_length=100),
    limit: int = Query(default=50, le=200),
    offset: int = Query(default=0, ge=0),
    ctx: OrgContext = Depends(get_org),
    db: Session = Depends(get_db),
) -> Page[ShipmentOut]:
    stmt = select(Shipment).where(Shipment.organization_id == ctx.org.id)
    if company_id:
        stmt = stmt.where(Shipment.company_id == company_id)
    if lane_id:
        stmt = stmt.where(Shipment.lane_id == lane_id)
    if status_:
        stmt = stmt.where(Shipment.status.in_(status_))
    if q:
        stmt = stmt.where(
            or_(Shipment.reference.ilike(f"%{q}%"), Shipment.vessel_name.ilike(f"%{q}%"))
        )
    stmt = stmt.order_by(Shipment.etd.desc().nulls_last(), Shipment.created_at.desc())
    rows, total = _paginate(db, stmt, limit, offset)
    return Page(items=[shipment_out(s) for s in rows], total=total, limit=limit, offset=offset)


@router.post("/orgs/{org_id}/shipments", response_model=ShipmentOut, status_code=201)
def create_shipment(
    body: ShipmentIn, ctx: OrgContext = Depends(get_org), db: Session = Depends(get_db)
) -> ShipmentOut:
    company = company_in_org(db, ctx, body.company_id)
    s = _domain(lambda: svc.new_shipment(db, company, body))
    db.flush()
    audit(
        db,
        actor=ctx.user.id,
        org=ctx.org.id,
        action="shipment.created",
        target_type="shipment",
        target_id=s.id,
        after=body.model_dump(mode="json"),
    )
    _commit(db, "Shipment conflicts with an existing record")
    request_screening(SubjectType.vessel, s.id)  # no-op (clears) when no vessel is named
    db.refresh(s)
    return shipment_out(s)


@router.get("/orgs/{org_id}/shipments/{shipment_id}", response_model=ShipmentOut)
def get_shipment(
    shipment_id: uuid.UUID, ctx: OrgContext = Depends(get_org), db: Session = Depends(get_db)
) -> ShipmentOut:
    return shipment_out(_shipment(db, ctx, shipment_id))


@router.put("/orgs/{org_id}/shipments/{shipment_id}", response_model=ShipmentOut)
def update_shipment(
    shipment_id: uuid.UUID,
    body: ShipmentPatch,
    ctx: OrgContext = Depends(get_org),
    db: Session = Depends(get_db),
) -> ShipmentOut:
    s = _shipment(db, ctx, shipment_id)
    company = company_in_org(db, ctx, s.company_id)
    before = shipment_out(s).model_dump(mode="json")
    _domain(lambda: svc.apply_shipment(db, company, s, body))
    audit(
        db,
        actor=ctx.user.id,
        org=ctx.org.id,
        action="shipment.updated",
        target_type="shipment",
        target_id=s.id,
        before=before,
        after=body.model_dump(mode="json"),
    )
    _commit(db, "Shipment conflicts with an existing record")
    request_screening(SubjectType.vessel, s.id)  # no-op (clears) when no vessel is named
    db.refresh(s)
    return shipment_out(s)


@router.delete("/orgs/{org_id}/shipments/{shipment_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_shipment(
    shipment_id: uuid.UUID, ctx: OrgContext = Depends(get_org), db: Session = Depends(get_db)
) -> None:
    s = _shipment(db, ctx, shipment_id)
    audit(
        db,
        actor=ctx.user.id,
        org=ctx.org.id,
        action="shipment.deleted",
        target_type="shipment",
        target_id=s.id,
        before=shipment_out(s).model_dump(mode="json"),
    )
    forget_subject(db, s.id)
    db.delete(s)
    db.commit()
