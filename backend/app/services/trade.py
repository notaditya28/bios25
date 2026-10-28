"""Trade-entity writes shared by the REST routes and the CSV importer.

Every function takes the tenant (org id + company) explicitly and re-checks that referenced
rows (partners, lanes) belong to the same company, so cross-tenant references are impossible.
"""

import uuid

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import ClientCompany, Lane, Partner, Port, Shipment, TransportMode
from app.schemas.trade import LaneBase, PartnerBase, ShipmentBase


class DomainError(ValueError):
    def __init__(self, message: str, field: str | None = None):
        super().__init__(message)
        self.field = field


def port_by_locode(db: Session, locode: str, *, mode: TransportMode | None = None) -> Port:
    port = db.scalar(select(Port).where(Port.unlocode == locode))
    if port is None:
        raise DomainError(f"unknown port '{locode}'", "port")
    if mode == TransportMode.sea and not port.is_seaport:
        raise DomainError(f"{locode} ({port.name}) is not listed as a seaport", "port")
    if mode == TransportMode.air and not port.is_airport:
        raise DomainError(f"{locode} ({port.name}) is not listed as an airport", "port")
    return port


def _partner_in_company(db: Session, company: ClientCompany, pid: uuid.UUID | None) -> None:
    if pid is None:
        return
    p = db.get(Partner, pid)
    if p is None or p.company_id != company.id:
        raise DomainError("partner not found for this company", "partner_id")


def apply_partner(db: Session, partner: Partner, data: PartnerBase) -> Partner:
    partner.kind = data.kind
    partner.name = data.name
    partner.country_iso2 = data.country
    partner.city = data.city
    partner.port_id = port_by_locode(db, data.port).id if data.port else None
    partner.address = data.address
    partner.contact_email = data.contact_email
    partner.notes = data.notes
    return partner


def new_partner(db: Session, company: ClientCompany, data: PartnerBase) -> Partner:
    p = Partner(organization_id=company.organization_id, company_id=company.id)
    apply_partner(db, p, data)
    db.add(p)
    return p


def apply_lane(db: Session, company: ClientCompany, lane: Lane, data: LaneBase) -> Lane:
    origin = port_by_locode(db, data.origin_port, mode=data.mode)
    dest = port_by_locode(db, data.dest_port, mode=data.mode)
    _partner_in_company(db, company, data.partner_id)
    lane.origin_port_id = origin.id
    lane.dest_port_id = dest.id
    lane.mode = data.mode
    lane.partner_id = data.partner_id
    lane.label = data.label
    lane.active = data.active
    return lane


def new_lane(db: Session, company: ClientCompany, data: LaneBase) -> Lane:
    lane = Lane(organization_id=company.organization_id, company_id=company.id)
    apply_lane(db, company, lane, data)
    dup = db.scalar(
        select(Lane).where(
            Lane.company_id == company.id,
            Lane.origin_port_id == lane.origin_port_id,
            Lane.dest_port_id == lane.dest_port_id,
            Lane.mode == lane.mode,
        )
    )
    if dup is not None:
        raise DomainError("this lane already exists", "origin_port")
    db.add(lane)
    return lane


def find_lane(
    db: Session, company: ClientCompany, origin: str, dest: str, mode: TransportMode
) -> Lane:
    o = port_by_locode(db, origin)
    d = port_by_locode(db, dest)
    lane = db.scalar(
        select(Lane).where(
            Lane.company_id == company.id,
            Lane.origin_port_id == o.id,
            Lane.dest_port_id == d.id,
            Lane.mode == mode,
        )
    )
    if lane is None:
        raise DomainError(f"no {mode.value} lane {origin} → {dest}; create the lane first", "lane")
    return lane


def apply_shipment(
    db: Session, company: ClientCompany, s: Shipment, data: ShipmentBase
) -> Shipment:
    lane = db.get(Lane, data.lane_id)
    if lane is None or lane.company_id != company.id:
        raise DomainError("lane not found for this company", "lane_id")
    _partner_in_company(db, company, data.partner_id)
    for field in (
        "lane_id",
        "partner_id",
        "reference",
        "container_refs",
        "container_count",
        "etd",
        "eta",
        "value_inr",
        "cargo_description",
        "hs_code",
        "vessel_name",
        "vessel_imo",
        "status",
    ):
        setattr(s, field, getattr(data, field))
    if s.partner_id is None and lane.partner_id is not None:
        s.partner_id = lane.partner_id
    return s


def new_shipment(db: Session, company: ClientCompany, data: ShipmentBase) -> Shipment:
    s = Shipment(organization_id=company.organization_id, company_id=company.id)
    apply_shipment(db, company, s, data)
    db.add(s)
    return s
