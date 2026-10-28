"""ORM → API schema conversion (kept in one place so routes and imports agree)."""

from geoalchemy2.shape import to_shape

from app.models import Lane, Partner, Port, Shipment
from app.schemas.trade import LaneOut, PartnerOut, PortOut, ShipmentOut


def port_out(p: Port) -> PortOut:
    lat = lon = None
    if p.geom is not None:
        pt = to_shape(p.geom)
        lat, lon = round(pt.y, 5), round(pt.x, 5)
    return PortOut(
        unlocode=p.unlocode,
        name=p.name,
        country_iso2=p.country_iso2,
        subdivision=p.subdivision,
        is_seaport=p.is_seaport,
        is_airport=p.is_airport,
        lat=lat,
        lon=lon,
        coord_source=p.coord_source,
        harbor_size=p.harbor_size,
    )


def partner_out(p: Partner) -> PartnerOut:
    return PartnerOut(
        id=p.id,
        company_id=p.company_id,
        kind=p.kind,
        name=p.name,
        country=p.country_iso2,
        city=p.city,
        port=port_out(p.port) if p.port else None,
        address=p.address,
        contact_email=p.contact_email,
        notes=p.notes,
        is_demo=p.is_demo,
    )


def lane_out(lane: Lane) -> LaneOut:
    return LaneOut(
        id=lane.id,
        company_id=lane.company_id,
        origin_port=port_out(lane.origin_port),
        dest_port=port_out(lane.dest_port),
        mode=lane.mode,
        partner_id=lane.partner_id,
        partner_name=lane.partner.name if lane.partner else None,
        label=lane.label,
        active=lane.active,
        is_demo=lane.is_demo,
    )


def shipment_out(s: Shipment) -> ShipmentOut:
    return ShipmentOut(
        id=s.id,
        company_id=s.company_id,
        lane=lane_out(s.lane),
        partner_id=s.partner_id,
        partner_name=s.partner.name if s.partner else None,
        reference=s.reference,
        container_refs=list(s.container_refs or []),
        container_count=s.container_count,
        etd=s.etd,
        eta=s.eta,
        value_inr=s.value_inr,
        cargo_description=s.cargo_description,
        hs_code=s.hs_code,
        vessel_name=s.vessel_name,
        vessel_imo=s.vessel_imo,
        status=s.status,
        is_demo=s.is_demo,
    )
