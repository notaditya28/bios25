import enum
import uuid
from datetime import date, datetime
from decimal import Decimal

from geoalchemy2 import Geography
from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Date,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, Timestamps, UUIDPk


class CoordSource(enum.StrEnum):
    manual = "manual"  # set by a platform admin; never overwritten by ingestion
    wpi = "wpi"  # NGA World Port Index
    unlocode = "unlocode"  # UN/LOCODE (minute precision)
    natural_earth = "natural_earth"


# Lower number wins when several sources provide coordinates.
COORD_PRIORITY = {
    CoordSource.manual: 0,
    CoordSource.wpi: 1,
    CoordSource.unlocode: 2,
    CoordSource.natural_earth: 3,
}


class PartnerKind(enum.StrEnum):
    buyer = "buyer"
    supplier = "supplier"


class TransportMode(enum.StrEnum):
    sea = "sea"
    air = "air"


class ShipmentStatus(enum.StrEnum):
    planned = "planned"
    booked = "booked"
    in_transit = "in_transit"
    delivered = "delivered"
    cancelled = "cancelled"


class Port(Timestamps, Base):
    """Reference location (seaport or airport), keyed by UN/LOCODE, e.g. INNSA."""

    __tablename__ = "ports"
    __table_args__ = (
        Index(
            "ix_ports_name_trgm",
            "name_ascii",
            postgresql_using="gin",
            postgresql_ops={"name_ascii": "gin_trgm_ops"},
        ),
        Index("idx_ports_geom", "geom", postgresql_using="gist"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    unlocode: Mapped[str] = mapped_column(String(5), unique=True)
    name: Mapped[str] = mapped_column(String(200))
    name_ascii: Mapped[str] = mapped_column(String(200))
    country_iso2: Mapped[str] = mapped_column(String(2), index=True)
    subdivision: Mapped[str | None] = mapped_column(String(10))
    is_seaport: Mapped[bool] = mapped_column(Boolean, default=False)
    is_airport: Mapped[bool] = mapped_column(Boolean, default=False)
    geom = mapped_column(Geography("POINT", srid=4326, spatial_index=False), nullable=True)
    coord_source: Mapped[CoordSource | None] = mapped_column(Enum(CoordSource, name="coord_source"))
    wpi_port_number: Mapped[int | None] = mapped_column(Integer)
    harbor_size: Mapped[str | None] = mapped_column(String(2))  # WPI: V/S/M/L
    source_as_of: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class Partner(UUIDPk, Timestamps, Base):
    __tablename__ = "partners"
    __table_args__ = (
        Index("ix_partners_company_name", "company_id", "name"),
        CheckConstraint("char_length(country_iso2) = 2", name="country_iso2_len"),
    )

    organization_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("organizations.id", ondelete="CASCADE"), index=True
    )
    company_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("client_companies.id", ondelete="CASCADE")
    )
    kind: Mapped[PartnerKind] = mapped_column(Enum(PartnerKind, name="partner_kind"))
    name: Mapped[str] = mapped_column(String(300))
    country_iso2: Mapped[str] = mapped_column(String(2))
    city: Mapped[str | None] = mapped_column(String(120))
    port_id: Mapped[int | None] = mapped_column(ForeignKey("ports.id", ondelete="SET NULL"))
    address: Mapped[str | None] = mapped_column(Text)
    contact_email: Mapped[str | None] = mapped_column(String(254))
    notes: Mapped[str | None] = mapped_column(Text)
    is_demo: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")

    port: Mapped[Port | None] = relationship(lazy="joined")


class Lane(UUIDPk, Timestamps, Base):
    __tablename__ = "lanes"
    __table_args__ = (
        UniqueConstraint("company_id", "origin_port_id", "dest_port_id", "mode"),
        CheckConstraint("origin_port_id <> dest_port_id", name="distinct_ports"),
    )

    organization_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("organizations.id", ondelete="CASCADE"), index=True
    )
    company_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("client_companies.id", ondelete="CASCADE")
    )
    origin_port_id: Mapped[int] = mapped_column(ForeignKey("ports.id"))
    dest_port_id: Mapped[int] = mapped_column(ForeignKey("ports.id"))
    mode: Mapped[TransportMode] = mapped_column(Enum(TransportMode, name="transport_mode"))
    partner_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("partners.id", ondelete="SET NULL")
    )
    label: Mapped[str | None] = mapped_column(String(200))
    active: Mapped[bool] = mapped_column(Boolean, default=True, server_default="true")
    is_demo: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")

    origin_port: Mapped[Port] = relationship(foreign_keys=[origin_port_id], lazy="joined")
    dest_port: Mapped[Port] = relationship(foreign_keys=[dest_port_id], lazy="joined")
    partner: Mapped[Partner | None] = relationship(lazy="joined")


class Shipment(UUIDPk, Timestamps, Base):
    __tablename__ = "shipments"
    __table_args__ = (
        CheckConstraint("eta IS NULL OR etd IS NULL OR eta >= etd", name="eta_after_etd"),
        CheckConstraint("value_inr IS NULL OR value_inr >= 0", name="value_nonneg"),
        CheckConstraint("vessel_imo IS NULL OR vessel_imo ~ '^[0-9]{7}$'", name="imo_format"),
        Index("ix_shipments_company_status", "company_id", "status"),
    )

    organization_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("organizations.id", ondelete="CASCADE"), index=True
    )
    company_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("client_companies.id", ondelete="CASCADE")
    )
    lane_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("lanes.id", ondelete="RESTRICT"))
    partner_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("partners.id", ondelete="SET NULL")
    )
    reference: Mapped[str] = mapped_column(String(100))  # booking / internal ref
    container_refs: Mapped[list[str]] = mapped_column(ARRAY(String(20)), default=list)
    container_count: Mapped[int | None] = mapped_column(Integer)
    etd: Mapped[date | None] = mapped_column(Date)
    eta: Mapped[date | None] = mapped_column(Date)
    value_inr: Mapped[Decimal | None] = mapped_column(Numeric(16, 2))
    cargo_description: Mapped[str | None] = mapped_column(String(300))
    hs_code: Mapped[str | None] = mapped_column(String(10))
    vessel_name: Mapped[str | None] = mapped_column(String(120))
    vessel_imo: Mapped[str | None] = mapped_column(String(7))
    status: Mapped[ShipmentStatus] = mapped_column(
        Enum(ShipmentStatus, name="shipment_status"), default=ShipmentStatus.planned
    )
    is_demo: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")

    lane: Mapped[Lane] = relationship(lazy="joined")
    partner: Mapped[Partner | None] = relationship(lazy="joined")


class CsvImport(UUIDPk, Timestamps, Base):
    __tablename__ = "csv_imports"

    organization_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("organizations.id", ondelete="CASCADE"), index=True
    )
    company_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("client_companies.id", ondelete="CASCADE")
    )
    created_by: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL")
    )
    kind: Mapped[str] = mapped_column(String(20))  # partners | lanes | shipments
    filename: Mapped[str] = mapped_column(String(255))
    row_count: Mapped[int] = mapped_column(Integer, default=0)
    created_count: Mapped[int] = mapped_column(Integer, default=0)
    error_count: Mapped[int] = mapped_column(Integer, default=0)
    errors: Mapped[list[dict]] = mapped_column(JSONB, default=list)
    status: Mapped[str] = mapped_column(String(20))  # committed | rejected
