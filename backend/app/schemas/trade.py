import uuid
from datetime import date
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, EmailStr, Field, field_validator, model_validator

from app.models import CoordSource, PartnerKind, ShipmentStatus, TransportMode
from app.validators import normalize_container, normalize_country, normalize_imo, normalize_locode


class PortOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    unlocode: str
    name: str
    country_iso2: str
    subdivision: str | None
    is_seaport: bool
    is_airport: bool
    lat: float | None = None
    lon: float | None = None
    coord_source: CoordSource | None
    harbor_size: str | None


class PortCoordsIn(BaseModel):
    lat: float = Field(ge=-90, le=90)
    lon: float = Field(ge=-180, le=180)
    note: str = Field(min_length=3, max_length=500)  # provenance, required for audit


# ---- partners ---------------------------------------------------------------


class PartnerBase(BaseModel):
    kind: PartnerKind
    name: str = Field(min_length=1, max_length=300)
    country: str = Field(min_length=2, max_length=60)
    city: str | None = Field(default=None, max_length=120)
    port: str | None = None  # UN/LOCODE
    address: str | None = Field(default=None, max_length=1000)
    contact_email: EmailStr | None = None
    notes: str | None = Field(default=None, max_length=2000)

    @field_validator("name")
    @classmethod
    def _name(cls, v: str) -> str:
        return " ".join(v.split())

    @field_validator("country")
    @classmethod
    def _country(cls, v: str) -> str:
        return normalize_country(v)

    @field_validator("port")
    @classmethod
    def _port(cls, v: str | None) -> str | None:
        return normalize_locode(v) if v else None


class PartnerIn(PartnerBase):
    company_id: uuid.UUID


class PartnerPatch(PartnerBase):
    pass


class PartnerOut(BaseModel):
    id: uuid.UUID
    company_id: uuid.UUID
    kind: PartnerKind
    name: str
    country: str
    city: str | None
    port: PortOut | None
    address: str | None
    contact_email: str | None
    notes: str | None
    is_demo: bool


# ---- lanes ------------------------------------------------------------------


class LaneBase(BaseModel):
    origin_port: str
    dest_port: str
    mode: TransportMode = TransportMode.sea
    partner_id: uuid.UUID | None = None
    label: str | None = Field(default=None, max_length=200)
    active: bool = True

    @field_validator("origin_port", "dest_port")
    @classmethod
    def _locode(cls, v: str) -> str:
        return normalize_locode(v)

    @model_validator(mode="after")
    def _distinct(self) -> "LaneBase":
        if self.origin_port == self.dest_port:
            raise ValueError("origin and destination must differ")
        return self


class LaneIn(LaneBase):
    company_id: uuid.UUID


class LanePatch(LaneBase):
    pass


class LaneOut(BaseModel):
    id: uuid.UUID
    company_id: uuid.UUID
    origin_port: PortOut
    dest_port: PortOut
    mode: TransportMode
    partner_id: uuid.UUID | None
    partner_name: str | None
    label: str | None
    active: bool
    is_demo: bool


# ---- shipments --------------------------------------------------------------


class ShipmentBase(BaseModel):
    lane_id: uuid.UUID
    partner_id: uuid.UUID | None = None
    reference: str = Field(min_length=1, max_length=100)
    container_refs: list[str] = Field(default_factory=list, max_length=500)
    container_count: int | None = Field(default=None, ge=0, le=10000)
    etd: date | None = None
    eta: date | None = None
    value_inr: Decimal | None = Field(default=None, ge=0, max_digits=16, decimal_places=2)
    cargo_description: str | None = Field(default=None, max_length=300)
    hs_code: str | None = Field(default=None, pattern=r"^[0-9]{4,10}$")
    vessel_name: str | None = Field(default=None, max_length=120)
    vessel_imo: str | None = None
    status: ShipmentStatus = ShipmentStatus.planned

    @field_validator("container_refs")
    @classmethod
    def _containers(cls, v: list[str]) -> list[str]:
        out = [normalize_container(c) for c in v if c.strip()]
        if len(set(out)) != len(out):
            raise ValueError("duplicate container numbers")
        return out

    @field_validator("vessel_imo")
    @classmethod
    def _imo(cls, v: str | None) -> str | None:
        return normalize_imo(v) if v else None

    @model_validator(mode="after")
    def _dates(self) -> "ShipmentBase":
        if self.etd and self.eta and self.eta < self.etd:
            raise ValueError("ETA must be on or after ETD")
        if self.container_count is None and self.container_refs:
            self.container_count = len(self.container_refs)
        return self


class ShipmentIn(ShipmentBase):
    company_id: uuid.UUID


class ShipmentPatch(ShipmentBase):
    pass


class ShipmentOut(BaseModel):
    id: uuid.UUID
    company_id: uuid.UUID
    lane: LaneOut
    partner_id: uuid.UUID | None
    partner_name: str | None
    reference: str
    container_refs: list[str]
    container_count: int | None
    etd: date | None
    eta: date | None
    value_inr: Decimal | None
    cargo_description: str | None
    hs_code: str | None
    vessel_name: str | None
    vessel_imo: str | None
    status: ShipmentStatus
    is_demo: bool


class Page[T](BaseModel):
    items: list[T]
    total: int
    limit: int
    offset: int


class ImportRowError(BaseModel):
    row: int  # 1-based data row (header excluded)
    field: str | None
    message: str


class ImportResult(BaseModel):
    id: uuid.UUID | None
    kind: str
    dry_run: bool
    row_count: int
    created_count: int
    errors: list[ImportRowError]
    status: str
