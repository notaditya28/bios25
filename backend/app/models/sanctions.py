import enum
import uuid
from datetime import datetime

from sqlalchemy import (
    Boolean,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, Timestamps, UUIDPk


class EntityType(enum.StrEnum):
    individual = "individual"
    entity = "entity"
    vessel = "vessel"
    aircraft = "aircraft"
    unknown = "unknown"


class SubjectType(enum.StrEnum):
    partner = "partner"
    company = "company"  # a forwarder's client company (or the org's own company)
    vessel = "vessel"  # vessel on a shipment (name + IMO)


class MatchStatus(enum.StrEnum):
    pending = "pending"
    confirmed = "confirmed"
    dismissed = "dismissed"


class SanctionList(Base):
    """One official list. version_hash changes whenever the published file content changes."""

    __tablename__ = "sanction_lists"

    code: Mapped[str] = mapped_column(String(20), primary_key=True)
    name: Mapped[str] = mapped_column(String(200))
    publisher: Mapped[str] = mapped_column(String(200))
    version_hash: Mapped[str | None] = mapped_column(String(64))
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    fetched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    entry_count: Mapped[int] = mapped_column(Integer, default=0)


class SanctionEntry(UUIDPk, Timestamps, Base):
    __tablename__ = "sanction_entries"
    __table_args__ = (
        UniqueConstraint("list_code", "source_uid"),
        Index("ix_sanction_entries_imo", "imo_numbers", postgresql_using="gin"),
    )

    list_code: Mapped[str] = mapped_column(ForeignKey("sanction_lists.code"), index=True)
    source_uid: Mapped[str] = mapped_column(String(80))
    entity_type: Mapped[EntityType] = mapped_column(Enum(EntityType, name="sanction_entity_type"))
    primary_name: Mapped[str] = mapped_column(Text)
    programs: Mapped[list[str]] = mapped_column(ARRAY(Text), default=list)
    countries: Mapped[list[str]] = mapped_column(ARRAY(String(2)), default=list)
    birth_dates: Mapped[list[str]] = mapped_column(ARRAY(String(20)), default=list)
    imo_numbers: Mapped[list[str]] = mapped_column(ARRAY(String(7)), default=list)
    listed_on: Mapped[str | None] = mapped_column(String(20))
    remarks: Mapped[str | None] = mapped_column(Text)
    content_hash: Mapped[str] = mapped_column(String(64))
    active: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    removed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    names: Mapped[list["SanctionName"]] = relationship(
        back_populates="entry", cascade="all, delete-orphan"
    )


class SanctionName(Base):
    __tablename__ = "sanction_names"
    __table_args__ = (
        Index(
            "ix_sanction_names_norm_trgm",
            "name_norm",
            postgresql_using="gin",
            postgresql_ops={"name_norm": "gin_trgm_ops"},
        ),
        Index("ix_sanction_names_keys", "token_keys", postgresql_using="gin"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    entry_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("sanction_entries.id", ondelete="CASCADE"), index=True
    )
    name: Mapped[str] = mapped_column(Text)  # as published (any script)
    name_norm: Mapped[str] = mapped_column(Text)  # transliterated, normalised, sorted tokens
    token_keys: Mapped[list[str]] = mapped_column(ARRAY(String(40)))  # phonetic skeletons
    script: Mapped[str | None] = mapped_column(String(40))
    is_primary: Mapped[bool] = mapped_column(Boolean, default=False)
    is_weak: Mapped[bool] = mapped_column(Boolean, default=False)  # "low quality"/weak alias

    entry: Mapped[SanctionEntry] = relationship(back_populates="names")


class Screening(UUIDPk, Base):
    """Latest screening of one subject. Re-runs update the row in place."""

    __tablename__ = "screenings"
    __table_args__ = (UniqueConstraint("subject_type", "subject_id"),)

    organization_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("organizations.id", ondelete="CASCADE"), index=True
    )
    subject_type: Mapped[SubjectType] = mapped_column(Enum(SubjectType, name="subject_type"))
    subject_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True))
    subject_name: Mapped[str] = mapped_column(Text)
    subject_country: Mapped[str | None] = mapped_column(String(2))
    subject_imo: Mapped[str | None] = mapped_column(String(7))
    fingerprint: Mapped[str] = mapped_column(String(64))  # hash of the screened inputs
    list_versions: Mapped[dict] = mapped_column(JSONB)
    run_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    candidates_checked: Mapped[int] = mapped_column(Integer, default=0)


class ScreeningMatch(UUIDPk, Timestamps, Base):
    __tablename__ = "screening_matches"
    __table_args__ = (
        UniqueConstraint("screening_id", "entry_id"),
        Index("ix_screening_matches_org_status", "organization_id", "status"),
    )

    organization_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("organizations.id", ondelete="CASCADE")
    )
    screening_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("screenings.id", ondelete="CASCADE"), index=True
    )
    entry_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("sanction_entries.id", ondelete="CASCADE")
    )
    score: Mapped[float] = mapped_column(Float)
    matched_name: Mapped[str] = mapped_column(Text)
    breakdown: Mapped[dict] = mapped_column(JSONB)
    status: Mapped[MatchStatus] = mapped_column(
        Enum(MatchStatus, name="match_status"), default=MatchStatus.pending
    )
    # A decision applies to exactly what was reviewed: if the subject's inputs or the list
    # entry change later, the match is re-opened for review.
    decided_fingerprint: Mapped[str | None] = mapped_column(String(64))
    decided_entry_hash: Mapped[str | None] = mapped_column(String(64))
    reviewed_by: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL")
    )
    reviewed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    review_note: Mapped[str | None] = mapped_column(Text)
    # False once a later screening no longer produces this match (e.g. delisted or renamed).
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    entry: Mapped[SanctionEntry] = relationship(lazy="joined")
    screening: Mapped[Screening] = relationship(lazy="joined")
