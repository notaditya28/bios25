import enum
import uuid
from datetime import datetime

from sqlalchemy import (
    Boolean,
    DateTime,
    Enum,
    ForeignKey,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import CITEXT, JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, Timestamps, UUIDPk


class OrgType(enum.StrEnum):
    exporter = "exporter"
    importer = "importer"
    forwarder = "forwarder"


class Role(enum.StrEnum):
    owner = "owner"
    staff = "staff"


class TokenType(enum.StrEnum):
    verify_email = "verify_email"
    reset_password = "reset_password"  # noqa: S105 - enum label, not a secret
    invite = "invite"


class Organization(UUIDPk, Timestamps, Base):
    __tablename__ = "organizations"

    name: Mapped[str] = mapped_column(String(200))
    type: Mapped[OrgType] = mapped_column(Enum(OrgType, name="org_type"))
    is_demo: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")

    memberships: Mapped[list["Membership"]] = relationship(back_populates="organization")
    companies: Mapped[list["ClientCompany"]] = relationship(back_populates="organization")


class User(UUIDPk, Timestamps, Base):
    __tablename__ = "users"

    email: Mapped[str] = mapped_column(CITEXT, unique=True)
    full_name: Mapped[str] = mapped_column(String(200))
    password_hash: Mapped[str | None] = mapped_column(String(255))  # null for OAuth-only users
    email_verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, server_default="true")
    is_platform_admin: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
    last_login_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    memberships: Mapped[list["Membership"]] = relationship(back_populates="user")


class Membership(UUIDPk, Timestamps, Base):
    __tablename__ = "memberships"
    __table_args__ = (UniqueConstraint("user_id", "organization_id"),)

    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"))
    organization_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("organizations.id", ondelete="CASCADE"), index=True
    )
    role: Mapped[Role] = mapped_column(Enum(Role, name="member_role"))

    user: Mapped[User] = relationship(back_populates="memberships")
    organization: Mapped[Organization] = relationship(back_populates="memberships")


class UserSession(UUIDPk, Base):
    """Server-side session. Only the SHA-256 hash of the cookie token is stored."""

    __tablename__ = "user_sessions"

    user_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), index=True
    )
    token_hash: Mapped[str] = mapped_column(String(64), unique=True)
    csrf_token: Mapped[str] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    ip: Mapped[str | None] = mapped_column(String(64))
    user_agent: Mapped[str | None] = mapped_column(String(400))

    user: Mapped[User] = relationship()


class AuthToken(UUIDPk, Base):
    """Single-use emailed token (verification, password reset, invitation)."""

    __tablename__ = "auth_tokens"

    type: Mapped[TokenType] = mapped_column(Enum(TokenType, name="auth_token_type"))
    token_hash: Mapped[str] = mapped_column(String(64), unique=True)
    user_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"))
    # Invitations target an email + org before a user may exist.
    email: Mapped[str | None] = mapped_column(CITEXT)
    organization_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("organizations.id", ondelete="CASCADE")
    )
    role: Mapped[Role | None] = mapped_column(Enum(Role, name="member_role"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    used_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class OAuthIdentity(UUIDPk, Timestamps, Base):
    """Reserved for Google sign-in (not wired yet)."""

    __tablename__ = "oauth_identities"
    __table_args__ = (UniqueConstraint("provider", "subject"),)

    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"))
    provider: Mapped[str] = mapped_column(String(40))
    subject: Mapped[str] = mapped_column(String(255))


class ClientCompany(UUIDPk, Timestamps, Base):
    """The trading company whose partners/lanes/shipments are tracked.

    Exporter/importer orgs have exactly one (is_self=True). Forwarders have one per client.
    """

    __tablename__ = "client_companies"
    __table_args__ = (UniqueConstraint("organization_id", "name"),)

    organization_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("organizations.id", ondelete="CASCADE"), index=True
    )
    name: Mapped[str] = mapped_column(String(200))
    is_self: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
    iec_code: Mapped[str | None] = mapped_column(String(10))  # DGFT Importer-Exporter Code
    gstin: Mapped[str | None] = mapped_column(String(15))
    contact_email: Mapped[str | None] = mapped_column(String(254))
    is_demo: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")

    organization: Mapped[Organization] = relationship(back_populates="companies")


class AuditLog(UUIDPk, Base):
    __tablename__ = "audit_log"

    at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), index=True
    )
    actor_user_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL")
    )
    organization_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("organizations.id", ondelete="CASCADE"), index=True
    )
    action: Mapped[str] = mapped_column(String(80))
    target_type: Mapped[str] = mapped_column(String(60))
    target_id: Mapped[str | None] = mapped_column(String(80))
    before: Mapped[dict | None] = mapped_column(JSONB)
    after: Mapped[dict | None] = mapped_column(JSONB)
    note: Mapped[str | None] = mapped_column(Text)
