"""initial

Revision ID: 0001
Revises:
Create Date: 2026-10-28 10:27:10.839180
"""

from collections.abc import Sequence

import geoalchemy2
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS postgis")
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
    op.execute("CREATE EXTENSION IF NOT EXISTS citext")
    op.create_table(
        "data_sources",
        sa.Column("code", sa.String(length=40), nullable=False),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("url", sa.String(length=500), nullable=False),
        sa.Column("licence_note", sa.Text(), nullable=False),
        sa.Column("attribution", sa.Text(), nullable=True),
        sa.Column("stale_after_hours", sa.Integer(), nullable=False),
        sa.Column("last_success_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_data_as_of", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("code", name=op.f("pk_data_sources")),
    )
    op.create_table(
        "organizations",
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column(
            "type", sa.Enum("exporter", "importer", "forwarder", name="org_type"), nullable=False
        ),
        sa.Column("is_demo", sa.Boolean(), server_default="false", nullable=False),
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_organizations")),
    )
    op.create_table(
        "ports",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("unlocode", sa.String(length=5), nullable=False),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column("name_ascii", sa.String(length=200), nullable=False),
        sa.Column("country_iso2", sa.String(length=2), nullable=False),
        sa.Column("subdivision", sa.String(length=10), nullable=True),
        sa.Column("is_seaport", sa.Boolean(), nullable=False),
        sa.Column("is_airport", sa.Boolean(), nullable=False),
        sa.Column(
            "geom",
            geoalchemy2.types.Geography(
                geometry_type="POINT",
                srid=4326,
                spatial_index=False,
                from_text="ST_GeogFromText",
                name="geography",
            ),
            nullable=True,
        ),
        sa.Column(
            "coord_source",
            sa.Enum("manual", "wpi", "unlocode", "natural_earth", name="coord_source"),
            nullable=True,
        ),
        sa.Column("wpi_port_number", sa.Integer(), nullable=True),
        sa.Column("harbor_size", sa.String(length=2), nullable=True),
        sa.Column("source_as_of", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_ports")),
        sa.UniqueConstraint("unlocode", name=op.f("uq_ports_unlocode")),
    )
    op.create_index("idx_ports_geom", "ports", ["geom"], unique=False, postgresql_using="gist")
    op.create_index(op.f("ix_ports_country_iso2"), "ports", ["country_iso2"], unique=False)
    op.create_index(
        "ix_ports_name_trgm",
        "ports",
        ["name_ascii"],
        unique=False,
        postgresql_using="gin",
        postgresql_ops={"name_ascii": "gin_trgm_ops"},
    )
    op.create_table(
        "source_runs",
        sa.Column("source_code", sa.String(length=40), nullable=False),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("rows", sa.Integer(), nullable=True),
        sa.Column("content_hash", sa.String(length=64), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("details", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("id", sa.UUID(), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_source_runs")),
    )
    op.create_index(
        op.f("ix_source_runs_source_code"), "source_runs", ["source_code"], unique=False
    )
    op.create_table(
        "users",
        sa.Column("email", postgresql.CITEXT(), nullable=False),
        sa.Column("full_name", sa.String(length=200), nullable=False),
        sa.Column("password_hash", sa.String(length=255), nullable=True),
        sa.Column("email_verified_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("is_active", sa.Boolean(), server_default="true", nullable=False),
        sa.Column("is_platform_admin", sa.Boolean(), server_default="false", nullable=False),
        sa.Column("last_login_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_users")),
        sa.UniqueConstraint("email", name=op.f("uq_users_email")),
    )
    op.create_table(
        "audit_log",
        sa.Column(
            "at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
        ),
        sa.Column("actor_user_id", sa.UUID(), nullable=True),
        sa.Column("organization_id", sa.UUID(), nullable=True),
        sa.Column("action", sa.String(length=80), nullable=False),
        sa.Column("target_type", sa.String(length=60), nullable=False),
        sa.Column("target_id", sa.String(length=80), nullable=True),
        sa.Column("before", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("after", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("note", sa.Text(), nullable=True),
        sa.Column("id", sa.UUID(), nullable=False),
        sa.ForeignKeyConstraint(
            ["actor_user_id"],
            ["users.id"],
            name=op.f("fk_audit_log_actor_user_id_users"),
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["organization_id"],
            ["organizations.id"],
            name=op.f("fk_audit_log_organization_id_organizations"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_audit_log")),
    )
    op.create_index(op.f("ix_audit_log_at"), "audit_log", ["at"], unique=False)
    op.create_index(
        op.f("ix_audit_log_organization_id"), "audit_log", ["organization_id"], unique=False
    )
    op.create_table(
        "auth_tokens",
        sa.Column(
            "type",
            sa.Enum("verify_email", "reset_password", "invite", name="auth_token_type"),
            nullable=False,
        ),
        sa.Column("token_hash", sa.String(length=64), nullable=False),
        sa.Column("user_id", sa.UUID(), nullable=True),
        sa.Column("email", postgresql.CITEXT(), nullable=True),
        sa.Column("organization_id", sa.UUID(), nullable=True),
        sa.Column("role", sa.Enum("owner", "staff", name="member_role"), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("used_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("id", sa.UUID(), nullable=False),
        sa.ForeignKeyConstraint(
            ["organization_id"],
            ["organizations.id"],
            name=op.f("fk_auth_tokens_organization_id_organizations"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["user_id"], ["users.id"], name=op.f("fk_auth_tokens_user_id_users"), ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_auth_tokens")),
        sa.UniqueConstraint("token_hash", name=op.f("uq_auth_tokens_token_hash")),
    )
    op.create_table(
        "client_companies",
        sa.Column("organization_id", sa.UUID(), nullable=False),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column("is_self", sa.Boolean(), server_default="false", nullable=False),
        sa.Column("iec_code", sa.String(length=10), nullable=True),
        sa.Column("gstin", sa.String(length=15), nullable=True),
        sa.Column("contact_email", sa.String(length=254), nullable=True),
        sa.Column("is_demo", sa.Boolean(), server_default="false", nullable=False),
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["organization_id"],
            ["organizations.id"],
            name=op.f("fk_client_companies_organization_id_organizations"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_client_companies")),
        sa.UniqueConstraint(
            "organization_id", "name", name=op.f("uq_client_companies_organization_id")
        ),
    )
    op.create_index(
        op.f("ix_client_companies_organization_id"),
        "client_companies",
        ["organization_id"],
        unique=False,
    )
    op.create_table(
        "memberships",
        sa.Column("user_id", sa.UUID(), nullable=False),
        sa.Column("organization_id", sa.UUID(), nullable=False),
        sa.Column(
            "role",
            postgresql.ENUM("owner", "staff", name="member_role", create_type=False),
            nullable=False,
        ),
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["organization_id"],
            ["organizations.id"],
            name=op.f("fk_memberships_organization_id_organizations"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["user_id"], ["users.id"], name=op.f("fk_memberships_user_id_users"), ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_memberships")),
        sa.UniqueConstraint("user_id", "organization_id", name=op.f("uq_memberships_user_id")),
    )
    op.create_index(
        op.f("ix_memberships_organization_id"), "memberships", ["organization_id"], unique=False
    )
    op.create_table(
        "oauth_identities",
        sa.Column("user_id", sa.UUID(), nullable=False),
        sa.Column("provider", sa.String(length=40), nullable=False),
        sa.Column("subject", sa.String(length=255), nullable=False),
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["users.id"],
            name=op.f("fk_oauth_identities_user_id_users"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_oauth_identities")),
        sa.UniqueConstraint("provider", "subject", name=op.f("uq_oauth_identities_provider")),
    )
    op.create_table(
        "user_sessions",
        sa.Column("user_id", sa.UUID(), nullable=False),
        sa.Column("token_hash", sa.String(length=64), nullable=False),
        sa.Column("csrf_token", sa.String(length=64), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("ip", sa.String(length=64), nullable=True),
        sa.Column("user_agent", sa.String(length=400), nullable=True),
        sa.Column("id", sa.UUID(), nullable=False),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["users.id"],
            name=op.f("fk_user_sessions_user_id_users"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_user_sessions")),
        sa.UniqueConstraint("token_hash", name=op.f("uq_user_sessions_token_hash")),
    )
    op.create_index(op.f("ix_user_sessions_user_id"), "user_sessions", ["user_id"], unique=False)
    op.create_table(
        "csv_imports",
        sa.Column("organization_id", sa.UUID(), nullable=False),
        sa.Column("company_id", sa.UUID(), nullable=False),
        sa.Column("created_by", sa.UUID(), nullable=True),
        sa.Column("kind", sa.String(length=20), nullable=False),
        sa.Column("filename", sa.String(length=255), nullable=False),
        sa.Column("row_count", sa.Integer(), nullable=False),
        sa.Column("created_count", sa.Integer(), nullable=False),
        sa.Column("error_count", sa.Integer(), nullable=False),
        sa.Column("errors", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["company_id"],
            ["client_companies.id"],
            name=op.f("fk_csv_imports_company_id_client_companies"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["created_by"],
            ["users.id"],
            name=op.f("fk_csv_imports_created_by_users"),
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["organization_id"],
            ["organizations.id"],
            name=op.f("fk_csv_imports_organization_id_organizations"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_csv_imports")),
    )
    op.create_index(
        op.f("ix_csv_imports_organization_id"), "csv_imports", ["organization_id"], unique=False
    )
    op.create_table(
        "partners",
        sa.Column("organization_id", sa.UUID(), nullable=False),
        sa.Column("company_id", sa.UUID(), nullable=False),
        sa.Column("kind", sa.Enum("buyer", "supplier", name="partner_kind"), nullable=False),
        sa.Column("name", sa.String(length=300), nullable=False),
        sa.Column("country_iso2", sa.String(length=2), nullable=False),
        sa.Column("city", sa.String(length=120), nullable=True),
        sa.Column("port_id", sa.Integer(), nullable=True),
        sa.Column("address", sa.Text(), nullable=True),
        sa.Column("contact_email", sa.String(length=254), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("is_demo", sa.Boolean(), server_default="false", nullable=False),
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "char_length(country_iso2) = 2", name=op.f("ck_partners_country_iso2_len")
        ),
        sa.ForeignKeyConstraint(
            ["company_id"],
            ["client_companies.id"],
            name=op.f("fk_partners_company_id_client_companies"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["organization_id"],
            ["organizations.id"],
            name=op.f("fk_partners_organization_id_organizations"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["port_id"], ["ports.id"], name=op.f("fk_partners_port_id_ports"), ondelete="SET NULL"
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_partners")),
    )
    op.create_index("ix_partners_company_name", "partners", ["company_id", "name"], unique=False)
    op.create_index(
        op.f("ix_partners_organization_id"), "partners", ["organization_id"], unique=False
    )
    op.create_table(
        "lanes",
        sa.Column("organization_id", sa.UUID(), nullable=False),
        sa.Column("company_id", sa.UUID(), nullable=False),
        sa.Column("origin_port_id", sa.Integer(), nullable=False),
        sa.Column("dest_port_id", sa.Integer(), nullable=False),
        sa.Column("mode", sa.Enum("sea", "air", name="transport_mode"), nullable=False),
        sa.Column("partner_id", sa.UUID(), nullable=True),
        sa.Column("label", sa.String(length=200), nullable=True),
        sa.Column("active", sa.Boolean(), server_default="true", nullable=False),
        sa.Column("is_demo", sa.Boolean(), server_default="false", nullable=False),
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint("origin_port_id <> dest_port_id", name=op.f("ck_lanes_distinct_ports")),
        sa.ForeignKeyConstraint(
            ["company_id"],
            ["client_companies.id"],
            name=op.f("fk_lanes_company_id_client_companies"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["dest_port_id"], ["ports.id"], name=op.f("fk_lanes_dest_port_id_ports")
        ),
        sa.ForeignKeyConstraint(
            ["organization_id"],
            ["organizations.id"],
            name=op.f("fk_lanes_organization_id_organizations"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["origin_port_id"], ["ports.id"], name=op.f("fk_lanes_origin_port_id_ports")
        ),
        sa.ForeignKeyConstraint(
            ["partner_id"],
            ["partners.id"],
            name=op.f("fk_lanes_partner_id_partners"),
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_lanes")),
        sa.UniqueConstraint(
            "company_id", "origin_port_id", "dest_port_id", "mode", name=op.f("uq_lanes_company_id")
        ),
    )
    op.create_index(op.f("ix_lanes_organization_id"), "lanes", ["organization_id"], unique=False)
    op.create_table(
        "shipments",
        sa.Column("organization_id", sa.UUID(), nullable=False),
        sa.Column("company_id", sa.UUID(), nullable=False),
        sa.Column("lane_id", sa.UUID(), nullable=False),
        sa.Column("partner_id", sa.UUID(), nullable=True),
        sa.Column("reference", sa.String(length=100), nullable=False),
        sa.Column("container_refs", postgresql.ARRAY(sa.String(length=20)), nullable=False),
        sa.Column("container_count", sa.Integer(), nullable=True),
        sa.Column("etd", sa.Date(), nullable=True),
        sa.Column("eta", sa.Date(), nullable=True),
        sa.Column("value_inr", sa.Numeric(precision=16, scale=2), nullable=True),
        sa.Column("cargo_description", sa.String(length=300), nullable=True),
        sa.Column("hs_code", sa.String(length=10), nullable=True),
        sa.Column("vessel_name", sa.String(length=120), nullable=True),
        sa.Column("vessel_imo", sa.String(length=7), nullable=True),
        sa.Column(
            "status",
            sa.Enum(
                "planned", "booked", "in_transit", "delivered", "cancelled", name="shipment_status"
            ),
            nullable=False,
        ),
        sa.Column("is_demo", sa.Boolean(), server_default="false", nullable=False),
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "vessel_imo IS NULL OR vessel_imo ~ '^[0-9]{7}$'", name=op.f("ck_shipments_imo_format")
        ),
        sa.CheckConstraint(
            "eta IS NULL OR etd IS NULL OR eta >= etd", name=op.f("ck_shipments_eta_after_etd")
        ),
        sa.CheckConstraint(
            "value_inr IS NULL OR value_inr >= 0", name=op.f("ck_shipments_value_nonneg")
        ),
        sa.ForeignKeyConstraint(
            ["company_id"],
            ["client_companies.id"],
            name=op.f("fk_shipments_company_id_client_companies"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["lane_id"], ["lanes.id"], name=op.f("fk_shipments_lane_id_lanes"), ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(
            ["organization_id"],
            ["organizations.id"],
            name=op.f("fk_shipments_organization_id_organizations"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["partner_id"],
            ["partners.id"],
            name=op.f("fk_shipments_partner_id_partners"),
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_shipments")),
    )
    op.create_index(
        "ix_shipments_company_status", "shipments", ["company_id", "status"], unique=False
    )
    op.create_index(
        op.f("ix_shipments_organization_id"), "shipments", ["organization_id"], unique=False
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_shipments_organization_id"), table_name="shipments")
    op.drop_index("ix_shipments_company_status", table_name="shipments")
    op.drop_table("shipments")
    op.drop_index(op.f("ix_lanes_organization_id"), table_name="lanes")
    op.drop_table("lanes")
    op.drop_index(op.f("ix_partners_organization_id"), table_name="partners")
    op.drop_index("ix_partners_company_name", table_name="partners")
    op.drop_table("partners")
    op.drop_index(op.f("ix_csv_imports_organization_id"), table_name="csv_imports")
    op.drop_table("csv_imports")
    op.drop_index(op.f("ix_user_sessions_user_id"), table_name="user_sessions")
    op.drop_table("user_sessions")
    op.drop_table("oauth_identities")
    op.drop_index(op.f("ix_memberships_organization_id"), table_name="memberships")
    op.drop_table("memberships")
    op.drop_index(op.f("ix_client_companies_organization_id"), table_name="client_companies")
    op.drop_table("client_companies")
    op.drop_table("auth_tokens")
    op.drop_index(op.f("ix_audit_log_organization_id"), table_name="audit_log")
    op.drop_index(op.f("ix_audit_log_at"), table_name="audit_log")
    op.drop_table("audit_log")
    op.drop_table("users")
    op.drop_index(op.f("ix_source_runs_source_code"), table_name="source_runs")
    op.drop_table("source_runs")
    op.drop_index(
        "ix_ports_name_trgm",
        table_name="ports",
        postgresql_using="gin",
        postgresql_ops={"name_ascii": "gin_trgm_ops"},
    )
    op.drop_index(op.f("ix_ports_country_iso2"), table_name="ports")
    op.drop_index("idx_ports_geom", table_name="ports", postgresql_using="gist")
    op.drop_table("ports")
    op.drop_table("organizations")
    op.drop_table("data_sources")
    for enum in (
        "shipment_status",
        "transport_mode",
        "partner_kind",
        "member_role",
        "auth_token_type",
        "coord_source",
        "org_type",
    ):
        op.execute(f"DROP TYPE IF EXISTS {enum}")
