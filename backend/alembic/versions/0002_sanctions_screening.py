"""sanctions_screening

Revision ID: 0002
Revises: 0001
Create Date: 2026-10-28 11:38:19.829398
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "sanction_lists",
        sa.Column("code", sa.String(length=20), nullable=False),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column("publisher", sa.String(length=200), nullable=False),
        sa.Column("version_hash", sa.String(length=64), nullable=True),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("entry_count", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("code", name=op.f("pk_sanction_lists")),
    )
    op.create_table(
        "sanction_entries",
        sa.Column("list_code", sa.String(length=20), nullable=False),
        sa.Column("source_uid", sa.String(length=80), nullable=False),
        sa.Column(
            "entity_type",
            sa.Enum(
                "individual", "entity", "vessel", "aircraft", "unknown", name="sanction_entity_type"
            ),
            nullable=False,
        ),
        sa.Column("primary_name", sa.Text(), nullable=False),
        sa.Column("programs", postgresql.ARRAY(sa.Text()), nullable=False),
        sa.Column("countries", postgresql.ARRAY(sa.String(length=2)), nullable=False),
        sa.Column("birth_dates", postgresql.ARRAY(sa.String(length=20)), nullable=False),
        sa.Column("imo_numbers", postgresql.ARRAY(sa.String(length=7)), nullable=False),
        sa.Column("listed_on", sa.String(length=20), nullable=True),
        sa.Column("remarks", sa.Text(), nullable=True),
        sa.Column("content_hash", sa.String(length=64), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.Column("removed_at", sa.DateTime(timezone=True), nullable=True),
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
            ["list_code"],
            ["sanction_lists.code"],
            name=op.f("fk_sanction_entries_list_code_sanction_lists"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_sanction_entries")),
        sa.UniqueConstraint("list_code", "source_uid", name=op.f("uq_sanction_entries_list_code")),
    )
    op.create_index(
        op.f("ix_sanction_entries_active"), "sanction_entries", ["active"], unique=False
    )
    op.create_index(
        "ix_sanction_entries_imo",
        "sanction_entries",
        ["imo_numbers"],
        unique=False,
        postgresql_using="gin",
    )
    op.create_index(
        op.f("ix_sanction_entries_list_code"), "sanction_entries", ["list_code"], unique=False
    )
    op.create_table(
        "screenings",
        sa.Column("organization_id", sa.UUID(), nullable=False),
        sa.Column(
            "subject_type",
            sa.Enum("partner", "company", "vessel", name="subject_type"),
            nullable=False,
        ),
        sa.Column("subject_id", sa.UUID(), nullable=False),
        sa.Column("subject_name", sa.Text(), nullable=False),
        sa.Column("subject_country", sa.String(length=2), nullable=True),
        sa.Column("subject_imo", sa.String(length=7), nullable=True),
        sa.Column("fingerprint", sa.String(length=64), nullable=False),
        sa.Column("list_versions", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "run_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
        ),
        sa.Column("candidates_checked", sa.Integer(), nullable=False),
        sa.Column("id", sa.UUID(), nullable=False),
        sa.ForeignKeyConstraint(
            ["organization_id"],
            ["organizations.id"],
            name=op.f("fk_screenings_organization_id_organizations"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_screenings")),
        sa.UniqueConstraint("subject_type", "subject_id", name=op.f("uq_screenings_subject_type")),
    )
    op.create_index(
        op.f("ix_screenings_organization_id"), "screenings", ["organization_id"], unique=False
    )
    op.create_table(
        "sanction_names",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("entry_id", sa.UUID(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("name_norm", sa.Text(), nullable=False),
        sa.Column("token_keys", postgresql.ARRAY(sa.String(length=40)), nullable=False),
        sa.Column("script", sa.String(length=40), nullable=True),
        sa.Column("is_primary", sa.Boolean(), nullable=False),
        sa.Column("is_weak", sa.Boolean(), nullable=False),
        sa.ForeignKeyConstraint(
            ["entry_id"],
            ["sanction_entries.id"],
            name=op.f("fk_sanction_names_entry_id_sanction_entries"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_sanction_names")),
    )
    op.create_index(
        op.f("ix_sanction_names_entry_id"), "sanction_names", ["entry_id"], unique=False
    )
    op.create_index(
        "ix_sanction_names_keys",
        "sanction_names",
        ["token_keys"],
        unique=False,
        postgresql_using="gin",
    )
    op.create_index(
        "ix_sanction_names_norm_trgm",
        "sanction_names",
        ["name_norm"],
        unique=False,
        postgresql_using="gin",
        postgresql_ops={"name_norm": "gin_trgm_ops"},
    )
    op.create_table(
        "screening_matches",
        sa.Column("organization_id", sa.UUID(), nullable=False),
        sa.Column("screening_id", sa.UUID(), nullable=False),
        sa.Column("entry_id", sa.UUID(), nullable=False),
        sa.Column("score", sa.Float(), nullable=False),
        sa.Column("matched_name", sa.Text(), nullable=False),
        sa.Column("breakdown", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "status",
            sa.Enum("pending", "confirmed", "dismissed", name="match_status"),
            nullable=False,
        ),
        sa.Column("decided_fingerprint", sa.String(length=64), nullable=True),
        sa.Column("decided_entry_hash", sa.String(length=64), nullable=True),
        sa.Column("reviewed_by", sa.UUID(), nullable=True),
        sa.Column("reviewed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("review_note", sa.Text(), nullable=True),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.Column(
            "last_seen_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
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
            ["entry_id"],
            ["sanction_entries.id"],
            name=op.f("fk_screening_matches_entry_id_sanction_entries"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["organization_id"],
            ["organizations.id"],
            name=op.f("fk_screening_matches_organization_id_organizations"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["reviewed_by"],
            ["users.id"],
            name=op.f("fk_screening_matches_reviewed_by_users"),
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["screening_id"],
            ["screenings.id"],
            name=op.f("fk_screening_matches_screening_id_screenings"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_screening_matches")),
        sa.UniqueConstraint(
            "screening_id", "entry_id", name=op.f("uq_screening_matches_screening_id")
        ),
    )
    op.create_index(
        "ix_screening_matches_org_status",
        "screening_matches",
        ["organization_id", "status"],
        unique=False,
    )
    op.create_index(
        op.f("ix_screening_matches_screening_id"),
        "screening_matches",
        ["screening_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_screening_matches_screening_id"), table_name="screening_matches")
    op.drop_index("ix_screening_matches_org_status", table_name="screening_matches")
    op.drop_table("screening_matches")
    op.drop_index(
        "ix_sanction_names_norm_trgm",
        table_name="sanction_names",
        postgresql_using="gin",
        postgresql_ops={"name_norm": "gin_trgm_ops"},
    )
    op.drop_index("ix_sanction_names_keys", table_name="sanction_names", postgresql_using="gin")
    op.drop_index(op.f("ix_sanction_names_entry_id"), table_name="sanction_names")
    op.drop_table("sanction_names")
    op.drop_index(op.f("ix_screenings_organization_id"), table_name="screenings")
    op.drop_table("screenings")
    op.drop_index(op.f("ix_sanction_entries_list_code"), table_name="sanction_entries")
    op.drop_index("ix_sanction_entries_imo", table_name="sanction_entries", postgresql_using="gin")
    op.drop_index(op.f("ix_sanction_entries_active"), table_name="sanction_entries")
    op.drop_table("sanction_entries")
    op.drop_table("sanction_lists")
    for enum in ("match_status", "subject_type", "sanction_entity_type"):
        op.execute(f"DROP TYPE IF EXISTS {enum}")
