"""Add derived_sleep_daily table for nightly sleep summaries (stages + range)

Revision ID: l6m7n8o9p0q1
Revises: k0l1m2n3o4p5
Create Date: 2026-01-04
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "l6m7n8o9p0q1"
down_revision = "k0l1m2n3o4p5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "derived_sleep_daily",
        sa.Column("user_id", sa.Text(), nullable=False),
        sa.Column("sleep_date", sa.Date(), nullable=False),
        sa.Column("sleep_start_ts", sa.DateTime(timezone=True), nullable=True),
        sa.Column("sleep_end_ts", sa.DateTime(timezone=True), nullable=True),
        sa.Column("asleep_minutes", sa.Float(), nullable=True),
        sa.Column("rem_minutes", sa.Float(), nullable=True),
        sa.Column("core_minutes", sa.Float(), nullable=True),
        sa.Column("deep_minutes", sa.Float(), nullable=True),
        sa.Column("awake_minutes", sa.Float(), nullable=True),
        sa.Column("in_bed_minutes", sa.Float(), nullable=True),
        sa.Column("asleep_unspecified_minutes", sa.Float(), nullable=True),
        sa.Column("hk_sources", postgresql.JSONB(), nullable=True),
        sa.Column("meta", postgresql.JSONB(), nullable=True),
        sa.PrimaryKeyConstraint("user_id", "sleep_date", name="pk_derived_sleep_daily"),
    )
    op.create_index(
        "idx_derived_sleep_daily_user_date_desc",
        "derived_sleep_daily",
        ["user_id", "sleep_date"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("idx_derived_sleep_daily_user_date_desc", table_name="derived_sleep_daily")
    op.drop_table("derived_sleep_daily")


