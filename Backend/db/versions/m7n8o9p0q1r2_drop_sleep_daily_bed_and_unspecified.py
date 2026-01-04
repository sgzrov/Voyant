"""Drop in_bed_minutes and asleep_unspecified_minutes from derived_sleep_daily

Revision ID: m7n8o9p0q1r2
Revises: l6m7n8o9p0q1
Create Date: 2026-01-04
"""

from alembic import op
import sqlalchemy as sa


revision = "m7n8o9p0q1r2"
down_revision = "l6m7n8o9p0q1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_column("derived_sleep_daily", "in_bed_minutes")
    op.drop_column("derived_sleep_daily", "asleep_unspecified_minutes")


def downgrade() -> None:
    op.add_column("derived_sleep_daily", sa.Column("in_bed_minutes", sa.Float(), nullable=True))
    op.add_column("derived_sleep_daily", sa.Column("asleep_unspecified_minutes", sa.Float(), nullable=True))


