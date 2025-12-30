"""add meta jsonb to health_metrics and health_rollup_hourly

Revision ID: 4f1a2b3c4d5e
Revises: 3c4d5e6f7a8b
Create Date: 2025-12-30
"""

from alembic import op


# revision identifiers, used by Alembic.
revision = "4f1a2b3c4d5e"
down_revision = "3c4d5e6f7a8b"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE health_metrics ADD COLUMN IF NOT EXISTS meta JSONB;")
    op.execute("ALTER TABLE health_rollup_hourly ADD COLUMN IF NOT EXISTS meta JSONB;")


def downgrade() -> None:
    op.execute("ALTER TABLE health_rollup_hourly DROP COLUMN IF EXISTS meta;")
    op.execute("ALTER TABLE health_metrics DROP COLUMN IF EXISTS meta;")


