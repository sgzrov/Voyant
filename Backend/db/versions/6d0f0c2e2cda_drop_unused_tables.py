"""drop unused rollup/summary tables for metrics-only mode

Revision ID: 6d0f0c2e2cda
Revises: c610270318a2
Create Date: 2025-11-20
"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "6d0f0c2e2cda"
down_revision = "c610270318a2"
branch_labels = None
depends_on = None


def upgrade():
    # Drop tables if they exist; this migration is idempotent across environments.
    op.execute("DROP TABLE IF EXISTS health_rollup_daily CASCADE;")
    op.execute("DROP TABLE IF EXISTS health_rollup_hourly CASCADE;")
    op.execute("DROP TABLE IF EXISTS health_daily_features CASCADE;")
    op.execute("DROP TABLE IF EXISTS health_session_features CASCADE;")
    op.execute("DROP TABLE IF EXISTS health_summaries CASCADE;")


def downgrade():
    # No automatic recreation; these tables are intentionally removed for metrics-only mode.
    # Provide placeholders to keep Alembic happy.
    pass


