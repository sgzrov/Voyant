"""
drop activity_type column from health_sessions

Revision ID: 9fb2
Revises: 9fb1
Create Date: 2025-11-14
"""

from alembic import op
import sqlalchemy as sa


revision = "9fb2"
down_revision = "9fb1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("health_sessions") as b:
        b.drop_column("activity_type")


def downgrade() -> None:
    with op.batch_alter_table("health_sessions") as b:
        b.add_column(sa.Column("activity_type", sa.Text(), nullable=True))


