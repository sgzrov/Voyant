"""
drop notes column from health_sessions

Revision ID: 9fb1
Revises: 9fb0
Create Date: 2025-11-14
"""

from alembic import op
import sqlalchemy as sa


revision = "9fb1"
down_revision = "9fb0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Drop optional notes column; not used
    with op.batch_alter_table("health_sessions") as batch_op:
        batch_op.drop_column("notes")


def downgrade() -> None:
    # Recreate notes column as TEXT if we roll back
    with op.batch_alter_table("health_sessions") as batch_op:
        batch_op.add_column(sa.Column("notes", sa.Text(), nullable=True))


