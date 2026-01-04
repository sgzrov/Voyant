"""Merge heads: chat table rename + derived_sleep_segments.

This is an Alembic merge migration to resolve multiple heads:
- c3a1f0b2d4e5 (rename chat tables)
- d4e5f6a7b8c9 (add derived_sleep_segments)
"""

from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "e5f6a7b8c9d0"
down_revision: Union[str, Sequence[str], None] = ("c3a1f0b2d4e5", "d4e5f6a7b8c9")
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Merge migration: no-op.
    pass


def downgrade() -> None:
    # Merge migration: no-op.
    pass


