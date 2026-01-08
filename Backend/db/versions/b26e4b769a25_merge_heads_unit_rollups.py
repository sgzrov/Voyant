"""merge heads (unit rollups)

Revision ID: b26e4b769a25
Revises: a1b2c3d4e5f7, f0e1d2c3b4a5
Create Date: 2026-01-08 14:59:02.069550

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b26e4b769a25'
down_revision: Union[str, Sequence[str], None] = ('a1b2c3d4e5f7', 'f0e1d2c3b4a5')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
