"""merge heads

Revision ID: 279896da8e8e
Revises: 9fb4, a1b2c3d4e5f6
Create Date: 2025-11-18 23:51:16.541129

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '279896da8e8e'
down_revision: Union[str, Sequence[str], None] = ('9fb4', 'a1b2c3d4e5f6')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
