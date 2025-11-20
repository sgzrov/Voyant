"""empty message

Revision ID: 53caa0a69459
Revises: 279896da8e8e, 9fb6
Create Date: 2025-11-19 20:59:44.956374

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '53caa0a69459'
down_revision: Union[str, Sequence[str], None] = ('279896da8e8e', '9fb6')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
