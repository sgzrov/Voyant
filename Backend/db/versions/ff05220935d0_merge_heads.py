"""merge heads

Revision ID: ff05220935d0
Revises: 9fae, 9fb0
Create Date: 2025-11-12 22:44:29.702934

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'ff05220935d0'
down_revision: Union[str, Sequence[str], None] = ('9fae', '9fb0')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
