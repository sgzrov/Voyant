"""merge heads after dropping columns

Revision ID: c610270318a2
Revises: 9fb2, ff05220935d0
Create Date: 2025-11-13 18:08:49.027934

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c610270318a2'
down_revision: Union[str, Sequence[str], None] = ('9fb2', 'ff05220935d0')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
