"""empty message

Revision ID: 306e53342d40
Revises: 53caa0a69459, 9fb7
Create Date: 2025-11-19 22:40:25.547216

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '306e53342d40'
down_revision: Union[str, Sequence[str], None] = ('53caa0a69459', '9fb7')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
