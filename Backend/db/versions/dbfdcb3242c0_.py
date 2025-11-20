"""empty message

Revision ID: dbfdcb3242c0
Revises: 306e53342d40, 6d0f0c2e2cda
Create Date: 2025-11-20 14:47:30.593605

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'dbfdcb3242c0'
down_revision: Union[str, Sequence[str], None] = ('306e53342d40', '6d0f0c2e2cda')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
