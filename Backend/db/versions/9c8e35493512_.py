"""empty message

Revision ID: 9c8e35493512
Revises: dbfdcb3242c0, e2b1c4d5f6a7
Create Date: 2025-11-22 23:40:51.358032

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '9c8e35493512'
down_revision: Union[str, Sequence[str], None] = ('dbfdcb3242c0', 'e2b1c4d5f6a7')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
