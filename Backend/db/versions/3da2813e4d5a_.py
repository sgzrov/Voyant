"""empty message

Revision ID: 3da2813e4d5a
Revises: 9c8e35493512, d1e2f3g4h5i6
Create Date: 2025-11-22 23:49:12.242725

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '3da2813e4d5a'
down_revision: Union[str, Sequence[str], None] = ('9c8e35493512', 'd1e2f3g4h5i6')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
