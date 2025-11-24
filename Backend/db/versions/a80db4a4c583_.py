"""empty message

Revision ID: a80db4a4c583
Revises: 3da2813e4d5a, ee12ab34cd56
Create Date: 2025-11-23 19:59:14.403161

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a80db4a4c583'
down_revision: Union[str, Sequence[str], None] = ('3da2813e4d5a', 'ee12ab34cd56')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
