"""merge_chat_conversations

Revision ID: 8c83a63a7591
Revises: 9fb8, b1c2d3e4f5g6
Create Date: 2025-11-27 13:01:07.482292

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '8c83a63a7591'
down_revision: Union[str, Sequence[str], None] = ('9fb8', 'b1c2d3e4f5g6')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
