"""Add hk_source_bundle_id back to derived_sleep_segments.

We keep this for debugging / provenance by app bundle id, but do not select it by default in the SQL tool.
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision: str = "e2f3a4b5c6d7"
down_revision: Union[str, Sequence[str], None] = "d1c2b3a4f5e6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("derived_sleep_segments") as batch_op:
        batch_op.add_column(sa.Column("hk_source_bundle_id", sa.Text(), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("derived_sleep_segments") as batch_op:
        batch_op.drop_column("hk_source_bundle_id")


