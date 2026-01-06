"""Drop hk_source_bundle_id from derived_sleep_segments.

Rationale: bundle ids are not user-facing and add noise; provenance is already available via hk_source_name/version + hk_sources.
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision: str = "c0a1b2c3d4e5"
down_revision: Union[str, Sequence[str], None] = "b8c9d0e1f2a3"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("derived_sleep_segments") as batch_op:
        batch_op.drop_column("hk_source_bundle_id")


def downgrade() -> None:
    with op.batch_alter_table("derived_sleep_segments") as batch_op:
        batch_op.add_column(sa.Column("hk_source_bundle_id", sa.Text(), nullable=True))


