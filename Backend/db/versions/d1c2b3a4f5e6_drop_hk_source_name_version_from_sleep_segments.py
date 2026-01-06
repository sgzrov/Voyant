"""Drop hk_source_name and hk_source_version from derived_sleep_segments.

Rationale:
- Keep sleep provenance in hk_sources JSONB (array of {name, version}) only.
- Avoid duplicating provenance fields and reduce token/noise in SQL tool responses.
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision: str = "d1c2b3a4f5e6"
down_revision: Union[str, Sequence[str], None] = "c0a1b2c3d4e5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("derived_sleep_segments") as batch_op:
        batch_op.drop_column("hk_source_name")
        batch_op.drop_column("hk_source_version")


def downgrade() -> None:
    with op.batch_alter_table("derived_sleep_segments") as batch_op:
        batch_op.add_column(sa.Column("hk_source_name", sa.Text(), nullable=True))
        batch_op.add_column(sa.Column("hk_source_version", sa.Text(), nullable=True))


