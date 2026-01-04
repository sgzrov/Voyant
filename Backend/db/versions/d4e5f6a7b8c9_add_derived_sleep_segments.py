"""Add derived_sleep_segments table for per-stage sleep time ranges.

This table is a materialized, query-friendly representation of sleep stage intervals
derived from raw mirrored HealthKit samples in main_health_metrics.
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "d4e5f6a7b8c9"
down_revision = "m7n8o9p0q1r2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "derived_sleep_segments",
        sa.Column("user_id", sa.Text(), nullable=False),
        sa.Column("hk_uuid", sa.Text(), nullable=False),
        sa.Column("sleep_date", sa.Date(), nullable=False),
        sa.Column("stage", sa.Text(), nullable=False),  # awake|in_bed|rem|core|deep|asleep_unspecified
        sa.Column("segment_start_ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("segment_end_ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("minutes", sa.Float(), nullable=False),
        sa.Column("hk_source_bundle_id", sa.Text(), nullable=True),
        sa.Column("hk_source_name", sa.Text(), nullable=True),
        sa.Column("hk_source_version", sa.Text(), nullable=True),
        sa.Column("hk_metadata", postgresql.JSONB(), nullable=True),
        sa.Column("meta", postgresql.JSONB(), nullable=True),
        sa.PrimaryKeyConstraint("user_id", "hk_uuid", name="pk_derived_sleep_segments"),
    )

    op.create_index(
        "idx_derived_sleep_segments_user_date",
        "derived_sleep_segments",
        ["user_id", "sleep_date"],
        unique=False,
    )
    op.create_index(
        "idx_derived_sleep_segments_user_start_desc",
        "derived_sleep_segments",
        ["user_id", "segment_start_ts"],
        unique=False,
    )
    op.create_index(
        "idx_derived_sleep_segments_user_date_stage",
        "derived_sleep_segments",
        ["user_id", "sleep_date", "stage"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("idx_derived_sleep_segments_user_date_stage", table_name="derived_sleep_segments")
    op.drop_index("idx_derived_sleep_segments_user_start_desc", table_name="derived_sleep_segments")
    op.drop_index("idx_derived_sleep_segments_user_date", table_name="derived_sleep_segments")
    op.drop_table("derived_sleep_segments")


