"""Fix upload tracking PK to be per-user

Revision ID: 1a2b3c4d5e6f
Revises: 8c83a63a7591
Create Date: 2025-12-16

"""

from alembic import op


# revision identifiers, used by Alembic.
revision = "1a2b3c4d5e6f"
down_revision = "8c83a63a7591"
branch_labels = None
depends_on = None


def upgrade():
    # The original table used `id` (content hash) as the sole primary key, which prevents
    # different users from uploading identical content. Make the primary key composite:
    # (user_id, id). This aligns with application queries that always scope by user_id.
    #
    # Postgres default PK constraint name is typically `{table}_pkey`.
    op.drop_constraint("health_upload_tracking_pkey", "health_upload_tracking", type_="primary")
    op.create_primary_key("health_upload_tracking_pkey", "health_upload_tracking", ["user_id", "id"])

    # Drop redundant unique index (same columns as the new PK).
    try:
        op.drop_index("ix_health_upload_tracking_user_id_hash", table_name="health_upload_tracking")
    except Exception:
        # If it doesn't exist in a given environment, ignore.
        pass


def downgrade():
    # Recreate the old single-column primary key on `id`.
    op.drop_constraint("health_upload_tracking_pkey", "health_upload_tracking", type_="primary")
    op.create_primary_key("health_upload_tracking_pkey", "health_upload_tracking", ["id"])

    # Restore the old unique index (best-effort).
    try:
        op.create_index(
            "ix_health_upload_tracking_user_id_hash",
            "health_upload_tracking",
            ["user_id", "id"],
            unique=True,
        )
    except Exception:
        pass


