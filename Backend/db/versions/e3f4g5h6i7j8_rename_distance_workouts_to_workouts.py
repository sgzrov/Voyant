"""Rename distance_workouts to workouts

Revision ID: e3f4g5h6i7j8
Revises: d2e3f4g5h6i7
Create Date: 2026-01-03
"""

from alembic import op


revision = "e3f4g5h6i7j8"
down_revision = "d2e3f4g5h6i7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Rename table if it exists.
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = 'distance_workouts'
          ) THEN
            ALTER TABLE distance_workouts RENAME TO workouts;
          END IF;
        END$$;
        """
    )
    # Rename index if it exists (Postgres keeps index names stable on table rename).
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind = 'i'
              AND c.relname = 'idx_distance_workouts_user_start_desc'
          ) THEN
            ALTER INDEX idx_distance_workouts_user_start_desc RENAME TO idx_workouts_user_start_desc;
          END IF;
        END$$;
        """
    )


def downgrade() -> None:
    # Best-effort rollback.
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind = 'i'
              AND c.relname = 'idx_workouts_user_start_desc'
          ) THEN
            ALTER INDEX idx_workouts_user_start_desc RENAME TO idx_distance_workouts_user_start_desc;
          END IF;
        END$$;
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = 'workouts'
          ) THEN
            ALTER TABLE workouts RENAME TO distance_workouts;
          END IF;
        END$$;
        """
    )


