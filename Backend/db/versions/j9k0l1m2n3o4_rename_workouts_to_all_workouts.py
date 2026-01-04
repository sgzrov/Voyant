"""Rename workouts to all_workouts

Revision ID: j9k0l1m2n3o4
Revises: i8j9k0l1m2n3
Create Date: 2026-01-03
"""

from alembic import op


revision = "j9k0l1m2n3o4"
down_revision = "i8j9k0l1m2n3"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = 'workouts'
          ) THEN
            ALTER TABLE workouts RENAME TO all_workouts;
          END IF;
        END$$;
        """
    )
    # Rename index if it exists.
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
            ALTER INDEX idx_workouts_user_start_desc RENAME TO idx_all_workouts_user_start_desc;
          END IF;
        END$$;
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind = 'i'
              AND c.relname = 'idx_all_workouts_user_start_desc'
          ) THEN
            ALTER INDEX idx_all_workouts_user_start_desc RENAME TO idx_workouts_user_start_desc;
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
            WHERE table_schema = 'public' AND table_name = 'all_workouts'
          ) THEN
            ALTER TABLE all_workouts RENAME TO workouts;
          END IF;
        END$$;
        """
    )


