"""Add upload tracking table

Revision ID: b1c2d3e4f5g6
Revises: a80db4a4c583
Create Date: 2025-11-24 04:10:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'b1c2d3e4f5g6'
down_revision = 'a80db4a4c583'
branch_labels = None
depends_on = None


def upgrade():
    # Create table for tracking CSV uploads and preventing duplicates
    op.create_table('health_upload_tracking',
        sa.Column('id', sa.String(length=64), primary_key=True),  # SHA256 hash of content
        sa.Column('user_id', sa.String(length=100), nullable=False),
        sa.Column('task_id', sa.String(length=100), nullable=True),
        sa.Column('file_size', sa.Integer(), nullable=False),
        sa.Column('file_name', sa.String(length=255), nullable=True),
        sa.Column('status', sa.String(length=50), nullable=False, default='pending'),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.func.now(), onupdate=sa.func.now()),
        sa.Column('completed_at', sa.DateTime(), nullable=True),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.Column('row_count', sa.Integer(), nullable=True),
        sa.Column('idempotency_key', sa.String(length=64), nullable=True),
        sa.Column('request_count', sa.Integer(), nullable=False, default=1),
    )
    
    # Create indexes for efficient lookups
    op.create_index('ix_health_upload_tracking_user_id', 'health_upload_tracking', ['user_id'])
    op.create_index('ix_health_upload_tracking_task_id', 'health_upload_tracking', ['task_id'])
    op.create_index('ix_health_upload_tracking_status', 'health_upload_tracking', ['status'])
    op.create_index('ix_health_upload_tracking_created_at', 'health_upload_tracking', ['created_at'])
    op.create_index('ix_health_upload_tracking_idempotency_key', 'health_upload_tracking', ['idempotency_key'])
    
    # Unique constraint to prevent duplicate uploads with same content hash
    op.create_index('ix_health_upload_tracking_user_id_hash', 'health_upload_tracking', ['user_id', 'id'], unique=True)


def downgrade():
    op.drop_index('ix_health_upload_tracking_user_id_hash', 'health_upload_tracking')
    op.drop_index('ix_health_upload_tracking_idempotency_key', 'health_upload_tracking')
    op.drop_index('ix_health_upload_tracking_created_at', 'health_upload_tracking')
    op.drop_index('ix_health_upload_tracking_status', 'health_upload_tracking')
    op.drop_index('ix_health_upload_tracking_task_id', 'health_upload_tracking')
    op.drop_index('ix_health_upload_tracking_user_id', 'health_upload_tracking')
    op.drop_table('health_upload_tracking')
