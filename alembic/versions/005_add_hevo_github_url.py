"""Add hevo_github_url column to connectors table

Revision ID: 005
Revises: 004
Create Date: 2026-01-17

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '005'
down_revision = '004'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add hevo_github_url column to connectors table
    op.add_column('connectors', sa.Column('hevo_github_url', sa.String(500), nullable=True))


def downgrade() -> None:
    # Remove hevo_github_url column
    op.drop_column('connectors', 'hevo_github_url')
