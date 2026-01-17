"""Add citation validation columns

Revision ID: 003_citation_validation
Revises: 002_pgvector
Create Date: 2026-01-16

This migration adds columns for citation validation tracking:
- citation_report_json: JSON storing validation reports
- citation_overrides_json: JSON storing user overrides (sanitized)
- validation_attempts: Integer count of regeneration attempts
- assumptions_section: Text field for approved assumptions
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '003_citation_validation'
down_revision: Union[str, None] = '002_pgvector'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add citation validation columns to research_documents table
    op.add_column('research_documents', 
                  sa.Column('citation_report_json', postgresql.JSON(astext_type=sa.Text()), nullable=True))
    op.add_column('research_documents', 
                  sa.Column('citation_overrides_json', postgresql.JSON(astext_type=sa.Text()), nullable=True))
    op.add_column('research_documents', 
                  sa.Column('validation_attempts', sa.Integer(), server_default='0', nullable=True))
    op.add_column('research_documents', 
                  sa.Column('assumptions_section', sa.Text(), nullable=True))


def downgrade() -> None:
    # Remove citation validation columns
    op.drop_column('research_documents', 'assumptions_section')
    op.drop_column('research_documents', 'validation_attempts')
    op.drop_column('research_documents', 'citation_overrides_json')
    op.drop_column('research_documents', 'citation_report_json')
