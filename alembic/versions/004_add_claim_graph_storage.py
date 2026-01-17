"""Add claim graph storage columns

Revision ID: 004_claim_graph
Revises: 003_citation_validation
Create Date: 2026-01-16

This migration adds columns for claim graph storage:
- claims_json: JSONB column for structured claims
- canonical_facts_json: JSONB column for final registry
- evidence_map_json: JSONB column for citation → evidence mapping (with stable IDs)
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '004_claim_graph'
down_revision: Union[str, None] = '003_citation_validation'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add claim graph storage columns to research_documents table
    # Use JSONB for better querying performance in PostgreSQL
    op.add_column('research_documents', 
                  sa.Column('claims_json', postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    op.add_column('research_documents', 
                  sa.Column('canonical_facts_json', postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    op.add_column('research_documents', 
                  sa.Column('evidence_map_json', postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    
    # Add indexes for JSONB columns for better query performance
    op.create_index('ix_research_documents_claims_json', 'research_documents', ['claims_json'], 
                    postgresql_using='gin', unique=False)
    op.create_index('ix_research_documents_evidence_map_json', 'research_documents', ['evidence_map_json'], 
                    postgresql_using='gin', unique=False)


def downgrade() -> None:
    # Remove indexes
    op.drop_index('ix_research_documents_evidence_map_json', table_name='research_documents')
    op.drop_index('ix_research_documents_claims_json', table_name='research_documents')
    
    # Remove claim graph storage columns
    op.drop_column('research_documents', 'evidence_map_json')
    op.drop_column('research_documents', 'canonical_facts_json')
    op.drop_column('research_documents', 'claims_json')
