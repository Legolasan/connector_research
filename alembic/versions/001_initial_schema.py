"""Initial schema

Revision ID: 001_initial
Revises: 
Create Date: 2026-01-16

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '001_initial'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create connectors table
    op.create_table(
        'connectors',
        sa.Column('id', sa.String(length=255), nullable=False),
        sa.Column('name', sa.String(length=255), nullable=False),
        sa.Column('connector_type', sa.String(length=50), nullable=False),
        sa.Column('status', sa.String(length=50), server_default='not_started', nullable=True),
        sa.Column('github_url', sa.String(length=500), nullable=True),
        sa.Column('description', sa.Text(), server_default='', nullable=True),
        sa.Column('fivetran_urls', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column('objects_count', sa.Integer(), server_default='0', nullable=True),
        sa.Column('vectors_count', sa.Integer(), server_default='0', nullable=True),
        sa.Column('fivetran_parity', sa.Float(), nullable=True),
        sa.Column('progress', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.Column('completed_at', sa.DateTime(), nullable=True),
        sa.Column('sources', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column('pinecone_index', sa.String(length=255), server_default='', nullable=True),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_connectors_id'), 'connectors', ['id'], unique=False)

    # Create research_documents table
    op.create_table(
        'research_documents',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('connector_id', sa.String(length=255), nullable=False),
        sa.Column('content', sa.Text(), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_research_documents_connector_id'), 'research_documents', ['connector_id'], unique=False)

    # Create document_chunks table
    op.create_table(
        'document_chunks',
        sa.Column('id', sa.String(length=255), nullable=False),
        sa.Column('connector_id', sa.String(length=255), nullable=False),
        sa.Column('connector_name', sa.String(length=255), nullable=False),
        sa.Column('chunk_index', sa.Integer(), nullable=False),
        sa.Column('text', sa.Text(), nullable=False),
        sa.Column('section', sa.String(length=255), server_default='General', nullable=True),
        sa.Column('source_type', sa.String(length=50), server_default='research', nullable=True),
        sa.Column('embedding_json', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('idx_chunks_connector', 'document_chunks', ['connector_id'], unique=False)

    # Note: The 'embedding' VECTOR column is added conditionally if pgvector extension is available
    # Run the following manually if pgvector is installed:
    # ALTER TABLE document_chunks ADD COLUMN embedding VECTOR(1536);


def downgrade() -> None:
    # Drop indexes
    op.drop_index('idx_chunks_connector', table_name='document_chunks')
    op.drop_index(op.f('ix_research_documents_connector_id'), table_name='research_documents')
    op.drop_index(op.f('ix_connectors_id'), table_name='connectors')
    
    # Drop tables
    op.drop_table('document_chunks')
    op.drop_table('research_documents')
    op.drop_table('connectors')