"""Add pgvector embedding column

Revision ID: 002_pgvector
Revises: 001_initial
Create Date: 2026-01-16

This migration adds the VECTOR embedding column for pgvector support.
It should only be applied if the pgvector extension is available in the database.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision: str = '002_pgvector'
down_revision: Union[str, None] = '001_initial'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Check if pgvector extension is available
    connection = op.get_bind()
    
    # Try to enable pgvector extension if not already enabled
    try:
        connection.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
        connection.commit()
    except Exception:
        pass  # Extension might not be available, continue without it
    
    # Check if extension is now available
    result = connection.execute(text("SELECT 1 FROM pg_extension WHERE extname = 'vector'"))
    
    if result.fetchone():
        # pgvector is available, add VECTOR column if it doesn't exist
        try:
            connection.execute(text("""
                ALTER TABLE document_chunks 
                ADD COLUMN IF NOT EXISTS embedding VECTOR(1536)
            """))
            connection.commit()
        except Exception as e:
            # Column might already exist or VECTOR type not available
            # This is safe to ignore - embedding_json will be used instead
            connection.rollback()
            print(f"⚠ Could not add VECTOR column: {e}")
            print("  → document_chunks.embedding_json will continue to be used for embeddings")
    else:
        # pgvector not available, skip this migration
        # The embedding_json column will continue to be used
        print("⚠ pgvector extension not found. Skipping VECTOR column creation.")
        print("  → document_chunks.embedding_json will continue to be used for embeddings")


def downgrade() -> None:
    # Remove VECTOR column if it exists
    connection = op.get_bind()
    
    # Check if column exists
    result = connection.execute(text("""
        SELECT EXISTS (
            SELECT FROM information_schema.columns 
            WHERE table_name = 'document_chunks' 
            AND column_name = 'embedding'
        )
    """))
    
    if result.scalar():
        connection.execute(text("ALTER TABLE document_chunks DROP COLUMN embedding"))
        connection.commit()