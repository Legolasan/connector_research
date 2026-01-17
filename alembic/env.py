"""
Alembic environment configuration for database migrations.
Integrates with existing SQLAlchemy models from webapp/services/database.py
"""
import os
import sys
from logging.config import fileConfig

from sqlalchemy import engine_from_config
from sqlalchemy import pool
from alembic import context

# Add the parent directory to the path to import webapp modules
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# Change to project root for imports
os.chdir(project_root)

# Import Base and models from database.py
from webapp.services.database import Base, ConnectorModel, ResearchDocumentModel, DocumentChunkModel

# Import pgvector if available
try:
    from pgvector.sqlalchemy import Vector
    PGVECTOR_AVAILABLE = True
except ImportError:
    PGVECTOR_AVAILABLE = False
    Vector = None

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Get DATABASE_URL from environment
DATABASE_URL = os.getenv("DATABASE_URL")
if DATABASE_URL:
    # Handle Railway's postgres:// vs postgresql:// URL format
    if DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
    config.set_main_option("sqlalchemy.url", DATABASE_URL)

# add your model's MetaData object here
# for 'autogenerate' support
target_metadata = Base.metadata

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = config.get_main_option("sqlalchemy.url")
    if not url:
        raise ValueError("DATABASE_URL environment variable is not set")
    
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        # Include pgvector types in autogenerate
        include_object=include_object,
        compare_type=True,
        compare_server_default=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    url = config.get_main_option("sqlalchemy.url")
    if not url:
        raise ValueError("DATABASE_URL environment variable is not set")
    
    # Create engine with connection pooling
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            # Include pgvector types in autogenerate
            include_object=include_object,
            compare_type=True,
            compare_server_default=True,
        )

        with context.begin_transaction():
            context.run_migrations()


def include_object(object, name, type_, reflected, compare_to):
    """Custom include_object to handle pgvector VECTOR type."""
    # Always include tables
    if type_ == "table":
        return True
    
    # For columns, check if it's a VECTOR type
    if type_ == "column" and hasattr(object, "type"):
        # If pgvector is not available, skip VECTOR columns
        if isinstance(object.type, type(Vector)) if PGVECTOR_AVAILABLE else False:
            # Check if pgvector extension is enabled in database
            # For now, we'll include VECTOR columns but migrations will handle errors gracefully
            return True
    
    return True


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()