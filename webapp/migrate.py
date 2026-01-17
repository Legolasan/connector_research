#!/usr/bin/env python3
"""
Migration management script for database migrations using Alembic.
Run this script explicitly before deployments to apply pending migrations.
"""
import os
import sys
import subprocess
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

def check_database_url():
    """Check if DATABASE_URL is set."""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        print("❌ DATABASE_URL environment variable is not set")
        sys.exit(1)
    return database_url

def run_alembic_command(command: str, *args):
    """Run an alembic command."""
    alembic_dir = Path(__file__).parent.parent
    os.chdir(alembic_dir)
    
    cmd = ["alembic"] + command.split() + list(args)
    try:
        result = subprocess.run(cmd, check=True, capture_output=False)
        return result.returncode == 0
    except subprocess.CalledProcessError as e:
        print(f"❌ Alembic command failed: {e}")
        sys.exit(1)
    except FileNotFoundError:
        print("❌ Alembic not found. Install it with: pip install alembic")
        sys.exit(1)

def check():
    """Check if migrations are needed."""
    print("🔍 Checking migration status...")
    database_url = check_database_url()
    
    # Get current revision
    result = subprocess.run(
        ["alembic", "current"],
        cwd=Path(__file__).parent.parent,
        capture_output=True,
        text=True
    )
    
    # Check for pending revisions
    result_check = subprocess.run(
        ["alembic", "check"],
        cwd=Path(__file__).parent.parent,
        capture_output=True,
        text=True
    )
    
    print(result.stdout)
    if result_check.returncode != 0:
        print("⚠️  Database schema is out of sync with models")
        return False
    
    print("✓ Database schema is up to date")
    return True

def upgrade(revision="head"):
    """Apply pending migrations."""
    print(f"📊 Upgrading database to revision: {revision}")
    database_url = check_database_url()
    
    try:
        run_alembic_command("upgrade", revision)
        print("✓ Database migration completed successfully")
        return True
    except Exception as e:
        print(f"❌ Migration failed: {e}")
        sys.exit(1)

def downgrade(revision="-1"):
    """Rollback last migration (for emergencies)."""
    print(f"⚠️  Downgrading database by {revision} revision(s)")
    database_url = check_database_url()
    
    confirm = input("Are you sure you want to downgrade? This may cause data loss. (yes/no): ")
    if confirm.lower() != "yes":
        print("Downgrade cancelled")
        return False
    
    try:
        run_alembic_command("downgrade", revision)
        print("✓ Database downgrade completed")
        return True
    except Exception as e:
        print(f"❌ Downgrade failed: {e}")
        sys.exit(1)

def current():
    """Show current database version."""
    print("📋 Current database version:")
    database_url = check_database_url()
    
    result = subprocess.run(
        ["alembic", "current"],
        cwd=Path(__file__).parent.parent,
        capture_output=False
    )
    
    return result.returncode == 0

def history():
    """Show migration history."""
    print("📚 Migration history:")
    database_url = check_database_url()
    
    result = subprocess.run(
        ["alembic", "history"],
        cwd=Path(__file__).parent.parent,
        capture_output=False
    )
    
    return result.returncode == 0

def main():
    """Main entry point."""
    if len(sys.argv) < 2:
        print("Usage: python migrate.py <command> [args]")
        print("\nCommands:")
        print("  check       - Check if migrations are needed")
        print("  upgrade     - Apply pending migrations (default: head)")
        print("  downgrade   - Rollback last migration (use with caution)")
        print("  current     - Show current database version")
        print("  history     - Show migration history")
        print("\nExamples:")
        print("  python migrate.py check")
        print("  python migrate.py upgrade")
        print("  python migrate.py downgrade -1")
        sys.exit(1)
    
    command = sys.argv[1].lower()
    
    if command == "check":
        success = check()
        sys.exit(0 if success else 1)
    elif command == "upgrade":
        revision = sys.argv[2] if len(sys.argv) > 2 else "head"
        upgrade(revision)
    elif command == "downgrade":
        revision = sys.argv[2] if len(sys.argv) > 2 else "-1"
        downgrade(revision)
    elif command == "current":
        current()
    elif command == "history":
        history()
    else:
        print(f"❌ Unknown command: {command}")
        sys.exit(1)

if __name__ == "__main__":
    main()