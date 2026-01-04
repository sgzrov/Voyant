from logging.config import fileConfig
import os
import re
import sys
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, pool
from alembic import context

# Ensure project root is on sys.path so `import Backend...` works when CWD is Backend/
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

try:
    load_dotenv(Path(PROJECT_ROOT) / ".env", override=False)
except Exception:
    pass

from Backend.database import Base  # type: ignore  # After sys.path adjustment
# Import all models so Alembic autogenerate can see tables in Base.metadata.
import Backend.models  # noqa: F401  # side-effect import

# Alembic Config object
config = context.config

# Prefer explicit alembic.ini URL, otherwise fall back to DATABASE_URL from env
def _get_migration_url() -> str:
    url = config.get_main_option("sqlalchemy.url")
    if url:
        url = url.strip()
        # Support placeholder syntax like: sqlalchemy.url = ${DATABASE_URL}
        m = re.fullmatch(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}", url)
        if m:
            env_name = m.group(1)
            env_val = os.getenv(env_name) or ""
            if env_val:
                return env_val
            # Treat unresolved placeholder as missing and fall through to DATABASE_URL lookup.
        else:
            return url

    db_url = os.getenv("DATABASE_URL") or ""
    if db_url:
        return db_url
    raise RuntimeError("DATABASE_URL is not configured for Alembic migrations.")

# Configure logging
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Target metadata from your models
target_metadata = Base.metadata


# Runs migrations in "offline" mode (generates SQL without a live DB connection)
def run_migrations_offline() -> None:
    url = _get_migration_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        compare_server_default=True,
    )

    with context.begin_transaction():
        context.run_migrations()


# Runs migrations in "online" mode (executes against a live DB connection).
def run_migrations_online() -> None:
    url = _get_migration_url()

    connectable = create_engine(url, poolclass=pool.NullPool)

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            compare_server_default=True,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
