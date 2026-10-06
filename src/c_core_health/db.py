from collections.abc import Generator
from contextlib import contextmanager

from sqlalchemy import Connection, Engine, create_engine

from c_core_health.settings import Settings


def create_db_engine(settings: Settings, *, read_only: bool) -> Engine:
    options = f"-c statement_timeout={settings.statement_timeout_ms}"
    if read_only:
        options += " -c default_transaction_read_only=on"
    return create_engine(
        settings.pg_dsn.get_secret_value(),
        pool_pre_ping=True,
        connect_args={"options": options, "application_name": "c-core-health"},
    )


@contextmanager
def read_connection(engine: Engine) -> Generator[Connection]:
    """A connection whose implicit transaction is always rolled back."""
    with engine.connect() as conn:
        try:
            yield conn
        finally:
            conn.rollback()
