"""SQLite connection helper for CarData.

Centralizes the connection pragmas and schema bootstrap so every script opens
the database the same way.

Usage:
    from db import connect
    with connect() as conn:
        rows = conn.execute("SELECT ads_id, url FROM listings WHERE availability_status='available'").fetchall()
"""

import os
import sqlite3
from pathlib import Path
from typing import Union

DEFAULT_DB_PATH = "data/master/cardata.db"
SCHEMA_PATH = "schema.sql"


def connect(path: Union[str, os.PathLike] = DEFAULT_DB_PATH, *, init: bool = True) -> sqlite3.Connection:
    """Open a tuned connection to the CarData SQLite database.

    - WAL mode so readers don't block writers (and vice versa).
    - foreign_keys ON because SQLite leaves it off by default.
    - busy_timeout=5000 ms so concurrent opens wait briefly instead of throwing.
    - autocommit (isolation_level=None); callers wrap batches in
      `with conn:` blocks for explicit transactions.

    Set `init=False` to skip applying schema.sql (e.g. for read-only scripts
    where you don't want to touch the file).
    """
    db_path = Path(path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        PRAGMA journal_mode=WAL;
        PRAGMA synchronous=NORMAL;
        PRAGMA foreign_keys=ON;
        PRAGMA busy_timeout=5000;
        PRAGMA temp_store=MEMORY;
        """
    )

    if init:
        schema_file = Path(SCHEMA_PATH)
        if not schema_file.exists():
            raise FileNotFoundError(
                f"Schema file not found at {SCHEMA_PATH}. Run from the project root."
            )
        conn.executescript(schema_file.read_text(encoding="utf-8"))

    return conn


def schema_version(conn: sqlite3.Connection) -> int:
    """Return the current schema version recorded in the meta table."""
    row = conn.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
    return int(row["value"]) if row else 0
