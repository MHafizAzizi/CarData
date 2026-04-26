"""SQLite connection helper for CarData.

Two physical databases — one per category — share the same connection pragmas
and similar (but not identical) schemas:

    cars        -> data/master/cardata_cars.db        / schema_cars.sql
    motorcycles -> data/master/cardata_motorcycles.db / schema_motorcycles.sql

Usage:
    from db import connect
    with connect("cars") as conn:
        rows = conn.execute("SELECT ads_id, url FROM listings WHERE availability_status='available'").fetchall()
"""

import os
import sqlite3
from pathlib import Path
from typing import Dict, Optional, Union

# Project root = one level above this file (src/ -> CarData/)
_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parent

CATEGORIES = ("cars", "motorcycles")

DB_PATHS: Dict[str, Path] = {
    "cars": _ROOT / "data" / "master" / "cardata_cars.db",
    "motorcycles": _ROOT / "data" / "master" / "cardata_motorcycles.db",
}

SCHEMA_PATHS: Dict[str, Path] = {
    "cars": _ROOT / "schema_cars.sql",
    "motorcycles": _ROOT / "schema_motorcycles.sql",
}


def db_path_for(category: str) -> Path:
    if category not in DB_PATHS:
        raise ValueError(f"Unknown category: {category!r}. Expected one of {CATEGORIES}.")
    return DB_PATHS[category]


def schema_path_for(category: str) -> Path:
    if category not in SCHEMA_PATHS:
        raise ValueError(f"Unknown category: {category!r}. Expected one of {CATEGORIES}.")
    return SCHEMA_PATHS[category]


def connect(
    category: str = "cars",
    *,
    path: Optional[Union[str, os.PathLike]] = None,
    init: bool = True,
) -> sqlite3.Connection:
    """Open a tuned connection to one of the CarData SQLite databases.

    - WAL mode so readers don't block writers (and vice versa).
    - foreign_keys ON because SQLite leaves it off by default.
    - busy_timeout=5000 ms so concurrent opens wait briefly instead of throwing.
    - autocommit (isolation_level=None); callers wrap batches in
      `with conn:` blocks for explicit transactions.

    Pass `path=...` to override the default location (useful for tests).
    Pass `init=False` to skip applying the schema (e.g. read-only scripts that
    must not modify the file).
    """
    db_file = Path(path) if path is not None else Path(db_path_for(category))
    db_file.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_file, timeout=30, isolation_level=None)
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
        schema_file = Path(schema_path_for(category))
        if not schema_file.exists():
            raise FileNotFoundError(
                f"Schema file not found at {schema_file}. Run from the project root."
            )
        conn.executescript(schema_file.read_text(encoding="utf-8"))

    return conn


def schema_version(conn: sqlite3.Connection) -> int:
    """Return the current schema version recorded in the meta table."""
    row = conn.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
    return int(row["value"]) if row else 0


def db_category(conn: sqlite3.Connection) -> Optional[str]:
    """Return the category recorded in the meta table, if any."""
    row = conn.execute("SELECT value FROM meta WHERE key='category'").fetchone()
    return row["value"] if row else None


def get_meta(conn: sqlite3.Connection, key: str, default: Optional[str] = None) -> Optional[str]:
    """Read a single value from the meta table. Returns *default* if the key is absent."""
    row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else default


def set_meta(conn: sqlite3.Connection, key: str, value: str) -> None:
    """Upsert a key/value pair in the meta table."""
    with conn:
        conn.execute(
            "INSERT INTO meta (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )
