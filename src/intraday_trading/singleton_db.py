"""
Thread-safe singleton DuckDB connection manager.

Ensures only one active DuckDB connection exists at a time, preventing
filesystem locking conflicts when multiple components need database access.
"""

from __future__ import annotations

import threading
from contextlib import contextmanager
from typing import Generator, Optional

import duckdb


class DatabaseConnectionManager:
    _instance: Optional["DatabaseConnectionManager"] = None
    _lock = threading.Lock()
    _connection: Optional[duckdb.DuckDBPyConnection] = None
    _connection_count = 0
    _db_path: Optional[str] = None

    def __new__(cls) -> "DatabaseConnectionManager":
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
        return cls._instance

    @classmethod
    def get_connection(cls, db_path: str = "tick_data_production.db") -> duckdb.DuckDBPyConnection:
        with cls._lock:
            if cls._connection is None:
                cls._connection = duckdb.connect(db_path)
                cls._db_path = db_path
                print("🔓 Database connection established")
            cls._connection_count += 1
            return cls._connection

    @classmethod
    def release_connection(cls) -> None:
        with cls._lock:
            if cls._connection_count > 0:
                cls._connection_count -= 1
            if cls._connection_count <= 0 and cls._connection is not None:
                cls._connection.close()
                cls._connection = None
                cls._db_path = None
                print("🔒 Database connection closed")

    @classmethod
    @contextmanager
    def connection_scope(
        cls,
        db_path: Optional[str] = None,
    ) -> Generator[duckdb.DuckDBPyConnection, None, None]:
        """Context manager for safely borrowing the shared connection."""
        conn = cls.get_connection(db_path or cls._db_path or "tick_data_production.db")
        try:
            yield conn
        finally:
            cls.release_connection()

