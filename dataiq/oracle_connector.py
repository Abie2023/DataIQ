"""
Oracle database connector for DataIQ.

Responsibilities:
- Load DB config from YAML
- Create SQLAlchemy engine for python-oracledb (thin mode, no Instant Client required)
- Provide helpers to test connection, list schemas/tables/columns
- Execute queries and return pandas DataFrames (with chunking)
- Centralized logging

Uses python-oracledb in thin mode by default (no Oracle Instant Client needed).
For thick mode with Instant Client, pass thick_mode config in db_config.yaml.

Usage:
    from dataiq.oracle_connector import OracleConnector
    oc = OracleConnector(config_path="config/db_config.yaml")
    with oc.connect() as conn:
        df = oc.read_query("SELECT * FROM DUAL", conn=conn)
"""

from __future__ import annotations

import os
import sys
import logging
from logging.handlers import RotatingFileHandler
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
import yaml
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, Connection, URL
from sqlalchemy.exc import SQLAlchemyError


DEFAULT_CONFIG_PATH = os.path.join("config", "db_config.yaml")
LOCAL_OVERRIDE_PATH = os.path.join("config", "db_config.local.yaml")


def _ensure_dirs(paths: List[str]) -> None:
    for p in paths:
        if p and not os.path.exists(p):
            os.makedirs(p, exist_ok=True)


def _setup_logger(logs_dir: str) -> logging.Logger:
    _ensure_dirs([logs_dir])
    logger = logging.getLogger("dataiq")
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    log_path = os.path.join(logs_dir, "app.log")
    file_handler = RotatingFileHandler(log_path, maxBytes=5 * 1024 * 1024, backupCount=3)
    file_fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler.setFormatter(file_fmt)
    logger.addHandler(file_handler)

    # Console handler for development
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(file_fmt)
    logger.addHandler(console)

    logger.debug("Logger initialized for DataIQ")
    return logger


class OracleConnector:
    """High-level Oracle connection helper for DataIQ.

    Config structure (YAML):
        oracle:
          host: 127.0.0.1
          port: 1521
          service_name: XE
          username: system
          password: change_me
          dsn: null  # optional; overrides host/port/service_name
        sqlalchemy:
          pool_size: 5
          max_overflow: 10
          pool_timeout: 30
          pool_recycle: 1800
          echo: false
          encoding: UTF-8
        paths:
          logs_dir: logs
          outputs_dir: outputs
        fetch:
          sample_rows: 1000
          chunksize: 50000
    """

    def __init__(self, config_path: Optional[str] = None) -> None:
        self.config_path = config_path or DEFAULT_CONFIG_PATH
        self.config = self._load_config(self.config_path)
        self.paths = self.config.get("paths", {})
        logs_dir = self.paths.get("logs_dir", "logs")
        self.logger = _setup_logger(logs_dir)
        self.engine: Optional[Engine] = None

    # ---------------------- Config & Engine ----------------------
    @staticmethod
    def _load_yaml(path: str) -> Dict[str, Any]:
        if not os.path.exists(path):
            return {}
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}

    def _load_config(self, path: str) -> Dict[str, Any]:
        base = self._load_yaml(path)
        override = self._load_yaml(LOCAL_OVERRIDE_PATH)
        # Deep-merge dictionaries: override > base
        def deep_merge(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
            out = dict(a)
            for k, v in b.items():
                if isinstance(v, dict) and isinstance(out.get(k), dict):
                    out[k] = deep_merge(out[k], v)
                else:
                    out[k] = v
            return out

        merged = deep_merge(base, override)
        # Set defaults if missing
        merged.setdefault("paths", {}).setdefault("logs_dir", "logs")
        merged.setdefault("paths", {}).setdefault("outputs_dir", "outputs")
        merged.setdefault("fetch", {}).setdefault("sample_rows", 1000)
        merged.setdefault("fetch", {}).setdefault("chunksize", 50000)
        return merged

    def _build_connection_url(self) -> URL:
        o = self.config.get("oracle", {})
        username = o.get("username")
        password = o.get("password")
        dsn = o.get("dsn")
        if not dsn:
            host = o.get("host", "127.0.0.1")
            port = o.get("port", 1521)
            service_name = o.get("service_name", "XE")
            # Build URL with service_name in query to avoid manual DSN strings
            return URL.create(
                drivername="oracle+oracledb",
                username=username,
                password=password,
                host=host,
                port=int(port) if port else None,
                database=None,
                query={"service_name": service_name},
            )
        else:
            # When DSN is provided (e.g., "localhost:1521/XE" or TNS alias), use it as host and leave others None
            return URL.create(
                drivername="oracle+oracledb",
                username=username,
                password=password,
                host=None,
                port=None,
                database=None,
                query={"dsn": dsn},
            )

    def _engine_kwargs(self) -> Dict[str, Any]:
        sa = self.config.get("sqlalchemy", {})
        kwargs = {
            "pool_size": sa.get("pool_size", 5),
            "max_overflow": sa.get("max_overflow", 10),
            "pool_timeout": sa.get("pool_timeout", 30),
            "pool_recycle": sa.get("pool_recycle", 1800),
            "echo": sa.get("echo", False),
        }
        # python-oracledb uses UTF-8 by default, no encoding parameter needed
        # Optionally pass thick_mode config if present
        thick_mode = sa.get("thick_mode")
        if thick_mode:
            if not hasattr(kwargs, "connect_args"):
                kwargs["connect_args"] = {}
            kwargs["connect_args"]["thick_mode"] = thick_mode
        return kwargs

    def get_engine(self, force_new: bool = False) -> Engine:
        if self.engine is not None and not force_new:
            return self.engine
        url = self._build_connection_url()
        kwargs = self._engine_kwargs()
        try:
            self.engine = create_engine(url, **kwargs)
            self.logger.info("SQLAlchemy engine created")
            return self.engine
        except Exception as e:
            self.logger.exception("Failed to create SQLAlchemy engine: %s", e)
            raise

    def connect(self) -> Connection:
        engine = self.get_engine()
        try:
            conn = engine.connect()
            self.logger.info("Connected to Oracle database")
            return conn
        except SQLAlchemyError as e:
            self.logger.exception("Failed to connect to Oracle: %s", e)
            raise

    def test_connection(self) -> bool:
        try:
            with self.connect() as conn:
                conn.execute(text("SELECT 1 FROM DUAL"))
            self.logger.info("Connection test succeeded")
            return True
        except Exception as e:
            self.logger.error("Connection test failed: %s", e)
            return False

    # Convenience methods to align with STEP 1 required API
    def get_table_names(self, schema: Optional[str] = None) -> List[str]:
        """Return list of table names in the current or specified schema."""
        return self.list_tables(schema=schema)

    def fetch_table_data(self, table_name: str, limit: int = 1000,
                          schema: Optional[str] = None) -> pd.DataFrame:
        """Return a pandas DataFrame with a sample of table data.

        Parameters:
            table_name: Name of the table
            limit: Max rows to fetch
            schema: Optional schema/owner
        """
        return self.sample_table(table=table_name, schema=schema, rows=limit)

    # ---------------------- Metadata helpers ----------------------
    def list_schemas(self, conn: Optional[Connection] = None) -> List[str]:
        close_after = False
        if conn is None:
            conn = self.connect()
            close_after = True
        try:
            # ALL_USERS shows all schemas (users)
            rs = conn.execute(text("SELECT USERNAME FROM ALL_USERS ORDER BY USERNAME"))
            schemas = [r[0] for r in rs]
            self.logger.info("Found %d schemas", len(schemas))
            return schemas
        finally:
            if close_after:
                conn.close()

    def list_tables(self, schema: Optional[str] = None, conn: Optional[Connection] = None) -> List[str]:
        close_after = False
        if conn is None:
            conn = self.connect()
            close_after = True
        try:
            if schema:
                q = text("SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER = :owner ORDER BY TABLE_NAME")
                rs = conn.execute(q, {"owner": schema.upper()})
            else:
                q = text("SELECT TABLE_NAME FROM USER_TABLES ORDER BY TABLE_NAME")
                rs = conn.execute(q)
            tables = [r[0] for r in rs]
            self.logger.info("Found %d tables%s", len(tables), f" in {schema}" if schema else "")
            return tables
        finally:
            if close_after:
                conn.close()

    def get_columns(self, table: str, schema: Optional[str] = None, conn: Optional[Connection] = None) -> pd.DataFrame:
        close_after = False
        if conn is None:
            conn = self.connect()
            close_after = True
        try:
            if schema:
                q = text(
                    """
                    SELECT OWNER, TABLE_NAME, COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION,
                           DATA_SCALE, NULLABLE
                      FROM ALL_TAB_COLUMNS
                     WHERE OWNER = :owner AND TABLE_NAME = :table
                     ORDER BY COLUMN_ID
                    """
                )
                params = {"owner": schema.upper(), "table": table.upper()}
            else:
                q = text(
                    """
                    SELECT USER AS OWNER, TABLE_NAME, COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION,
                           DATA_SCALE, NULLABLE
                      FROM USER_TAB_COLUMNS
                     WHERE TABLE_NAME = :table
                     ORDER BY COLUMN_ID
                    """
                )
                params = {"table": table.upper()}
            df = pd.read_sql(q, conn, params=params)
            self.logger.info("Loaded %d columns for %s", len(df), table)
            return df
        finally:
            if close_after:
                conn.close()

    # ---------------------- Query helpers ----------------------
    def read_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        conn: Optional[Connection] = None,
        chunksize: Optional[int] = None,
    ) -> pd.DataFrame:
        """Execute a SELECT query and return DataFrame.

        If chunksize is provided, concatenates chunks into a single DataFrame.
        """
        close_after = False
        if conn is None:
            conn = self.connect()
            close_after = True
        chunksize = chunksize or self.config.get("fetch", {}).get("chunksize", 50000)
        try:
            if chunksize:
                iterator = pd.read_sql(text(query), conn, params=params, chunksize=chunksize)
                frames = list(iterator)
                df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
            else:
                df = pd.read_sql(text(query), conn, params=params)
            self.logger.info("Query returned %d rows", len(df))
            return df
        finally:
            if close_after:
                conn.close()

    def sample_table(self, table: str, schema: Optional[str] = None, rows: Optional[int] = None,
                     conn: Optional[Connection] = None) -> pd.DataFrame:
        """Fetch a sample of rows from a table using Oracle SAMPLE clause or FETCH FIRST."""
        close_after = False
        if conn is None:
            conn = self.connect()
            close_after = True
        try:
            rows = rows or self.config.get("fetch", {}).get("sample_rows", 1000)
            ident = f"{schema.upper()}.{table.upper()}" if schema else table.upper()
            # Use FETCH FIRST with literal integer for compatibility across drivers
            n = int(rows)
            q = f"SELECT * FROM {ident} FETCH FIRST {n} ROWS ONLY"
            df = pd.read_sql(text(q), conn)
            self.logger.info("Sampled %d rows from %s", len(df), ident)
            return df
        finally:
            if close_after:
                conn.close()

    # ---------------------- Utility ----------------------
    def dispose(self) -> None:
        if self.engine is not None:
            self.engine.dispose()
            self.logger.info("Disposed SQLAlchemy engine")


__all__ = ["OracleConnector"]
