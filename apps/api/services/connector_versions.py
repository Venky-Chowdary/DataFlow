"""Connector release strings for a proof pack.

``migration_proven`` refuses a format or kind name (``postgresql``,
``snowflake``, ``excel``). The string has to name a release: a parser
version, a driver version, and the server version when the session that
wrote the rows can still answer.
"""

from __future__ import annotations

import sys
from typing import Any, Mapping


def version_has_release(value: Any) -> bool:
    """True when ``value`` contains a digit — a release, not a format name."""
    return isinstance(value, str) and any(ch.isdigit() for ch in value)


def versions_include_release(versions: Mapping[str, Any] | None) -> bool:
    """Both sides of the route name a release."""
    if not isinstance(versions, Mapping) or not versions:
        return False
    return version_has_release(versions.get("source")) and version_has_release(
        versions.get("destination")
    )


def file_parser_version(file_type: str) -> str:
    """Parser that will re-read this file, with its package release."""
    kind = (file_type or "").strip().lower()
    py = sys.version.split()[0]
    if kind == "excel":
        try:
            import openpyxl

            return f"openpyxl {openpyxl.__version__}"
        except Exception:
            return "excel"
    if kind in {"csv", "tsv"}:
        return f"python-csv {py}"
    if kind in {"json", "jsonl", "ndjson"}:
        return f"python-json {py}"
    if kind == "parquet":
        try:
            import pyarrow

            return f"pyarrow {pyarrow.__version__}"
        except Exception:
            return "parquet"
    if kind == "xml":
        return f"python-xml {py}"
    if kind == "yaml":
        return f"python-yaml {py}"
    if kind in {"avro", "orc", "fixed_width"}:
        return kind
    return kind or "file"


def database_driver_version(engine: str) -> str:
    """Client library release. A bare engine name is not a version."""
    from services.source_reread import engine_family

    kind = engine_family(engine)
    if kind == "postgresql":
        try:
            import psycopg2

            release = str(psycopg2.__version__).split()[0]
            return f"psycopg2 {release}"
        except Exception:
            return "postgresql"
    if kind == "mysql":
        try:
            import pymysql

            return f"pymysql {pymysql.__version__}"
        except Exception:
            return "mysql"
    if kind == "sqlite":
        import sqlite3

        return f"sqlite3 {sqlite3.sqlite_version}"
    if kind == "snowflake":
        try:
            import snowflake.connector

            return f"snowflake-connector-python {snowflake.connector.__version__}"
        except Exception:
            return "snowflake"
    if (engine or "").strip().lower() == "bigquery":
        try:
            import google.cloud.bigquery as bq

            return f"google-cloud-bigquery {bq.__version__}"
        except Exception:
            return "bigquery"
    return (engine or "").strip().lower()


def server_version_from_connection(conn: Any) -> str:
    """``SHOW server_version`` on a connection the write already holds.

    A failed probe returns ``""``. Callers must not open a new session just
    to learn a version — a 15s connect timeout must not sit on the proof stamp.
    """
    if conn is None:
        return ""
    try:
        cur = conn.cursor()
        try:
            cur.execute("SHOW server_version")
            row = cur.fetchone()
        finally:
            try:
                cur.close()
            except Exception:
                pass
        if not row:
            return ""
        return str(row[0]).strip()
    except Exception:
        return ""


def _with_server(driver: str, server: str) -> str:
    server = str(server or "").strip()
    if not server:
        return driver
    if not version_has_release(driver):
        return server if version_has_release(server) else driver
    return f"{driver} / server {server}"


def capture_route_versions(
    *,
    source_file_type: str = "",
    source_engine: str = "",
    dest_type: str = "",
    source_server: str = "",
    dest_server: str = "",
) -> dict[str, str]:
    """Source and destination release strings for this route.

    File sources use the parser. Database sources and destinations use the
    driver, plus ``server`` when the write session reported one.
    """
    if (source_file_type or "").strip():
        source = file_parser_version(source_file_type)
    elif (source_engine or "").strip():
        source = _with_server(database_driver_version(source_engine), source_server)
    else:
        source = ""
    destination = ""
    if (dest_type or "").strip():
        destination = _with_server(database_driver_version(dest_type), dest_server)
    out: dict[str, str] = {}
    if source:
        out["source"] = source
    if destination:
        out["destination"] = destination
    return out
