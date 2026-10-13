"""MX3-08 — diff_schemas must classify source -> destination, not the reverse.

QA (PG vs PG): only_in_source=[email, full_name] was reported as a breaking
``drop`` of email, and only_in_destination=[legacy] as an ``add_column``.
A source-only column is what a load would add to the destination; a
destination-only column is what the destination has that the source lacks.
"""

from __future__ import annotations

import socket
import uuid

import pytest


def _pg_reachable() -> bool:
    try:
        with socket.create_connection(("localhost", 5432), timeout=1.5):
            return True
    except OSError:
        return False


@pytest.mark.skipif(not _pg_reachable(), reason="Postgres not reachable")
def test_live_diff_schemas_classifies_source_to_destination():
    import psycopg2

    from services.connector_store import create_connector, delete_connector
    from src.ai.copilot.tools import DataPilotTools

    sfx = uuid.uuid4().hex[:6]
    src, dst = f"mx308s_{sfx}", f"mx308d_{sfx}"
    conn = psycopg2.connect(
        host="localhost", port=5432, dbname="dataflow", user="dataflow", password="dataflow"
    )
    conn.autocommit = True
    saved = create_connector({
        "name": f"MX308{sfx}", "type": "postgresql", "host": "localhost", "port": 5432,
        "database": "dataflow", "username": "dataflow", "password": "dataflow",
        "schema": "public",
    })
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"CREATE TABLE {src} (id INT PRIMARY KEY, email VARCHAR(60), "
                "price DECIMAL(10,2), qty BIGINT)"
            )
            cur.execute(
                f"CREATE TABLE {dst} (id INT PRIMARY KEY, legacy INT, "
                "price DECIMAL(10,2), qty BIGINT)"
            )
        res = DataPilotTools().execute("diff_schemas", {
            "source_connector_name": f"MX308{sfx}", "source_table": src,
            "dest_connector_name": f"MX308{sfx}", "dest_table": dst,
        })
        assert res.success, res.error
        out = res.output
        assert out["only_in_source"] == ["email"]
        assert out["only_in_destination"] == ["legacy"]
        added = {c["column"] for c in out["additive"] if c["kind"] == "add_column"}
        dropped = {c["column"] for c in out["breaking"] if c["kind"] == "drop"}
        assert added == {"email"}, out
        assert dropped == {"legacy"}, out
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {src}")
            cur.execute(f"DROP TABLE IF EXISTS {dst}")
        conn.close()
        delete_connector(saved.id)
