"""QA1 — an uploaded SQLite database (.db / .sqlite / .sqlite3) is a real source.

Before: the Studio's file input refused the extensions and ``/connectors/upload``
answered ``Unsupported file type: sqlite`` because ``FileParser.parse`` had no
SQLite reader, so the SQLite→PostgreSQL route could not even be profiled. The
upload now profiles the table through the same immutable read-only open the
transfer stages the file with, lists the tables for the picker, and the picked
table (the read window's ``sheet``) is the table the run loads.
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from decimal import Decimal

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import pytest  # noqa: E402

from tests.helpers.live_env import pg_creds, pg_up  # noqa: E402

ROWS = [
    (1, "Ada", "1234.5600", None),
    (2, "Grace", "0.0100", "naïve café"),
    (3, "Linus", "-99.9900", ""),
]


def _sqlite_bytes(tmp_path, *, extra_table: bool) -> bytes:
    path = tmp_path / f"src_{uuid.uuid4().hex[:6]}.db"
    with sqlite3.connect(str(path)) as conn:
        conn.execute(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, name TEXT NOT NULL, "
            "amount NUMERIC, note TEXT)"
        )
        conn.executemany("INSERT INTO orders VALUES (?, ?, ?, ?)", ROWS)
        if extra_table:
            conn.execute("CREATE TABLE audit (k TEXT)")
            conn.execute("INSERT INTO audit VALUES ('x')")
    return path.read_bytes()


@pytest.mark.parametrize("filename", ["shop.db", "shop.sqlite", "shop.sqlite3"])
def test_upload_profiles_sqlite_for_every_extension(tmp_path, filename):
    from src.routers.connectors_router import profile_file_upload

    out = profile_file_upload(_sqlite_bytes(tmp_path, extra_table=False), filename)
    assert out.get("success") is True, out
    assert out["file_type"] == "sqlite"
    assert out["row_count"] == 3
    assert [c["name"] if isinstance(c, dict) else c for c in out["columns"]][:4] == [
        "id", "name", "amount", "note",
    ]
    assert [s["name"] for s in out["sheets"]] == ["orders"]


def test_multi_table_sqlite_previews_first_table_and_names_it(tmp_path):
    from services.read_options import ReadOptions
    from src.routers.connectors_router import profile_file_upload

    from services.file_parser import FileParser

    content = _sqlite_bytes(tmp_path, extra_table=True)
    refused = FileParser.parse(content, "shop.db")
    assert not refused.success and "pick the table" in (refused.error or "").lower()

    # The Source step still gets a preview (so it can render the table picker),
    # and the window it returns names the table that preview was built from.
    first = profile_file_upload(content, "shop.db")
    assert first["read_options"]["sheet"] == "audit"
    assert sorted(s["name"] for s in first["sheets"]) == ["audit", "orders"]

    picked = profile_file_upload(content, "shop.db", read_options=ReadOptions(sheet="orders"))
    assert picked["row_count"] == 3 and picked["file_type"] == "sqlite"
    assert sorted(s["name"] for s in picked["sheets"]) == ["audit", "orders"]


def test_non_sqlite_db_file_is_not_claimed_as_sqlite():
    from services.file_parser import FileParser

    assert FileParser.detect_file_type("notes.db", b"id,name\n1,a\n") != "sqlite"


@pytest.mark.skipif(not pg_up(), reason="PostgreSQL not reachable")
def test_uploaded_db_file_transfers_to_postgres_and_reads_back(tmp_path):
    import psycopg2

    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    c = pg_creds()
    table = f"qa1_sqlite_{uuid.uuid4().hex[:8]}"
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="sqlite"),
        source_filename="shop.db",
        source_content=_sqlite_bytes(tmp_path, extra_table=True),
        read_options={"sheet": "orders"},
        destination=EndpointConfig(
            kind="database", format="postgresql", host=c["host"], port=int(c["port"]),
            database=c["database"], username=c["username"], password=c["password"],
            schema="public", table=table,
        ),
        sync_mode="full_refresh",
        skip_preflight=True,
    )
    result = UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])
    conn = psycopg2.connect(
        host=c["host"], port=int(c["port"]), dbname=c["database"],
        user=c["username"], password=c["password"],
    )
    try:
        assert result.success is True, result.error
        assert result.records_transferred == 3
        with conn.cursor() as cur:
            cur.execute(f'SELECT id, name, amount, note FROM public."{table}" ORDER BY id')
            got = cur.fetchall()
        assert [(int(r[0]), r[1], Decimal(str(r[2])), r[3]) for r in got] == [
            (i, n, Decimal(a), note) for i, n, a, note in ROWS
        ]
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
        conn.commit()
        conn.close()


@pytest.mark.skipif(not pg_up(), reason="PostgreSQL not reachable")
def test_uploaded_db_validate_artifact_executes_without_source_drift(tmp_path):
    """Validate binds the upload as a file; Execute stages it as database/sqlite.

    QA (PR #380 browser run): Studio Validate passed, Execute failed with
    "Decision Artifact source schema drifted since Validate" and wrote no rows.
    """
    import psycopg2

    from services.file_parser import FileParser
    from services.preflight_service import run_file_preflight
    from services.read_options import ReadOptions
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    content = _sqlite_bytes(tmp_path, extra_table=True)
    parsed = FileParser.parse(content, "shop.db", read_options=ReadOptions(sheet="orders"))
    assert parsed.success, parsed.error
    schema = FileParser.infer_schema(parsed.data)
    types = {c: str(schema[c]) for c in parsed.columns}
    mappings = [
        {"source": c, "target": c, "source_type": types[c], "target_type": types[c], "confidence": 1.0}
        for c in parsed.columns
    ]
    c = pg_creds()
    table = f"qa1_sqlite_va_{uuid.uuid4().hex[:8]}"
    def _validate(maps):
        out = run_file_preflight(
            columns=parsed.columns, column_types=types, row_count=parsed.row_count,
            mappings=maps, destination_connected=True, source_kind="file",
            source_format="sqlite", sync_mode="full_refresh_overwrite",
            sample_rows=parsed.data, destination_db_type="postgresql",
            destination_table=table, destination_table_exists=False,
            source_filename="shop.db",
        )
        return out, (out.get("proof_bundle") or {}).get("decision_artifact") or {}

    # Map as Studio does: the target types are the DDL Validate settles on.
    validate, artifact = _validate(mappings)
    ddl = (artifact.get("ddl") or {}).get("column_ddl") or {}
    mappings = [{**m, "target_type": ddl.get(m["target"], m["target_type"])} for m in mappings]
    validate, artifact = _validate(mappings)
    assert artifact.get("content_hash"), validate.get("proof_bundle", {}).keys()

    request = TransferRequest(
        source=EndpointConfig(kind="file", format="sqlite"),
        source_filename="shop.db",
        source_content=content,
        read_options={"sheet": "orders"},
        destination=EndpointConfig(
            kind="database", format="postgresql", host=c["host"], port=int(c["port"]),
            database=c["database"], username=c["username"], password=c["password"],
            schema="public", table=table,
        ),
        mappings=mappings,
        sync_mode="full_refresh_overwrite",
        skip_preflight=False,
        decision_artifact=artifact,
        approved_decision_artifact_hash=artifact["content_hash"],
    )
    result = UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])
    conn = psycopg2.connect(
        host=c["host"], port=int(c["port"]), dbname=c["database"],
        user=c["username"], password=c["password"],
    )
    try:
        assert "drifted since Validate" not in (result.error or ""), result.error
        assert result.success is True, result.error
        with conn.cursor() as cur:
            cur.execute(f'SELECT id, name FROM public."{table}" ORDER BY id')
            got = cur.fetchall()
        assert [(int(r[0]), r[1]) for r in got] == [(i, n) for i, n, _a, _note in ROWS]
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
        conn.commit()
        conn.close()
