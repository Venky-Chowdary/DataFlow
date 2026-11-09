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


def test_staging_honours_sheet_index_table_pick(tmp_path):
    from src.transfer.endpoint_kind import stage_sqlite_upload
    from src.transfer.models import EndpointConfig, TransferRequest

    request = TransferRequest(
        source=EndpointConfig(kind="file", format="sqlite"),
        source_filename="shop.db",
        source_content=_sqlite_bytes(tmp_path, extra_table=True),
        read_options={"sheet_index": 1},
        destination=EndpointConfig(kind="database", format="postgresql", table="t"),
    )
    assert stage_sqlite_upload(request, "idx") is True
    try:
        # Tables are listed by name (audit, orders), as the profile picker lists them.
        assert request.source.table == "orders"
        assert "sheet_index" not in (request.read_options or {})
    finally:
        os.remove(request.source.database)


def test_binary_blob_preview_is_json_serializable(tmp_path):
    import json

    from fastapi.encoders import jsonable_encoder

    from src.routers.connectors_router import profile_file_upload

    path = tmp_path / "blob.db"
    with sqlite3.connect(str(path)) as conn:
        conn.execute("CREATE TABLE files (id INTEGER PRIMARY KEY, body BLOB)")
        conn.execute("INSERT INTO files VALUES (1, ?)", (b"\xff\x00\xfe",))
    out = profile_file_upload(path.read_bytes(), "blob.db")
    assert out.get("success") is True, out
    json.dumps(jsonable_encoder(out))
    assert out["data"][0]["body"] == "\\xff00fe"


# QA1 S12 (job 6af15131…7af7, Validate pf_77b26986d684): a REAL column holding
# 0.5–4.0 was profiled from sample digits as DECIMAL(2,1); Execute reads the
# staged copy through the SQLite connector (declared affinity → DOUBLE), so
# Execute refused "weight DOUBLE PRECISION → NUMERIC(2,1)" and wrote nothing.
WEIGHTS = [(1, 0.5, 12.25, 7), (2, 1.5, 3.0, 8), (3, 4.0, 0.125, 9)]


def _real_affinity_bytes(tmp_path) -> bytes:
    path = tmp_path / f"parcels_{uuid.uuid4().hex[:6]}.db"
    with sqlite3.connect(str(path)) as conn:
        conn.execute(
            "CREATE TABLE parcels (id INTEGER PRIMARY KEY, weight REAL, "
            "price NUMERIC(10,3), qty INT)"
        )
        conn.executemany("INSERT INTO parcels VALUES (?, ?, ?, ?)", WEIGHTS)
    return path.read_bytes()


def _profile_types(out: dict) -> dict[str, str]:
    """The column types the Studio carries from the upload into Validate."""
    return {str(name): str(out["schema"][name]) for name in out["columns"]}


def _studio_validate(content: bytes, table: str) -> tuple[list[dict], dict]:
    """Upload profile → Validate twice (as Studio maps), returning mappings + artifact."""
    from services.preflight_service import run_file_preflight
    from src.routers.connectors_router import profile_file_upload

    out = profile_file_upload(content, "parcels.db")
    assert out.get("success") is True, out
    types = _profile_types(out)
    names = [str(n) for n in out["columns"]]
    rows = [dict(zip(names, r, strict=True)) for r in WEIGHTS]

    def _validate(maps):
        res = run_file_preflight(
            columns=names, column_types=types, row_count=len(WEIGHTS),
            mappings=maps, destination_connected=True, source_kind="file",
            source_format="sqlite", sync_mode="full_refresh_overwrite",
            sample_rows=rows, destination_db_type="postgresql",
            destination_table=table, destination_table_exists=False,
            source_filename="parcels.db",
        )
        return (res.get("proof_bundle") or {}).get("decision_artifact") or {}

    mappings = [
        {"source": n, "target": n, "source_type": types[n], "target_type": types[n], "confidence": 1.0}
        for n in names
    ]
    artifact = _validate(mappings)
    ddl = (artifact.get("ddl") or {}).get("column_ddl") or {}
    mappings = [{**m, "target_type": ddl.get(m["target"], m["target_type"])} for m in mappings]
    artifact = _validate(mappings)
    assert artifact.get("content_hash")
    return mappings, artifact


def test_sqlite_upload_profile_uses_declared_affinity_not_sample_digits(tmp_path):
    from services.schema_introspect import _introspect_sqlite
    from src.routers.connectors_router import profile_file_upload

    content = _real_affinity_bytes(tmp_path)
    out = profile_file_upload(content, "parcels.db")
    assert out.get("success") is True, out
    types = _profile_types(out)
    assert types["weight"].upper() == "DOUBLE PRECISION", types

    # One rule: the upload profile (Validate) and the connector read of the
    # same file (what Execute stages) name every column the same type.
    staged = tmp_path / "staged.db"
    staged.write_bytes(content)
    intro = _introspect_sqlite(database=str(staged), table="parcels")
    assert intro["ok"], intro
    assert types == {c["name"]: c["inferred_type"] for c in intro["columns"]}


@pytest.mark.skipif(not pg_up(), reason="PostgreSQL not reachable")
def test_sqlite_real_column_validate_then_execute_lands_double_in_postgres(tmp_path):
    import psycopg2

    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    content = _real_affinity_bytes(tmp_path)
    c = pg_creds()
    table = f"qa1_s12_real_{uuid.uuid4().hex[:8]}"
    mappings, artifact = _studio_validate(content, table)

    request = TransferRequest(
        source=EndpointConfig(kind="file", format="sqlite"),
        source_filename="parcels.db",
        source_content=content,
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
        assert "NUMERIC(2,1)" not in (result.error or ""), result.error
        assert result.success is True, result.error
        with conn.cursor() as cur:
            cur.execute(
                "SELECT data_type FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = %s AND column_name = 'weight'",
                (table,),
            )
            assert cur.fetchone()[0] == "double precision"
            cur.execute(f'SELECT id, weight, price FROM public."{table}" ORDER BY id')
            got = cur.fetchall()
        assert [(int(i), float(w), Decimal(str(p))) for i, w, p in got] == [
            (i, w, Decimal(str(p))) for i, w, p, _q in WEIGHTS
        ]
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
        conn.commit()
        conn.close()


# SQLite "Sync now": the child request is the parent's serialized request, which
# never carries the upload bytes — only the durable ``source_path``. The read
# window check sniffed in-memory bytes only, so ``parcels.db`` resolved to
# "unknown" and the child failed: "A sheet selection applies to Excel workbooks
# and SQLite databases (the table); this source is unknown".
def _durable_upload(content: bytes, name: str) -> str:
    from services.platform_config import upload_dir

    root = upload_dir()
    root.mkdir(parents=True, exist_ok=True)
    path = root / f"{uuid.uuid4().hex[:8]}_{name}"
    path.write_bytes(content)
    return str(path)


def test_read_window_on_a_path_only_sqlite_upload_is_honoured(tmp_path):
    from src.transfer.engine import resolve_read_options
    from src.transfer.models import EndpointConfig, TransferRequest

    path = tmp_path / "shop.db"
    path.write_bytes(_sqlite_bytes(tmp_path, extra_table=True))
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="sqlite"),
        source_filename="shop.db",
        source_path=str(path),
        read_options={"sheet": "orders"},
        destination=EndpointConfig(kind="database", format="postgresql", table="t"),
    )
    assert resolve_read_options(request).sheet == "orders"


@pytest.mark.skipif(not pg_up(), reason="PostgreSQL not reachable")
def test_sync_now_of_a_sqlite_upload_job_reloads_the_picked_table(tmp_path):
    import psycopg2

    from services.sync_now import child_request_dict
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import (
        EndpointConfig,
        TransferRequest,
        transfer_request_from_dict,
        transfer_request_to_dict,
    )

    content = _real_affinity_bytes(tmp_path)
    c = pg_creds()
    table = f"qa_sync_now_sqlite_{uuid.uuid4().hex[:8]}"
    mappings, artifact = _studio_validate(content, table)
    parent = TransferRequest(
        source=EndpointConfig(kind="file", format="sqlite"),
        source_filename="parcels.db",
        source_path=_durable_upload(content, "parcels.db"),
        read_options={"sheet": "parcels"},
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
    saved = transfer_request_to_dict(parent)
    engine = UniversalTransferEngine()
    first = engine.execute_tracked(parent, uuid.uuid4().hex[:24])
    conn = psycopg2.connect(
        host=c["host"], port=int(c["port"]), dbname=c["database"],
        user=c["username"], password=c["password"],
    )
    try:
        assert first.success is True, first.error
        child_dict = child_request_dict(saved, mode="sync", parent_job_id="parent")
        child_dict["destination"]["password"] = c["password"]
        child = transfer_request_from_dict(child_dict)
        assert not child.source_content and child.source_path
        result = engine.execute_tracked(child, uuid.uuid4().hex[:24])
        assert "source is unknown" not in (result.error or ""), result.error
        assert result.success is True, result.error
        with conn.cursor() as cur:
            cur.execute(f'SELECT id, weight FROM public."{table}" ORDER BY id')
            got = cur.fetchall()
        assert [(int(i), float(w)) for i, w in got] == [(i, w) for i, w, _p, _q in WEIGHTS]
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
        conn.commit()
        conn.close()
