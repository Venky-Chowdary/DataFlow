"""D209 — a pgvector destination builds exactly the table its plan promised.

QA on 557f556c: the plan, preflight and Map treated a pgvector destination as
plain PostgreSQL and promised ``id BIGINT, name, long_txt, payload, note``.
The writer always built a vector-document table instead: ``id`` was text, the
four columns did not exist, their values sat only inside ``metadata`` (NULLs
dropped), the embedded text was the payload when filled and a JSON copy of the
row otherwise, and Gate-8 called the cells "unproven" while the run succeeded.

One destination contract now: the layout is decided once (an explicit
``vector_layout``, else the existing table's shape, else the document layout
unless ``DATAFLOW_PGVECTOR_TYPED_TABLE_DEFAULT=1``) and the plan shows it
column by column. Every test drives the user's route — ``start_transfer`` ->
``/api/v1/copilot/confirm`` -> job — PostgreSQL -> pgvector on a real
PostgreSQL with the pgvector extension, run 1 and run 2, NULL / '' / unicode,
and compares the plan's promised columns with ``pg_attribute`` cell by cell.

D140 (pgvector as a source) stays fixed: a strict key-based run reads it.
"""

from __future__ import annotations

import json
import os
import socket
import time
import uuid

import pytest

psycopg2 = pytest.importorskip("psycopg2")

PG = {
    "host": os.environ.get("PGHOST") or "127.0.0.1",
    "port": int(os.environ.get("PGPORT") or 5432),
    "user": os.environ.get("PGUSER") or "dataflow",
    "password": os.environ.get("PGPASSWORD") or "dataflow",
    "dbname": os.environ.get("PGDATABASE") or "dataflow",
}

LONG = "Ünïcødé long text — " + "x" * 40


def _pg():
    try:
        socket.create_connection((PG["host"], PG["port"]), timeout=1).close()
    except OSError:
        pytest.skip(f"PostgreSQL not reachable on {PG['host']}:{PG['port']}")
    try:
        conn = psycopg2.connect(connect_timeout=3, **PG)
    except Exception as exc:  # environmental: wrong credentials on this host
        pytest.skip(f"PostgreSQL login failed: {exc}")
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_available_extensions WHERE name = 'vector'")
        if not cur.fetchone():
            conn.close()
            pytest.skip("pgvector extension is not installed on this PostgreSQL")
        cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
    return conn


def _sql(statement: str, params=None) -> list[tuple]:
    conn = _pg()
    try:
        with conn.cursor() as cur:
            cur.execute(statement, params)
            return cur.fetchall() if cur.description else []
    finally:
        conn.close()


@pytest.fixture
def client(tmp_path, monkeypatch):
    for key, value in {
        "DATAFLOW_JOB_STORE": "memory",
        "DATAFLOW_DISABLE_OBJECT_STORE": "1",
        "DATAFLOW_PII_HASH_KEY": "test-pii-key",
        "DATAFLOW_EGRESS_ALLOWLIST": "127.0.0.0/8,::1/128",
        "DATAFLOW_PILOT_ACK_PATH": str(tmp_path / "acks.json"),
        "DATAFLOW_CONNECTOR_STORE": str(tmp_path / "connectors.json"),
        "DATAFLOW_CONNECTOR_STORE_BACKEND": "file",
        "DATAFLOW_PILOT_ENGINE": "local",
        "DATAFLOW_SEED_DEMO": "",
        # Deterministic local embedder: no model download, real vectors.
        "DATAFLOW_EMBEDDING_MODEL": "hash/32",
    }.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("DATAFLOW_PGVECTOR_TYPED_TABLE_DEFAULT", raising=False)
    monkeypatch.delenv("DATAWRAP_PGVECTOR_TYPED_TABLE_DEFAULT", raising=False)
    monkeypatch.setattr("src.services.auth_service.auth_required", lambda: False)

    import services.connector_store as connector_store
    import services.mongodb_service as mongo_mod
    import services.sync_cursor as sc
    import src.ai.copilot.ack_ledger as ack_mod
    from fastapi.testclient import TestClient
    from services.mongodb_service import MemoryMongoDBService
    from src.ai.copilot import tools as tools_mod
    from src.main import app

    memory = MemoryMongoDBService()
    memory.connect()
    monkeypatch.setattr(mongo_mod, "_mongodb_service", memory)
    monkeypatch.setattr(connector_store, "_backend_choice", None)
    monkeypatch.setattr(ack_mod, "_ledger", None)
    monkeypatch.setattr(tools_mod, "_tools", None)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def tables():
    _pg().close()
    names = {"src": "d209s_" + uuid.uuid4().hex[:8], "dst": "d209d_" + uuid.uuid4().hex[:8]}
    _sql(
        f"CREATE TABLE {names['src']} (id BIGINT PRIMARY KEY, name TEXT, "
        "long_txt TEXT, payload JSONB, note VARCHAR(40), rev INTEGER NOT NULL DEFAULT 1)"
    )
    _sql(
        f"INSERT INTO {names['src']} VALUES "
        "(1, 'Zoë ✓', %s, '{\"a\": 1, \"ü\": [true, null]}', NULL, 1), "
        "(2, '', NULL, NULL, '', 1), "
        "(3, NULL, '', NULL, '  ', 1), "
        "(4, '日本語', 'short', '{}', 'n4', 1)",
        (LONG,),
    )
    try:
        yield names
    finally:
        _sql(f"DROP TABLE IF EXISTS {names['src']}")
        _sql(f"DROP TABLE IF EXISTS {names['dst']}")


def _connector(kind: str, role: str, extra: dict | None = None) -> str:
    from services.connector_store import create_connector

    payload = {
        "name": f"d209-{kind}-{uuid.uuid4().hex[:6]}",
        "type": kind,
        "role": role,
        "host": PG["host"],
        "port": PG["port"],
        "database": PG["dbname"],
        "username": PG["user"],
        "password": PG["password"],
        "schema": "public",
        "ssl": False,
        "workspace_id": "",
    }
    if extra:
        payload["extra"] = extra
    return str(create_connector(payload).id)


def _wait(job_id: str, timeout_s: float = 180.0) -> dict:
    from services.mongodb_service import get_mongodb_service

    deadline = time.monotonic() + timeout_s
    job: dict = {}
    while time.monotonic() < deadline:
        job = get_mongodb_service().get_job(job_id) or {}
        if str(job.get("status") or "") in {"completed", "failed", "partial", "cancelled"}:
            return job
        time.sleep(0.25)
    return job


def _run(client, **kwargs) -> tuple[dict, dict]:
    """``start_transfer`` -> ``/copilot/confirm`` -> finished job."""
    from src.ai.copilot.transfer_tools import start_transfer

    staged = start_transfer(validation_mode="strict", **kwargs)
    out = staged.output or {}
    assert staged.success, {"error": staged.error, "blockers": (out.get("preflight") or {}).get("blockers")}
    plan = out.get("plan") or {}
    assert (plan.get("preflight") or {}).get("passed") is True
    from src.ai.copilot.ack_ledger import get_ack_ledger

    payload, _why = get_ack_ledger().get_pending_payload(out["ack_id"])
    plan = {**plan, "engine_mappings": list((payload or {}).get("mappings") or [])}
    confirmed = client.post(
        "/api/v1/copilot/confirm", json={"ack_id": out["ack_id"], "reason": "d209"}
    )
    assert confirmed.status_code == 200, confirmed.text
    body = confirmed.json()
    assert body.get("ok") is True, body
    job = _wait(str(body.get("job_id") or ""))
    assert job.get("status") == "completed", {
        "status": job.get("status"),
        "error": job.get("error") or job.get("message"),
        "gate8": (job.get("reconciliation") or {}).get("message"),
    }
    return plan, job


def _physical(table: str) -> list[tuple[str, str]]:
    return [
        (str(name), str(ftype))
        for name, ftype in _sql(
            "SELECT a.attname, format_type(a.atttypid, a.atttypmod) FROM pg_attribute a "
            "WHERE a.attrelid = %s::regclass AND a.attnum > 0 AND NOT a.attisdropped "
            "ORDER BY a.attnum",
            (f"public.{table}",),
        )
    ]


def _canon_type(declared: str) -> str:
    t = declared.strip().lower()
    t = t.replace("varchar(", "character varying(")
    return {"timestamp": "timestamp without time zone", "int": "integer"}.get(t, t)


def _assert_plan_is_the_table(plan: dict, table: str) -> dict:
    """The plan's promised columns are the created columns, name and type."""
    contract = plan.get("destination_contract") or {}
    # Before D209 the plan's only promise was the Map: target name and type.
    promised = [
        (c["name"], _canon_type(c["type"])) for c in contract.get("columns") or []
    ] or [
        (str(m.get("target")), _canon_type(str(m.get("target_type") or "")))
        for m in plan.get("engine_mappings") or []
    ]
    assert promised
    assert promised == _physical(table), {"promised": promised, "created": _physical(table)}
    assert contract, "the plan names no destination contract"
    pre = plan.get("preflight") or {}
    assert pre.get("destination_contract") == contract
    assert any(w.get("id") == "pgvector_destination_layout" for w in pre.get("warnings") or [])
    return contract


def _source_rows(table: str) -> list[tuple]:
    return _sql(f"SELECT id, name, long_txt, payload, note, rev FROM {table} ORDER BY id")


# ------------------------------------------------------------------ typed table


def _assert_typed_cells(src: str, dst: str) -> None:
    got = _sql(f"SELECT id, name, long_txt, payload, note, rev FROM {dst} ORDER BY id")
    assert got == _source_rows(src)  # NULL is NULL, '' is '', unicode exact


def test_typed_layout_creates_the_planned_table_run1_and_run2(client, tables):
    src, dst = tables["src"], tables["dst"]
    source = _connector("postgresql", "source")
    dest = _connector("pgvector", "destination", {"vector_layout": "table"})

    plan, job = _run(
        client, source_connector_id=source, source_table=src, dest_connector_id=dest,
        dest_table=dst, sync_mode="full_refresh_overwrite",
    )
    contract = _assert_plan_is_the_table(plan, dst)
    assert contract["layout"] == "table"
    assert _physical(dst)[0] == ("id", "bigint")
    _assert_typed_cells(src, dst)
    recon = job.get("reconciliation") or {}
    assert recon.get("passed") is True and recon.get("checksum_match") is True, recon

    # Run 2 into the table run 1 created: values change to NULL / '' / unicode.
    _sql(f"UPDATE {src} SET note = NULL, name = 'ñ2', rev = 2 WHERE id = 2")
    _sql(f"UPDATE {src} SET note = '' , long_txt = NULL, rev = 2 WHERE id = 1")
    _sql(f"INSERT INTO {src} VALUES (5, 'ⓕive', NULL, '[1, \"2\"]', NULL, 2)")
    plan2, job2 = _run(
        client, source_connector_id=source, source_table=src, dest_connector_id=dest,
        dest_table=dst, sync_mode="incremental_upsert", primary_key="id", cursor_column="rev", cursor_semantics="monotonic_sequence",
    )
    _assert_plan_is_the_table(plan2, dst)
    _assert_typed_cells(src, dst)
    assert (job2.get("reconciliation") or {}).get("passed") is True


def test_typed_default_flag_and_existing_typed_table_keeps_its_shape(client, tables, monkeypatch):
    src, dst = tables["src"], tables["dst"]
    source = _connector("postgresql", "source")
    dest = _connector("pgvector", "destination")

    monkeypatch.setenv("DATAFLOW_PGVECTOR_TYPED_TABLE_DEFAULT", "1")
    plan, _job = _run(
        client, source_connector_id=source, source_table=src, dest_connector_id=dest,
        dest_table=dst, sync_mode="full_refresh_overwrite",
    )
    contract = _assert_plan_is_the_table(plan, dst)
    assert (contract["layout"], contract["layout_reason"]) == ("table", "typed_default_flag")
    _assert_typed_cells(src, dst)

    # Flag back off: the table DataFlow created stays typed on run 2.
    monkeypatch.delenv("DATAFLOW_PGVECTOR_TYPED_TABLE_DEFAULT")
    _sql(f"UPDATE {src} SET payload = NULL, rev = 2 WHERE id = 1")
    plan2, _job2 = _run(
        client, source_connector_id=source, source_table=src, dest_connector_id=dest,
        dest_table=dst, sync_mode="incremental_upsert", primary_key="id", cursor_column="rev", cursor_semantics="monotonic_sequence",
    )
    contract2 = _assert_plan_is_the_table(plan2, dst)
    assert contract2["layout_reason"] == "existing_typed_table"
    _assert_typed_cells(src, dst)


# -------------------------------------------------------------- document layout


def _documents(dst: str) -> dict[str, tuple]:
    rows = _sql(
        f"SELECT source_id, id, content, embedding IS NULL, metadata FROM {dst} "
        "ORDER BY source_id, chunk_index"
    )
    out: dict[str, tuple] = {}
    for source_id, doc_id, content, no_vector, meta in rows:
        out.setdefault(str(source_id), (doc_id, content, no_vector, meta))
    return out


def _assert_document_cells(src: str, dst: str, *, rule_column: str | None) -> None:
    docs = _documents(dst)
    source = _source_rows(src)
    assert sorted(docs) == sorted(str(r[0]) for r in source)
    for row in source:
        doc_id, content, no_vector, meta = docs[str(row[0])]
        assert doc_id == str(row[0])  # the source key, verbatim
        expected = dict(zip(("id", "name", "long_txt", "payload", "note", "rev"), row))
        for key, value in expected.items():
            assert key in meta, (row[0], key, meta)  # NULL kept as JSON null
            stored = meta[key]
            if key == "payload" and isinstance(stored, str):
                stored = json.loads(stored)
            assert stored == value, (row[0], key, stored, value)
        if rule_column:
            # One rule for every row: the declared column; NULL / '' embed nothing.
            want = expected[rule_column] or ""
            assert content == want, (row[0], content)
            assert no_vector is (want == ""), (row[0], no_vector)
        else:
            # One rule for every row: the whole row as JSON, never a column pick.
            assert json.loads(content)["id"] == row[0], content
            assert no_vector is False


def _assert_gate8_proved_every_value(job: dict, rows: int) -> None:
    recon = job.get("reconciliation") or {}
    proof = recon.get("document_value_proof") or {}
    assert recon.get("passed") is True and proof.get("passed") is True, recon
    assert proof.get("source_rows") == rows and proof.get("unmatched_source_rows") == 0, proof
    assert set(proof.get("fields") or []) == {"id", "name", "long_txt", "payload", "note", "rev"}


def test_document_layout_matches_the_plan_run1_and_run2(client, tables):
    src, dst = tables["src"], tables["dst"]
    source = _connector("postgresql", "source")
    dest = _connector(
        "pgvector", "destination",
        {"vector_layout": "document", "content_column": "long_txt", "skip_chunking": True},
    )

    plan, job = _run(
        client, source_connector_id=source, source_table=src, dest_connector_id=dest,
        dest_table=dst, sync_mode="full_refresh_overwrite",
    )
    contract = _assert_plan_is_the_table(plan, dst)
    assert contract["layout"] == "document"
    assert contract["embedded_text_rule"]["kind"] == "column"
    assert contract["embedded_text_rule"]["column"] == "long_txt"
    assert [f["key"] for f in contract["metadata_fields"]] == [
        "id", "name", "long_txt", "payload", "note", "rev"
    ]
    _assert_document_cells(src, dst, rule_column="long_txt")
    _assert_gate8_proved_every_value(job, 4)

    _sql(f"UPDATE {src} SET long_txt = NULL, note = 'é', rev = 2 WHERE id = 1")
    _sql(f"UPDATE {src} SET long_txt = 'now filled ✓', rev = 2 WHERE id = 2")
    _sql(f"INSERT INTO {src} VALUES (5, NULL, NULL, NULL, NULL, 2)")
    plan2, job2 = _run(
        client, source_connector_id=source, source_table=src, dest_connector_id=dest,
        dest_table=dst, sync_mode="incremental_upsert", primary_key="id", cursor_column="rev", cursor_semantics="monotonic_sequence",
    )
    _assert_plan_is_the_table(plan2, dst)
    _assert_document_cells(src, dst, rule_column="long_txt")
    _assert_gate8_proved_every_value(job2, 5)


def test_undeclared_pgvector_shows_the_document_table_it_builds(client, tables, monkeypatch):
    """Flag off (production default): the plan promises the document table."""
    audits: list[str] = []
    import services.would_apply_audit as would

    real = would.audit_would_apply
    monkeypatch.setattr(
        would, "audit_would_apply",
        lambda action, **kw: (audits.append(action), real(action, **kw)),
    )
    src, dst = tables["src"], tables["dst"]
    source = _connector("postgresql", "source")
    dest = _connector("pgvector", "destination")

    plan, job = _run(
        client, source_connector_id=source, source_table=src, dest_connector_id=dest,
        dest_table=dst, sync_mode="full_refresh_overwrite",
    )
    contract = _assert_plan_is_the_table(plan, dst)
    assert (contract["layout"], contract["layout_reason"]) == ("document", "default_document")
    assert contract["embedded_text_rule"]["kind"] == "row_json"
    assert "pgvector.would_create_typed_table" in audits
    _assert_document_cells(src, dst, rule_column=None)
    _assert_gate8_proved_every_value(job, 4)

    # Run 2: the document table DataFlow created keeps working, undeclared.
    _sql(f"UPDATE {src} SET name = NULL, rev = 2 WHERE id = 4")
    plan2, job2 = _run(
        client, source_connector_id=source, source_table=src, dest_connector_id=dest,
        dest_table=dst, sync_mode="incremental_upsert", primary_key="id", cursor_column="rev", cursor_semantics="monotonic_sequence",
    )
    contract2 = _assert_plan_is_the_table(plan2, dst)
    assert contract2["layout_reason"] == "existing_document_table"
    _assert_document_cells(src, dst, rule_column=None)
    _assert_gate8_proved_every_value(job2, 4)


def test_document_gate8_fails_when_metadata_lost_a_value(client, tables, monkeypatch):
    """Gate-8 reads every value back: a dropped NULL key is a failed run."""
    import connectors.writer_common as wc

    real = wc.vector_prepare_metadata

    def lossy(meta, *, keep_null=False):
        return real(meta, keep_null=False)

    monkeypatch.setattr(wc, "vector_prepare_metadata", lossy)
    src, dst = tables["src"], tables["dst"]
    source = _connector("postgresql", "source")
    dest = _connector("pgvector", "destination", {"vector_layout": "document"})
    from src.ai.copilot.transfer_tools import start_transfer

    staged = start_transfer(
        validation_mode="strict", source_connector_id=source, source_table=src,
        dest_connector_id=dest, dest_table=dst, sync_mode="full_refresh_overwrite",
    )
    assert staged.success, staged.error
    body = client.post(
        "/api/v1/copilot/confirm", json={"ack_id": staged.output["ack_id"], "reason": "d209"}
    ).json()
    job = _wait(str(body.get("job_id") or ""))
    assert job.get("status") == "failed", job.get("status")
    proof = (job.get("reconciliation") or {}).get("document_value_proof") or {}
    assert proof.get("passed") is False and proof.get("unmatched_source_rows", 0) >= 1, proof


# ------------------------------------------------------------- D140 stays fixed


def test_d140_pgvector_source_still_reads_through_a_strict_key_run(client, tables):
    src, dst = tables["src"], tables["dst"]
    source = _connector("pgvector", "source")
    dest = _connector("postgresql", "destination")
    _plan, job = _run(
        client, source_connector_id=source, source_table=src, dest_connector_id=dest,
        dest_table=dst, sync_mode="incremental_upsert", primary_key="id", cursor_column="rev", cursor_semantics="monotonic_sequence",
    )
    assert (job.get("reconciliation") or {}).get("passed") is True
    _assert_typed_cells(src, dst)
