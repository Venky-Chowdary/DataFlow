"""Gate-8 leftover MERGE + live PostgreSQL logical CDC dest COUNT.

Honesty
-------
* Only Postgres ``:5432`` is a real desktop engine in this VM (wal_level=logical).
  MySQL binlog / Mongo change streams skip with reason — not greened by absence.
* CDC default is **at-least-once upsert**. ``PLATFORM_EXACTLY_ONCE_CLAIMED`` stays
  False. Leftover MERGE is a hard no-op on CDC (a changelog is not S).
* ``100%`` is not claimed. Named fixtures only.
"""

from __future__ import annotations

import os
import socket
import sqlite3
import sys
import uuid
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import pytest

from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest

PG = dict(
    host="localhost",
    port=5432,
    database="dataflow",
    username="dataflow",
    password="dataflow",
)


def _reachable(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=1.5):
            return True
    except OSError:
        return False


def _pg_connect():
    import psycopg2

    conn = psycopg2.connect(
        host=PG["host"],
        port=PG["port"],
        database=PG["database"],
        user=PG["username"],
        password=PG["password"],
    )
    conn.autocommit = True
    return conn


def _wal_logical() -> bool:
    if not _reachable("127.0.0.1", 5432):
        return False
    try:
        conn = _pg_connect()
        try:
            with conn.cursor() as cur:
                cur.execute("SHOW wal_level")
                row = cur.fetchone()
                return bool(row) and str(row[0]) == "logical"
        finally:
            conn.close()
    except Exception:
        return False


def _pg_exec(sql: str, params: tuple | None = None) -> None:
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
    finally:
        conn.close()


def _pg_fetch(sql: str, params: tuple | None = None) -> list:
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            return list(cur.fetchall())
    finally:
        conn.close()


def _pg_count(table: str) -> int:
    rows = _pg_fetch(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = 'public' AND table_name = %s",
        (table,),
    )
    if not rows or int(rows[0][0]) == 0:
        return 0
    return int(_pg_fetch(f'SELECT COUNT(*) FROM public."{table}"')[0][0])


def _pg_ids(table: str) -> list[int]:
    return [int(r[0]) for r in _pg_fetch(f'SELECT id FROM public."{table}" ORDER BY id')]


def _pg_drop(*tables: str) -> None:
    for table in tables:
        _pg_exec(f'DROP TABLE IF EXISTS public."{table}" CASCADE')


def _pg_drop_slot(slot: str) -> None:
    if not slot:
        return
    try:
        _pg_exec(
            "SELECT pg_drop_replication_slot(%s) WHERE EXISTS "
            "(SELECT 1 FROM pg_replication_slots WHERE slot_name = %s)",
            (slot, slot),
        )
    except Exception:
        pass


def _pg_ep(table: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database",
        format="postgresql",
        schema="public",
        table=table,
        ssl=False,
        **PG,
    )


def _maps() -> list[dict]:
    return [
        {
            "source": "id",
            "target": "id",
            "source_type": "INTEGER",
            "target_type": "INTEGER",
            "confidence": 0.99,
        },
        {
            "source": "label",
            "target": "label",
            "source_type": "TEXT",
            "target_type": "TEXT",
            "confidence": 0.99,
        },
    ]


def _run(req: TransferRequest):
    return UniversalTransferEngine().execute_tracked(req, "g8-" + uuid.uuid4().hex[:16])


# --------------------------------------------------------------------------- leftover overwrite Gate-8


def test_sqlite_overwrite_leftover_gate8_dest_count(tmp_path: Path) -> None:
    src_t = "lo_src_" + uuid.uuid4().hex[:8]
    dst_t = "lo_dst_" + uuid.uuid4().hex[:8]
    src = tmp_path / "lo_src.db"
    dst = tmp_path / "lo_dst.db"
    for path, table, rows in (
        (src, src_t, [(1, "a"), (2, "b"), (3, "c")]),
        (dst, dst_t, [(1, "old"), (2, "old"), (3, "old"), (99, "ghost")]),
    ):
        con = sqlite3.connect(str(path))
        try:
            con.execute(f'CREATE TABLE "{table}" (id INTEGER PRIMARY KEY, label TEXT)')
            con.executemany(f'INSERT INTO "{table}" VALUES (?, ?)', rows)
            con.commit()
        finally:
            con.close()
    result = _run(
        TransferRequest(
            source=EndpointConfig(
                kind="database", format="sqlite", database=str(src), table=src_t
            ),
            destination=EndpointConfig(
                kind="database", format="sqlite", database=str(dst), table=dst_t
            ),
            mappings=_maps(),
            sync_mode="full_refresh_overwrite",
            validation_mode="strict",
            skip_preflight=True,
            stream_contracts=[
                {
                    "name": src_t,
                    "selected": True,
                    "sync_mode": "full_refresh_overwrite",
                    "primary_key": "id",
                    "mappings": _maps(),
                }
            ],
        )
    )
    assert result.success, result.error
    con = sqlite3.connect(str(dst))
    try:
        dest_count = int(con.execute(f'SELECT COUNT(*) FROM "{dst_t}"').fetchone()[0])
        ids = [int(r[0]) for r in con.execute(f'SELECT id FROM "{dst_t}" ORDER BY id')]
    finally:
        con.close()
    recon = result.reconciliation or {}
    assert dest_count == 3
    assert ids == [1, 2, 3]
    assert recon.get("passed") is True, recon
    assert recon.get("target_rows") == 3 or (result.row_accounting or {}).get("dest_count") == 3


@pytest.mark.skipif(not _reachable("127.0.0.1", 5432), reason="Postgres not reachable")
def test_live_pg_overwrite_leftover_gate8_dest_count() -> None:
    src_t, dst_t = "g8s_" + uuid.uuid4().hex[:8], "g8d_" + uuid.uuid4().hex[:8]
    try:
        _pg_drop(src_t, dst_t)
        _pg_exec(
            f'CREATE TABLE public."{src_t}" (id INT PRIMARY KEY, label TEXT)'
        )
        _pg_exec(
            f'CREATE TABLE public."{dst_t}" (id INT PRIMARY KEY, label TEXT)'
        )
        _pg_exec(f"""INSERT INTO public."{src_t}" VALUES (1,'a'),(2,'b'),(3,'c')""")
        _pg_exec(
            f"""INSERT INTO public."{dst_t}" VALUES (1,'old'),(2,'old'),(3,'old'),(99,'ghost')"""
        )
        result = _run(
            TransferRequest(
                source=_pg_ep(src_t),
                destination=_pg_ep(dst_t),
                mappings=_maps(),
                sync_mode="full_refresh_overwrite",
                validation_mode="strict",
                skip_preflight=True,
                stream_contracts=[
                    {
                        "name": src_t,
                        "selected": True,
                        "sync_mode": "full_refresh_overwrite",
                        "primary_key": "id",
                        "mappings": _maps(),
                    }
                ],
            )
        )
        assert result.success, result.error
        assert _pg_count(dst_t) == 3
        assert _pg_ids(dst_t) == [1, 2, 3]
        recon = result.reconciliation or {}
        assert recon.get("passed") is True, recon
        # Dest COUNT is the leftover identity. Some overwrite paths replace the
        # table (leftover_deleted unset) instead of MERGE-DELETE; ghost 99 gone
        # is the proof either way.
    finally:
        _pg_drop(src_t, dst_t)


# --------------------------------------------------------------------------- live PG logical CDC through execute_tracked (Gate-8)


def _cdc_contract(name: str) -> list[dict]:
    return [
        {
            "name": name,
            "selected": True,
            "snapshot_mode": "initial",
            "primary_key": "id",
            "sync_mode": "cdc",
            "mappings": _maps(),
        }
    ]


@pytest.mark.skipif(not _wal_logical(), reason="PostgreSQL wal_level=logical not reachable")
def test_live_pg_logical_cdc_execute_tracked_dest_count_and_gate8() -> None:
    src_t, dst_t = "cdc_s_" + uuid.uuid4().hex[:8], "cdc_d_" + uuid.uuid4().hex[:8]
    slot = ""
    try:
        _pg_drop(src_t, dst_t)
        _pg_exec(
            f'CREATE TABLE public."{src_t}" (id INT PRIMARY KEY, label TEXT)'
        )
        _pg_exec(f"""INSERT INTO public."{src_t}" VALUES (1,'a'),(2,'b')""")
        req_kw = dict(
            source=_pg_ep(src_t),
            destination=_pg_ep(dst_t),
            mappings=_maps(),
            sync_mode="cdc",
            validation_mode="strict",
            skip_preflight=True,
            stream_contracts=_cdc_contract(src_t),
            delivery_guarantee="at_least_once",
        )
        snap = _run(TransferRequest(**req_kw, limit=2))
        slot = str((snap.destination_summary or {}).get("cdc_slot_name") or "")
        assert snap.success, snap.error or snap.reconciliation
        assert _pg_count(dst_t) == 2, _pg_ids(dst_t)
        recon1 = snap.reconciliation or {}
        assert recon1.get("passed") is True, recon1
        assert recon1.get("source_rows") == 2, recon1
        summary1 = snap.destination_summary or {}
        assert summary1.get("source_row_count") == 2, summary1.get("source_row_count")
        assert summary1.get("checksum_mode") == "cdc_source_image"
        plugin = str(summary1.get("cdc_plugin") or (summary1.get("cdc") or {}).get("cdc_plugin") or "")
        assert plugin in {"pgoutput", "test_decoding", ""}, plugin

        _pg_exec(f"""INSERT INTO public."{src_t}" VALUES (3,'c')""")
        _pg_exec(f"""UPDATE public."{src_t}" SET label = 'aa' WHERE id = 1""")
        _pg_exec(f"""DELETE FROM public."{src_t}" WHERE id = 2""")
        stream = _run(TransferRequest(**req_kw))
        slot = slot or str((stream.destination_summary or {}).get("cdc_slot_name") or "")
        assert stream.success, stream.error or stream.reconciliation
        dest_ids = _pg_ids(dst_t)
        assert dest_ids == [1, 3], dest_ids
        assert _pg_count(dst_t) == 2
        labels = {
            int(r[0]): str(r[1])
            for r in _pg_fetch(f'SELECT id, label FROM public."{dst_t}"')
        }
        assert labels.get(1) == "aa"
        recon2 = stream.reconciliation or {}
        assert recon2.get("passed") is True, recon2
        assert recon2.get("source_rows") == 2, recon2
        assert (stream.destination_summary or {}).get("leftover_deleted") in {None, 0}

        replay = _run(TransferRequest(**req_kw))
        assert replay.success, replay.error
        assert _pg_ids(dst_t) == [1, 3]
        assert _pg_count(dst_t) == 2
        cdc = replay.destination_summary or {}
        assert str(cdc.get("cdc_delivery") or "").startswith("at-least-once") or (
            (cdc.get("cdc") or {}).get("cdc_delivery") in {None, "at-least-once"}
        )
        assert cdc.get("exactly_once_claimed_platform") in {None, False}
    finally:
        _pg_drop_slot(slot)
        _pg_drop(src_t, dst_t)


@pytest.mark.skipif(not _wal_logical(), reason="PostgreSQL wal_level=logical not reachable")
def test_live_pg_cdc_leftover_dest_key_is_not_merge_deleted() -> None:
    src_t, dst_t = "cdc_g_" + uuid.uuid4().hex[:8], "cdc_h_" + uuid.uuid4().hex[:8]
    slot = ""
    try:
        _pg_drop(src_t, dst_t)
        _pg_exec(
            f'CREATE TABLE public."{src_t}" (id INT PRIMARY KEY, label TEXT)'
        )
        _pg_exec(f"""INSERT INTO public."{src_t}" VALUES (1,'a'),(2,'b')""")
        req = TransferRequest(
            source=_pg_ep(src_t),
            destination=_pg_ep(dst_t),
            mappings=_maps(),
            sync_mode="cdc",
            validation_mode="strict",
            skip_preflight=True,
            stream_contracts=_cdc_contract(src_t),
            delivery_guarantee="at_least_once",
            limit=2,
        )
        first = _run(req)
        slot = str((first.destination_summary or {}).get("cdc_slot_name") or "")
        assert first.success, first.error or first.reconciliation
        assert _pg_ids(dst_t) == [1, 2]
        _pg_exec(f"""INSERT INTO public."{dst_t}" (id, label) VALUES (99, 'ghost')""")
        second = _run(
            TransferRequest(
                source=_pg_ep(src_t),
                destination=_pg_ep(dst_t),
                mappings=_maps(),
                sync_mode="cdc",
                validation_mode="strict",
                skip_preflight=True,
                stream_contracts=_cdc_contract(src_t),
                delivery_guarantee="at_least_once",
            )
        )
        slot = slot or str((second.destination_summary or {}).get("cdc_slot_name") or "")
        assert second.success, second.error or second.reconciliation
        ids = _pg_ids(dst_t)
        assert 99 in ids, ids
        assert _pg_count(dst_t) == 3
        assert (second.destination_summary or {}).get("leftover_deleted") in {None, 0}
        recon = second.reconciliation or {}
        assert recon.get("passed") is True, recon
        assert recon.get("source_rows") == 2
        assert recon.get("target_rows") == 3
    finally:
        _pg_drop_slot(slot)
        _pg_drop(src_t, dst_t)


def test_cdc_source_image_count_scope_does_not_claim_full_checksum() -> None:
    from services.reconciliation import reconcile
    from services.reconcile_coverage import CDC_SOURCE_IMAGE_COUNT

    report = reconcile(
        source_rows=2,
        target_rows=2,
        source_checksum="last-batch",
        target_checksum="full-dest",
        checksum_scope=CDC_SOURCE_IMAGE_COUNT,
    )
    assert report.passed is True
    assert report.assurance_level == CDC_SOURCE_IMAGE_COUNT
    assert report.population_proof is False
    assert report.checksum_match is False

    mismatch = reconcile(
        source_rows=2,
        target_rows=1,
        source_checksum="x",
        target_checksum="y",
        checksum_scope=CDC_SOURCE_IMAGE_COUNT,
    )
    assert mismatch.passed is False


def test_stamp_cdc_source_image_sqlite(tmp_path: Path) -> None:
    from src.transfer.cdc_transfer import _stamp_cdc_source_image

    table = "img_" + uuid.uuid4().hex[:8]
    db = tmp_path / "img.db"
    con = sqlite3.connect(str(db))
    try:
        con.execute(f'CREATE TABLE "{table}" (id INTEGER PRIMARY KEY, label TEXT)')
        con.executemany(f'INSERT INTO "{table}" VALUES (?, ?)', [(1, "a"), (2, "b")])
        con.commit()
    finally:
        con.close()
    summary = _stamp_cdc_source_image(
        {},
        src_type="sqlite",
        src_cfg={"database": str(db), "type": "sqlite"},
        schema="",
        table_name=table,
        events=5,
    )
    assert summary["source_row_count"] == 2
    assert summary["cdc_events_applied"] == 5
    assert summary["checksum_mode"] == "cdc_source_image"
    assert summary["source_row_count_source"] == "cdc_source_image_count"


def test_mysql_binlog_cdc_skipped_when_closed() -> None:
    if _reachable("127.0.0.1", 3306):
        pytest.skip(
            "MySQL :3306 is open — binlog dest COUNT belongs on the MySQL CDC e2e, "
            "not this Postgres-only Gate-8 file"
        )
    pytest.skip("MySQL :3306 closed — no live binlog CDC dest COUNT in this VM")
