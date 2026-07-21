"""_df_lsn effectively-once proofs for PK sinks (not platform exactly-once)."""

from __future__ import annotations

import socket
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.writer_common import (  # noqa: E402
    DF_LSN_COL,
    compare_lsn,
    dedupe_rows_by_pk_and_lsn,
    postgres_lsn_update_guard_sql,
)
from services.cdc_effectively_once import (  # noqa: E402
    PkSinkState,
    chaos_redeliver_older_then_newer,
    honesty_dict,
    should_apply_pk_row,
)


def test_honesty_dict_refuses_exactly_once_claim() -> None:
    h = honesty_dict()
    assert h["exactly_once_claimed"] is False
    assert h["delivery_default"] == "at-least-once"
    assert h["effectively_once_pk_sinks"] is True
    assert h["append_only_sinks_effectively_once"] is False


def test_classify_and_gate_append_only_sink() -> None:
    from services.cdc_effectively_once import (
        CdcAppendOnlySinkError,
        SINK_APPEND_ONLY,
        SINK_EFFECTIVELY_ONCE_ELIGIBLE,
        classify_sink_delivery,
        gate_cdc_destination,
    )

    pg = classify_sink_delivery(
        dest_type="postgresql", has_primary_key=True, write_mode="upsert"
    )
    assert pg["class"] == SINK_EFFECTIVELY_ONCE_ELIGIBLE
    assert pg["exactly_once"] is False

    csv = classify_sink_delivery(
        dest_type="csv", has_primary_key=True, write_mode="upsert"
    )
    assert csv["class"] == SINK_APPEND_ONLY
    assert csv["duplicates_on_redelivery"] is True

    try:
        gate_cdc_destination(dest_type="csv", has_primary_key=True)
        raise AssertionError("expected CdcAppendOnlySinkError")
    except CdcAppendOnlySinkError as exc:
        assert "allow_append_only" in str(exc)

    allowed = gate_cdc_destination(
        dest_type="csv", has_primary_key=True, allow_append_only=True
    )
    assert allowed["class"] == SINK_APPEND_ONLY

    gate_cdc_destination(dest_type="postgresql", has_primary_key=True)


def test_should_apply_rejects_stale_lsn() -> None:
    ok = should_apply_pk_row(existing_lsn="0/200", incoming_lsn="0/100")
    assert ok.applied is False
    assert ok.reason == "stale_lsn_rejected"

    newer = should_apply_pk_row(existing_lsn="0/100", incoming_lsn="0/200")
    assert newer.applied is True


def test_chaos_redeliver_older_then_newer_holds_state() -> None:
    sink = chaos_redeliver_older_then_newer("42")
    assert sink.rejected_stale >= 1
    row = sink.rows["42"]
    assert row["v"] in ("new", "new-again")
    assert compare_lsn(row[DF_LSN_COL], "0/200") == 0


def test_dedupe_batch_keeps_highest_lsn_per_pk() -> None:
    cols = ["id", "v", DF_LSN_COL]
    rows = [
        ("1", "old", "0/100"),
        ("1", "new", "0/300"),
        ("1", "mid", "0/200"),
        ("2", "a", "0/50"),
    ]
    out = dedupe_rows_by_pk_and_lsn(rows, ["id"], cols)
    by_id = {r[0]: r for r in out}
    assert by_id["1"][1] == "new"
    assert by_id["1"][2] == "0/300"
    assert by_id["2"][1] == "a"


def test_mixed_token_compare_is_stable_text() -> None:
    # MySQL file:pos / GTID stamps sort as opaque text — still monotonic within kind.
    assert compare_lsn("mysql-bin.000003:154", "mysql-bin.000003:100") == 1
    assert should_apply_pk_row(
        existing_lsn="mysql-bin.000003:154",
        incoming_lsn="mysql-bin.000003:100",
    ).applied is False


def _pg_ready() -> bool:
    try:
        with socket.create_connection(("localhost", 5432), timeout=1):
            pass
    except OSError:
        return False
    try:
        from connectors.postgresql_conn import get_connection

        with get_connection(
            host="localhost",
            port=5432,
            database="dataflow",
            username="dataflow",
            password="dataflow",
            connection_string="",
            ssl=False,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                return bool(cur.fetchone())
    except Exception:
        return False


@pytest.mark.skipif(not _pg_ready(), reason="PostgreSQL not reachable on localhost:5432")
def test_pg_upsert_guard_rejects_stale_df_lsn_live():
    """Real PG ON CONFLICT + postgres_lsn_update_guard_sql — stale _df_lsn must not win."""
    from connectors.postgresql_conn import get_connection
    from psycopg2 import sql

    table = f"cdc_eo_{uuid.uuid4().hex[:8]}"
    with get_connection(
        host="localhost",
        port=5432,
        database="dataflow",
        username="dataflow",
        password="dataflow",
        connection_string="",
        ssl=False,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    "CREATE TABLE {} (id INT PRIMARY KEY, v TEXT, {} TEXT)"
                ).format(sql.Identifier(table), sql.Identifier(DF_LSN_COL))
            )
            conn.commit()
            try:
                guard = postgres_lsn_update_guard_sql(table)
                insert_sql = sql.SQL(
                    "INSERT INTO {} (id, v, {}) VALUES (%s, %s, %s) "
                    "ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v, {} = EXCLUDED.{} "
                    "WHERE {}"
                ).format(
                    sql.Identifier(table),
                    sql.Identifier(DF_LSN_COL),
                    sql.Identifier(DF_LSN_COL),
                    sql.Identifier(DF_LSN_COL),
                    sql.SQL(guard),
                )
                cur.execute(insert_sql, (1, "first", "0/100"))
                cur.execute(insert_sql, (1, "new", "0/200"))
                cur.execute(insert_sql, (1, "stale", "0/100"))
                conn.commit()
                cur.execute(
                    sql.SQL("SELECT v, {} FROM {} WHERE id = 1").format(
                        sql.Identifier(DF_LSN_COL), sql.Identifier(table)
                    )
                )
                v, lsn = cur.fetchone()
                assert v == "new", v
                assert str(lsn) == "0/200", lsn
            finally:
                cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(table)))
                conn.commit()


def test_in_memory_sink_chaos_sequence() -> None:
    sink = PkSinkState()
    assert sink.upsert("1", {"id": "1", "v": "a", DF_LSN_COL: "0/10"}).applied
    assert sink.upsert("1", {"id": "1", "v": "b", DF_LSN_COL: "0/20"}).applied
    assert not sink.upsert("1", {"id": "1", "v": "a", DF_LSN_COL: "0/10"}).applied
    assert sink.rows["1"]["v"] == "b"
    assert sink.rejected_stale == 1
