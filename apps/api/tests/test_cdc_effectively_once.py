"""_df_lsn PK-sink LSN-guarded idempotent upsert proofs (not platform exactly-once)."""

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


def test_honesty_dict_explicit_delivery_classes() -> None:
    """Platform must classify Exactly Once / At Least Once / At Most Once — never invent."""
    h = honesty_dict()
    classes = h["delivery_classes"]
    assert set(classes.keys()) == {"exactly_once", "at_least_once", "at_most_once"}
    assert classes["exactly_once"]["claimed"] is False
    assert classes["exactly_once"]["available"] is True
    assert classes["exactly_once"]["opt_in"] is True
    assert classes["at_least_once"]["claimed"] is True
    assert classes["at_least_once"]["default"] is True
    assert classes["at_most_once"]["claimed"] is False
    assert classes["at_most_once"]["available"] is False
    # Silent loss is not a migration-assurance posture.
    assert "loss" in classes["at_most_once"]["note"].lower() or "not offered" in classes[
        "at_most_once"
    ]["note"].lower()
    assert h["append_sink_acknowledgement_required"] is True
    assert "append_only_redelivery_duplicates_rows" in h["duplicate_scenarios"]


def test_classify_sink_stamps_delivery_class_not_exactly_once() -> None:
    from services.cdc_effectively_once import classify_sink_delivery

    pg = classify_sink_delivery(
        dest_type="postgresql", has_primary_key=True, write_mode="upsert"
    )
    assert pg["delivery_class"] == "at_least_once"
    assert pg["exactly_once"] is False
    assert pg["at_most_once"] is False
    assert pg["at_least_once"] is True

    csv = classify_sink_delivery(
        dest_type="csv", has_primary_key=True, write_mode="upsert"
    )
    assert csv["delivery_class"] == "at_least_once"
    assert csv["duplicates_on_redelivery"] is True
    assert csv["duplicate_scenario"] == "append_only_redelivery_duplicates_rows"
    assert csv["exactly_once"] is False


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
    # SQLAlchemy generic path now runs sparse CDC + `_df_lsn` guards.
    generic = classify_sink_delivery(
        dest_type="generic_sql", has_primary_key=True, write_mode="upsert"
    )
    assert generic["class"] == SINK_EFFECTIVELY_ONCE_ELIGIBLE
    gate_cdc_destination(dest_type="generic_sql", has_primary_key=True)
    # SQL Server MERGE is the upsert path — CDC must treat it as PK-eligible.
    mssql = classify_sink_delivery(
        dest_type="sqlserver", has_primary_key=True, write_mode="upsert"
    )
    assert mssql["class"] == SINK_EFFECTIVELY_ONCE_ELIGIBLE
    gate_cdc_destination(dest_type="sqlserver", has_primary_key=True)


def test_vector_pk_upsert_is_at_least_once_not_exactly_once() -> None:
    from services.cdc_effectively_once import (
        CdcAppendOnlySinkError,
        SINK_PK_UPSERT_AT_LEAST_ONCE,
        classify_sink_delivery,
        gate_cdc_destination,
    )

    for dest in ("qdrant", "milvus", "weaviate", "pinecone"):
        posture = classify_sink_delivery(
            dest_type=dest, has_primary_key=True, write_mode="upsert"
        )
        assert posture["class"] == SINK_PK_UPSERT_AT_LEAST_ONCE, dest
        assert posture["exactly_once"] is False
        assert posture["delivery_class"] == "at_least_once"
        assert posture["duplicates_on_redelivery"] is False
        assert posture["effectively_once_pk_sink"] is False
        allowed = gate_cdc_destination(
            dest_type=dest, has_primary_key=True, write_mode="upsert"
        )
        assert allowed["class"] == SINK_PK_UPSERT_AT_LEAST_ONCE
        try:
            gate_cdc_destination(
                dest_type=dest,
                has_primary_key=True,
                write_mode="upsert",
                require_effectively_once=True,
            )
            raise AssertionError("expected CdcAppendOnlySinkError")
        except CdcAppendOnlySinkError as exc:
            assert "exactly-once" in str(exc).lower() or "least-once" in str(exc).lower()


def test_should_apply_rejects_stale_lsn() -> None:
    ok = should_apply_pk_row(existing_lsn="0/200", incoming_lsn="0/100")
    assert ok.applied is False
    assert ok.reason == "stale_lsn_rejected"

    newer = should_apply_pk_row(existing_lsn="0/100", incoming_lsn="0/200")
    assert newer.applied is True
    assert newer.reason == "newer_lsn"

    # Equal LSN matches writers (filter_stale / SQL >) — skip, do not rewrite.
    equal = should_apply_pk_row(existing_lsn="0/200", incoming_lsn="0/200")
    assert equal.applied is False
    assert equal.reason == "equal_lsn_skipped"


def test_chaos_redeliver_older_then_newer_holds_state() -> None:
    sink = chaos_redeliver_older_then_newer("42")
    assert sink.rejected_stale >= 1
    row = sink.rows["42"]
    # Equal redelivery of 0/200 with "new-again" is skipped — payload stays "new".
    assert row["v"] == "new"
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
