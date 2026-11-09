"""QA1 (#372 audit): a quarantined upsert key failed Gate-8; Validate missed VARCHAR overflow.

1. MySQL -> PostgreSQL upsert, 7/10 names overflow the live VARCHAR(20) and are
   quarantined. Gate-8's keyed read-back still compared those keys: the row
   for key 1 is the one stored before the run ('old1'), so the run failed
   "row 1 column name->name: source='customer-long-…' dest='old1'". The
   finding's ``value`` is masked/truncated for display, so the (column,
   value) hold-out never matched it; the key it names does.
2. Validate's cell preview never checked the mapped VARCHAR(n), so 10/10 read
   OK and Execute quarantined the 44-char values. The preview now uses the
   writer's own reason (``varchar_overflow_reason``).
Live legs run when MySQL :3306 and PostgreSQL :5432 are up.
"""

from __future__ import annotations

import os
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")
os.environ.setdefault("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128")

from services.reconciliation import sample_compare_rows  # noqa: E402
from services.transform_engine import preview_quarantine_cells  # noqa: E402

MAPS = [{"source": c, "target": c} for c in ("id", "name", "qty")]


def test_held_out_key_is_not_compared_against_the_row_stored_before_the_run():
    source = [{"id": 1, "name": "x" * 40, "qty": 10}, {"id": 8, "name": "short8", "qty": 80}]
    stored = [{"id": 1, "name": "old1", "qty": 1}, {"id": 8, "name": "short8", "qty": 80}]
    out = sample_compare_rows(source, stored, MAPS, sort_key="id", held_out_keys={"1"})
    assert out["passed"] is True, out["mismatches"]
    assert out["rows_excluded_quarantined"] == 1 and out["rows_compared"] == 1


def test_held_out_key_never_hides_a_landed_row_that_differs():
    source = [{"id": 8, "name": "short8", "qty": 80}]
    stored = [{"id": 8, "name": "WRONG", "qty": 80}]
    out = sample_compare_rows(source, stored, MAPS, sort_key="id", held_out_keys={"1"})
    assert out["passed"] is False and out["mismatches"]


def test_held_out_keys_come_from_value_refusals_not_row_refusals():
    from src.transfer.reconcile_step import _held_out_keys

    summary = {"rejected_details": [
        {"column": "name", "value": "custom…xxxx", "source_pk": "1",
         "reason": "value length 40 exceeds PostgreSQL VARCHAR(20) — quarantined (would truncate on write)"},
        {"column": "id", "value": "8", "source_pk": "8", "reason": "duplicate key violates unique constraint"},
    ]}
    assert _held_out_keys(summary, "id") == {"1"}


def test_validate_preview_predicts_the_writers_varchar_overflow():
    from connectors.writer_quarantine_fit import varchar_overflow_reason

    long = "customer-long-name-number-01-" + "x" * 15  # 44 chars
    out = preview_quarantine_cells(
        headers=["id", "name"],
        sample_rows=[["1", long], ["2", "short"]],
        mappings=[{"source": "id", "target": "id"},
                  {"source": "name", "target": "name", "target_type": "VARCHAR(20)"}],
        dest_db="postgresql",
    )
    assert out["quarantine_count"] == 1, out
    cell = out["cells"][0]
    assert cell["row"] == 0 and cell["status"] == "quarantine"
    assert cell["message"] == varchar_overflow_reason(
        long, "VARCHAR(20)", dialect_label="PostgreSQL VARCHAR"
    )
    assert "exceeds PostgreSQL VARCHAR(20)" in cell["message"]


def test_preview_does_not_flag_values_that_fit_or_unbounded_carriers():
    out = preview_quarantine_cells(
        headers=["a", "b"],
        sample_rows=[["x" * 20, "y" * 500]],
        mappings=[{"source": "a", "target": "a", "target_type": "VARCHAR(20)"},
                  {"source": "b", "target": "b", "target_type": "TEXT"}],
        dest_db="postgresql",
    )
    assert out["quarantine_count"] == 0, out


def _up() -> bool:
    try:
        import psycopg2
        import pymysql

        pymysql.connect(host="127.0.0.1", user="dataflow", password="dataflow",
                        database="dataflow", connect_timeout=2).close()
        psycopg2.connect(host="127.0.0.1", dbname="dataflow", user="dataflow",
                         password="dataflow", connect_timeout=2).close()
        return True
    except Exception:  # noqa: BLE001 - either driver's connect error means skip
        return False


@pytest.mark.parametrize("mode", ["incremental_upsert", "incremental_deduped"])
def test_live_mysql_to_pg_upsert_with_quarantined_keys_verifies(mode):
    if not _up():
        pytest.skip("MySQL :3306 / PostgreSQL :5432 not reachable")
    import psycopg2
    import pymysql

    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    t = "qa1_" + uuid.uuid4().hex[:6]
    my = pymysql.connect(host="127.0.0.1", user="dataflow", password="dataflow",
                         database="dataflow", autocommit=True)
    pg = psycopg2.connect(host="127.0.0.1", dbname="dataflow", user="dataflow", password="dataflow")
    pg.autocommit = True
    try:
        with my.cursor() as c:
            c.execute(f"CREATE TABLE {t} (id INT PRIMARY KEY, name VARCHAR(100), qty INT)")
            for i in range(1, 11):
                name = f"customer-long-name-number-{i:02d}-" + "x" * (10 + i) if i <= 7 else f"short{i}"
                c.execute(f"INSERT INTO {t} VALUES (%s,%s,%s)", (i, name, i * 10))
        with pg.cursor() as c:
            c.execute(f"CREATE TABLE {t} (id INT PRIMARY KEY, name VARCHAR(20), qty INT)")
            c.execute(f"INSERT INTO {t} VALUES (1,'old1',1),(2,'old2',2),(3,'old3',3)")

        def ep(fmt: str, port: int, schema: str = "") -> EndpointConfig:
            return EndpointConfig(kind="database", format=fmt, host="127.0.0.1", port=port,
                                  database="dataflow", username="dataflow",
                                  password="dataflow", schema=schema, table=t)

        r = UniversalTransferEngine().execute(TransferRequest(
            source=ep("mysql", 3306), destination=ep("postgresql", 5432, "public"),
            sync_mode=mode, skip_preflight=True, validation_mode="balanced",
            stream_contracts=[{"primary_key": "id"}], mappings=MAPS,
        ))
        with pg.cursor() as c:
            c.execute(f"SELECT id, name FROM {t} ORDER BY id")
            rows = c.fetchall()
        ds = r.destination_summary or {}
        assert r.success, r.error
        assert int(ds.get("rejected_rows") or 0) == 7
        assert rows == [(1, "old1"), (2, "old2"), (3, "old3"),
                        (8, "short8"), (9, "short9"), (10, "short10")]
    finally:
        with my.cursor() as c:
            c.execute(f"DROP TABLE IF EXISTS {t}")
        with pg.cursor() as c:
            c.execute(f"DROP TABLE IF EXISTS {t}")
            c.execute(f"DROP TABLE IF EXISTS {t}_df_quarantine")
        my.close()
        pg.close()


def test_coerced_null_row_landed_so_its_key_is_still_compared():
    from src.transfer.reconcile_step import _held_out_keys

    summary = {"rejected_details": [
        {"source_pk": 1, "column": "amt", "reason": "value too long", "disposition": "coerced_null"},
        {"source_pk": 2, "column": "amt", "reason": "value too long"},
    ]}
    assert _held_out_keys(summary, "id") == {"2"}
