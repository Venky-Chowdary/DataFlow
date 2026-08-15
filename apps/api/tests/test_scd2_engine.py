"""Unit tests for the SCD2 history engine."""

from __future__ import annotations

import sqlite3
import sys
import tempfile
from pathlib import Path

import pytest


_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.services.scd2_engine import (
    IS_CURRENT_COLUMN,
    VALID_FROM_COLUMN,
    VALID_TO_COLUMN,
    apply_scd2,
)


def _sqlite_endpoint(path: Path, table: str = "products"):
    from src.transfer.models import EndpointConfig

    return EndpointConfig(
        kind="database",
        format="sqlite",
        connection_string=f"sqlite:///{path}",
        database=str(path),
        table=table,
    )


def _records():
    return [
        {"id": "1", "name": "A", "price": "10.00"},
        {"id": "2", "name": "B", "price": "20.00"},
    ]


def test_scd2_initial_load_creates_history_table():
    fd, db_path = tempfile.mkstemp(suffix=".db")
    try:
        endpoint = _sqlite_endpoint(Path(db_path))
        summary = apply_scd2(
            endpoint,
            _records(),
            columns=["id", "name", "price"],
            schema={"id": "string", "name": "string", "price": "decimal"},
            mappings=None,
            conflict_columns=["id"],
        )
        assert summary["rows_written"] == 2
        assert summary["active_rows"] == 2
        assert summary["updated_rows"] == 0
        assert summary["active_checksum"]

        conn = sqlite3.connect(db_path)
        cur = conn.execute(f"SELECT * FROM products WHERE {IS_CURRENT_COLUMN} = 1")
        rows = cur.fetchall()
        assert len(rows) == 2
        conn.close()
    finally:
        try:
            Path(db_path).unlink(missing_ok=True)
        except PermissionError:
            pass


def test_scd2_update_closes_old_version_and_inserts_new():
    fd, db_path = tempfile.mkstemp(suffix=".db")
    try:
        endpoint = _sqlite_endpoint(Path(db_path))
        apply_scd2(
            endpoint,
            _records(),
            columns=["id", "name", "price"],
            schema={"id": "string", "name": "string", "price": "decimal"},
            mappings=None,
            conflict_columns=["id"],
        )

        changed = [
            {"id": "1", "name": "A-updated", "price": "10.00"},
            {"id": "2", "name": "B", "price": "20.00"},
        ]
        summary = apply_scd2(
            endpoint,
            changed,
            columns=["id", "name", "price"],
            schema={"id": "string", "name": "string", "price": "decimal"},
            mappings=None,
            conflict_columns=["id"],
        )
        assert summary["rows_written"] == 1
        assert summary["updated_rows"] == 1
        assert summary["active_rows"] == 2

        conn = sqlite3.connect(db_path)
        cur = conn.execute(f"SELECT id, name, {IS_CURRENT_COLUMN}, {VALID_TO_COLUMN} FROM products ORDER BY id, {VALID_FROM_COLUMN}")
        rows = cur.fetchall()
        assert len(rows) == 3
        # Two current rows: id 1 updated and id 2 unchanged.
        assert sum(1 for r in rows if r[2] == 1) == 2
        # One historical row for id 1 with valid_to set.
        historical = [r for r in rows if r[0] == "1" and r[2] == 0]
        assert len(historical) == 1
        assert historical[0][3] is not None
        conn.close()
    finally:
        try:
            Path(db_path).unlink(missing_ok=True)
        except PermissionError:
            pass


def test_scd2_reidentical_snapshot_is_idempotent():
    fd, db_path = tempfile.mkstemp(suffix=".db")
    try:
        endpoint = _sqlite_endpoint(Path(db_path))
        apply_scd2(
            endpoint,
            _records(),
            columns=["id", "name", "price"],
            schema={"id": "string", "name": "string", "price": "decimal"},
            mappings=None,
            conflict_columns=["id"],
        )
        summary = apply_scd2(
            endpoint,
            _records(),
            columns=["id", "name", "price"],
            schema={"id": "string", "name": "string", "price": "decimal"},
            mappings=None,
            conflict_columns=["id"],
        )
        assert summary["rows_written"] == 0
        assert summary["updated_rows"] == 0
        assert summary["active_rows"] == 2
    finally:
        try:
            Path(db_path).unlink(missing_ok=True)
        except PermissionError:
            pass


def test_scd2_composite_primary_key():
    fd, db_path = tempfile.mkstemp(suffix=".db")
    try:
        endpoint = _sqlite_endpoint(Path(db_path), table="line_items")
        rows = [
            {"order_id": "o1", "line": "1", "sku": "A"},
            {"order_id": "o1", "line": "2", "sku": "B"},
        ]
        summary = apply_scd2(
            endpoint,
            rows,
            columns=["order_id", "line", "sku"],
            schema={"order_id": "string", "line": "string", "sku": "string"},
            mappings=None,
            conflict_columns=["order_id", "line"],
        )
        assert summary["rows_written"] == 2
        assert summary["primary_key_columns"] == ["order_id", "line"]

        updated = [
            {"order_id": "o1", "line": "1", "sku": "A2"},
            {"order_id": "o1", "line": "2", "sku": "B"},
        ]
        summary2 = apply_scd2(
            endpoint,
            updated,
            columns=["order_id", "line", "sku"],
            schema={"order_id": "string", "line": "string", "sku": "string"},
            mappings=None,
            conflict_columns=["order_id", "line"],
        )
        assert summary2["rows_written"] == 1
        assert summary2["updated_rows"] == 1
        assert summary2["active_rows"] == 2
    finally:
        try:
            Path(db_path).unlink(missing_ok=True)
        except PermissionError:
            pass


def test_scd2_quarantines_bad_cells_without_silent_drop():
    """Transform failures must surface in rejected_details — not vanish from history."""
    from services.migration_risk_contract import create_migration_risk_contract

    fd, db_path = tempfile.mkstemp(suffix=".db")
    try:
        endpoint = _sqlite_endpoint(Path(db_path))
        c = create_migration_risk_contract(
            column="price",
            source_type="TEXT",
            destination_type="DECIMAL",
            approved_by="admin@dataflow.app",
            reason="SCD2 cast-fail holdout",
            execution_policy="CAST_AND_CONTINUE",
        )
        summary = apply_scd2(
            endpoint,
            [
                {"id": "1", "name": "A", "price": "10.00"},
                {"id": "2", "name": "B", "price": "not-a-number"},
            ],
            columns=["id", "name", "price"],
            schema={"id": "string", "name": "string", "price": "decimal"},
            mappings=[
                {"source": "id", "target": "id"},
                {"source": "name", "target": "name"},
                {
                    "source": "price",
                    "target": "price",
                    "transform": "decimal",
                    "target_type": "decimal",
                    "risk_contract": c.to_dict(),
                },
            ],
            conflict_columns=["id"],
        )
        assert summary.get("ok") is not False
        assert int(summary.get("rejected_rows") or 0) >= 1
        assert summary.get("rejected_details")
        # Good row still lands; bad row held out of SCD2 merge.
        assert int(summary.get("rows_written") or 0) == 1
        assert int(summary.get("active_rows") or 0) == 1
    finally:
        try:
            Path(db_path).unlink(missing_ok=True)
        except PermissionError:
            pass


def test_scd2_fail_job_contract_aborts_before_merge():
    from services.migration_risk_contract import create_migration_risk_contract

    fd, db_path = tempfile.mkstemp(suffix=".db")
    try:
        endpoint = _sqlite_endpoint(Path(db_path))
        c = create_migration_risk_contract(
            column="price",
            source_type="TEXT",
            destination_type="DECIMAL",
            approved_by="admin@dataflow.app",
            reason="SCD2 FAIL_JOB must abort",
            execution_policy="FAIL_JOB",
        )
        summary = apply_scd2(
            endpoint,
            [{"id": "1", "name": "A", "price": "nope"}],
            columns=["id", "name", "price"],
            schema={"id": "string", "name": "string", "price": "decimal"},
            mappings=[
                {"source": "id", "target": "id"},
                {"source": "name", "target": "name"},
                {
                    "source": "price",
                    "target": "price",
                    "transform": "decimal",
                    "target_type": "decimal",
                    "risk_contract": c.to_dict(),
                },
            ],
            conflict_columns=["id"],
        )
        assert summary.get("ok") is False
        assert summary.get("error")
        assert int(summary.get("rows_written") or 0) == 0
        assert summary.get("rejected_details")
    finally:
        try:
            Path(db_path).unlink(missing_ok=True)
        except PermissionError:
            pass


def test_scd2_write_quarantine_matrix_blocks_decimal_overflow_under_strict():
    """SCD2 must run the same write-quarantine matrix as SQL writers before history merge."""
    from src.services.scd2_engine import prepare_scd2_mapped_rows

    fd, db_path = tempfile.mkstemp(suffix=".db")
    try:
        endpoint = _sqlite_endpoint(Path(db_path))
        prepared = prepare_scd2_mapped_rows(
            endpoint,
            [
                {"id": "1", "name": "A", "price": "12.34"},
                {"id": "2", "name": "B", "price": "999999999.99"},
            ],
            columns=["id", "name", "price"],
            schema={"id": "string", "name": "string", "price": "DECIMAL(5,2)"},
            mappings=[
                {"source": "id", "target": "id"},
                {"source": "name", "target": "name"},
                {
                    "source": "price",
                    "target": "price",
                    "transform": "none",
                    "target_type": "DECIMAL(5,2)",
                },
            ],
            conflict_columns=["id"],
            validation_mode="strict",
        )
        assert prepared.get("ok") is False, prepared
        assert prepared.get("error")
        assert int(prepared.get("rejected_rows") or 0) >= 1
        assert any(
            "decimal" in str(d.get("reason") or "").lower()
            or "overflow" in str(d.get("reason") or "").lower()
            or "fit" in str(d.get("reason") or "").lower()
            for d in (prepared.get("rejected_details") or [])
        )
    finally:
        try:
            Path(db_path).unlink(missing_ok=True)
        except PermissionError:
            pass


def test_scd2_sanitized_target_stamp_still_quarantines_overflow():
    """Map target_type stamps must align with sanitized target_cols (hyphen→underscore)."""
    from src.services.scd2_engine import prepare_scd2_mapped_rows

    fd, db_path = tempfile.mkstemp(suffix=".db")
    try:
        endpoint = _sqlite_endpoint(Path(db_path))
        prepared = prepare_scd2_mapped_rows(
            endpoint,
            [
                {"id": "1", "unit-price": "12.34"},
                {"id": "2", "unit-price": "999999999.99"},
            ],
            columns=["id", "unit-price"],
            schema={"id": "string", "unit-price": "DECIMAL(5,2)"},
            mappings=[
                {"source": "id", "target": "id"},
                {
                    "source": "unit-price",
                    "target": "unit-price",
                    "transform": "none",
                    "target_type": "DECIMAL(5,2)",
                },
            ],
            conflict_columns=["id"],
            validation_mode="strict",
        )
        assert prepared.get("ok") is False, prepared
        assert int(prepared.get("rejected_rows") or 0) >= 1
    finally:
        try:
            Path(db_path).unlink(missing_ok=True)
        except PermissionError:
            pass


def test_scd2_empty_pk_quarantined_not_silent():
    """Blank primary keys must surface in rejected_details — never vanish."""
    fd, db_path = tempfile.mkstemp(suffix=".db")
    try:
        endpoint = _sqlite_endpoint(Path(db_path))
        summary = apply_scd2(
            endpoint,
            [
                {"id": "", "name": "ghost"},
                {"id": "2", "name": "ok"},
            ],
            columns=["id", "name"],
            schema={"id": "string", "name": "string"},
            mappings=[
                {"source": "id", "target": "id"},
                {"source": "name", "target": "name"},
            ],
            conflict_columns=["id"],
            validation_mode="balanced",
        )
        assert summary.get("ok") is not False
        assert int(summary.get("rejected_rows") or 0) >= 1
        assert any(
            "primary key" in str(d.get("reason") or "").lower()
            for d in (summary.get("rejected_details") or [])
        )
        assert int(summary.get("active_rows") or 0) == 1
    finally:
        try:
            Path(db_path).unlink(missing_ok=True)
        except PermissionError:
            pass


def test_scd2_numeric_zero_pk_is_valid_identity():
    """Numeric 0 must not be treated as missing PK (truthiness trap)."""
    fd, db_path = tempfile.mkstemp(suffix=".db")
    try:
        endpoint = _sqlite_endpoint(Path(db_path))
        summary = apply_scd2(
            endpoint,
            [{"id": 0, "name": "zero"}, {"id": 1, "name": "one"}],
            columns=["id", "name"],
            schema={"id": "integer", "name": "string"},
            mappings=[
                {"source": "id", "target": "id"},
                {"source": "name", "target": "name"},
            ],
            conflict_columns=["id"],
            validation_mode="balanced",
        )
        assert summary.get("ok") is not False, summary.get("error")
        assert int(summary.get("rejected_rows") or 0) == 0
        assert int(summary.get("active_rows") or 0) == 2
    finally:
        try:
            Path(db_path).unlink(missing_ok=True)
        except PermissionError:
            pass


def test_scd2_df_missing_hydrates_prior_attr_not_null_history():
    """STOP_COLUMN / DF_MISSING must keep prior SCD2 attr — never invent NULL version."""
    from services.value_serializer import DF_MISSING_SENTINEL

    fd, db_path = tempfile.mkstemp(suffix=".db")
    try:
        endpoint = _sqlite_endpoint(Path(db_path))
        apply_scd2(
            endpoint,
            [{"id": 1, "name": "alice", "city": "NYC"}],
            columns=["id", "name", "city"],
            schema={"id": "integer", "name": "string", "city": "string"},
            mappings=[
                {"source": "id", "target": "id"},
                {"source": "name", "target": "name"},
                {"source": "city", "target": "city"},
            ],
            conflict_columns=["id"],
            validation_mode="balanced",
        )
        # Only name changes; city is STOP_COLUMN omit.
        summary = apply_scd2(
            endpoint,
            [{"id": 1, "name": "alice2", "city": DF_MISSING_SENTINEL}],
            columns=["id", "name", "city"],
            schema={"id": "integer", "name": "string", "city": "string"},
            mappings=[
                {"source": "id", "target": "id"},
                {"source": "name", "target": "name"},
                {"source": "city", "target": "city"},
            ],
            conflict_columns=["id"],
            validation_mode="balanced",
        )
        assert summary.get("ok") is not False, summary.get("error")
        import sqlite3

        conn = sqlite3.connect(db_path)
        row = conn.execute(
            "SELECT name, city FROM products WHERE is_current = 1 AND id = 1"
        ).fetchone()
        conn.close()
        assert row is not None
        assert row[0] == "alice2"
        assert row[1] == "NYC"
    finally:
        try:
            Path(db_path).unlink(missing_ok=True)
        except PermissionError:
            pass


def test_active_checksum_sql_is_a_full_scan_not_limit_offset():
    """Oracle/SQL Server reject LIMIT; OFFSET pagination is not this kernel."""
    from services.scd2_engine import _active_checksum

    captured: dict[str, str] = {}

    class _Row:
        def __init__(self, mapping):
            self._mapping = mapping

    class _Result:
        def keys(self):
            return ["id", "name"]

        def partitions(self, _n):
            yield [_Row({"id": "1", "name": "A"})]

    class _Conn:
        def execution_options(self, **_kw):
            return self

        def execute(self, statement):
            captured["sql"] = str(statement)
            return _Result()

        dialect = type("D", (), {"name": "mssql"})()

    count, digest = _active_checksum(_Conn(), "dbo.products", ["id", "name"], 10, "mssql")
    assert count == 1
    assert digest
    sql = captured["sql"].upper()
    assert "LIMIT" not in sql
    assert "OFFSET" not in sql
    assert "IS TRUE" not in sql
    assert "= 1" in sql


def _pg_reachable() -> bool:
    import socket

    try:
        with socket.create_connection(("127.0.0.1", 5432), timeout=1):
            return True
    except OSError:
        return False


@pytest.mark.skipif(not _pg_reachable(), reason="PostgreSQL :5432 not reachable")
def test_live_pg_scd2_active_checksum_streams_current_rows():
    """Live PG: current-row digest after two versions. Not migration_proven."""
    import uuid

    psycopg2 = pytest.importorskip("psycopg2")
    from src.transfer.models import EndpointConfig

    table = f"scd2_stream_{uuid.uuid4().hex[:8]}"
    conn = psycopg2.connect(
        host="127.0.0.1",
        port=5432,
        dbname="dataflow",
        user="dataflow",
        password="dataflow",
    )
    conn.autocommit = True
    try:
        endpoint = EndpointConfig(
            kind="database",
            format="postgresql",
            host="127.0.0.1",
            port=5432,
            database="dataflow",
            username="dataflow",
            password="dataflow",
            schema="public",
            table=table,
        )
        first = apply_scd2(
            endpoint,
            [{"id": "1", "name": "A"}, {"id": "2", "name": "B"}],
            columns=["id", "name"],
            schema={"id": "string", "name": "string"},
            mappings=None,
            conflict_columns=["id"],
        )
        assert first.get("ok") is not False, first.get("error")
        assert first["active_rows"] == 2
        second = apply_scd2(
            endpoint,
            [{"id": "1", "name": "A2"}, {"id": "2", "name": "B"}],
            columns=["id", "name"],
            schema={"id": "string", "name": "string"},
            mappings=None,
            conflict_columns=["id"],
        )
        assert second["active_rows"] == 2
        assert second["updated_rows"] >= 1
        assert second["active_checksum"]
        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM public."{table}" WHERE is_current IS TRUE')
            current = cur.fetchone()[0]
            cur.execute(f'SELECT COUNT(*) FROM public."{table}"')
            physical = cur.fetchone()[0]
        assert current == 2
        assert physical == 3
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
        conn.close()


def test_scd2_prepare_never_calls_records_to_matrix(monkeypatch):
    from src.services.scd2_engine import prepare_scd2_mapped_rows

    def _blocked(*_a, **_k):
        raise AssertionError("SCD2 must not build a retained records_to_matrix copy")

    monkeypatch.setattr("src.transfer.adapters.records_to_matrix", _blocked)
    monkeypatch.setattr("transfer.adapters.records_to_matrix", _blocked)

    fd, db_path = tempfile.mkstemp(suffix=".db")
    try:
        endpoint = _sqlite_endpoint(Path(db_path))
        prepared = prepare_scd2_mapped_rows(
            endpoint,
            _records(),
            columns=["id", "name", "price"],
            schema={"id": "string", "name": "string", "price": "decimal"},
            mappings=None,
            conflict_columns=["id"],
            validation_mode="balanced",
        )
        assert prepared.get("ok") is not False, prepared.get("error")
        assert len(prepared.get("mapped_rows") or []) == 2
    finally:
        Path(db_path).unlink(missing_ok=True)


def test_scd2_apply_clears_records_and_accepts_source_spool():
    from connectors.engine_record_spill import spill_engine_write_records
    from src.services.scd2_engine import apply_scd2

    fd, db_path = tempfile.mkstemp(suffix=".db")
    try:
        endpoint = _sqlite_endpoint(Path(db_path))
        first_records = _records()
        first = apply_scd2(
            endpoint,
            first_records,
            columns=["id", "name", "price"],
            schema={"id": "string", "name": "string", "price": "decimal"},
            mappings=None,
            conflict_columns=["id"],
            clear_records=True,
        )
        assert first_records == []
        assert first["rows_written"] == 2
        changed = [
            {"id": "1", "name": "A-updated", "price": "10.00"},
            {"id": "2", "name": "B", "price": "20.00"},
        ]
        spill = spill_engine_write_records(
            changed,
            ["id", "name", "price"],
            None,
            extra={},
            clear_records=True,
        )
        try:
            assert changed == []
            summary = apply_scd2(
                endpoint,
                [],
                columns=["id", "name", "price"],
                schema={"id": "string", "name": "string", "price": "decimal"},
                mappings=None,
                conflict_columns=["id"],
                source_spool=spill.spool,
            )
        finally:
            spill.close()
        assert summary["rows_written"] == 1
        assert summary["updated_rows"] == 1
        assert summary["active_rows"] == 2
    finally:
        Path(db_path).unlink(missing_ok=True)


def test_scd2_fail_scan_collects_every_reject_before_merge():
    """FAIL_JOB must scan every bundle, then refuse — no prefix history."""
    from services.migration_risk_contract import create_migration_risk_contract
    from src.services.scd2_engine import apply_scd2

    fd, db_path = tempfile.mkstemp(suffix=".db")
    try:
        endpoint = _sqlite_endpoint(Path(db_path))
        c = create_migration_risk_contract(
            column="price",
            source_type="TEXT",
            destination_type="DECIMAL",
            approved_by="admin@dataflow.app",
            reason="SCD2 fail-scan every bundle",
            execution_policy="FAIL_JOB",
        )
        mappings = [
            {"source": "id", "target": "id"},
            {"source": "name", "target": "name"},
            {
                "source": "price",
                "target": "price",
                "transform": "decimal",
                "target_type": "decimal",
                "risk_contract": c.to_dict(),
            },
        ]
        summary = apply_scd2(
            endpoint,
            [
                {"id": "1", "name": "A", "price": "nope"},
                {"id": "2", "name": "B", "price": "also-bad"},
            ],
            columns=["id", "name", "price"],
            schema={"id": "string", "name": "string", "price": "decimal"},
            mappings=mappings,
            conflict_columns=["id"],
            batch_size=1,
        )
        assert summary.get("ok") is False
        assert int(summary.get("rows_written") or 0) == 0
        assert int(summary.get("rejected_rows") or 0) >= 2
        conn = sqlite3.connect(db_path)
        try:
            tables = {
                r[0]
                for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
            if "products" in tables:
                assert conn.execute("SELECT COUNT(*) FROM products").fetchone()[0] == 0
        finally:
            conn.close()
    finally:
        Path(db_path).unlink(missing_ok=True)
