"""Sparse CDC DF_MISSING: omit-from-SET, never NULL-wipe or sentinel leak."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.writer_common import (  # noqa: E402
    assert_sparse_upsert_has_pk,
    build_mapped_rows_with_details,
    sparse_present_bindings,
    split_dense_sparse_rows,
)
from services.transform_engine import apply_transform  # noqa: E402
from services.value_serializer import DF_MISSING_SENTINEL, is_missing_sentinel  # noqa: E402


def test_apply_transform_preserves_df_missing():
    for transform in ("none", "integer", "decimal", "string"):
        val, err = apply_transform(DF_MISSING_SENTINEL, transform)
        assert err is None
        assert is_missing_sentinel(val), transform


def test_build_mapped_rows_preserves_missing_not_null():
    mapped, errors, details = build_mapped_rows_with_details(
        headers=["id", "note", "extra"],
        data_rows=[["1", "keep", DF_MISSING_SENTINEL]],
        mappings=[
            {"source": "id", "target": "id", "transform": "none"},
            {"source": "note", "target": "note", "transform": "none"},
            {"source": "extra", "target": "extra", "transform": "integer"},
        ],
        target_cols=["id", "note", "extra"],
        column_types={"id": "string", "note": "string", "extra": "integer"},
        dest_types={"id": "string", "note": "string", "extra": "integer"},
    )
    assert errors == []
    assert details == []
    assert mapped[0][0] == "1"
    assert mapped[0][1] == "keep"
    assert is_missing_sentinel(mapped[0][2])
    assert mapped[0][2] is not None


def test_sparse_present_bindings_omits_missing():
    row = ("1", "x", DF_MISSING_SENTINEL)
    present = sparse_present_bindings(row, ["id", "note", "extra"])
    assert present == {"id": "1", "note": "x"}
    assert "extra" not in present


def test_split_dense_sparse_rows():
    dense, sparse = split_dense_sparse_rows(
        [("1", "a"), ("2", DF_MISSING_SENTINEL), ("3", "c")]
    )
    assert dense == [("1", "a"), ("3", "c")]
    assert sparse == [("2", DF_MISSING_SENTINEL)]


def test_assert_sparse_upsert_requires_pk():
    try:
        assert_sparse_upsert_has_pk({"note": "x"}, ["id"])
        raise AssertionError("expected ValueError")
    except ValueError as exc:
        assert "primary-key" in str(exc).lower()


def test_mysql_sparse_upsert_omits_missing_column():
    from connectors.mysql_writer import _mysql_apply_sparse_upsert

    cur = MagicMock()
    # First SELECT for LSN (none), then UPDATE rowcount 1
    cur.fetchone.return_value = None
    cur.rowcount = 1
    written = _mysql_apply_sparse_upsert(
        cur,
        table_q="`t`",
        target_cols=["id", "note", "extra"],
        conflict_columns=["id"],
        sparse_rows=[("1", "only-note", DF_MISSING_SENTINEL)],
    )
    assert written == 1
    # UPDATE should only SET note, never extra
    update_sql = cur.execute.call_args_list[-1].args[0]
    assert "note" in update_sql.lower() or "NOTE" in update_sql
    assert "extra" not in update_sql.lower()
    # Bound values: note + pk only
    bound = cur.execute.call_args_list[-1].args[1]
    assert "only-note" in bound
    assert DF_MISSING_SENTINEL not in bound


def test_generic_sparse_upsert_omits_missing_column():
    import sqlalchemy as sa
    from connectors.generic_sql import _generic_apply_sparse_upsert

    meta = sa.MetaData()
    table = sa.Table(
        "t",
        meta,
        sa.Column("id", sa.String()),
        sa.Column("note", sa.String()),
        sa.Column("extra", sa.String()),
    )
    conn = MagicMock()
    upd_result = MagicMock()
    upd_result.rowcount = 1
    conn.execute.return_value = upd_result
    written = _generic_apply_sparse_upsert(
        conn,
        table,
        ["id", "note", "extra"],
        ["id"],
        [{"id": "1", "note": "only-note", "extra": DF_MISSING_SENTINEL}],
    )
    assert written == 1
    stmt = conn.execute.call_args_list[0].args[0]
    compiled = str(stmt.compile(compile_kwargs={"literal_binds": False})).upper()
    assert "UPDATE" in compiled
    assert "NOTE" in compiled
    assert "EXTRA" not in compiled.split("WHERE")[0]
    values = stmt._values or {}
    set_cols = {
        (c.name if hasattr(c, "name") else str(c)) for c in values
    }
    assert "note" in set_cols
    assert "extra" not in set_cols
    assert DF_MISSING_SENTINEL not in values.values()


def test_lsn_delete_fails_closed_when_fetch_unavailable():
    from connectors.table_manager import delete_by_primary_keys

    try:
        delete_by_primary_keys(
            "unsupported_engine_xyz",
            {},
            "t",
            "id",
            ["1"],
            incoming_lsn="0/100",
        )
        raise AssertionError("expected fail-closed RuntimeError")
    except RuntimeError as exc:
        assert "LSN-guarded" in str(exc) or "Refusing" in str(exc)


def test_mongo_sparse_upsert_uses_set_not_replace():
    """Partial CDC image must $set — ReplaceOne would wipe omitted fields."""
    from unittest.mock import patch

    from connectors.mongodb_writer import write_mapped_rows

    captured: list = []

    class _Coll:
        def find(self, *a, **k):
            return []

        def bulk_write(self, ops, ordered=False):
            captured.extend(ops)

    class _Db:
        def __getitem__(self, name):
            return _Coll()

    class _Client:
        def __getitem__(self, name):
            return _Db()

        def close(self):
            return None

    with patch("connectors.mongodb_common._mongo_client", return_value=_Client()):
        result = write_mapped_rows(
            host="localhost",
            port=27017,
            database="testdb",
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=False,
            table_name="orders",
            headers=["id", "note", "extra"],
            data_rows=[["1", "keep-note", DF_MISSING_SENTINEL]],
            mappings=[
                {"source": "id", "target": "id", "confidence": 1},
                {"source": "note", "target": "note", "confidence": 1},
                {"source": "extra", "target": "extra", "confidence": 1},
            ],
            column_types={"id": "string", "note": "string", "extra": "string"},
            create_table=True,
            write_mode="upsert",
            conflict_columns=["id"],
        )
    assert result.ok, result.error
    assert len(captured) == 1
    op = captured[0]
    assert type(op).__name__ == "UpdateOne"
    update = op._doc
    assert "$set" in update
    assert update["$set"].get("note") == "keep-note"
    assert "extra" not in update["$set"]
    assert DF_MISSING_SENTINEL not in update["$set"].values()


def test_records_for_bigquery_omits_missing():
    from connectors.warehouse_temporal import records_for_bigquery

    recs = records_for_bigquery(
        [("1", "keep", DF_MISSING_SENTINEL)],
        ["id", "note", "extra"],
        ["STRING", "STRING", "STRING"],
    )
    assert recs == [{"id": "1", "note": "keep"}]
    assert "extra" not in recs[0]


def test_bq_sparse_upsert_omits_missing_column():
    from connectors.bigquery_writer import _bq_apply_sparse_upsert

    queries: list[str] = []

    class _Job:
        num_dml_affected_rows = 1

        def result(self):
            return []

    class _Client:
        def query(self, sql, job_config=None):
            queries.append(sql)
            return _Job()

    written = _bq_apply_sparse_upsert(
        _Client(),
        "proj.ds.t",
        ["id", "note", "extra"],
        ["id"],
        [("1", "only-note", DF_MISSING_SENTINEL)],
        ["STRING", "STRING", "STRING"],
    )
    assert written == 1
    assert any("UPDATE" in q.upper() for q in queries)
    upd = next(q for q in queries if "UPDATE" in q.upper())
    assert "`note`" in upd
    assert "`extra`" not in upd


def test_snowflake_sparse_upsert_omits_missing_column():
    from connectors.snowflake_writer import _sf_apply_sparse_upsert

    cur = MagicMock()
    cur.fetchone.return_value = None
    cur.rowcount = 1
    written = _sf_apply_sparse_upsert(
        cur,
        "T",
        ["id", "note", "extra"],
        ["VARCHAR", "VARCHAR", "VARCHAR"],
        ["id"],
        [("1", "only-note", DF_MISSING_SENTINEL)],
    )
    assert written == 1
    upd = cur.execute.call_args_list[-1].args[0]
    assert "UPDATE" in upd.upper()
    assert "note" in upd.lower()
    assert "extra" not in upd.lower()
    assert DF_MISSING_SENTINEL not in cur.execute.call_args_list[-1].args[1]


def test_iceberg_sparse_merge_preserves_absent_fields():
    from connectors.iceberg_writer import _merge_upsert_rows

    existing = [{"id": "1", "note": "keep-me", "extra": "stay", "_df_lsn": "0/100"}]
    incoming = [
        {
            "id": "1",
            "note": "updated",
            "extra": DF_MISSING_SENTINEL,
            "_df_lsn": "0/200",
        }
    ]
    merged = _merge_upsert_rows(existing, incoming, pk_cols=["id"])
    assert len(merged) == 1
    assert merged[0]["note"] == "updated"
    assert merged[0]["extra"] == "stay"
    assert merged[0]["_df_lsn"] == "0/200"


def test_combined_mapped_rows_for_checksum_includes_sparse():
    from connectors.writer_common import combined_mapped_rows_for_checksum

    dense = [("1", "a")]
    sparse = [("2", DF_MISSING_SENTINEL)]
    assert combined_mapped_rows_for_checksum(dense, sparse) == dense + sparse
    assert combined_mapped_rows_for_checksum(dense, None) == dense


def test_iceberg_sparse_stale_lsn_preserves_row():
    from connectors.iceberg_writer import _merge_upsert_rows

    existing = [{"id": "1", "note": "keep", "extra": "stay", "_df_lsn": "0/100"}]
    incoming = [
        {
            "id": "1",
            "note": "stale",
            "extra": DF_MISSING_SENTINEL,
            "_df_lsn": "0/50",
        }
    ]
    merged = _merge_upsert_rows(existing, incoming, pk_cols=["id"])
    assert merged[0]["extra"] == "stay"
    assert merged[0]["note"] == "keep"
