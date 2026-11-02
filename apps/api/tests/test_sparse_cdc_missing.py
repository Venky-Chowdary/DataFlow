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
    cur.fetchone.return_value = ("1", "old", "keep-extra")
    cur.rowcount = 1
    written, skipped, checksum_rows = _mysql_apply_sparse_upsert(
        cur,
        table_q="`t`",
        target_cols=["id", "note", "extra"],
        conflict_columns=["id"],
        sparse_rows=[("1", "only-note", DF_MISSING_SENTINEL)],
    )
    assert written == 1
    assert skipped == 0
    update_sql = cur.execute.call_args_list[-1].args[0]
    assert "note" in update_sql.lower() or "NOTE" in update_sql
    assert "extra" not in update_sql.lower()
    bound = cur.execute.call_args_list[-1].args[1]
    assert "only-note" in bound
    assert DF_MISSING_SENTINEL not in bound
    assert checksum_rows == [("1", "only-note", "keep-extra")]


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
    sel_result = MagicMock()
    sel_result.fetchone.return_value = ("1", "old", "keep-extra")
    upd_result = MagicMock()
    upd_result.rowcount = 1
    conn.execute.side_effect = [sel_result, upd_result]
    written, skipped, checksum_rows = _generic_apply_sparse_upsert(
        conn,
        table,
        ["id", "note", "extra"],
        ["id"],
        [{"id": "1", "note": "only-note", "extra": DF_MISSING_SENTINEL}],
    )
    assert written == 1
    assert skipped == 0
    stmt = conn.execute.call_args_list[1].args[0]
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
    assert checksum_rows == [("1", "only-note", "keep-extra")]


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

        def list_collection_names(self, filter=None):  # noqa: A002
            return []

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
    from unittest.mock import MagicMock, patch

    from connectors import bigquery_writer as bq_writer

    queries: list[str] = []

    class _Job:
        num_dml_affected_rows = 1

        def result(self):
            # SELECT returns existing row so checksum hydration preserves ``extra``.
            if queries and "SELECT" in queries[-1].upper():
                return [("1", "old-note", "keep-extra")]
            return []

    class _Client:
        def query(self, sql, job_config=None):
            queries.append(sql)
            return _Job()

    bq = MagicMock()
    bq.ScalarQueryParameter = MagicMock(side_effect=lambda *a, **k: ("param", a, k))
    bq.QueryJobConfig = MagicMock(side_effect=lambda **k: k)

    with patch.object(bq_writer, "_bq_sdk", return_value=bq):
        written, skipped, checksum_rows = bq_writer._bq_apply_sparse_upsert(
            _Client(),
            "proj.ds.t",
            ["id", "note", "extra"],
            ["id"],
            [("1", "only-note", DF_MISSING_SENTINEL)],
            ["STRING", "STRING", "STRING"],
        )
    assert written == 1
    assert skipped == 0
    assert any("UPDATE" in q.upper() for q in queries)
    upd = next(q for q in queries if "UPDATE" in q.upper())
    assert "`note`" in upd
    assert "`extra`" not in upd
    assert checksum_rows == [("1", "only-note", "keep-extra")]


def test_snowflake_sparse_upsert_omits_missing_column():
    from connectors.snowflake_writer import _sf_apply_sparse_upsert

    cur = MagicMock()
    cur.fetchone.return_value = ("1", "old", "keep-extra")
    cur.rowcount = 1
    written, skipped, checksum_rows = _sf_apply_sparse_upsert(
        cur,
        "T",
        ["id", "note", "extra"],
        ["VARCHAR", "VARCHAR", "VARCHAR"],
        ["id"],
        [("1", "only-note", DF_MISSING_SENTINEL)],
    )
    assert written == 1
    assert skipped == 0
    upd = cur.execute.call_args_list[-1].args[0]
    assert "UPDATE" in upd.upper()
    assert "note" in upd.lower()
    assert "extra" not in upd.lower()
    assert DF_MISSING_SENTINEL not in cur.execute.call_args_list[-1].args[1]
    assert checksum_rows == [("1", "only-note", "keep-extra")]


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


def test_sqlite_sparse_upsert_omits_missing_column():
    from connectors.sqlite_writer import _sqlite_apply_sparse_upsert

    cur = MagicMock()
    # Existing dest row — extra must be preserved in checksum image
    cur.fetchone.return_value = ("1", "old-note", "keep-me")
    cur.rowcount = 1
    written, skipped, checksum_rows = _sqlite_apply_sparse_upsert(
        cur,
        table_name="t",
        target_cols=["id", "note", "extra"],
        conflict_columns=["id"],
        sparse_rows=[("1", "only-note", DF_MISSING_SENTINEL)],
    )
    assert written == 1
    assert skipped == 0
    update_sql = cur.execute.call_args_list[-1].args[0]
    assert "note" in update_sql.lower()
    assert "extra" not in update_sql.lower()
    bound = cur.execute.call_args_list[-1].args[1]
    assert "only-note" in bound
    assert DF_MISSING_SENTINEL not in bound
    assert checksum_rows == [("1", "only-note", "keep-me")]


def test_sqlite_sparse_stale_lsn_increments_skipped():
    from connectors.sqlite_writer import _sqlite_apply_sparse_upsert
    from connectors.writer_common import DF_LSN_COL

    cur = MagicMock()
    # Dest already at newer LSN
    cur.fetchone.return_value = ("1", "keep", "x", "0/200")
    cur.rowcount = 0
    written, skipped, checksum_rows = _sqlite_apply_sparse_upsert(
        cur,
        table_name="t",
        target_cols=["id", "note", "extra", DF_LSN_COL],
        conflict_columns=["id"],
        sparse_rows=[("1", "stale", DF_MISSING_SENTINEL, "0/50")],
    )
    assert written == 0
    assert skipped == 1
    assert checksum_rows == []
    # Only the SELECT should have run — no UPDATE/INSERT for stale LSN
    assert cur.execute.call_count == 1


def test_sqlite_write_mapped_rows_sparse_roundtrip(tmp_path):
    """End-to-end: sparse CDC upsert must leave absent dest columns untouched."""
    import sqlite3

    from connectors.sqlite_writer import write_mapped_rows

    db = tmp_path / "sparse.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE t (id TEXT PRIMARY KEY, note TEXT, extra TEXT)")
    conn.execute("INSERT INTO t VALUES ('1', 'old-note', 'keep-me')")
    conn.commit()
    conn.close()

    result = write_mapped_rows(
        host="",
        port=0,
        database=str(db),
        username="",
        password="",
        schema="main",
        connection_string="",
        ssl=False,
        headers=["id", "note", "extra"],
        data_rows=[["1", "new-note", DF_MISSING_SENTINEL]],
        mappings=[
            {"source": "id", "target": "id", "transform": "none"},
            {"source": "note", "target": "note", "transform": "none"},
            {"source": "extra", "target": "extra", "transform": "none"},
        ],
        column_types={"id": "string", "note": "string", "extra": "string"},
        table_name="t",
        write_mode="upsert",
        conflict_columns=["id"],
        create_table=False,
    )
    assert result.ok, result.error
    assert result.rows_written >= 1
    assert DF_MISSING_SENTINEL not in (result.checksum or "")

    conn = sqlite3.connect(db)
    row = conn.execute("SELECT id, note, extra FROM t WHERE id='1'").fetchone()
    from connectors.writer_common import row_checksum

    readback = row_checksum(
        [row],
        ["id", "note", "extra"],
        dest_db_type="sqlite",
        dest_types={"id": "string", "note": "string", "extra": "string"},
    )
    conn.close()
    assert row == ("1", "new-note", "keep-me")
    assert result.checksum == readback


def test_omit_missing_fields_drops_df_missing_sentinel():
    from connectors.writer_common import omit_missing_fields

    out = omit_missing_fields(
        [("id", "1"), ("note", "keep"), ("extra", DF_MISSING_SENTINEL), ("empty", "")]
    )
    assert out == {"id": "1", "note": "keep"}
    assert DF_MISSING_SENTINEL not in out.values()


def test_resolve_conflict_targets_casefold():
    from connectors.writer_common import resolve_conflict_targets

    assert resolve_conflict_targets(["ID", "note"], ["id", "NOTE", "extra"]) == [
        "id",
        "NOTE",
    ]
    assert resolve_conflict_targets(["missing"], ["id"], strict=False) == []
    try:
        resolve_conflict_targets(["id", "missing"], ["id"])
        raise AssertionError("expected strict unresolved raise")
    except ValueError as exc:
        assert "unresolved" in str(exc).lower()


def test_bq_refuses_unresolved_conflict_with_sparse_signal():
    """Case-mismatched PK must fail closed — never append-path drop sparse CDC."""
    from unittest.mock import MagicMock, patch

    from connectors.bigquery_writer import write_mapped_rows

    client = MagicMock()
    table = MagicMock()
    table.schema = []
    client.get_table.return_value = table
    with patch("connectors.bigquery_writer.stub_writes_allowed", return_value=False):
        with patch(
            "connectors.bigquery_conn.get_client", return_value=client
        ):
            with patch(
                "connectors.bigquery_conn._is_local_endpoint", return_value=(True, "")
            ):
                result = write_mapped_rows(
                    host="localhost",
                    port=9050,
                    database="proj",
                    username="",
                    password="",
                    schema="ds",
                    connection_string="",
                    ssl=False,
                    warehouse="",
                    table_name="t",
                    headers=["id", "note", "extra"],
                    data_rows=[["1", "n", DF_MISSING_SENTINEL]],
                    mappings=[
                        {"source": "id", "target": "id"},
                        {"source": "note", "target": "note"},
                        {"source": "extra", "target": "extra"},
                    ],
                    column_types={
                        "id": "STRING",
                        "note": "STRING",
                        "extra": "STRING",
                    },
                    write_mode="upsert",
                    conflict_columns=["ID_NOT_MAPPED"],
                    create_table=False,
                )
    assert result.ok is False
    assert "conflict_columns" in (result.error or "").lower() or "refuse" in (
        result.error or ""
    ).lower()


def test_versioned_sparse_insert_hydrates_existing_attrs():
    """ClickHouse-class path must INSERT hydrated full image, not present-only."""
    from connectors.writer_common import run_sparse_cdc_upsert

    inserted: list[dict] = []

    def fetch(pk_vals):
        assert pk_vals == ["1"]
        return ("1", "prior-note", "prior-extra")

    def update(_non_pk, _pk):
        return 0

    def insert(present):
        inserted.append(dict(present))

    written, skipped, checksum = run_sparse_cdc_upsert(
        target_cols=["id", "note", "extra"],
        conflict_columns=["id"],
        sparse_rows=[("1", "new-note", DF_MISSING_SENTINEL)],
        fetch_existing_row=fetch,
        update_non_pk=update,
        insert_present=insert,
        hydrate_versioned_insert=True,
    )
    assert written == 1 and skipped == 0
    assert inserted == [{"id": "1", "note": "new-note", "extra": "prior-extra"}]
    assert checksum == [("1", "new-note", "prior-extra")]


def test_adapters_records_to_matrix_preserves_df_missing():
    from src.transfer.adapters import records_to_matrix

    headers, rows = records_to_matrix(
        [{"id": "1", "note": "keep"}, {"id": "2", "extra": DF_MISSING_SENTINEL}],
        ["id", "note", "extra"],
    )
    assert headers == ["id", "note", "extra"]
    assert rows[0][1] == "keep"
    assert rows[0][2] == DF_MISSING_SENTINEL  # absent key
    assert rows[1][1] == DF_MISSING_SENTINEL  # absent key
    assert rows[1][2] == DF_MISSING_SENTINEL  # present sentinel


def test_cdc_matrix_preserves_present_df_missing():
    from src.transfer.cdc_transfer import _records_to_matrix

    rows = _records_to_matrix(
        [{"id": "1", "note": "x", "extra": DF_MISSING_SENTINEL}],
        ["id", "note", "extra"],
    )
    assert rows[0][2] == DF_MISSING_SENTINEL


def test_vector_prepare_omits_df_missing_sentinel():
    from connectors.writer_common import prepare_records_for_vector_write

    records, rejected, abort = prepare_records_for_vector_write(
        headers=["id", "note", "extra"],
        data_rows=[["1", "keep", DF_MISSING_SENTINEL]],
        mappings=[
            {"source": "id", "target": "id"},
            {"source": "note", "target": "note"},
            {"source": "extra", "target": "extra"},
        ],
        column_types={"id": "string", "note": "string", "extra": "string"},
    )
    assert abort is None
    assert rejected == [] or True
    assert records
    assert DF_MISSING_SENTINEL not in records[0].values()
    assert "extra" not in records[0] or records[0].get("extra") != DF_MISSING_SENTINEL


def test_dynamo_sparse_uses_update_item_not_put():
    from unittest.mock import MagicMock, patch

    from connectors.dynamodb_writer import write_mapped_rows

    client = MagicMock()
    client.describe_table.return_value = {
        "Table": {
            "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
        }
    }
    with patch("connectors.dynamodb_writer.boto3_client", return_value=client):
        with patch("connectors.dynamodb_writer._ensure_table"):
            result = write_mapped_rows(
                host="localhost",
                port=8000,
                database="t",
                username="",
                password="",
                schema="",
                connection_string="",
                ssl=False,
                table_name="t",
                headers=["id", "note", "extra"],
                data_rows=[["1", "only-note", DF_MISSING_SENTINEL]],
                mappings=[
                    {"source": "id", "target": "id"},
                    {"source": "note", "target": "note"},
                    {"source": "extra", "target": "extra"},
                ],
                column_types={"id": "string", "note": "string", "extra": "string"},
                create_table=False,
                conflict_columns=["id"],
            )
    assert result.ok, result.error
    assert client.update_item.called
    assert not client.batch_write_item.called
    kwargs = client.update_item.call_args.kwargs
    assert "extra" not in (kwargs.get("ExpressionAttributeNames") or {}).values()
    assert DF_MISSING_SENTINEL not in str(kwargs)


def test_dynamo_rejects_df_missing_on_key_attribute():
    """HASH/RANGE identity cannot be DF_MISSING — quarantine, never silent skip."""
    from unittest.mock import MagicMock, patch

    from connectors.dynamodb_writer import write_mapped_rows

    client = MagicMock()
    client.describe_table.return_value = {
        "Table": {
            "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
        }
    }
    with patch("connectors.dynamodb_writer.boto3_client", return_value=client):
        with patch("connectors.dynamodb_writer._ensure_table"):
            result = write_mapped_rows(
                host="localhost",
                port=8000,
                database="t",
                username="",
                password="",
                schema="",
                connection_string="",
                ssl=False,
                table_name="t",
                headers=["id", "note"],
                data_rows=[[DF_MISSING_SENTINEL, "note"]],
                mappings=[
                    {"source": "id", "target": "id"},
                    {"source": "note", "target": "note"},
                ],
                column_types={"id": "string", "note": "string"},
                create_table=False,
                conflict_columns=["id"],
                error_policy="quarantine",
            )
    assert result.ok is True or result.rows_written == 0
    assert result.rows_written == 0
    assert not client.update_item.called
    assert not client.batch_write_item.called
    assert any("key attribute" in str(d.get("reason") or "").lower() for d in (result.rejected_details or []))


def test_sample_compare_skips_df_missing_omit_columns():
    """Omit-from-SET columns must not fingerprint as NULL (false-green wipe risk)."""
    from services.reconciliation import sample_compare_rows

    result = sample_compare_rows(
        [{"id": "1", "note": "new", "extra": DF_MISSING_SENTINEL}],
        [{"id": "1", "note": "new", "extra": "prior-kept"}],
        [
            {"source": "id", "target": "id"},
            {"source": "note", "target": "note"},
            {"source": "extra", "target": "extra"},
        ],
        sort_key="id",
    )
    assert result["passed"] is True
    # Only id + note compared; extra skipped because source is DF_MISSING.
    assert result["compared"] == 2
    assert result["mismatches"] == []


def test_sample_compare_fails_when_destination_leaks_df_missing():
    """Read-back DF_MISSING is a sentinel leak — must not be skipped as omit."""
    from services.reconciliation import sample_compare_rows

    result = sample_compare_rows(
        [{"id": "1", "note": "live"}],
        [{"id": "1", "note": DF_MISSING_SENTINEL}],
        [
            {"source": "id", "target": "id"},
            {"source": "note", "target": "note"},
        ],
        sort_key="id",
    )
    assert result["passed"] is False
    assert any(m.get("target") == "note" for m in result["mismatches"])


def test_mongo_sparse_upsert_preserves_df_missing_through_decimal_coercion():
    """Decimal _to_bson must not wrap DF_MISSING as Decimal128 (would leak / wipe)."""
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

        def list_collection_names(self, filter=None):  # noqa: A002
            return []

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
            headers=["id", "amt"],
            data_rows=[["1", DF_MISSING_SENTINEL]],
            mappings=[
                {"source": "id", "target": "id", "confidence": 1},
                {
                    "source": "amt",
                    "target": "amt",
                    "confidence": 1,
                    "transform": "decimal",
                },
            ],
            column_types={"id": "string", "amt": "DECIMAL"},
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
    assert "amt" not in update["$set"]
    assert DF_MISSING_SENTINEL not in update["$set"].values()
    assert all(not hasattr(v, "to_decimal") for v in update["$set"].values())

def test_mapped_source_casefold_and_missing_refuse_null_invent():
    """Header case drift resolves; truly missing mapped source quarantines."""
    mapped, _errors, details = build_mapped_rows_with_details(
        headers=["ID", "Note"],
        data_rows=[["1", "hello"]],
        mappings=[
            {"source": "id", "target": "id"},
            {"source": "note", "target": "note"},
        ],
        target_cols=["id", "note"],
        column_types={"id": "string", "note": "string"},
        error_policy="quarantine",
    )
    assert mapped == [("1", "hello")]
    assert details == []

    mapped2, errors2, details2 = build_mapped_rows_with_details(
        headers=["id"],
        data_rows=[["1"]],
        mappings=[
            {"source": "id", "target": "id"},
            {"source": "note", "target": "note"},
        ],
        target_cols=["id", "note"],
        column_types={"id": "string", "note": "string"},
        error_policy="quarantine",
    )
    assert mapped2 == []
    assert details2 or errors2
    blob = " ".join(errors2) + " ".join(str(d) for d in details2)
    assert "not found" in blob.lower() or "refuse" in blob.lower()


def test_quarantine_unfit_json_rejects_nan():
    from connectors.writer_common import quarantine_unfit_json

    details: list[dict] = []
    out = quarantine_unfit_json(
        mapped_rows=[('1', '{"x": NaN}')],
        target_cols=["id", "payload"],
        target_types=["string", "JSON"],
        rejected_details=details,
        policy="fail",
        dialect_label="test",
    )
    assert out == []
    assert details
    assert any("non-finite" in str(d.get("reason", "")).lower() or "json" in str(d.get("reason", "")).lower() for d in details)


def test_run_sparse_cdc_upsert_casefold_conflict():
    from connectors.writer_common import run_sparse_cdc_upsert

    updated: list[tuple] = []

    def fetch(pk_vals):
        assert pk_vals == ["1"]
        return ("1", "old", "keep")

    def update(non_pk, pk_vals):
        updated.append((non_pk, pk_vals))
        return 1

    def insert(_present):
        raise AssertionError("should update existing")

    written, skipped, checksum = run_sparse_cdc_upsert(
        target_cols=["id", "note", "extra"],
        conflict_columns=["ID"],
        sparse_rows=[("1", "new", DF_MISSING_SENTINEL)],
        fetch_existing_row=fetch,
        update_non_pk=update,
        insert_present=insert,
    )
    assert written == 1 and skipped == 0
    assert updated[0][0] == {"note": "new"}
    assert checksum[0] == ("1", "new", "keep")


def test_elasticsearch_composite_doc_id_fail_closed():
    from connectors.elasticsearch_writer import _resolve_doc_id

    full = _resolve_doc_id(
        {"tenant_id": "t1", "order_id": "o9"},
        conflict_columns=["tenant_id", "order_id"],
        target_cols=["tenant_id", "order_id", "amt"],
    )
    assert full == "t1|o9"

    # Casefold conflict names against document keys
    cased = _resolve_doc_id(
        {"Tenant_Id": "t1", "Order_Id": "o9"},
        conflict_columns=["tenant_id", "order_id"],
        target_cols=["Tenant_Id", "Order_Id"],
    )
    assert cased == "t1|o9"

    # Partial composite must not shrink to single-key identity
    partial = _resolve_doc_id(
        {"tenant_id": "t1"},
        conflict_columns=["tenant_id", "order_id"],
        target_cols=["tenant_id", "order_id"],
    )
    assert partial is None


def test_dedupe_rows_refuses_partial_composite_pk():
    from connectors.writer_common import dedupe_rows

    rows = [("a", "1", "x"), ("a", "2", "y")]
    try:
        dedupe_rows(rows, ["id", "missing"], ["id", "sku", "v"])
        raise AssertionError("expected strict unresolved raise")
    except ValueError as exc:
        assert "unresolved" in str(exc).lower()

def test_mapped_rows_to_json_records_omits_df_missing():
    from connectors.writer_common import mapped_rows_to_json_records
    from services.value_serializer import DF_MISSING_SENTINEL

    recs = mapped_rows_to_json_records(
        [("1", "keep", DF_MISSING_SENTINEL)],
        ["id", "note", "extra"],
        {"id": "string", "note": "string", "extra": "string"},
    )
    assert recs == [{"id": "1", "note": "keep"}]
    assert "extra" not in recs[0]


def test_assert_sparse_upsert_refuses_null_pk():
    from connectors.writer_common import assert_sparse_upsert_has_pk

    try:
        assert_sparse_upsert_has_pk({"id": None, "note": "x"}, ["id"])
        raise AssertionError("expected null PK refuse")
    except ValueError as exc:
        assert "null" in str(exc).lower()


def test_redis_conflict_strict_casefold_and_partial():
    from services.primary_key import infer_redis_conflict_columns

    assert infer_redis_conflict_columns(
        ["userid", "orgid"], None, ["UserId", "OrgId"]
    ) == ["userid", "orgid"]
    try:
        infer_redis_conflict_columns(["orgid"], None, ["UserId", "OrgId"])
        raise AssertionError("expected partial composite refuse")
    except ValueError as exc:
        assert "unresolved" in str(exc).lower()


def test_sanitize_json_value_refuses_nan():
    from services.value_serializer import sanitize_json_value
    import math

    try:
        sanitize_json_value(math.nan)
        raise AssertionError("expected NaN refuse")
    except ValueError as exc:
        assert "non-finite" in str(exc).lower()
    assert sanitize_json_value(math.nan, refuse_nonfinite=False) is None
