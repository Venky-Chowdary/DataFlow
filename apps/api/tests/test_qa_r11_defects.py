"""Round 11 regressions: a failed replay must not replace landed rows.

R11-001 is a header-only S3 promote that deleted four rows that had already
landed. The same class of replace is covered for Postgres and a SQLite file.
R7-001 keeps a Postgres NULL distinct from an empty CSV cell. R8-003 keeps an
empty object-store integer as SQL NULL instead of a quarantined cast. R4-D02
keeps a bad cast at the end of a file from failing the rows above it.
"""

from __future__ import annotations

import sqlite3
import uuid

import pytest

from services.value_serializer import SQL_NULL_SENTINEL


_LANDED = [(1, "a"), (2, "b"), (3, "c"), (4, "d")]


def test_s3_promote_failure_leaves_the_landed_object():
    """Copy onto the live key is the only mutation. A failed copy deletes staging."""
    from connectors.object_store_common import ObjectStoreExport
    from connectors.object_store_multipart import land_object_store_export

    live = b"id,name\n1,a\n2,b\n3,c\n4,d\n"
    store = {"exports/people.csv": live}

    class _S3:
        def put_object(self, *, Bucket, Key, Body, ContentType):
            store[Key] = bytes(Body)

        def head_object(self, *, Bucket, Key):
            if Key not in store:
                raise KeyError(Key)
            return {"ContentLength": len(store[Key])}

        def copy_object(self, **_kwargs):
            raise RuntimeError("promote failed mid-replay")

        def delete_object(self, *, Bucket, Key):
            store.pop(Key, None)

    body = b"id,name\n"
    export = ObjectStoreExport(
        content_type="text/csv", size=len(body), spilled=False, _spool=_Bytes(body)
    )
    with pytest.raises(RuntimeError, match="promote failed"):
        land_object_store_export(
            "s3",
            export=export,
            staging_key="exports/people.csv.__df_staging__",
            live_key="exports/people.csv",
            client=_S3(),
            bucket="qa",
            content_type="text/csv",
        )
    assert store["exports/people.csv"] == live
    assert "exports/people.csv.__df_staging__" not in store


def test_s3_header_only_replay_does_not_replace_landed_rows():
    from connectors.s3_writer import write_mapped_rows

    live = b"id,name\n1,a\n2,b\n3,c\n4,d\n"
    store = {"exports/people.csv": live}
    puts: list[str] = []

    class _S3:
        def head_bucket(self, **_kwargs):
            return {}

        def head_object(self, *, Bucket, Key):
            if Key not in store:
                from botocore.exceptions import ClientError

                raise ClientError(
                    {"Error": {"Code": "404", "Message": "missing"}}, "HeadObject"
                )
            return {"ContentLength": len(store[Key])}

        def put_object(self, *, Bucket, Key, Body, ContentType):
            puts.append(Key)
            store[Key] = bytes(Body)

        def copy_object(self, **kwargs):
            source = kwargs["CopySource"]["Key"]
            store[kwargs["Key"]] = store[source]

        def delete_object(self, *, Bucket, Key):
            store.pop(Key, None)

    from unittest.mock import patch

    with patch("connectors.s3_writer.boto3_client", return_value=_S3()):
        result = write_mapped_rows(
            host="",
            port=0,
            database="qa",
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=False,
            table_name="exports/people.csv",
            headers=["id", "name"],
            data_rows=[],
            mappings=[
                {"source": "id", "target": "id", "target_type": "INTEGER"},
                {"source": "name", "target": "name", "target_type": "TEXT"},
            ],
            column_types={"id": "INTEGER", "name": "TEXT"},
            sync_mode="incremental_deduped",
            create_table=False,
        )
    assert result.ok is False
    assert "left untouched" in (result.error or "")
    assert store["exports/people.csv"] == live
    assert puts == []


def test_sqlite_publish_failure_keeps_the_landed_table(tmp_path):
    from connectors.sqlite_writer import _publish_sqlite_stage
    from connectors.sql_identifiers import quote_sql_identifier

    path = tmp_path / "landed.db"
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE live (id INTEGER, name TEXT)")
    conn.executemany("INSERT INTO live VALUES (?, ?)", _LANDED)
    conn.execute("CREATE TABLE stage (id INTEGER, name TEXT)")
    conn.execute("INSERT INTO stage VALUES (9, 'partial')")
    conn.commit()
    with pytest.raises(sqlite3.OperationalError):
        _publish_sqlite_stage(
            conn,
            stage_quoted=quote_sql_identifier("stage"),
            stage_name="stage",
            live_name="live",
            columns=["id", "missing"],
        )
    rows = conn.execute("SELECT id, name FROM live ORDER BY id").fetchall()
    conn.close()
    assert rows == _LANDED


def test_postgres_replay_failure_keeps_landed_rows():
    import psycopg2

    from connectors.postgresql_writer import write_mapped_rows

    table = "r11_replay_" + uuid.uuid4().hex[:8]
    kwargs = dict(
        host="127.0.0.1",
        port=5432,
        database="dataflow",
        username="dataflow",
        password="dataflow",
        schema="public",
        connection_string="",
        ssl=False,
        table_name=table,
        headers=["id", "name"],
        mappings=[
            {"source": "id", "target": "id", "target_type": "INTEGER"},
            {"source": "name", "target": "name", "target_type": "TEXT"},
        ],
        column_types={"id": "INTEGER", "name": "TEXT"},
    )
    conn = psycopg2.connect(
        host="127.0.0.1",
        port=5432,
        dbname="dataflow",
        user="dataflow",
        password="dataflow",
    )
    try:
        first = write_mapped_rows(
            **kwargs,
            data_rows=[[str(i), name] for i, name in _LANDED],
            create_table=True,
            write_mode="insert",
        )
        assert first.ok, first.error
        boom = conn.cursor()
        boom.execute(
            f"""
            CREATE OR REPLACE FUNCTION {table}_boom() RETURNS trigger AS $$
            BEGIN
              RAISE EXCEPTION 'disk full mid-replay';
            END;
            $$ LANGUAGE plpgsql
            """
        )
        boom.execute(
            f"""
            CREATE TRIGGER {table}_boom
            BEFORE INSERT ON public."{table}"
            FOR EACH ROW EXECUTE FUNCTION {table}_boom()
            """
        )
        conn.commit()
        second = write_mapped_rows(
            **kwargs,
            data_rows=[["9", "replay"]],
            create_table=False,
            write_mode="upsert",
            conflict_columns=["id"],
            sync_mode="incremental_deduped",
        )
        assert second.ok is False
        assert "disk full" in (second.error or "").lower() or second.rows_written == 0
        check = conn.cursor()
        check.execute(f'SELECT id, name FROM public."{table}" ORDER BY id')
        assert check.fetchall() == _LANDED
    finally:
        try:
            conn.rollback()
        except Exception:
            pass
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(f'DROP TABLE IF EXISTS public."{table}" CASCADE')
        cur.execute(f"DROP FUNCTION IF EXISTS {table}_boom()")
        conn.close()


def test_postgres_null_csv_readback_is_not_an_empty_string():
    """SQL NULL and ``""`` stay distinct on an S3 CSV export (R7-001)."""
    from connectors.object_store_materialize import materialize_object_store_export
    from services.csv_profiler import iter_csv_dicts
    from services.reconciliation import fingerprint_for_reconcile

    mat = materialize_object_store_export(
        key="people.csv",
        headers=["id", "name"],
        data_rows=[["1", None], ["2", ""]],
        mappings=[
            {"source": "id", "target": "id"},
            {"source": "name", "target": "name"},
        ],
        target_cols=["id", "name"],
        column_types={"id": "INTEGER", "name": "TEXT"},
        dest_types={"id": "INTEGER", "name": "TEXT"},
        error_policy="quarantine",
        dest_kind="s3",
        dialect_label="S3",
        spill_max_size=1024 * 1024,
        dest_db_type="s3",
    )
    assert mat.abort_error is None
    assert mat.rows_written == 2
    body = mat.export.read_all()
    mat.export.close()
    text = body.decode("utf-8")
    # C04 codec contract: NULL is an empty *unquoted* cell, a real '' stays
    # quoted "" — the distinction rides on quoting, no sentinel in the file.
    assert SQL_NULL_SENTINEL not in text
    assert "\x00NULL\x00" not in text
    assert '\n1,\n' in text and '\n2,""\n' in text
    import io as _io
    from services.csv_codec import iter_csv_records

    recs = list(iter_csv_records(_io.StringIO(text)))
    assert recs[1][1] is None
    assert recs[2][1] == ""
    list(iter_csv_dicts(body))
    source_null = fingerprint_for_reconcile(None, ddl_type="TEXT", engine="postgresql")
    dest_null = fingerprint_for_reconcile(recs[1][1], ddl_type="TEXT", engine="s3")
    dest_empty = fingerprint_for_reconcile(recs[2][1], ddl_type="TEXT", engine="s3")
    from services.reconciliation import operator_cell_text

    assert operator_cell_text(source_null) == "null"
    assert "\x00" not in operator_cell_text(source_null)
    assert dest_null == source_null
    assert dest_empty == ""
    assert dest_empty != dest_null


def test_empty_object_store_integer_is_null_not_quarantine():
    """An empty stock cell on S3→S3 is absence, not a failed integer cast (R8-003)."""
    from connectors.writer_common import build_mapped_rows_with_details
    from services.preflight_source_kind import source_is_record_batch

    assert source_is_record_batch("database", "s3") is True
    mapped, errors, details = build_mapped_rows_with_details(
        headers=["sku", "stock"],
        data_rows=[["A", "4"], ["B", ""]],
        mappings=[
            {"source": "sku", "target": "sku", "target_type": "TEXT"},
            {
                "source": "stock",
                "target": "stock",
                "target_type": "INTEGER",
                "transform": "integer",
            },
        ],
        target_cols=["sku", "stock"],
        column_types={"sku": "TEXT", "stock": "INTEGER"},
        dest_types={"sku": "TEXT", "stock": "INTEGER"},
        error_policy="quarantine",
        empty_cells_as_null=True,
        dest_kind="s3",
    )
    assert errors == []
    assert details == []
    assert mapped == [("A", 4), ("B", None)]


def test_bad_rows_at_the_end_of_a_file_do_not_drop_the_prefix(tmp_path):
    """Bad casts after a good prefix quarantine those rows and keep the rest (R4-D02)."""
    from src.transfer.file_stream import stream_file_to_database
    from src.transfer.models import EndpointConfig
    from services.preflight_service import run_file_preflight

    lines = ["id,qty"] + [f"{i},{i}" for i in range(1, 9)] + ["9,bad", "10,also-bad"]
    csv_path = tmp_path / "pii2.csv"
    csv_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    sample = [{"id": str(i), "qty": str(i)} for i in range(1, 9)]
    sample.append({"id": "9", "qty": "bad"})
    sample.append({"id": "10", "qty": "also-bad"})
    result = run_file_preflight(
        columns=["id", "qty"],
        column_types={"id": "INTEGER", "qty": "INTEGER"},
        row_count=len(sample),
        mappings=[
            {"source": "id", "target": "id", "target_type": "INTEGER", "confidence": 0.99},
            {
                "source": "qty",
                "target": "qty",
                "target_type": "INTEGER",
                "transform": "integer",
                "confidence": 0.99,
            },
        ],
        destination_connected=True,
        sample_rows=sample,
        estimated_bytes=256,
        destination_column_types={"id": "INTEGER", "qty": "INTEGER"},
        destination_table_exists=True,
        destination_db_type="sqlite",
        source_kind="file",
        skip_population_fit=True,
    )
    decision = (result.get("proof_bundle") or {}).get("transfer_decision") or {}
    reason = str(decision.get("reason") or "")
    assert "Sample transform / cast failures" not in reason
    assert "Lossy / fidelity collapse" not in reason
    assert decision.get("decision") != "block"

    db_path = tmp_path / "out.db"
    written, _ddl, summary, _columns = stream_file_to_database(
        str(csv_path),
        "pii2.csv",
        EndpointConfig(
            kind="database",
            format="sqlite",
            database=str(db_path),
            table="pii2",
        ),
        mappings=[
            {"source": "id", "target": "id", "target_type": "INTEGER", "confidence": 0.99},
            {
                "source": "qty",
                "target": "qty",
                "target_type": "INTEGER",
                "transform": "integer",
                "confidence": 0.99,
            },
        ],
        schema={"id": "INTEGER", "qty": "INTEGER"},
        validation_mode="balanced",
        skip_preflight=True,
    )
    assert written == 8
    assert int(summary.get("rejected_rows") or 0) == 2
    with sqlite3.connect(db_path) as out:
        rows = out.execute("SELECT id, qty FROM pii2 ORDER BY id").fetchall()
    assert rows == [(i, i) for i in range(1, 9)]


def test_new_tenant_egress_allowlist_is_off_and_metadata_cannot_be_saved(tmp_path, monkeypatch):
    import services.tenant_store as store

    monkeypatch.setattr(store, "STORE_PATH", tmp_path / "tenants.json")
    tenant = store.create_tenant("ws-r11", "QA")
    assert tenant.egress_allowlist == []
    updated = store.update_tenant(tenant.id, egress_allowlist=["192.168.0.0/16"])
    assert updated is not None
    assert updated.egress_allowlist == ["192.168.0.0/16"]
    with pytest.raises(ValueError, match="metadata or link-local"):
        store.update_tenant(tenant.id, egress_allowlist=["169.254.169.254"])


class _Bytes:
    """Minimal spool for a staged export that is already in memory."""

    def __init__(self, payload: bytes):
        self._payload = payload
        self._pos = 0

    def read(self, n=-1):
        if n is None or n < 0:
            n = len(self._payload) - self._pos
        chunk = self._payload[self._pos : self._pos + n]
        self._pos += len(chunk)
        return chunk

    def seek(self, pos, _whence=0):
        self._pos = pos
        return pos

    def tell(self):
        return self._pos

    def close(self):
        return None
