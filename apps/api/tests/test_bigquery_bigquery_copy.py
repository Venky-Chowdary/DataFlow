"""BigQuery → BigQuery CTAS / INSERT SELECT — dest COUNT(*)."""

from __future__ import annotations

import socket
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.copy_bigquery_bigquery import (  # noqa: E402
    bigquery_bigquery_copy_enabled,
    copy_bigquery_to_bigquery,
)
from services.copy_bigquery_common import (  # noqa: E402
    bigquery_family_name,
    bigquery_type_is_copy_safe,
)
from services.copy_fast_path import FastPathUnavailable  # noqa: E402
from services.dest_precount import destination_row_count  # noqa: E402


def _bq_or_skip() -> None:
    try:
        with socket.create_connection(("127.0.0.1", 9050), timeout=1):
            pass
    except OSError:
        pytest.skip("BigQuery emulator 9050 not reachable")


def _bq_cfg(table: str) -> dict:
    return {
        "type": "bigquery",
        "format": "bigquery",
        "host": "127.0.0.1",
        "port": 9050,
        "database": "dataflow-test",
        "schema": "dataflow",
        "table": table,
        "connection_string": "http://127.0.0.1:9050",
    }


def _client():
    _bq_or_skip()
    from connectors.bigquery_conn import get_client

    return get_client(
        project_id="dataflow-test",
        host="127.0.0.1",
        port=9050,
        connection_string="http://127.0.0.1:9050",
    )


def _run(sql: str) -> None:
    from services.dest_precount import _bigquery_run_job

    client = _client()
    try:
        _bigquery_run_job(client, sql)
    finally:
        try:
            client.close()
        except Exception:
            pass


def _drop(name: str) -> None:
    _run(f"DROP TABLE IF EXISTS `dataflow-test`.`dataflow`.`{name}`")


def _dest_count(name: str) -> int:
    n = destination_row_count(
        "bigquery", _bq_cfg(name), schema="dataflow", table_name=name
    )
    assert n is not None
    return int(n)


def _seed(name: str, rows: int) -> None:
    _drop(name)
    _run(f"CREATE TABLE `dataflow-test`.`dataflow`.`{name}` (id INT64, label STRING)")
    batch = 100
    for start in range(1, rows + 1, batch):
        end = min(start + batch - 1, rows)
        values = ", ".join(f"({i}, 'r{i}')" for i in range(start, end + 1))
        _run(
            f"INSERT INTO `dataflow-test`.`dataflow`.`{name}` (id, label) VALUES {values}"
        )


def test_bigquery_family_and_copy_safe_types():
    assert bigquery_family_name("bigquery") == "bigquery"
    assert bigquery_family_name("google_bigquery") == "bigquery"
    assert bigquery_family_name("bigquery_us") == "bigquery"
    assert bigquery_family_name("bigquery_eu") == "bigquery"
    assert bigquery_type_is_copy_safe("INT64") is True
    assert bigquery_type_is_copy_safe("INTEGER") is True
    assert bigquery_type_is_copy_safe("STRING") is True
    assert bigquery_type_is_copy_safe("NUMERIC") is True
    assert bigquery_type_is_copy_safe("JSON") is True
    assert bigquery_type_is_copy_safe("GEOGRAPHY") is False
    assert bigquery_type_is_copy_safe("STRUCT") is False
    assert bigquery_type_is_copy_safe("INTERVAL") is False


def test_bigquery_bigquery_copy_kill_switch(monkeypatch):
    monkeypatch.setenv("DATAFLOW_BIGQUERY_BIGQUERY_COPY", "0")
    assert bigquery_bigquery_copy_enabled() is False
    with pytest.raises(FastPathUnavailable, match="disabled"):
        copy_bigquery_to_bigquery(
            source_cfg=_bq_cfg("missing"),
            source_table="missing",
            dest_cfg=_bq_cfg("nope"),
            dest_table="nope",
            pairs=[("id", "id")],
            bigquery_ddls=["INT64"],
            replace_destination=True,
        )


def test_bigquery_bigquery_same_table_declines(monkeypatch):
    monkeypatch.delenv("DATAFLOW_BIGQUERY_BIGQUERY_COPY", raising=False)
    cfg = _bq_cfg("same_table")
    with pytest.raises(FastPathUnavailable, match="same table"):
        copy_bigquery_to_bigquery(
            source_cfg=cfg,
            source_table="same_table",
            dest_cfg=cfg,
            dest_table="same_table",
            pairs=[("id", "id")],
            bigquery_ddls=["INT64"],
            replace_destination=True,
        )


def test_bigquery_bigquery_public_proxy_declines():
    dest = {
        **_bq_cfg("b"),
        "host": "proxy.rlwy.net",
        "connection_string": "https://proxy.rlwy.net:9050",
    }
    with pytest.raises(FastPathUnavailable, match="public proxy"):
        copy_bigquery_to_bigquery(
            source_cfg=_bq_cfg("a"),
            source_table="a",
            dest_cfg=dest,
            dest_table="b",
            pairs=[("id", "id")],
            bigquery_ddls=["INT64"],
            replace_destination=True,
        )


def test_bigquery_bigquery_rename_declines(monkeypatch):
    monkeypatch.delenv("DATAFLOW_BIGQUERY_BIGQUERY_COPY", raising=False)
    with pytest.raises(FastPathUnavailable, match="rename"):
        copy_bigquery_to_bigquery(
            source_cfg=_bq_cfg("a"),
            source_table="a",
            dest_cfg=_bq_cfg("b"),
            dest_table="b",
            pairs=[("id", "emp_id")],
            bigquery_ddls=["INT64"],
            replace_destination=True,
        )


def test_live_bigquery_bigquery_dest_count(monkeypatch):
    monkeypatch.delenv("DATAFLOW_BIGQUERY_BIGQUERY_COPY", raising=False)
    tag = uuid.uuid4().hex[:8]
    src = f"bq_copy_src_{tag}"
    dest = f"bq_copy_dst_{tag}"
    try:
        _seed(src, 800)
        _drop(dest)
        result = copy_bigquery_to_bigquery(
            source_cfg=_bq_cfg(src),
            source_table=src,
            dest_cfg=_bq_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            bigquery_ddls=["INT64", "STRING"],
            replace_destination=True,
        )
        assert result.source_rows == 800
        assert result.target_rows == 800
        assert result.source_snapshot.get("copy_split") == "serial"
        assert result.source_snapshot.get("bigquery_read") == "insert_select"
        assert result.source_snapshot.get("bigquery_write") == "insert"
        assert _dest_count(dest) == 800
        assert _dest_count(src) == 800
    finally:
        _drop(src)
        _drop(dest)


def test_live_bigquery_bigquery_not_merge_or_insert_rows(monkeypatch):
    monkeypatch.delenv("DATAFLOW_BIGQUERY_BIGQUERY_COPY", raising=False)
    tag = uuid.uuid4().hex[:8]
    src = f"bq_copy_merge_{tag}"
    dest = f"bq_copy_merge_dst_{tag}"
    _seed(src, 80)
    _drop(dest)

    from services import copy_bigquery_bigquery as mod

    orig_run = mod.bigquery_run_sql

    def _wrap_run(client, sql: str) -> None:
        text = str(sql).upper()
        if "MERGE " in f" {text} " and text.lstrip().startswith("MERGE"):
            raise AssertionError("BigQuery identity COPY must not MERGE")
        if " CLONE " in f" {text} ":
            raise AssertionError("BigQuery identity COPY must not CLONE")
        if text.lstrip().startswith("COPY "):
            raise AssertionError("BigQuery identity COPY must not COPY")
        if "LOAD DATA" in text:
            raise AssertionError("BigQuery identity COPY must not LOAD DATA")
        return orig_run(client, sql)

    monkeypatch.setattr(mod, "bigquery_run_sql", _wrap_run)

    from google.cloud.bigquery import Client as BQClient

    orig_insert = BQClient.insert_rows_json

    def _no_insert(self, *a, **k):
        raise AssertionError("BigQuery identity COPY must not insert_rows_json")

    monkeypatch.setattr(BQClient, "insert_rows_json", _no_insert)
    try:
        result = copy_bigquery_to_bigquery(
            source_cfg=_bq_cfg(src),
            source_table=src,
            dest_cfg=_bq_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            bigquery_ddls=["INT64", "STRING"],
            replace_destination=True,
        )
        assert result.target_rows == 80
        assert result.source_snapshot.get("bigquery_read") == "insert_select"
        assert _dest_count(dest) == 80
    finally:
        monkeypatch.setattr(BQClient, "insert_rows_json", orig_insert)
        _drop(src)
        _drop(dest)


def test_live_bigquery_bigquery_empty_string_and_null_preserved():
    tag = uuid.uuid4().hex[:8]
    src = f"bq_copy_null_{tag}"
    dest = f"bq_copy_null_dst_{tag}"
    try:
        _drop(src)
        _drop(dest)
        _run(
            f"CREATE TABLE `dataflow-test`.`dataflow`.`{src}` (id INT64, label STRING)"
        )
        _run(
            f"""INSERT INTO `dataflow-test`.`dataflow`.`{src}` (id, label) VALUES
                (1, NULL), (2, ''), (3, 'x')"""
        )
        result = copy_bigquery_to_bigquery(
            source_cfg=_bq_cfg(src),
            source_table=src,
            dest_cfg=_bq_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            bigquery_ddls=["INT64", "STRING"],
            replace_destination=True,
        )
        assert result.source_rows == 3
        from services.dest_precount import _bigquery_run_query

        client = _client()
        try:
            rows = list(
                _bigquery_run_query(
                    client,
                    f"SELECT id, label FROM `dataflow-test`.`dataflow`.`{dest}` ORDER BY id",
                )
            )
        finally:
            client.close()
        assert rows[0][1] is None
        assert rows[1][1] == ""
        assert rows[2][1] == "x"
    finally:
        _drop(src)
        _drop(dest)


def test_live_bigquery_bigquery_skip_when_dest_count_matches():
    tag = uuid.uuid4().hex[:8]
    src = f"bq_copy_skip_{tag}"
    dest = f"bq_copy_skip_dst_{tag}"
    try:
        _seed(src, 800)
        _drop(dest)
        first = copy_bigquery_to_bigquery(
            source_cfg=_bq_cfg(src),
            source_table=src,
            dest_cfg=_bq_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            bigquery_ddls=["INT64", "STRING"],
            replace_destination=False,
        )
        assert first.target_rows == 800
        second = copy_bigquery_to_bigquery(
            source_cfg=_bq_cfg(src),
            source_table=src,
            dest_cfg=_bq_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            bigquery_ddls=["INT64", "STRING"],
            replace_destination=False,
        )
        assert second.source_snapshot.get("copy_split") == "skip"
        assert second.source_snapshot.get("partitions_skipped") == 1
        assert _dest_count(dest) == 800
    finally:
        _drop(src)
        _drop(dest)


def test_live_bigquery_bigquery_occupied_mismatch_declines():
    tag = uuid.uuid4().hex[:8]
    src = f"bq_copy_occ_{tag}"
    dest = f"bq_copy_occ_dst_{tag}"
    try:
        _seed(src, 800)
        _drop(dest)
        _run(
            f"CREATE TABLE `dataflow-test`.`dataflow`.`{dest}` (id INT64, label STRING)"
        )
        _run(
            f"INSERT INTO `dataflow-test`.`dataflow`.`{dest}` (id, label) "
            "VALUES (1, 'ghost'), (2, 'ghost')"
        )
        assert _dest_count(dest) == 2
        with pytest.raises(FastPathUnavailable, match="occupied BigQuery dest"):
            copy_bigquery_to_bigquery(
                source_cfg=_bq_cfg(src),
                source_table=src,
                dest_cfg=_bq_cfg(dest),
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                bigquery_ddls=["INT64", "STRING"],
                replace_destination=False,
            )
        assert _dest_count(dest) == 2
    finally:
        _drop(src)
        _drop(dest)


def test_live_bigquery_bigquery_overwrite_replaces_dest():
    tag = uuid.uuid4().hex[:8]
    src = f"bq_copy_ow_{tag}"
    dest = f"bq_copy_ow_dst_{tag}"
    try:
        _seed(src, 800)
        _drop(dest)
        _run(
            f"CREATE TABLE `dataflow-test`.`dataflow`.`{dest}` (id INT64, label STRING)"
        )
        _run(
            f"INSERT INTO `dataflow-test`.`dataflow`.`{dest}` (id, label) VALUES (1, 'ghost')"
        )
        result = copy_bigquery_to_bigquery(
            source_cfg=_bq_cfg(src),
            source_table=src,
            dest_cfg=_bq_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            bigquery_ddls=["INT64", "STRING"],
            replace_destination=True,
        )
        assert result.source_snapshot.get("bigquery_write") == "overwrite"
        assert _dest_count(dest) == 800
    finally:
        _drop(src)
        _drop(dest)


def test_live_bigquery_bigquery_stream_load_method(monkeypatch):
    monkeypatch.delenv("DATAFLOW_BIGQUERY_BIGQUERY_COPY", raising=False)
    tag = uuid.uuid4().hex[:8]
    src = f"bq_copy_stream_{tag}"
    dest = f"bq_copy_stream_dst_{tag}"
    try:
        _seed(src, 800)
        _drop(dest)
        from services.million_row_proof import ensure_memory_job_store_if_mongo_down
        from services.mongodb_service import get_mongodb_service
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        ensure_memory_job_store_if_mongo_down()
        job_id = f"bq-bq-copy-{tag}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        source = EndpointConfig.from_dict("database", _bq_cfg(src))
        destination = EndpointConfig.from_dict("database", _bq_cfg(dest))
        mappings = [
            {"source": "id", "target": "id", "type": "INT64", "transform": "none"},
            {"source": "label", "target": "label", "type": "STRING", "transform": "none"},
        ]
        transferred, ddl_log, summary, _cols = stream_database_transfer(
            source,
            destination,
            mappings,
            {"id": "INT64", "label": "STRING"},
            sync_mode="full_refresh_overwrite",
            job_id=job_id,
        )
        assert transferred == 800
        assert summary.get("load_method") == "insert_select_bigquery_bigquery"
        assert summary.get("source_row_count") == 800
        assert int(summary.get("rejected_rows") or 0) == 0
        assert any("BigQuery" in line for line in ddl_log)
        assert _dest_count(dest) == 800
    finally:
        _drop(src)
        _drop(dest)


def test_live_bigquery_local_scan_total_is_list_not_num_rows(monkeypatch):
    """Row-path scan must not treat Table.num_rows as the snapshot size."""
    tag = uuid.uuid4().hex[:8]
    src = f"bq_scan_src_{tag}"
    try:
        _seed(src, 80)
        from google.cloud.bigquery.table import Table

        orig_num = Table.num_rows

        def _lie(_self):
            return 0

        monkeypatch.setattr(Table, "num_rows", property(_lie))
        from connectors.bigquery_reader import read_table_scan_batch

        scan_state: dict = {}
        rows = []
        offset = 0
        total = None
        while True:
            batch = read_table_scan_batch(
                host="127.0.0.1",
                port=9050,
                database="dataflow-test",
                username="",
                password="",
                schema="dataflow",
                connection_string="http://127.0.0.1:9050",
                ssl=False,
                table=src,
                columns=["id", "label"],
                offset=offset,
                limit=50,
                scan_state=scan_state,
            )
            total = batch.total_rows
            if not batch.rows:
                break
            rows.extend(batch.rows)
            offset += len(batch.rows)
            if len(rows) >= 80:
                break
        assert len(rows) == 80
        assert int(total or 0) == 80
    finally:
        monkeypatch.setattr(Table, "num_rows", orig_num)
        _drop(src)
