"""Configure-dialog uploads are files Test can open.

A typed path that is not on this server stays a Path-not-found error.
``uploaded_file_id`` from the same upload the dialog stored resolves to that
file for CSV and SQLite. A path that already exists wins over the attachment.
"""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

from services.file_parser import (
    publish_upload_path,
    resolve_attached_upload_path,
    store_upload,
)
from src.routers.connectors_router import TestConnectionRequest, _test_connection_impl
from src.transfer.connector_registry import locate_connector_file, probe_file_source


class _Op:
    def update(self, **_kwargs):
        return None

    def failed(self, **_kwargs):
        return None

    def succeeded(self, **_kwargs):
        return None


def test_csv_attachment_is_the_file_test_opens(tmp_path):
    body = b"order_id,sku\n1,a\n2,b\n"
    stored = store_upload("mahi_qa_sample.csv", body)
    path = publish_upload_path(stored["path"])
    assert path
    assert Path(path).read_bytes() == body
    assert resolve_attached_upload_path(stored["file_id"]) == path

    missing = str(tmp_path / "not-on-the-server.csv")
    assert locate_connector_file(
        connection_string=missing,
        extra={"uploaded_file_id": stored["file_id"]},
    ) == path
    ok, message = probe_file_source("csv", path)
    assert ok is True
    assert "2 rows" in message

    ok, message = probe_file_source("csv", missing)
    assert ok is False
    assert "Path not found" in message


def test_remote_uri_is_not_replaced_by_the_attachment():
    stored = store_upload("attached.csv", b"order_id\n1\n")
    assert locate_connector_file(
        connection_string="s3://bucket/data.csv",
        extra={"uploaded_file_id": stored["file_id"]},
    ) == "s3://bucket/data.csv"


def test_existing_typed_path_wins_over_the_attachment(tmp_path):
    stored = store_upload("attached.csv", b"order_id\n1\n")
    local = tmp_path / "local.csv"
    local.write_text("order_id\n9\n", encoding="utf-8")
    assert locate_connector_file(
        connection_string=str(local),
        extra={"uploaded_file_id": stored["file_id"]},
    ) == str(local)


def test_other_workspace_upload_is_not_readable(monkeypatch):
    stored = store_upload("private.csv", b"order_id\n1\n")
    from services.file_parser import get_file

    get_file(stored["file_id"])["workspace_id"] = "other-tenant"
    monkeypatch.setattr(
        "services.workspace_filter.bound_allows", lambda _owner: False
    )
    assert resolve_attached_upload_path(stored["file_id"]) == ""
    assert locate_connector_file(
        connection_string="",
        extra={"uploaded_file_id": stored["file_id"]},
    ) == ""


def test_saved_csv_test_uses_the_attachment_not_the_missing_path():
    stored = store_upload("orders.csv", b"order_id,sku\n1,a\n")
    result = asyncio.run(
        _test_connection_impl(
            TestConnectionRequest(
                type="csv",
                connection_string="/workspace/missing/mahi_qa_sample.csv",
                extra={"uploaded_file_id": stored["file_id"]},
            ),
            _Op(),
        )
    )
    assert result["success"] is True
    assert "1 rows" in result["message"]
    assert result["details"]["path"] == stored["path"]


def test_saved_sqlite_test_opens_the_attached_database():
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "shop.sqlite"
        conn = sqlite3.connect(db)
        conn.execute("create table orders (order_id integer primary key, sku text)")
        conn.execute("insert into orders values (1, 'a')")
        conn.commit()
        conn.close()
        stored = store_upload("shop.sqlite", db.read_bytes())
    result = asyncio.run(
        _test_connection_impl(
            TestConnectionRequest(
                type="sqlite",
                database="/workspace/missing/shop.sqlite",
                extra={"uploaded_file_id": stored["file_id"]},
            ),
            _Op(),
        )
    )
    assert result["success"] is True
    assert "orders" in result["message"] or "SQLite connected" in result["message"]


def test_json_excel_and_tsv_uploads_keep_their_bytes():
    tsv = store_upload("rows.tsv", b"order_id\tsku\n1\ta\n")
    js = store_upload("rows.json", b'[{"order_id": 1, "sku": "a"}]')
    assert publish_upload_path(tsv["path"]).endswith("rows.tsv") or "rows.tsv" in tsv["path"]
    assert Path(js["path"]).read_bytes().startswith(b"[")
    ok, message = probe_file_source("tsv", tsv["path"])
    assert ok is True
    assert "1 rows" in message
    ok, message = probe_file_source("json", js["path"])
    assert ok is True
