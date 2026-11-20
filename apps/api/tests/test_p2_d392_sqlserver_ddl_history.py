"""D392 against a faithful fake of SQL Server's CDC catalog.

Two facts from the SQL Server documentation the PR's fake did not model:

* ``ALTER TABLE ... ADD <column>`` on a CDC-enabled table is recorded in
  ``cdc.ddl_history``, but the existing capture instance does **not** start
  capturing the new column: ``cdc.captured_columns`` is unchanged until a new
  capture instance is created (``sys.sp_cdc_enable_table`` with another
  ``@capture_instance``; at most two per table).
  https://learn.microsoft.com/sql/relational-databases/track-changes/about-change-data-capture-sql-server
  ("Handling changes to source tables")
* ``cdc.ddl_history`` keeps one row per DDL change for as long as the capture
  instance exists; it is not emptied after the change is read.
  https://learn.microsoft.com/sql/relational-databases/system-tables/cdc-ddl-history-transact-sql

So a reader that treats "``ddl_history`` is not empty" as new DDL opens a
fresh review on every DML-only run of any table that ever had DDL (the D392
symptom, back again), and names the captured columns instead of the added
one. A review must open once per new ``ddl_lsn``, name the column the DDL
added, and an acknowledged change must never be raised again.
"""

from __future__ import annotations

import pytest

from connectors.sqlserver_cdc_native import SqlServerNativeCdc


class _Catalog:
    """cdc.captured_columns + cdc.ddl_history for one capture instance."""

    def __init__(self, columns):
        self.columns = list(columns)
        self.ddl: list[tuple[str, bytes]] = []
        self.fail_ddl_read = False

    def alter(self, command: str, lsn: int) -> None:
        self.ddl.append((command, lsn.to_bytes(10, "big")))

    def cursor(self):
        return _Cursor(self)


class _Cursor:
    def __init__(self, catalog: _Catalog):
        self.catalog = catalog
        self.mode = "cols"

    def execute(self, sql, params=None):
        self.mode = "ddl" if "ddl_history" in str(sql) else "cols"
        if self.mode == "ddl" and self.catalog.fail_ddl_read:
            raise RuntimeError("The SELECT permission was denied on the object 'ddl_history'")

    def fetchall(self):
        if self.mode == "ddl":
            # Newest first, as the reader asks.
            return sorted(self.catalog.ddl, key=lambda row: row[1], reverse=True)
        return list(self.catalog.columns)


def _reader():
    return SqlServerNativeCdc(
        {"host": "h", "database": "db", "connector_id": "src-ss"},
        table="orders",
        primary_key="id",
        schema="dbo",
        capture_instance="dbo_orders",
    )


@pytest.fixture
def stores(monkeypatch, tmp_path):
    import services.cdc_mapping_review as rev
    import services.cdc_schema_history as history

    path = tmp_path / "reviews.jsonl"
    monkeypatch.setattr(rev, "STORE_PATH", path)
    monkeypatch.setenv("CDC_MAPPING_REVIEW_STORE", str(path))
    stored: dict[tuple[str, str], list[dict]] = {}

    def list_history(source_key, table):
        return list(stored.get((source_key, table), []))

    def record_ddl(source_key, table, ddl, offset, schema_snapshot):
        entry = {
            "version": len(stored.get((source_key, table), [])) + 1,
            "ddl": ddl,
            "offset": offset,
            "schema_snapshot": schema_snapshot,
        }
        stored.setdefault((source_key, table), []).append(entry)
        return entry

    monkeypatch.setattr(history, "list_history", list_history)
    monkeypatch.setattr(history, "record_ddl", record_ddl)
    return rev, stored


def _run(catalog: _Catalog, offset: str) -> None:
    # Every scheduled run is a new process / reader instance.
    _reader()._maybe_record_capture_schema(catalog.cursor(), offset=offset)


def _open(rev):
    return rev.list_reviews(source_key=_reader().source_key, status="open")


COLS = [("id", "int", 1), ("name", "nvarchar", 2)]


def test_dml_runs_add_column_once_and_an_acknowledged_change_never_returns(stores):
    rev, _stored = stores
    catalog = _Catalog(COLS)
    for n in range(4):  # QA: four DML-only runs
        _run(catalog, f"0{n}")
        assert _open(rev) == []

    catalog.alter("ALTER TABLE dbo.orders ADD tier int NULL", lsn=0x2A)
    _run(catalog, "05")
    reviews = _open(rev)
    assert len(reviews) == 1, reviews
    assert reviews[0]["column_names"] == ["tier"]
    assert "tier" in reviews[0]["ddl"]
    assert rev.acknowledge_review(reviews[0]["id"])["status"] == "acknowledged"

    # ddl_history still holds the ALTER; captured columns are unchanged.
    for n in range(6, 10):
        _run(catalog, f"0{n}")
        assert _open(rev) == [], "an acknowledged DDL was raised again"


def test_ddl_that_predates_the_pipeline_is_its_baseline(stores):
    rev, _stored = stores
    catalog = _Catalog(COLS)
    catalog.alter("ALTER TABLE dbo.orders ADD name nvarchar(40)", lsn=0x10)
    _run(catalog, "01")
    _run(catalog, "02")
    assert _open(rev) == []


def test_two_columns_in_one_alter_and_a_drop_are_named(stores):
    rev, _stored = stores
    catalog = _Catalog(COLS)
    _run(catalog, "01")
    catalog.alter("ALTER TABLE [dbo].[orders] ADD [tier] int NULL, note nvarchar(20) NULL", lsn=0x30)
    catalog.alter("ALTER TABLE dbo.orders DROP COLUMN name", lsn=0x31)
    _run(catalog, "02")
    reviews = _open(rev)
    assert len(reviews) == 1, reviews
    assert sorted(reviews[0]["column_names"]) == ["name", "note", "tier"]


def test_an_unreadable_ddl_history_is_not_new_ddl_and_keeps_the_watermark(stores):
    rev, _stored = stores
    catalog = _Catalog(COLS)
    _run(catalog, "01")
    catalog.alter("ALTER TABLE dbo.orders ADD tier int NULL", lsn=0x2A)
    catalog.fail_ddl_read = True
    _run(catalog, "02")
    assert _open(rev) == []
    # Once the history can be read again, the change it missed is reviewed.
    catalog.fail_ddl_read = False
    _run(catalog, "03")
    reviews = _open(rev)
    assert len(reviews) == 1 and reviews[0]["column_names"] == ["tier"]
