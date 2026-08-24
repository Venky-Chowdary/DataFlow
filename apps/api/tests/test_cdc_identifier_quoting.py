"""CDC readers must quote source identifiers, not paste them into SQL.

A source column or table name is *data* the customer controls. The snapshot
and poll queries built these names by hand (``ORDER BY [{pk}]``,
``t."{pk}"``), so a name containing the dialect's closing quote character
ended the identifier early — a broken query at best, appended SQL at worst,
on a connector whose whole job is reading hostile-shaped foreign schemas.
"""

from __future__ import annotations

import os

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from connectors.oracle_change_stream import OracleFlashbackCdc
from connectors.oracle_logminer import OracleLogMinerCdc
from connectors.sql_identifiers import require_safe_identifier
from connectors.sqlserver_cdc_native import SqlServerNativeCdc, _qualified_ref
from connectors.sqlserver_change_stream import SqlServerChangeTrackingCdc

# ``;`` is rejected outright upstream, so the interesting case is a name that
# is legal everywhere except that it carries the dialect's closing quote.
HOSTILE_TABLE = "orders] --"
HOSTILE_BRACKET_COL = "id] --"
HOSTILE_QUOTE_COL = 'id" --'


class _Cur:
    """Records executed SQL; returns no rows so the snapshot loop exits."""

    def __init__(self, sink: list[str]) -> None:
        self.sink = sink
        self.description: list[tuple] = []

    def execute(self, sql, params=None):
        self.sink.append(str(sql))

    def fetchone(self):
        return (0,)

    def fetchall(self):
        return []

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False


class _Conn:
    def __init__(self, sink: list[str]) -> None:
        self.sink = sink

    def cursor(self):
        return _Cur(self.sink)

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False


def _drain(reader, sink: list[str], monkeypatch) -> list[str]:
    monkeypatch.setattr(reader, "_conn", lambda: _Conn(sink))
    monkeypatch.setattr(reader, "_acquire_cdc_lease", lambda: None)
    for _ in reader.snapshot():
        break
    return sink


def test_sqlserver_qualified_ref_escapes_closing_bracket():
    assert _qualified_ref("dbo", HOSTILE_TABLE) == "[dbo].[orders]] --]"


def test_statement_terminator_in_an_identifier_is_refused_not_escaped():
    with pytest.raises(ValueError):
        require_safe_identifier("orders; DROP TABLE users", allow_raw=True)


@pytest.mark.parametrize(
    "factory",
    [
        lambda: SqlServerNativeCdc(
            {"host": "h"}, table="t", primary_key=HOSTILE_BRACKET_COL, schema="dbo"
        ),
        lambda: SqlServerChangeTrackingCdc(
            {"host": "h"}, table="t", primary_key=HOSTILE_BRACKET_COL, schema="dbo"
        ),
    ],
    ids=["cdc_native", "change_tracking"],
)
def test_sqlserver_snapshot_quotes_primary_key(factory, monkeypatch):
    reader = factory()
    sink: list[str] = []
    _drain(reader, sink, monkeypatch)

    ordering = [s for s in sink if "ORDER BY" in s and "FROM [dbo]" in s]
    assert ordering, sink
    for sql in ordering:
        assert "[id]] --]" in sql
        assert "[id] --]" not in sql  # the unescaped form ends the identifier


@pytest.mark.parametrize(
    "factory",
    [
        lambda: OracleFlashbackCdc(
            {"host": "h"}, table="t", primary_key=HOSTILE_QUOTE_COL, schema="APP"
        ),
        lambda: OracleLogMinerCdc(
            {"host": "h"}, table="t", primary_key=HOSTILE_QUOTE_COL, schema="APP"
        ),
    ],
    ids=["flashback", "logminer"],
)
def test_oracle_snapshot_doubles_embedded_double_quote(factory, monkeypatch):
    reader = factory()
    sink: list[str] = []
    _drain(reader, sink, monkeypatch)

    ordering = [s for s in sink if "ROW_NUMBER()" in s]
    assert ordering, sink
    for sql in ordering:
        assert '"ID"" --"' in sql.upper()
