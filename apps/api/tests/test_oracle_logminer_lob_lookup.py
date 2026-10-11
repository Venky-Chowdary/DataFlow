"""Oracle LogMiner LOB lookup behavior."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any

import pytest

from connectors.oracle_logminer import (
    OracleLogMinerCdc,
    OracleLogMinerReadError,
    classify_sql_redo,
    logminer_contents_sql,
    logminer_txn_contents_sql,
)
from services.value_serializer import SQL_NULL_SENTINEL


def _reader() -> OracleLogMinerCdc:
    return OracleLogMinerCdc(
        {
            "host": "localhost",
            "port": 1521,
            "database": "FREEPDB1",
            "username": "APP",
            "password": "unit-test",
        },
        table="T",
        primary_key="ID",
        schema="APP",
        cursor_key="oracle-lob-unit",
    )


def _snapshot(*columns: tuple[str, str]) -> dict[str, Any]:
    return {
        "columns": [
            {"name": name, "type": data_type}
            for name, data_type in columns
        ]
    }


class _FakeCursor:
    def __init__(
        self,
        rows: dict[str, tuple[Any, ...]],
    ) -> None:
        self.rows = rows
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.statement = ""
        self.binds: dict[str, Any] = {}

    def __enter__(self) -> _FakeCursor:
        return self

    def __exit__(self, *_args: Any) -> bool:
        return False

    def execute(
        self, statement: str, binds: dict[str, Any] | None = None
    ) -> None:
        self.statement = statement
        self.binds = binds or {}
        self.calls.append((statement, dict(self.binds)))

    def fetchall(self) -> list[tuple[Any, ...]]:
        requested = {
            str(value)
            for key, value in self.binds.items()
            if key.startswith("pk_")
        }
        return [
            row for key, row in self.rows.items() if key in requested
        ]


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    def __enter__(self) -> _FakeConnection:
        return self

    def __exit__(self, *_args: Any) -> bool:
        return False

    def cursor(self) -> _FakeCursor:
        return self._cursor


def _attach_source(
    reader: OracleLogMinerCdc,
    rows: dict[str, tuple[Any, ...]],
) -> _FakeCursor:
    cursor = _FakeCursor(rows)
    connection = _FakeConnection(cursor)

    @contextmanager
    def connect():
        yield connection

    reader._conn = connect
    reader._mining_conn = lambda: pytest.fail("LOB lookup must use the PDB connection")
    reader._oracle_ddl_schema_snapshot = lambda _table: _snapshot(
        ("ID", "NUMBER"),
        ("C", "CLOB"),
        ("N", "NCLOB"),
        ("B", "BLOB"),
        ("V", "VARCHAR2(40)"),
    )
    return cursor


def test_locator_sql_is_selected_by_both_logminer_queries() -> None:
    legacy = logminer_contents_sql(table_predicate="TABLE_NAME = :tbl")
    buffered = logminer_txn_contents_sql(table_predicate="TABLE_NAME = :tbl")
    assert "SEL_LOB_LOCATOR" in legacy
    assert "SEL_LOB_LOCATOR" in buffered


def test_locator_parser_reuses_update_where_values_and_ignores_row_id() -> None:
    reader = _reader()
    sql_redo = (
        'select "C" into loc_c from "APP"."T" '
        'where "ID" = \'2\' and "V" = \'step-2\' for update;'
    )
    parsed = reader._parse_lob_locator_pk(sql_redo, "ID")
    assert parsed == {"ID": "2"}


def test_locator_without_parseable_primary_key_fails_closed() -> None:
    reader = _reader()
    with pytest.raises(OracleLogMinerReadError, match="primary key"):
        reader._parse_lob_locator_pk(
            'select "C" into loc_c from "APP"."T" where "V" = \'step-2\' for update;',
            "ID",
        )


def test_empty_update_redo_is_rejected_instead_of_emitting_an_empty_row() -> None:
    kind, rejected = classify_sql_redo("", op="update", table="T")
    assert kind == "unparsed"
    assert rejected["reason"] == "empty SQL_REDO"


def test_lob_columns_are_discovered_from_typed_schema_and_cached() -> None:
    reader = _reader()
    calls: list[str] = []
    reader._oracle_ddl_schema_snapshot = lambda table: calls.append(table) or _snapshot(
        ("ID", "NUMBER"),
        ("C", "CLOB"),
        ("NC", "NCLOB"),
        ("B", "BLOB"),
        ("NOTE", "VARCHAR2(80)"),
    )
    assert reader._lob_columns_for_table("T") == ["C", "NC", "B"]
    assert reader._lob_columns_for_table("T") == ["C", "NC", "B"]
    assert calls == ["T"]


def test_recorded_ddl_refreshes_cached_lob_column_types(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from services import cdc_mapping_review, cdc_schema_history

    reader = _reader()
    schema = [_snapshot(("ID", "NUMBER"), ("C", "CLOB"))]
    calls: list[str] = []

    def snapshot(table: str) -> dict[str, Any]:
        calls.append(table)
        return schema[0]

    reader._oracle_ddl_schema_snapshot = snapshot
    assert reader._lob_columns_for_table("T") == ["C"]
    schema[0] = _snapshot(
        ("ID", "NUMBER"), ("C", "NCLOB"), ("B", "BLOB")
    )
    monkeypatch.setattr(cdc_schema_history, "list_history", lambda *_a, **_k: [])
    monkeypatch.setattr(
        cdc_schema_history,
        "record_ddl",
        lambda _source, _table, ddl, *, offset, schema_snapshot: {
            "version": 1,
            "recorded_at": "2026-10-12T00:00:00Z",
            "ddl": ddl,
            "offset": offset,
            "schema_snapshot": schema_snapshot,
        },
    )
    monkeypatch.setattr(
        cdc_mapping_review,
        "flag_mapping_review",
        lambda **_kwargs: None,
    )
    reader._record_oracle_ddl(
        owner="APP",
        table="T",
        ddl="ALTER TABLE T MODIFY C NCLOB",
        offset={"scn": 100, "rs_id": "redo", "ssn": 1},
    )
    assert reader._lob_columns_for_table("T") == ["C", "B"]
    assert calls == ["T", "T", "T"]


def test_insert_lob_update_and_locator_lookup_all_lobs_by_pk_at_emit_time() -> None:
    reader = _reader()
    cursor = _attach_source(
        reader,
        {
            "1": (1, "clob-1", None, b"\x00"),
            "2": (2, "clob-2", "national-2", b"b"),
            "3": (3, "clob-3", None, b""),
        },
    )
    events = [
        {
            "op": "insert",
            "table": "T",
            "row": {"ID": 1, "C": "EMPTY_CLOB()", "B": "EMPTY_BLOB()", "V": "i"},
        },
        {
            "op": "update",
            "table": "T",
            "row": {"ID": 2, "C": "EMPTY_CLOB()"},
        },
        {
            "op": "update",
            "table": "T",
            "row": {"ID": 2, "V": "v-only"},
        },
        {
            "op": "update",
            "table": "T",
            "row": {"ID": 3},
            "lob_locator": True,
        },
    ]

    reader._enrich_lob_lookup_events(events)

    assert len(cursor.calls) == 1
    sql, binds = cursor.calls[0]
    assert '"APP"."T"' in sql
    assert all(key.startswith("pk_") or key == "owner" for key in binds)
    assert events[0]["row"] == {
        "ID": 1,
        "C": "clob-1",
        "N": SQL_NULL_SENTINEL,
        "B": "AA==",
        "V": "i",
    }
    assert events[1]["row"] == {
        "ID": 2,
        "C": "clob-2",
        "N": "national-2",
        "B": "Yg==",
    }
    assert events[2]["row"] == {"ID": 2, "V": "v-only"}
    assert events[3]["row"] == {
        "ID": 3,
        "C": "clob-3",
        "N": SQL_NULL_SENTINEL,
        "B": "",
    }
    assert events[3]["op"] == "update"


def test_locator_triggers_merge_into_same_transaction_dml() -> None:
    reader = _reader()
    cursor = _attach_source(reader, {"2": (2, "clob-2", None, b"b")})
    events = [
        {
            "op": "insert",
            "table": "T",
            "xid": (1, 2, 3),
            "row": {"ID": "2", "C": "EMPTY_CLOB()", "B": "EMPTY_BLOB()"},
        },
        {
            "op": "update",
            "table": "T",
            "xid": (1, 2, 3),
            "lob_locator": True,
            "row": {"ID": "2"},
        },
        {
            "op": "update",
            "table": "T",
            "xid": (1, 2, 3),
            "lob_locator": True,
            "row": {"ID": "2"},
        },
    ]

    merged = reader._coalesce_lob_locator_events(events)
    reader._enrich_lob_lookup_events(merged)

    assert len(merged) == 1
    assert merged[0]["op"] == "insert"
    assert merged[0]["row"] == {
        "ID": "2",
        "C": "clob-2",
        "N": SQL_NULL_SENTINEL,
        "B": "Yg==",
    }
    assert len(cursor.calls) == 1


def test_locator_triggers_for_same_transaction_pk_coalesce_to_one_update() -> None:
    reader = _reader()
    events = [
        {
            "op": "update",
            "table": "T",
            "xid": (1, 2, 3),
            "position": (10, "redo-a", 0),
            "lob_locator": True,
            "row": {"ID": "2"},
        },
        {
            "op": "update",
            "table": "T",
            "xid": (1, 2, 3),
            "position": (11, "redo-b", 0),
            "lob_locator": True,
            "row": {"ID": "2"},
        },
    ]

    merged = reader._coalesce_lob_locator_events(events)

    assert len(merged) == 1
    assert merged[0]["position"] == (11, "redo-b", 0)
    assert merged[0]["row"] == {"ID": "2"}


def test_lob_lookup_is_chunked_at_500_primary_keys() -> None:
    reader = _reader()
    rows = {
        str(value): (value, f"clob-{value}", None, b"")
        for value in range(1, 1002)
    }
    cursor = _attach_source(reader, rows)
    events = [
        {"op": "insert", "table": "T", "row": {"ID": value}}
        for value in range(1, 1002)
    ]

    reader._enrich_lob_lookup_events(events)

    assert len(cursor.calls) == 3
    assert [
        sum(key.startswith("pk_") for key in binds)
        for _sql, binds in cursor.calls
    ] == [500, 500, 1]
    assert events[0]["row"]["C"] == "clob-1"
    assert events[-1]["row"]["C"] == "clob-1001"


def test_missing_source_row_drops_lobs_and_increments_reader_metadata() -> None:
    reader = _reader()
    _attach_source(reader, {})
    events = [
        {
            "op": "insert",
            "table": "T",
            "row": {"ID": 4, "C": "EMPTY_CLOB()", "B": "EMPTY_BLOB()"},
        }
    ]

    reader._enrich_lob_lookup_events(events)

    assert events[0]["row"] == {"ID": 4}
    assert reader.cdc_metadata()["lob_lookup_missing"] == 1


def test_lob_activity_without_primary_key_fails_with_remedy() -> None:
    reader = _reader()
    reader.primary_keys = {}
    _attach_source(reader, {})
    events = [{"op": "insert", "table": "T", "row": {"ID": 5}}]

    with pytest.raises(OracleLogMinerReadError, match="add a primary key"):
        reader._enrich_lob_lookup_events(events)


def test_oracle_cdc_capability_notes_describe_lob_capture_semantics() -> None:
    from services.connector_capability_registry import CAPABILITY_REGISTRY

    notes = str(CAPABILITY_REGISTRY["oracle"].get("cdc_prerequisites") or "").lower()
    assert "primary key" in notes
    assert "capture time" in notes
    assert "converge" in notes
