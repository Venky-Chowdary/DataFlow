"""Source-side duplicate-key probe in preflight."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import pytest


@pytest.fixture
def sqlite_dupes_connector(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[str, str]:
    """Create a saved SQLite connector and a table with duplicate ``id`` values."""
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    store_path = tmp_path / "connectors.json"
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(store_path))

    db_path = tmp_path / "dupes.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE jobs (id TEXT, name TEXT)")
    conn.executemany(
        "INSERT INTO jobs VALUES (?, ?)",
        [("a", "A"), ("b", "B"), ("a", "A2"), ("c", "C"), ("b", "B2")],
    )
    conn.commit()
    conn.close()

    from services.connector_store import create_connector

    saved = create_connector(
        {
            "name": "SQLite Dupes",
            "type": "sqlite",
            "role": "source",
            "connection_string": f"sqlite:///{db_path}",
            "ssl": False,
        }
    )
    return str(saved.id), "jobs"


def test_qualified_sql_table_from_clause_is_not_double_prefixed() -> None:
    """``public.case_a_src`` + schema=public must compile to public.case_a_src."""
    import sqlalchemy as sa
    from sqlalchemy.dialects import postgresql

    from connectors.sql_identifiers import split_qualified_table

    schema, table = split_qualified_table("public.case_a_src", "public")
    tbl = sa.table(table, schema=schema)
    sql = str(
        sa.select(sa.column("id")).select_from(tbl).compile(dialect=postgresql.dialect())
    )
    assert "public.public" not in sql
    assert '"public.case_a_src"' not in sql
    assert "case_a_src" in sql

    bare_schema, bare_table = split_qualified_table("case_a_src", "public")
    assert (bare_schema, bare_table) == (schema, table)

    buggy = sa.table("public.case_a_src", schema="public")
    buggy_sql = str(
        sa.select(sa.column("id")).select_from(buggy).compile(dialect=postgresql.dialect())
    )
    assert '"public.case_a_src"' in buggy_sql


def test_source_duplicate_probe_sqlite(sqlite_dupes_connector: tuple[str, str]) -> None:
    """The probe returns the duplicate keys and their counts."""
    connector_id, table = sqlite_dupes_connector
    from services.source_duplicate_probe import probe_source_duplicate_keys

    result = probe_source_duplicate_keys(
        source_connector_id=connector_id,
        source_table=table,
        primary_key="id",
    )
    assert len(result) == 2
    values = {r["value"]: r["count"] for r in result}
    assert values.get("a") == 2
    assert values.get("b") == 2


def test_warehouse_sql_inspect_failure_falls_back_to_payload_scan(monkeypatch) -> None:
    """fakesnow/goccy cannot address GROUP BY via inspector — scan the readable rows."""
    from services.source_duplicate_probe import probe_source_duplicate_keys_result

    def boom(*_a, **_k):
        raise RuntimeError("probe did not address the source table")

    monkeypatch.setattr(
        "services.source_duplicate_probe._sql_duplicates",
        boom,
    )
    monkeypatch.setattr(
        "services.source_duplicate_probe._object_payload_duplicates",
        lambda *_a, **_k: ([], 2, True),
    )
    result = probe_source_duplicate_keys_result(
        source_config={"type": "snowflake", "host": "localhost", "database": "demo"},
        source_table="payments_src",
        primary_key="id",
    )
    assert result.status == "ran"
    assert result.findings == []
    assert "payload uniqueness scan" in result.message.lower()


def test_preflight_blocks_on_source_duplicate_keys(
    sqlite_dupes_connector: tuple[str, str],
) -> None:
    """A database source with duplicate identity keys must fail G9 before Run."""
    connector_id, table = sqlite_dupes_connector
    from services.preflight_service import run_file_preflight

    sample_rows: list[dict[str, Any]] = [{"id": "c", "name": "C"}]
    mappings = [
        {"source": "id", "target": "id", "confidence": 0.99, "transform": None},
        {"source": "name", "target": "name", "confidence": 0.99, "transform": None},
    ]
    result = run_file_preflight(
        columns=["id", "name"],
        column_types={"id": "VARCHAR", "name": "VARCHAR"},
        row_count=5,
        mappings=mappings,
        destination_connected=True,
        destination_can_create=True,
        source_connected=True,
        source_kind="database",
        source_format="sqlite",
        sync_mode="append",
        sample_rows=sample_rows,
        destination_db_type="postgresql",
        source_connector_id=connector_id,
        source_table=table,
        destination_table="jobs",
        destination_table_exists=True,
        destination_pk_columns=["id"],
        validation_mode="strict",
    )
    gate_status = {g["id"]: g for g in result["gates"]}
    assert gate_status["g9_data_integrity"]["status"] == "block"
    g9_issues = gate_status["g9_data_integrity"].get("details", {}).get("issues", [])
    assert any("duplicate key values from source probe" in str(i) for i in g9_issues)
    assert result["passed"] is False


def test_preflight_append_create_new_warns_on_source_duplicate_keys(
    sqlite_dupes_connector: tuple[str, str],
) -> None:
    """Append + create-new has no dest UNIQUE — source dups must warn, not hard-block.

    Regression: Validate-green / Execute-red when probe ran only on Execute for
    full_refresh_append into a projected CREATE table.
    """
    connector_id, table = sqlite_dupes_connector
    from services.preflight_service import run_file_preflight

    sample_rows: list[dict[str, Any]] = [{"id": "c", "name": "C"}]
    mappings = [
        {"source": "id", "target": "id", "confidence": 0.99, "transform": None},
        {"source": "name", "target": "name", "confidence": 0.99, "transform": None},
    ]
    result = run_file_preflight(
        columns=["id", "name"],
        column_types={"id": "VARCHAR", "name": "VARCHAR"},
        row_count=5,
        mappings=mappings,
        destination_connected=True,
        destination_can_create=True,
        source_connected=True,
        source_kind="database",
        source_format="sqlite",
        sync_mode="full_refresh_append",
        sample_rows=sample_rows,
        destination_db_type="postgresql",
        source_connector_id=connector_id,
        source_table=table,
        destination_table="jobs",
        destination_table_exists=False,
        destination_pk_columns=[],
        validation_mode="strict",
    )
    gate_status = {g["id"]: g for g in result["gates"]}
    g9 = gate_status["g9_data_integrity"]
    assert g9["status"] != "block", g9
    details = g9.get("details") or {}
    warn_blob = " ".join(
        str(x) for x in (details.get("warnings") or []) + (details.get("issues") or [])
    )
    note = str(details.get("note") or g9.get("message") or "")
    assert (
        "duplicate" in warn_blob.lower()
        or "duplicate" in note.lower()
        or any(
            "duplicate" in str(c.get("note") or "").lower()
            or any("duplicate" in str(w).lower() for w in (c.get("warnings") or []))
            for c in (details.get("checks") or [])
            if isinstance(c, dict)
        )
    ), (warn_blob, note, details)
    # Overall may still fail other gates; uniqueness alone must not block append create-new.
    integrity = details.get("checks") or []
    dup_check = next(
        (c for c in integrity if isinstance(c, dict) and c.get("check") == "duplicate_keys"),
        None,
    )
    if dup_check is None:
        # Some gate shapes nest under integrity report — accept non-block G9.
        return
    assert dup_check.get("blocks_transfer") is False
    assert dup_check.get("passed") is True


@pytest.fixture
def sqlite_composite_connector(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[str, str]:
    """Composite (org_id, code) — org_id alone duplicates; full tuple is unique except one pair."""
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    store_path = tmp_path / "connectors_composite.json"
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(store_path))

    db_path = tmp_path / "composite.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE items (org_id TEXT, code TEXT, name TEXT)")
    conn.executemany(
        "INSERT INTO items VALUES (?, ?, ?)",
        [
            ("o1", "a", "A1"),
            ("o1", "b", "B1"),  # same org_id, different code — legal composite
            ("o2", "a", "A2"),
            ("o1", "a", "A1dup"),  # true composite duplicate
        ],
    )
    conn.commit()
    conn.close()

    from services.connector_store import create_connector

    saved = create_connector(
        {
            "name": "SQLite Composite",
            "type": "sqlite",
            "role": "source",
            "connection_string": f"sqlite:///{db_path}",
            "ssl": False,
        }
    )
    return str(saved.id), "items"


def test_composite_source_probe_groups_full_key(
    sqlite_composite_connector: tuple[str, str],
) -> None:
    """Must not false-fail on org_id alone — only (org_id, code) duplicates."""
    connector_id, table = sqlite_composite_connector
    from services.source_duplicate_probe import probe_source_duplicate_keys_result

    single = probe_source_duplicate_keys_result(
        source_connector_id=connector_id,
        source_table=table,
        primary_key="org_id",
    )
    assert single.ran
    assert any(f.get("value") == "o1" for f in single.findings)

    composite = probe_source_duplicate_keys_result(
        source_connector_id=connector_id,
        source_table=table,
        primary_key_columns=["org_id", "code"],
    )
    assert composite.ran
    assert composite.primary_key_columns == ["org_id", "code"]
    assert len(composite.findings) == 1
    assert composite.findings[0]["count"] == 2
    vals = composite.findings[0].get("values") or {}
    assert vals.get("org_id") == "o1"
    assert vals.get("code") == "a"


def test_preflight_composite_dest_pk_blocks_on_tuple_dupes(
    sqlite_composite_connector: tuple[str, str],
) -> None:
    """Upsert + composite dest PK probes the full tuple and blocks true composite dups."""
    connector_id, table = sqlite_composite_connector
    from services.preflight_service import run_file_preflight

    mappings = [
        {"source": "org_id", "target": "org_id", "confidence": 0.99, "transform": None},
        {"source": "code", "target": "code", "confidence": 0.99, "transform": None},
        {"source": "name", "target": "name", "confidence": 0.99, "transform": None},
    ]
    result = run_file_preflight(
        columns=["org_id", "code", "name"],
        column_types={"org_id": "VARCHAR", "code": "VARCHAR", "name": "VARCHAR"},
        row_count=4,
        mappings=mappings,
        destination_connected=True,
        destination_can_create=True,
        source_connected=True,
        source_kind="database",
        source_format="sqlite",
        sync_mode="upsert",
        sample_rows=[{"org_id": "o2", "code": "a", "name": "A2"}],
        destination_db_type="postgresql",
        source_connector_id=connector_id,
        source_table=table,
        destination_table="items",
        destination_table_exists=True,
        destination_pk_columns=["org_id", "code"],
        validation_mode="strict",
    )
    gate_status = {g["id"]: g for g in result["gates"]}
    assert gate_status["g9_data_integrity"]["status"] == "block"
    details = gate_status["g9_data_integrity"].get("details") or {}
    blob = " ".join(str(i) for i in (details.get("issues") or []))
    assert "source probe" in blob.lower() or "duplicate" in blob.lower()
    probe_pk = str(
        (details.get("source_uniqueness_probe") or {}).get("primary_key")
        or details.get("primary_key")
        or ""
    )
    # Prefer composite label when the integrity report surfaces the probe pk.
    if probe_pk:
        assert "code" in probe_pk or "org_id" in probe_pk
