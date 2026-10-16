"""Snowflake file manifest commits with the load, or not at all."""

from __future__ import annotations

import uuid

import pytest

pytest.importorskip("fakesnow")

from connectors.snowflake_conn import get_connection  # noqa: E402
from services.snowflake_file_manifest import (  # noqa: E402
    commit_staged_snowflake_file,
    snowflake_file_status,
)


def _conn():
    return get_connection(
        account="localhost",
        username="test",
        password="test",
        database="DATAFLOW",
        schema="PUBLIC",
        warehouse="",
        connection_string="",
    )


def _stage(cur, name: str, rows: list[tuple]) -> None:
    cur.execute("CREATE DATABASE IF NOT EXISTS DATAFLOW")
    cur.execute("CREATE SCHEMA IF NOT EXISTS PUBLIC")
    cur.execute("USE DATABASE DATAFLOW")
    cur.execute("USE SCHEMA PUBLIC")
    cur.execute(f'DROP TABLE IF EXISTS "{name}"')
    cur.execute(f'CREATE TABLE "{name}" ("id" NUMBER, "label" VARCHAR)')
    cur.executemany(f'INSERT INTO "{name}" ("id", "label") VALUES (%s, %s)', rows)


def _dest_rows(cur, table: str) -> list[tuple]:
    cur.execute(f'SELECT "id", "label" FROM "{table}" ORDER BY "id"')
    return [(int(i), str(label)) for i, label in cur.fetchall()]


def _claim(**overrides):
    base = dict(
        database="DATAFLOW",
        schema="PUBLIC",
        stage_table="ST",
        target_table="ORDERS",
        columns=["id", "label"],
        conflict_columns=["id"],
        write_mode="upsert",
        connector="sftp:/:orders_*.csv",
        path="orders_a.csv",
        size=12,
        mtime="2026-10-15T00:00:00Z",
        sha256="abc123",
        job_id="job-1",
        rows=1,
    )
    base.update(overrides)
    return base


def test_crash_before_commit_does_not_load_or_skip(monkeypatch, tmp_path):
    monkeypatch.setenv("FAKESNOW_DB_PATH", str(tmp_path / "fakesnow"))
    table = "ORDERS_" + uuid.uuid4().hex[:8].upper()
    stage = "STAGE_" + uuid.uuid4().hex[:8].upper()
    digest = uuid.uuid4().hex
    conn = _conn()
    cur = conn.cursor()
    _stage(cur, stage, [(1, "a")])

    def killed() -> None:
        raise RuntimeError("killed mid-load")

    with pytest.raises(RuntimeError, match="killed mid-load"):
        commit_staged_snowflake_file(
            conn,
            **_claim(stage_table=stage, target_table=table, sha256=digest),
            before_commit=killed,
        )

    other = _conn()
    ocur = other.cursor()
    ocur.execute("USE DATABASE DATAFLOW")
    ocur.execute("USE SCHEMA PUBLIC")
    assert _dest_rows(ocur, table) == []
    assert (
        snowflake_file_status(
            other,
            database="DATAFLOW",
            schema="PUBLIC",
            connector="sftp:/:orders_*.csv",
            path="orders_a.csv",
            sha256=digest,
        )
        != "loaded"
    )

    first = commit_staged_snowflake_file(
        conn, **_claim(stage_table=stage, target_table=table, sha256=digest)
    )
    assert first == "loaded"
    assert _dest_rows(ocur, table) == [(1, "a")]

    second = commit_staged_snowflake_file(
        conn, **_claim(stage_table=stage, target_table=table, sha256=digest)
    )
    assert second == "skip"
    assert _dest_rows(ocur, table) == [(1, "a")]
    other.close()
    conn.close()


def test_later_file_in_a_pickup_wins_a_shared_key(monkeypatch, tmp_path):
    monkeypatch.setenv("FAKESNOW_DB_PATH", str(tmp_path / "fakesnow"))
    table = "ORDERS_" + uuid.uuid4().hex[:8].upper()
    stage_a = "STAGE_A_" + uuid.uuid4().hex[:8].upper()
    stage_b = "STAGE_B_" + uuid.uuid4().hex[:8].upper()
    conn = _conn()
    cur = conn.cursor()
    _stage(cur, stage_a, [(1, "old"), (2, "keep")])
    _stage(cur, stage_b, [(1, "new")])
    assert (
        commit_staged_snowflake_file(
            conn,
            **_claim(
                stage_table=stage_a,
                target_table=table,
                path="orders_a.csv",
                sha256="a" * 64,
                rows=2,
            ),
        )
        == "loaded"
    )
    assert (
        commit_staged_snowflake_file(
            conn,
            **_claim(
                stage_table=stage_b,
                target_table=table,
                path="orders_b.csv",
                sha256="b" * 64,
                rows=1,
            ),
        )
        == "loaded"
    )
    cur.execute("USE DATABASE DATAFLOW")
    cur.execute("USE SCHEMA PUBLIC")
    assert _dest_rows(cur, table) == [(1, "new"), (2, "keep")]
    conn.close()


def test_stream_load_commits_the_manifest_and_a_rerun_skips(monkeypatch, tmp_path):
    monkeypatch.setenv("FAKESNOW_DB_PATH", str(tmp_path / "fakesnow"))
    from src.transfer.file_stream import stream_file_to_database
    from src.transfer.models import EndpointConfig

    table = "ORD_" + uuid.uuid4().hex[:8]
    dest = EndpointConfig(
        kind="database",
        format="snowflake",
        host="localhost",
        database="DATAFLOW",
        username="test",
        password="test",
        schema="PUBLIC",
        table=table,
    )
    content = b"id,label\n1,old\n2,keep\n"
    sha = "c" * 64
    identity = {
        "connector": "file:orders",
        "path": "orders_a.csv",
        "size": len(content),
        "mtime": "2026-10-15T00:00:00Z",
        "sha256": sha,
        "job_id": "job-stream",
    }
    common = dict(
        destination=dest,
        mappings=[
            {"source": "id", "target": "id"},
            {"source": "label", "target": "label"},
        ],
        schema={"id": "integer", "label": "string"},
        sync_mode="full_refresh_append",
        stream_contracts=[{"primary_key": "id", "sync_mode": "full_refresh_append"}],
        duplicate_policy="upsert",
        skip_preflight=True,
        validation_mode="balanced",
        file_identity=identity,
    )
    rows, _ddl, summary, _cols = stream_file_to_database(
        content, "orders_a.csv", **common
    )
    assert summary.get("file_manifest_status") == "loaded", summary
    assert rows == 2
    again, _ddl2, summary2, _cols2 = stream_file_to_database(
        content, "orders_a.csv", **common
    )
    assert again == 0
    assert summary2.get("source_row_count_source") == "file_manifest"

    changed = b"id,label\n1,new\n"
    changed_id = dict(identity, path="orders_b.csv", sha256="d" * 64, size=len(changed))
    rows_b, _ddl3, summary3, _cols3 = stream_file_to_database(
        changed,
        "orders_b.csv",
        **{**common, "file_identity": changed_id},
    )
    assert summary3.get("file_manifest_status") == "loaded", summary3
    assert rows_b == 1
    conn = _conn()
    cur = conn.cursor()
    cur.execute("USE DATABASE DATAFLOW")
    cur.execute("USE SCHEMA PUBLIC")
    cur.execute(f'SELECT "id", "label" FROM "{table.upper()}" ORDER BY "id"')
    found = [(int(i), str(label)) for i, label in cur.fetchall()]
    assert found == [(1, "new"), (2, "keep")]
    conn.close()
    from services.snowflake_file_manifest import read_loaded_manifest

    loaded = read_loaded_manifest(
        {
            "host": "localhost",
            "database": "DATAFLOW",
            "username": "test",
            "password": "test",
            "schema": "PUBLIC",
        },
        connector="file:orders",
        schema="PUBLIC",
        dest_table=table,
    )
    assert {row["path"] for row in loaded} == {"orders_a.csv", "orders_b.csv"}


def test_same_sha_loads_a_second_table_and_schedule(monkeypatch, tmp_path):
    """A loaded sha256 must not skip a different destination table."""
    monkeypatch.setenv("FAKESNOW_DB_PATH", str(tmp_path / "scope.db"))
    digest = uuid.uuid4().hex
    table_a = "A_" + uuid.uuid4().hex[:8].upper()
    table_b = "B_" + uuid.uuid4().hex[:8].upper()
    stage_a = "SA_" + uuid.uuid4().hex[:8].upper()
    stage_b = "SB_" + uuid.uuid4().hex[:8].upper()
    conn = _conn()
    cur = conn.cursor()
    _stage(cur, stage_a, [(1, "alpha")])
    _stage(cur, stage_b, [(1, "beta")])
    shared = dict(
        database="DATAFLOW",
        schema="PUBLIC",
        columns=["id", "label"],
        conflict_columns=["id"],
        write_mode="upsert",
        connector="sftp:/:orders_*.csv",
        path="orders_a.csv",
        size=12,
        mtime="2026-10-15T00:00:00Z",
        sha256=digest,
        job_id="job-scope",
        rows=1,
    )
    assert commit_staged_snowflake_file(
        conn, stage_table=stage_a, target_table=table_a,
        dest_table=table_a, schedule_id="sched-a", **shared,
    ) == "loaded"
    assert commit_staged_snowflake_file(
        conn, stage_table=stage_b, target_table=table_b,
        dest_table=table_b, schedule_id="sched-b", **shared,
    ) == "loaded"
    assert _dest_rows(cur, table_a) == [(1, "alpha")]
    assert _dest_rows(cur, table_b) == [(1, "beta")]
    assert commit_staged_snowflake_file(
        conn, stage_table=stage_a, target_table=table_a,
        dest_table=table_a, schedule_id="sched-a", **shared,
    ) == "skip"
    assert _dest_rows(cur, table_a) == [(1, "alpha")]
    assert _dest_rows(cur, table_b) == [(1, "beta")]


def test_shared_key_census_counts_the_net_insert():
    from services.file_pickup_load import merge_file_census
    from services.row_conservation import CENSUS_KEY

    summary: dict = {}
    merge_file_census(
        summary,
        {"unique_batch_keys": 2, "dest_preexisting": 0, "tombstones": 0},
    )
    merge_file_census(
        summary,
        {"unique_batch_keys": 2, "dest_preexisting": 2, "tombstones": 0},
    )
    census = summary[CENSUS_KEY]
    assert census["unique_batch_keys"] - census["dest_preexisting"] == 2
