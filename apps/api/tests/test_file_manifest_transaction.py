"""Processed-file manifest commits with the load, or not at all."""

from __future__ import annotations

import sqlite3

import pytest

from services.duplicate_policy import DuplicatePolicyError, validate_schedule_file_policy
from services.file_manifest import claim_file, complete_file, list_loaded
from services.file_pickup import RemoteFile, plan_pickup, select_new_files


def test_claim_rolls_back_with_the_load_and_a_commit_skips_the_same_sha():
    conn = sqlite3.connect(":memory:")
    try:
        conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, amount TEXT)")
        assert claim_file(
            conn,
            dialect="sqlite",
            connector="sftp-1",
            path="orders_a.csv",
            size=12,
            mtime="10",
            sha256="a" * 64,
            job_id="job-1",
        ) == "load"
        conn.execute("INSERT INTO orders (id, amount) VALUES (1, '10')")
        conn.rollback()
        assert conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0] == 0
        assert list_loaded(conn, dialect="sqlite", connector="sftp-1") == []

        assert claim_file(
            conn,
            dialect="sqlite",
            connector="sftp-1",
            path="orders_a.csv",
            size=12,
            mtime="10",
            sha256="a" * 64,
            job_id="job-2",
        ) == "load"
        conn.execute("INSERT INTO orders (id, amount) VALUES (1, '10')")
        complete_file(
            conn,
            dialect="sqlite",
            connector="sftp-1",
            path="orders_a.csv",
            sha256="a" * 64,
            rows=1,
        )
        conn.commit()
        assert claim_file(
            conn,
            dialect="sqlite",
            connector="sftp-1",
            path="orders_a.csv",
            size=12,
            mtime="99",
            sha256="a" * 64,
            job_id="job-3",
        ) == "skip"
        conn.rollback()
        loaded = list_loaded(conn, dialect="sqlite", connector="sftp-1")
        assert loaded[0]["rows"] == 1
        assert loaded[0]["sha256"] == "a" * 64
        assert conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0] == 1
    finally:
        conn.close()


def test_pickup_orders_by_name_and_ignores_a_touched_mtime():
    files = [
        RemoteFile("orders_b.csv", size=2, checksum="bbb", mtime="2"),
        RemoteFile("orders_a.csv", size=1, checksum="aaa", mtime="1"),
    ]
    plan = plan_pickup(pattern="orders_*.csv", files=files, manifest=[])
    assert [item.name for item in plan.files] == ["orders_a.csv", "orders_b.csv"]
    touched = [RemoteFile("orders_a.csv", size=9, checksum="aaa", mtime="50")]
    manifest = [{"name": "orders_a.csv", "size": 1, "checksum": "aaa", "mtime": "1", "status": "loaded"}]
    assert select_new_files(touched, manifest, "orders_*.csv") == []
    changed = [RemoteFile("orders_a.csv", size=9, checksum="ccc", mtime="50")]
    assert [item.name for item in select_new_files(changed, manifest, "orders_*.csv")] == [
        "orders_a.csv"
    ]
    missing = plan_pickup(pattern="orders_*.csv", files=[], manifest=[], wait_seconds=0)
    assert missing.status == "expected_file_not_arrived"
    assert "expected file not arrived" in missing.message
    assert "file_wait_seconds" in missing.message


def test_schedule_policy_is_rejected_when_it_is_unknown_or_unkeyed():
    assert validate_schedule_file_policy("skip_duplicates", "keep-last", primary_key="id") == (
        "dedupe_skip",
        "keep_last",
    )
    assert validate_schedule_file_policy("", "block", primary_key="") == ("", "block")
    with pytest.raises(DuplicatePolicyError, match="Unknown duplicate_policy"):
        validate_schedule_file_policy("merge-all", "block", primary_key="id")
    with pytest.raises(DuplicatePolicyError, match="primary key"):
        validate_schedule_file_policy("upsert", "block", primary_key="")
    with pytest.raises(DuplicatePolicyError, match="in_file_dedupe"):
        validate_schedule_file_policy("append", "drop", primary_key="")


def test_create_schedule_tool_exposes_duplicate_policy():
    from src.ai.copilot.tools import TOOL_DEFINITIONS

    spec = next(tool for tool in TOOL_DEFINITIONS if tool["name"] == "create_schedule")
    props = spec["input_schema"]["properties"]
    assert props["duplicate_policy"]["enum"] == ["upsert", "skip_duplicates", "append"]
    assert props["in_file_dedupe"]["enum"] == ["block", "keep_last"]
    assert "file_wait_seconds" in props
