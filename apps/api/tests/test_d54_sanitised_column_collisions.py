"""D54 — source columns that sanitise to one destination column went unnoticed.

``First Name`` and ``First-Name`` both become ``First_Name``. The writer
emitted the column twice and the driver answered "specified more than once"
after the DDL had run, with the table dropped again. Validate now names the
colliding columns and asks for a rename.
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from connectors.sql_identifiers import sanitized_name_collisions  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402


def test_collision_helper_is_exact_for_case_sensitive_engines():
    assert sanitized_name_collisions(["First Name", "First-Name", "k"], "postgresql") == [
        ("First_Name", ["First Name", "First-Name"])
    ]
    # Distinct once quoted on PostgreSQL; the same column on MySQL.
    assert sanitized_name_collisions(["ID", "id"], "postgresql") == []
    assert [o for _i, o in sanitized_name_collisions(["ID", "id"], "mysql")] == [["ID", "id"]]
    # Underscore runs stay distinct (the sanitiser does not merge them).
    assert sanitized_name_collisions(["first__name", "first_name"], "postgresql") == []


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


def test_colliding_columns_are_refused_in_preflight_with_a_rename_ask(tmp_path, isolated):
    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    conn = sqlite3.connect(str(src))
    conn.execute('CREATE TABLE t ("a b" TEXT, "a-b" TEXT, k INTEGER)')
    conn.execute("INSERT INTO t VALUES ('x', 'y', 1)")
    conn.commit()
    conn.close()
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t"),
        sync_mode="full_refresh_overwrite",
        validation_mode="strict",
    )
    job_id = "d54" + uuid.uuid4().hex[:16]
    isolated.update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(request, job_id)
    assert not result.success
    assert "both become 'a_b'" in str(result.error), result.error
    assert "Rename one" in str(result.error)
    assert "specified more than once" not in str(result.error)
