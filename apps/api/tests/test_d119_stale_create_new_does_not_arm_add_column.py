"""D119 follow-up — a stale create_new stamp must not arm ADD COLUMN.

Run 1 creates the destination, so every mapping it stores is stamped
``create_new``. A schedule replays those stored mappings against the table run 1
created. Folding the stamp into ``backfill_new_fields`` armed ADD COLUMN for the
whole run, so a later source column that reached the writer was added silently
under manual review.

A stamp only proposes ADD COLUMN for a target the live table lacks.
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
from services.batch_progress import (  # noqa: E402
    effective_backfill_new_fields,
    mappings_require_new_columns,
)
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

STALE = [
    {"source": c, "target": c, "create_new": True, "assignment_strategy": "create_compatible_new"}
    for c in ("id", "name", "qty")
]


def test_stamp_naming_a_column_the_table_has_does_not_arm_backfill():
    live = ["id", "name", "qty"]
    assert not mappings_require_new_columns(STALE, dest_columns=live)
    assert not effective_backfill_new_fields(mappings=STALE, schema_policy="manual_review", dest_columns=live)


def test_stamp_naming_a_column_the_table_lacks_still_proposes_add_column():
    assert mappings_require_new_columns(STALE, dest_columns=["id", "name"])
    assert effective_backfill_new_fields(
        mappings=STALE, schema_policy="manual_review", dest_columns=["id", "name"]
    )


def test_unknown_destination_keeps_the_legacy_reading():
    assert mappings_require_new_columns(STALE)
    assert effective_backfill_new_fields(mappings=STALE, schema_policy="manual_review")


def test_approved_evolution_arms_add_column_for_the_columns_it_added():
    from services.schema_drift import plan_destination_evolution

    planned, _types, _applied = plan_destination_evolution(
        [{"source": "id", "target": "id"}, {"source": "extra", "target": "extra"}],
        source_columns=["id", "extra"],
        source_schema={"id": "INTEGER", "extra": "TEXT"},
        dest_types={"id": "INTEGER"},
        dest_db="postgresql",
    )
    assert effective_backfill_new_fields(
        schema_policy="manual_review", mappings=planned, dest_columns=["id"]
    )
    # ...and only for them: the acknowledged run does not open the writer's
    # whole backfill lane, so no unrelated column is widened or added.
    assert not effective_backfill_new_fields(
        schema_policy="manual_review", mappings=planned[:1], dest_columns=["id"]
    )


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


def test_replayed_create_new_mappings_do_not_add_a_column_on_run_two(tmp_path, isolated):
    """Engine core (writer): the stored maps carry create_new, the source grew."""
    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    conn = sqlite3.connect(src)
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, qty INTEGER)")
    conn.executemany("INSERT INTO t VALUES (?,?,?)", [(i, f"n{i}", i) for i in range(1, 4)])
    conn.commit()

    def request(**extra):
        return TransferRequest(
            source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
            destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t"),
            sync_mode="incremental_append",
            stream_contracts=[
                {
                    "name": "t",
                    "cursor_field": "id",
                    "cursor_semantics": "monotonic_sequence",
                    "sync_mode": "incremental_append",
                    "selected": True,
                }
            ],
            skip_preflight=True,
            **extra,
        )

    def run(req):
        job_id = "d119" + uuid.uuid4().hex[:16]
        isolated.update_job_status(job_id, "pending", transfer_request={})
        return UniversalTransferEngine().execute_tracked(req, job_id)

    assert run(request()).success
    conn.execute("ALTER TABLE t ADD COLUMN extra TEXT")
    conn.execute("INSERT INTO t VALUES (4, 'n4', 4, 'x4')")
    conn.commit()

    result = run(
        request(mappings=[*STALE, {"source": "extra", "target": "extra", "confidence": 1.0}])
    )

    columns = [r[1] for r in sqlite3.connect(dst).execute("PRAGMA table_info(t)")]
    assert columns == ["id", "name", "qty"], "manual review must not ADD COLUMN"
    assert not result.success
