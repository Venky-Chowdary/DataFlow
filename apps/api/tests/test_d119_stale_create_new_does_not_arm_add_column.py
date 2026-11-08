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
from tests.test_sync_mode_append_vs_overwrite import two_run_world  # noqa: E402, F401

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


def test_sqlite_preflight_manual_review_then_acknowledged_evolution(tmp_path, two_run_world):
    from services.schema_drift import (
        destination_evolution_approved,
        plan_destination_evolution,
    )
    from services.mapping_constraints import write_mappings

    source_path = tmp_path / "evolution_source.sqlite"
    destination_path = tmp_path / "evolution_destination.sqlite"
    baseline = [(number, f"n{number}", number) for number in range(1, 4)]
    for path in (source_path, destination_path):
        with sqlite3.connect(path) as connection:
            connection.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, qty INTEGER)")
            if path == source_path:
                connection.executemany("INSERT INTO t VALUES (?, ?, ?)", baseline)

    def read_destination():
        with sqlite3.connect(destination_path) as connection:
            schema = connection.execute("PRAGMA table_info(t)").fetchall()
            rows = connection.execute("SELECT * FROM t ORDER BY id").fetchall()
        return schema, rows

    def request(mappings, *, acknowledged=False):
        return TransferRequest(
            source=EndpointConfig(
                kind="database", format="sqlite", database=str(source_path), table="t",
            ),
            destination=EndpointConfig(
                kind="database", format="sqlite", database=str(destination_path), table="t",
            ),
            mappings=mappings,
            sync_mode="incremental_append",
            stream_contracts=[{
                "name": "t", "primary_key": "id", "cursor_field": "id",
                "cursor_semantics": "insert_only", "sync_mode": "incremental_append",
                "selected": True,
            }],
            schema_policy="manual_review",
            validation_mode="balanced",
            schema_drift_acknowledged=acknowledged,
            acknowledgment_actor="qa@example.com" if acknowledged else "",
            acknowledgment_reason="Approve mapped extra only" if acknowledged else "",
        )

    def run(transfer_request):
        assert transfer_request.skip_preflight is False
        assert transfer_request.backfill_new_fields is False
        job_id = "d119_evolution_" + uuid.uuid4().hex
        two_run_world.update_job_status(job_id, "pending", transfer_request={})
        return UniversalTransferEngine().execute_tracked(transfer_request, job_id)

    initial = run(request([dict(mapping, confidence=1.0) for mapping in STALE]))
    assert initial.success, initial.error
    assert initial.validation_plan
    assert initial.reconciliation.get("passed") is True, initial.reconciliation
    baseline_schema, baseline_rows = read_destination()
    assert baseline_rows == baseline
    assert [column[1] for column in baseline_schema] == ["id", "name", "qty"]

    with sqlite3.connect(source_path) as connection:
        connection.execute("ALTER TABLE t ADD COLUMN extra TEXT")
        connection.execute("ALTER TABLE t ADD COLUMN unrelated TEXT")
        connection.execute("INSERT INTO t VALUES (4, 'n4', 4, 'x4', 'not approved')")
        source_schema = {
            column[1]: column[2]
            for column in connection.execute("PRAGMA table_info(t)")
        }

    mappings = [
        *[dict(mapping, confidence=1.0) for mapping in STALE],
        {"source": "extra", "target": "extra", "confidence": 1.0},
        {"source": "unrelated", "target": "", "intentional_omit": True},
    ]
    assert not destination_evolution_approved("manual_review", acknowledged=False)
    refused = run(request(mappings))
    assert read_destination() == (baseline_schema, baseline_rows)
    assert refused.success is False, refused
    assert refused.records_transferred == 0, refused
    assert refused.validation_plan, refused.error
    assert "extra" in str(refused.validation_plan) + str(refused.error)

    assert destination_evolution_approved("manual_review", acknowledged=True)
    planned, evolved_types, applied = plan_destination_evolution(
        mappings,
        source_columns=list(source_schema),
        source_schema=source_schema,
        dest_types={column[1]: column[2] for column in baseline_schema},
        dest_db="sqlite",
        adopt_unmapped=False,
    )
    assert [change["target"] for change in applied] == ["extra"]
    assert list(evolved_types) == ["id", "name", "qty", "extra"]
    assert [mapping["target"] for mapping in write_mappings(planned)] == ["id", "name", "qty", "extra"]

    approved = run(request(mappings, acknowledged=True))
    final_schema, final_rows = read_destination()
    evidence = {
        "error": approved.error, "validation_plan": approved.validation_plan,
        "reconciliation": approved.reconciliation,
        "schema": final_schema, "rows": final_rows,
    }
    assert approved.success is True, evidence
    assert approved.validation_plan, evidence
    assert approved.validation_plan.get("passed") is True, evidence
    assert final_schema[:len(baseline_schema)] == baseline_schema, evidence
    assert final_schema[len(baseline_schema):] == [(3, "extra", "TEXT", 0, None, 0)], evidence
    assert final_rows == [(*row, None) for row in baseline] + [(4, "n4", 4, "x4")], evidence
    assert approved.records_transferred == 1, evidence
    assert approved.reconciliation.get("passed") is True, evidence


@pytest.mark.parametrize("change", ["narrow", "primary_key_change"])
def test_acknowledged_schema_evolution_still_pauses_on_hard_changes(change):
    from services.schema_drift import detect_schema_drift, execute_must_pause
    from services.schema_fingerprint import fingerprint_schema

    previous_schema = {"id": "INTEGER", "qty": "DECIMAL(12,2)"}
    key = "sku" if change == "primary_key_change" else "id"
    live_schema = {
        key: "INTEGER", "qty": "DECIMAL(6,2)" if change == "narrow" else "DECIMAL(12,2)",
    }
    report = detect_schema_drift(
        source_columns=list(live_schema),
        source_schema=live_schema,
        target_columns=list(previous_schema),
        target_schema=previous_schema,
        mappings=[
            {"source": key, "target": "id", "confidence": 1.0},
            {"source": "qty", "target": "qty", "confidence": 1.0},
        ],
        previous_source_columns=list(previous_schema),
        previous_source_schema=previous_schema,
        stored_source_fp=fingerprint_schema(list(previous_schema), previous_schema),
        previous_primary_key=["id"],
        live_primary_key=[key],
        schema_policy="manual_review",
        destination_db_type="sqlite",
    )
    evolution = report["schema_evolution"]
    assert evolution["hard_breaking"], report
    if change == "primary_key_change":
        assert any(item["kind"] == change for item in evolution["hard_breaking"]), report
    else:
        assert any(
            item.get("column") == "qty" and item["kind"] == "narrow_type"
            for item in evolution["hard_breaking"]
        ), report
    assert execute_must_pause(evolution, schema_drift_acknowledged=True) is True, report
