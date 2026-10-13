"""Round-5 regressions: quarantine checksum, qty onto an existing column."""

from __future__ import annotations

import pytest

from services.mapping_pipeline import run_mapping_pipeline
from services.reconcile_coverage import WRITTEN_BATCH_KEYS
from services.reconciliation import reconcile
from services.transform_engine import infer_transform_for_mapping


def test_checksum_keeps_landed_rows_when_only_quarantine_differs() -> None:
    report = reconcile(
        source_rows=20,
        target_rows=15,
        source_checksum="a" * 64,
        target_checksum="b" * 64,
        rejected_rows=5,
        strict_checksum=False,
        sample_compare={"passed": True, "compared": 40, "rows_compared": 15},
    )
    assert report.passed is True
    assert "quarantined" in report.message
    assert report.checksum_match is None
    stamped = report.to_dict()
    assert stamped["passed"] is True
    assert stamped["assurance_level"] == "quarantine_excluded"
    assert stamped["checksum_match"] is None
    from services.verification_ladder import attach_ladder_to_reconcile_report

    kept = attach_ladder_to_reconcile_report(
        stamped,
        {
            "passed": False,
            "skipped": False,
            "layers": {
                "L1": {"passed": True},
                "L2": {"passed": False, "details": {}},
                "L3": {"passed": True},
            },
        },
    )
    assert kept["passed"] is True
    disagreed = attach_ladder_to_reconcile_report(
        dict(stamped),
        {
            "passed": False,
            "skipped": False,
            "layers": {
                "L1": {"passed": True},
                "L2": {"passed": False, "details": {}},
                "L3": {"passed": False},
            },
            "localization_summary": "cell mismatch",
        },
    )
    assert disagreed["passed"] is False


def test_written_key_digest_still_fails_when_cells_disagree() -> None:
    report = reconcile(
        source_rows=10,
        target_rows=8,
        source_checksum="a" * 64,
        target_checksum="b" * 64,
        rejected_rows=2,
        checksum_scope=WRITTEN_BATCH_KEYS,
        sample_compare={"passed": True, "compared": 8, "rows_compared": 8},
    )
    assert report.passed is False


def test_text_qty_stays_on_the_existing_integer_column() -> None:
    cols = [
        "id", "qty", "pct", "price", "payload", "blob", "label", "code", "updated_at",
    ]
    src = [{"name": c, "inferred_type": "TEXT", "samples": ["1", "2"]} for c in cols]
    src[1]["samples"] = ["1", "2", "10", "bad"]
    dst_types = {
        "id": "BIGINT",
        "qty": "INTEGER",
        "pct": "NUMERIC(8,4)",
        "price": "NUMERIC(12,2)",
        "payload": "JSONB",
        "blob": "BYTEA",
        "label": "TEXT",
        "code": "VARCHAR(2)",
        "updated_at": "TIMESTAMPTZ",
    }
    out = run_mapping_pipeline(
        cols,
        list(dst_types),
        source_schemas=src,
        target_schemas=[{"name": n, "inferred_type": t} for n, t in dst_types.items()],
        source_samples={c["name"]: list(c["samples"]) for c in src},
        use_llm=False,
        destination_db_type="postgresql",
        source_db_type="postgresql",
        destination_table_exists=True,
        source_types_authoritative=True,
        validation_mode="balanced",
    )
    qty = next(m for m in out["mappings"] if m["source"] == "qty")
    assert qty["target"] == "qty"
    assert not qty.get("create_new")
    assert "qty_text" not in {m["target"] for m in out["mappings"]}


def test_varchar_qty_plan_does_not_cast_as_integer() -> None:
    assert infer_transform_for_mapping("qty", "qty", "VARCHAR", "VARCHAR") == "none"
    assert infer_transform_for_mapping("qty", "qty", "TEXT", "INTEGER") == "integer"


def test_destination_sqlite_probe_creates_a_new_file(tmp_path) -> None:
    """A new destination file in an existing directory is not 'not on this server'."""
    from connectors.sqlite import test_sqlite
    from connectors.sqlite_common import require_sqlite_source_file

    dest = tmp_path / "new_dest.sqlite"
    opened = test_sqlite(
        host="",
        port=0,
        database=str(dest),
        username="",
        password="",
        schema="",
        connection_string="",
        ssl=False,
        require_object=False,
    )
    assert opened.ok is True, opened.error
    assert dest.is_file()
    # The read path still refuses a file that is not already there, and does
    # not create one (R3-H3).
    missing = tmp_path / "gone.sqlite"
    with pytest.raises(ValueError, match="not on this server"):
        require_sqlite_source_file(str(missing))
    assert not missing.exists()


def test_resumed_sqlite_overwrite_keeps_the_committed_prefix(tmp_path) -> None:
    """A tail-only stage swap used to replace the rows already committed."""
    import sqlite3

    from connectors.sqlite_writer import write_mapped_rows

    path = tmp_path / "resume.sqlite"
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
    conn.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)")
    conn.commit()
    conn.close()
    mappings = [
        {"source": "id", "target": "id"},
        {"source": "v", "target": "v"},
    ]
    result = write_mapped_rows(
        host="",
        port=0,
        database=str(path),
        username="",
        password="",
        schema="",
        connection_string="",
        ssl=False,
        table_name="t",
        headers=["id", "v"],
        data_rows=[["4", "40"], ["5", "50"], ["6", "60"]],
        mappings=mappings,
        column_types={"id": "INTEGER", "v": "INTEGER"},
        write_mode="upsert",
        conflict_columns=["id"],
        atomic_overwrite=True,
        # Stream batches are 1-based, so index 1 is the first chunk and must
        # open a fresh stage. Index 2 with no stage is a resumed tail: the
        # prefix is already on the live table.
        file_batch_idx=2,
        total_chunks=3,
    )
    assert result.ok, result.error
    seen = sqlite3.connect(path)
    try:
        rows = seen.execute("SELECT id, v FROM t ORDER BY id").fetchall()
    finally:
        seen.close()
    assert rows == [(1, 10), (2, 20), (3, 30), (4, 40), (5, 50), (6, 60)]
