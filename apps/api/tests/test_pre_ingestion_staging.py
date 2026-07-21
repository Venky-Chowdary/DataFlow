"""Pre-ingestion staging — real SQLite stage → promote clean rows only."""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_staging_table_name_stable():
    from services.pre_ingestion_staging import staging_table_name

    assert staging_table_name("users") == "users_df_staging"
    assert staging_table_name("public.orders") == "public_orders_df_staging"


def test_pre_ingestion_staging_balanced_excludes_bad_from_primary(tmp_path: Path):
    """Balanced + staging: clean row on primary; bad row only in staging + DLQ."""
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    dest_path = tmp_path / "stg.db"
    conn = f"sqlite:///{dest_path}"
    csv = b"id,age\n1,30\n2,not-a-number\n"
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        destination=EndpointConfig(
            kind="database",
            format="sqlite",
            table="users",
            connection_string=conn,
            database=str(dest_path),
        ),
        source_filename="users.csv",
        source_content=csv,
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        validation_mode="balanced",
        write_via_staging=True,
        mappings=[
            {"source": "id", "target": "id", "confidence": 0.95},
            {"source": "age", "target": "age", "confidence": 0.95, "target_type": "integer"},
        ],
        column_types={"id": "string", "age": "string"},
    )
    result = UniversalTransferEngine().execute(request)
    assert result.success is True
    ds = result.destination_summary or {}
    assert ds.get("staging_table") == "users_df_staging"
    assert int(ds.get("staged_rows") or 0) >= 2
    assert int(ds.get("promoted_rows") or 0) == 1
    assert int(ds.get("rejected_rows") or 0) >= 1

    with sqlite3.connect(dest_path) as db:
        tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        assert "users" in tables
        assert "users_df_staging" in tables
        assert "users_df_quarantine" in tables
        primary = db.execute("SELECT id, age FROM users ORDER BY id").fetchall()
        assert primary == [("1", "30")] or primary == [(1, 30)] or [
            (str(r[0]), str(r[1]) if r[1] is not None else None) for r in primary
        ] == [("1", "30")]
        # Primary must not contain the bad row (id=2) — that is the staging guarantee.
        ids = {str(r[0]) for r in db.execute("SELECT id FROM users").fetchall()}
        assert "2" not in ids
        assert len(ids) == 1
        assert "1" in ids
        staged = db.execute("SELECT COUNT(*) FROM users_df_staging").fetchone()[0]
        assert staged >= 2
        dlq = db.execute("SELECT COUNT(*) FROM users_df_quarantine").fetchone()[0]
        assert dlq >= 1


def test_pre_ingestion_staging_strict_blocks_promote(tmp_path: Path):
    """Strict + staging: primary untouched when any row fails."""
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    dest_path = tmp_path / "stg_strict.db"
    conn = f"sqlite:///{dest_path}"
    # Seed primary so we can prove it was not overwritten.
    with sqlite3.connect(dest_path) as db:
        db.execute("CREATE TABLE users (id TEXT, age TEXT)")
        db.execute("INSERT INTO users VALUES ('seed', '1')")
        db.commit()

    csv = b"id,age\n1,30\n2,bad\n"
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        destination=EndpointConfig(
            kind="database",
            format="sqlite",
            table="users",
            connection_string=conn,
            database=str(dest_path),
        ),
        source_filename="users.csv",
        source_content=csv,
        sync_mode="full_refresh_append",  # do not drop; prove untouched
        skip_preflight=True,
        validation_mode="strict",
        write_via_staging=True,
        mappings=[
            {"source": "id", "target": "id", "confidence": 0.95},
            {"source": "age", "target": "age", "confidence": 0.95, "target_type": "integer"},
        ],
    )
    result = UniversalTransferEngine().execute(request)
    assert result.success is False
    ds = result.destination_summary or {}
    assert ds.get("promote_blocked") is True
    assert ds.get("staging_table") == "users_df_staging"

    with sqlite3.connect(dest_path) as db:
        primary = db.execute("SELECT id FROM users").fetchall()
        assert primary == [("seed",)]
        assert db.execute("SELECT COUNT(*) FROM users_df_staging").fetchone()[0] >= 2
