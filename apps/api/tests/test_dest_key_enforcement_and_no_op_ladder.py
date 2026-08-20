"""Two Gate-8 honesty rules, read from the destination instead of assumed.

1. A key-scoped digest is only comparable when the *destination* rejects a
   duplicate of that key. A key named by the stream contract, the merge request
   or Map's identity inference guarantees nothing on an append-only write, so
   the enforcement question is answered from the destination catalog.
2. A quiet incremental poll read nothing past the watermark: there is no batch
   for the verification ladder to judge, and turning population evidence on it
   compares a zero-row write against a sink that legitimately holds earlier
   rows — which failed the normal outcome of every scheduled incremental sync.
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.reconcile_coverage import (  # noqa: E402
    NO_OP_DEST_UNCHANGED,
    is_no_op_report,
)
from src.transfer.models import EndpointConfig  # noqa: E402
from src.transfer.reconcile_step import (  # noqa: E402
    _destination_enforces_single_key,
    _maybe_attach_verification_ladder,
)


def _sqlite_dest(tmp_path: Path, ddl: str, table: str) -> dict[str, str]:
    db = tmp_path / "dest.db"
    conn = sqlite3.connect(db)
    try:
        conn.execute(ddl)
        conn.commit()
    finally:
        conn.close()
    return {"database": str(db), "table": table}


def test_keyless_table_does_not_enforce_the_mapped_identity(tmp_path: Path) -> None:
    cfg = _sqlite_dest(
        tmp_path, 'CREATE TABLE "t_keyless" ("id" INTEGER, "amount" TEXT)', "t_keyless"
    )
    assert (
        _destination_enforces_single_key(
            "sqlite", cfg, schema="", table_name="t_keyless", key_column="id"
        )
        is False
    )


def test_declared_primary_key_enforces_the_identity(tmp_path: Path) -> None:
    cfg = _sqlite_dest(
        tmp_path,
        'CREATE TABLE "t_pk" ("id" INTEGER PRIMARY KEY, "amount" TEXT)',
        "t_pk",
    )
    assert (
        _destination_enforces_single_key(
            "sqlite", cfg, schema="", table_name="t_pk", key_column="id"
        )
        is True
    )


def test_declared_unique_key_enforces_the_identity(tmp_path: Path) -> None:
    cfg = _sqlite_dest(
        tmp_path,
        'CREATE TABLE "t_uq" ("id" INTEGER UNIQUE, "amount" TEXT)',
        "t_uq",
    )
    assert (
        _destination_enforces_single_key(
            "sqlite", cfg, schema="", table_name="t_uq", key_column="id"
        )
        is True
    )


def test_composite_key_does_not_enforce_one_of_its_columns(tmp_path: Path) -> None:
    """Half of a composite key can repeat, so it names more than one row."""
    cfg = _sqlite_dest(
        tmp_path,
        'CREATE TABLE "t_comp" ("id" INTEGER, "day" TEXT, PRIMARY KEY ("id", "day"))',
        "t_comp",
    )
    assert (
        _destination_enforces_single_key(
            "sqlite", cfg, schema="", table_name="t_comp", key_column="id"
        )
        is False
    )


def test_missing_table_leaves_enforcement_unproven(tmp_path: Path) -> None:
    """A probe that cannot read the catalog must not claim comparability."""
    cfg = _sqlite_dest(
        tmp_path, 'CREATE TABLE "t_other" ("id" INTEGER)', "t_absent"
    )
    assert (
        _destination_enforces_single_key(
            "sqlite", cfg, schema="", table_name="t_absent", key_column="id"
        )
        is False
    )


def test_advisory_catalog_keys_do_not_enforce(monkeypatch) -> None:
    """Snowflake/BigQuery-class ``NOT ENFORCED`` keys are metadata, not a rule."""
    import src.transfer.reconcile_step as step

    def _fake(
        db_type, cfg, table, headers, records=None, *, strict_namespace=False
    ):  # noqa: ANN001, ANN202
        return (
            {"id": "NUMBER"},
            {"id": True},
            {
                "primary_key_columns": ["id"],
                "unique_keys": [],
                "warnings": ["Snowflake primary keys are NOT ENFORCED"],
            },
        )

    monkeypatch.setattr(step, "_introspect_table_schema_rich", _fake)
    assert (
        _destination_enforces_single_key(
            "snowflake", {"table": "t"}, schema="s", table_name="t", key_column="id"
        )
        is False
    )


def test_no_op_poll_is_not_re_judged_by_the_verification_ladder(tmp_path: Path) -> None:
    """The ladder must not veto a quiet poll it has no batch to verify."""
    report = {
        "passed": True,
        "message": "No new source rows since the last watermark — nothing written.",
        "assurance_level": NO_OP_DEST_UNCHANGED,
        "source_rows": 0,
        "target_rows": 2,
    }
    assert is_no_op_report(report) is True
    endpoint = EndpointConfig(
        kind="database",
        format="sqlite",
        database=str(tmp_path / "dest.db"),
        table="t",
    )
    out = _maybe_attach_verification_ladder(
        report,
        endpoint=endpoint,
        source_endpoint=endpoint,
        records=[],
        columns=["id"],
        dest_summary={},
        mappings=[{"source": "id", "target": "id"}],
        validation_mode="maximum",
    )
    assert out["passed"] is True
    assert out["assurance_level"] == NO_OP_DEST_UNCHANGED
    assert "verification_ladder" not in out
