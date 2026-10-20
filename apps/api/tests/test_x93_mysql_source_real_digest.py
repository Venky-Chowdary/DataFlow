"""X-93: a MySQL-source route must never be classified engine-copy/count-only.

Live at 616df100: MySQL -> PG / SQLite / SQL Server fast-path writers stamped
``dest_count:N`` + ``dest_count_equals_source_snapshot_count`` on every type
(ENUM, SET, YEAR, DECIMAL(65,30), TIME, TINYINT(1)) — the summary claimed
"The copy ran inside the engines: no row entered this process", which is
false: MySQL shares no engine with those destinations and every row crossed
this process. Correct clean runs only reached "partial", assurance none,
while a tampered destination still reported success.

Fix: the MySQL fast-path writers fold bound values into a two-sided
``sql_rows_sha256:`` digest on the destination DDL basis and re-read the
committed destination (scoped to the loaded partitions when a pk-shard
resume skipped some), same as the X-83 mongo writers. ``copy_mysql_mysql``
is exempt — same engine family, an in-engine count claim is legal there.

Guard: no cross-engine copy_mysql_* writer may stamp a count token.
"""

from __future__ import annotations

import os
import re
from pathlib import Path

import pytest

SERVICES_DIR = Path(__file__).resolve().parents[1] / "services"
MYSQL_CROSS_ENGINE_WRITERS = [
    SERVICES_DIR / "copy_mysql_pg.py",
    SERVICES_DIR / "copy_mysql_sqlite.py",
    SERVICES_DIR / "copy_mysql_sqlserver.py",
    SERVICES_DIR / "copy_mysql_oracle.py",
    SERVICES_DIR / "copy_mysql_s3.py",
    SERVICES_DIR / "copy_mysql_iceberg.py",
    SERVICES_DIR / "copy_mysql_mongo.py",
]

COUNT_TOKENS = re.compile(r'f"(?:dest_count|pk_join_count):')
COUNT_SCOPE = "dest_count_equals_source_snapshot_count"


def test_guard_no_mysql_cross_engine_writer_stamps_count_proof() -> None:
    """MySQL-source writers to a different engine must never emit
    engine-count-only proof (copy_mysql_mysql is exempt — same family)."""
    offenders: list[str] = []
    for path in MYSQL_CROSS_ENGINE_WRITERS:
        text = path.read_text()
        if COUNT_TOKENS.search(text) or f'proof_scope="{COUNT_SCOPE}"' in text:
            offenders.append(path.name)
    assert offenders == [], (
        "MySQL-source cross-engine writer(s) still stamp engine-count-only "
        "proof (rows entered this process; dest_count:N is not proof): "
        + ", ".join(offenders)
    )


def test_mysql_digest_summary_is_not_classified_count_only() -> None:
    """A sql_rows_sha256 summary must not resolve to engine-count proof."""
    from src.transfer.reconcile_step import _engine_count_proof_only

    summary = {
        "checksum": "sql_rows_sha256:abc",
        "source_checksum": "sql_rows_sha256:abc",
        "target_checksum": "sql_rows_sha256:abc",
        "proof_scope": "mapped_columns",
    }
    assert _engine_count_proof_only(summary) is None


def test_partition_skip_resume_proof_not_count_token() -> None:
    """The pk-shard all-skipped resume path must not stamp dest_count:N —
    its proof string is named differently so _engine_count_proof_only stays
    out even though nothing was written this run."""
    from src.transfer.reconcile_step import _engine_count_proof_only

    summary = {
        "checksum": "pk_partition_counts:5",
        "source_checksum": "pk_partition_counts:5",
        "target_checksum": "pk_partition_counts:5",
        "proof_scope": "partition_dest_count_equals_source_snapshot",
    }
    assert _engine_count_proof_only(summary) is None


def test_sql_copy_proof_names_tamper_culprit_key() -> None:
    """The shared helper names the differing row key on a dest tamper."""
    from services.copy_fast_path import SqlCopyDigest, sql_copy_proof

    digest = SqlCopyDigest(
        ["id", "v"],
        sort_key="id",
        dest_db_type="sqlite",
        dest_types={"id": "INTEGER", "v": "TEXT"},
    )
    digest.update([(1, "a"), (2, "b"), (3, "c")])
    with pytest.raises(ValueError, match="culprit key"):
        sql_copy_proof(digest, [(1, "a"), (2, "TAMPERED"), (3, "c")])


@pytest.mark.skipif(
    os.environ.get("DATAFLOW_X93_LIVE") != "1",
    reason="live engines required — DATAFLOW_X93_LIVE=1",
)
def test_x93_mysql_source_types_clean_and_tamper_live():
    """MySQL source {ENUM, SET, YEAR, DECIMAL(65,30), TIME(6) duration,
    TINYINT(1), JSON, VARBINARY} x {PG, SQLite, SQL Server, DuckDB}:
    clean = verified/full_checksum (never 'Row count verified by engine
    copy'), tamper fails naming id 1."""
    pytest.fail("live leg — runs on the user's engine matrix")
