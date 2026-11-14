"""A MySQL ``TIMESTAMP`` into the MySQL table Datawrap created is not graded lossy.

The fidelity predicate read the source token with no engine, so a bare
``TIMESTAMP`` was a zoneless wall clock, while the destination token on MySQL
is an instant: every MySQL → MySQL ``TIMESTAMP`` (including the
``TIMESTAMP(6)`` create-new itself emits) read as "invents a zone", and the
second run into Datawrap's own table needed a Risk Contract. With the source
engine bound, the same engine's token means the same thing on both sides.
Polarity changes that are real stay flagged.
"""

from __future__ import annotations

import os
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from services.source_engine_scope import bind_source_engine  # noqa: E402
from services.type_coercion import is_lossy_coercion, is_precision_collapse_coercion  # noqa: E402
from services.type_system import create_new_mapping_target_type  # noqa: E402
from tests.c3_live_helpers import mysql_endpoint, mysql_sql, run_transfer  # noqa: E402
from tests.test_commit_safe_review_round2 import isolated  # noqa: E402,F401


def _graded_lossy(engine: str, source_type: str, dest: str, target: str) -> bool:
    with bind_source_engine(engine):
        return bool(
            is_lossy_coercion(source_type, target, dest_db=dest, dest_table_exists=True)
            or is_precision_collapse_coercion(source_type, target, dest_db=dest, dest_table_exists=True)
        )


@pytest.mark.parametrize("engine", ["mysql", "mariadb"])
@pytest.mark.parametrize("source_type", ["timestamp", "timestamp(3)", "TIMESTAMP(6)"])
def test_create_new_timestamp_is_not_lossy_against_itself(engine, source_type):
    target = create_new_mapping_target_type(source_type, engine, source_db=engine)
    assert _graded_lossy(engine, source_type, engine, target) is False, target


@pytest.mark.parametrize(
    "engine,source_type,dest,target",
    [
        ("postgresql", "timestamp", "mysql", "TIMESTAMP(6)"),  # wall clock → instant
        ("mysql", "datetime", "mysql", "TIMESTAMP(6)"),        # MySQL wall clock → instant
        ("mysql", "timestamp", "postgresql", "TIMESTAMP"),     # instant → wall clock (C3)
    ],
)
def test_real_polarity_changes_stay_flagged(engine, source_type, dest, target):
    assert _graded_lossy(engine, source_type, dest, target) is True


def test_live_second_run_into_the_mysql_table_datawrap_created(isolated):  # noqa: F811
    from tests.helpers.live_env import mysql_up

    if not mysql_up():
        pytest.skip("live MySQL not reachable")
    src, dst = f"mts_{uuid.uuid4().hex[:8]}", None
    dst = src + "_d"
    try:
        mysql_sql(f"CREATE TABLE {src} (id INT, at TIMESTAMP(3) NULL)")
        mysql_sql(f"INSERT INTO {src} VALUES (1, '2024-03-10 01:30:00.125'), (2, NULL)")
        first = run_transfer(isolated, mysql_endpoint(src), mysql_endpoint(dst))
        assert first.success, first.error
        second = run_transfer(isolated, mysql_endpoint(src), mysql_endpoint(dst))
        assert second.success, second.error  # Validate does not refuse its own table
        rows = mysql_sql(f"SELECT id, at FROM {dst} ORDER BY id, at", fetch=True)
        assert len(rows) == 4 and {r[0] for r in rows} == {1, 2}, rows
    finally:
        mysql_sql(f"DROP TABLE IF EXISTS {src}")
        mysql_sql(f"DROP TABLE IF EXISTS {dst}")
