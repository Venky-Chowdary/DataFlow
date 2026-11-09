"""QA2 D-1 — an absent ``<table>_df_quarantine`` is a skip on every dialect.

``mark_dlq_promoted`` recognised PostgreSQL's "does not exist" by regex but not
MySQL's "Table '...' doesn't exist" (error 1146), so a MySQL replay whose
findings lived only in the control-plane DLQ came back with ``error`` and the
quarantine closure was held at ``diverging`` forever. The classification now
goes through the shared ``sql_object_identity.is_missing_relation_error``
(driver codes, never prose), so a privilege refusal still surfaces as an error.
"""

from __future__ import annotations

import os
import uuid

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import pytest  # noqa: E402

from services.dest_quarantine import mark_dlq_promoted  # noqa: E402
from services.quarantine_dlq import closure_with_promotion_outcome  # noqa: E402
from src.transfer.models import EndpointConfig  # noqa: E402
from tests.helpers.live_env import mysql_creds, mysql_up  # noqa: E402


class _Orig(Exception):
    def __init__(self, *args, pgcode=None):
        super().__init__(*args)
        self.pgcode = pgcode


class _Wrapped(Exception):
    """SQLAlchemy-style DBAPIError: the driver error rides on ``.orig``."""

    def __init__(self, orig):
        super().__init__(str(orig))
        self.orig = orig


class _RaisingEngine:
    def __init__(self, exc):
        self._exc = exc

    def begin(self):
        raise self._exc


@pytest.mark.parametrize(
    "fmt, orig, missing",
    [
        ("postgresql", _Orig('relation "t_df_quarantine" does not exist', pgcode="42P01"), True),
        ("mysql", _Orig(1146, "Table 'dataflow.t_df_quarantine' doesn't exist"), True),
        ("sqlserver", _Orig("42S02", "[42S02] Invalid object name 't_df_quarantine'. (208)"), True),
        ("sqlserver", _Orig(208, b"Invalid object name 't_df_quarantine'."), True),
        ("mysql", _Orig(1142, "UPDATE command denied to user 'low'@'%'"), False),
        ("postgresql", _Orig("permission denied for table t_df_quarantine", pgcode="42501"), False),
        ("sqlserver", _Orig("42000", "The UPDATE permission was denied on the object. (229)"), False),
    ],
)
def test_absent_quarantine_table_is_a_skip_privilege_is_an_error(monkeypatch, fmt, orig, missing):
    import connectors.generic_sql as gs

    monkeypatch.setattr(gs, "get_sqlalchemy_engine", lambda cfg: _RaisingEngine(_Wrapped(orig)))
    dest = EndpointConfig(kind="database", format=fmt, host="h", database="dataflow", table="t")
    meta = mark_dlq_promoted(dest, qids=[], job_id="d1job")
    if missing:
        assert meta.get("skipped") and not meta.get("error"), meta
        assert closure_with_promotion_outcome({"verdict": "closed"}, meta)["verdict"] == "closed"
    else:
        assert meta.get("error") and not meta.get("skipped"), meta
        assert closure_with_promotion_outcome({"verdict": "closed"}, meta)["verdict"] == "diverging"


def test_sqlite_absent_quarantine_table_is_a_skip(tmp_path):
    import sqlite3

    db = tmp_path / "dst.db"
    with sqlite3.connect(str(db)) as conn:
        conn.execute("CREATE TABLE t (id TEXT)")
    dest = EndpointConfig(kind="database", format="sqlite", database=str(db), table="t")
    meta = mark_dlq_promoted(dest, qids=["q1"], job_id="d1job")
    assert meta.get("skipped") and not meta.get("error"), meta


def _mysql_dest(table: str) -> EndpointConfig:
    c = mysql_creds()
    return EndpointConfig(
        kind="database",
        format="mysql",
        host=c["host"],
        port=int(c["port"]),
        database=c["database"],
        username=c["username"],
        password=c["password"],
        table=table,
    )


@pytest.mark.skipif(not mysql_up(), reason="MySQL not reachable")
def test_live_mysql_absent_quarantine_table_keeps_closure_closed():
    """Real MySQL 1146: no ``<table>_df_quarantine`` exists, closure stays closed."""
    import pymysql

    c = mysql_creds()
    table = f"d1_{uuid.uuid4().hex[:8]}"
    conn = pymysql.connect(
        host=c["host"], port=int(c["port"]), user=c["username"],
        password=c["password"], database=c["database"], autocommit=True,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE TABLE `{table}` (id INT PRIMARY KEY)")
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = DATABASE() AND table_name = %s",
                (f"{table}_df_quarantine",),
            )
            assert cur.fetchone()[0] == 0
        meta = mark_dlq_promoted(_mysql_dest(table), qids=[], job_id="d1live")
        assert meta.get("skipped") and not meta.get("error"), meta
        closure = closure_with_promotion_outcome({"verdict": "closed"}, meta, job_id="d1live")
        assert closure["verdict"] == "closed", closure

        # The stamp still lands when the quarantine table does exist.
        with conn.cursor() as cur:
            cur.execute(
                f"CREATE TABLE `{table}_df_quarantine` (`_df_qid` VARCHAR(64), "
                "`_df_job_id` VARCHAR(64), `_df_promoted_at` VARCHAR(64))"
            )
            cur.execute(
                f"INSERT INTO `{table}_df_quarantine` VALUES ('q1', 'd1live', NULL), ('q2', 'other', NULL)"
            )
        meta = mark_dlq_promoted(_mysql_dest(table), qids=[], job_id="d1live")
        assert meta.get("updated") == 1 and not meta.get("error"), meta
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}_df_quarantine`")
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        conn.close()
