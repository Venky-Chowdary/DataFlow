"""D378: the 5s lock waits of the empty-table drop end with the drop.

``SET SESSION lock_wait_timeout = DEFAULT`` is the GLOBAL value (MySQL
reference, SET syntax for variable assignment), a year on a stock server, so
the pooled session lost the bounded waits its guards had set. The drop now
restores the session's own earlier values. Live MySQL 8 (:3306).
"""

from __future__ import annotations

import uuid

import pytest


def _engine():
    sa = pytest.importorskip("sqlalchemy")
    pytest.importorskip("pymysql")
    engine = sa.create_engine(
        "mysql+pymysql://dataflow:dataflow@127.0.0.1:3306/dataflow",
        pool_size=1, max_overflow=0, connect_args={"connect_timeout": 3},
    )
    try:
        with engine.connect():
            pass
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"MySQL not reachable: {exc}")
    return engine


def test_the_empty_drop_restores_the_sessions_own_lock_waits():
    from services.batch_undo import _mysql_drop_if_still_empty

    engine = _engine()
    name = f"d378r_{uuid.uuid4().hex[:8]}"
    try:
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.exec_driver_sql("SET SESSION lock_wait_timeout = 77")
            conn.exec_driver_sql("SET SESSION innodb_lock_wait_timeout = 66")
            conn.exec_driver_sql(f"CREATE TABLE `{name}` (id INT)")
        outcome = _mysql_drop_if_still_empty(
            engine,
            f"LOCK TABLES `{name}` WRITE",
            f"SELECT COUNT(*) FROM `{name}`",
            f"DROP TABLE `{name}`",
        )
        assert outcome == "dropped"
        # pool_size=1: the same session the drop used.
        with engine.connect() as conn:
            row = conn.exec_driver_sql(
                "SELECT @@SESSION.lock_wait_timeout, @@SESSION.innodb_lock_wait_timeout"
            ).fetchone()
        assert (int(row[0]), int(row[1])) == (77, 66), row
    finally:
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.exec_driver_sql(f"DROP TABLE IF EXISTS `{name}`")
        engine.dispose()
