"""Engine dialects of Parity at Scale.

A dialect turns the shared column plan into SQL for one engine and exposes the
handful of operations the driver needs (:class:`.base.Side`). SQLite is the
reference engine (its UDFs call the Python CE1 encoder); PostgreSQL, MySQL /
MariaDB and SQL Server share one SQL shape (:mod:`.sqlbase`) with their own
spellings — the first certification wave (owner decision 3). Oracle and the
warehouses raise :class:`services.parity_scale.plan.PairNotCertified`.
"""

from __future__ import annotations

from typing import Any

from services.parity_scale.plan import PairNotCertified, normalize_engine

#: Engines with a dialect in this build (MariaDB is the ``mysql`` dialect),
#: and engines whose dialects come in later phases.
IMPLEMENTED = ("sqlite", "postgresql", "mysql", "mariadb", "sqlserver")
PLANNED = ("oracle", "snowflake", "bigquery")


def open_side(engine: str, **kwargs: Any):
    """Build the :class:`~.base.Side` of ``engine`` (``role``, ``plan``, connection args)."""
    eng = normalize_engine(engine)
    if eng == "sqlite":
        from services.parity_scale.dialects.sqlite import SqliteSide

        return SqliteSide(**kwargs)
    if eng == "postgresql":
        from services.parity_scale.dialects.postgresql import PostgresSide

        return PostgresSide(**kwargs)
    if eng == "mysql":
        from services.parity_scale.dialects.mysql import MySQLSide

        return MySQLSide(**kwargs)
    if eng == "sqlserver":
        from services.parity_scale.dialects.sqlserver import SqlServerSide

        return SqlServerSide(**kwargs)
    if eng in PLANNED:
        raise PairNotCertified(f"the {eng} dialect comes in a later phase (warehouses: 6, Oracle: 7)")
    raise PairNotCertified(f"no Parity at Scale dialect for {eng or 'an unknown engine'}")
