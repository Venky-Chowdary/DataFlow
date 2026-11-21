"""Engine dialects of Parity at Scale.

A dialect turns the shared column plan into SQL for one engine and exposes the
handful of operations the driver needs (:class:`.base.Side`). Phase 1 ships
SQLite (the reference engine: its UDFs call the Python CE1 encoder) and
PostgreSQL. The first certification wave (owner decision 3: PostgreSQL↔SQL
Server, MySQL↔PostgreSQL, SQL Server↔SQL Server, MySQL↔MySQL and MariaDB as
its own leg) plugs in here in phase 2; until then those engines raise
:class:`services.parity_scale.plan.PairNotCertified`.
"""

from __future__ import annotations

from typing import Any

from services.parity_scale.plan import PairNotCertified, normalize_engine

#: Engines with a dialect in this build, and the wave-1 engines still to come.
IMPLEMENTED = ("sqlite", "postgresql")
PLANNED = ("mysql", "mariadb", "sqlserver")


def open_side(engine: str, **kwargs: Any):
    """Build the :class:`~.base.Side` of ``engine`` (``role``, ``plan``, connection args)."""
    eng = normalize_engine(engine)
    if eng == "sqlite":
        from services.parity_scale.dialects.sqlite import SqliteSide

        return SqliteSide(**kwargs)
    if eng == "postgresql":
        from services.parity_scale.dialects.postgresql import PostgresSide

        return PostgresSide(**kwargs)
    if eng in PLANNED:
        raise PairNotCertified(f"the {eng} dialect is part of the first certification wave (phase 2)")
    raise PairNotCertified(f"no Parity at Scale dialect for {eng or 'an unknown engine'}")
