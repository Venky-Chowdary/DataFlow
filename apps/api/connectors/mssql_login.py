"""One FreeTDS login attempt per login timeout for SQL Server (D281 / R10).

pymssql's default ``tds_version`` is FreeTDS's ``auto``. ``auto`` opens a
TDS 7.4 login (PRELOGIN, packet type 0x12) and, when that attempt fails, dials
the host *again* with a TDS 5.0 login (packet type 0x02): Sybase ASE's
protocol, which no SQL Server speaks. Against a host that accepts TCP and never
answers, the second dial waited about as long again as the first, so a 15 s
``login_timeout`` ended at ~35 s and a connector Test broke its stated budget
on every host without the Microsoft ODBC driver (production's image included).

``7.4`` is exactly ``auto``'s first attempt, so a reachable server is reached
the same way as before: SQL Server answers LOGIN7 with the highest TDS version
it supports, older 7.x servers included. Only the Sybase retry goes away.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

#: The TDS 7.x login FreeTDS ``auto`` tries first.
PYMSSQL_TDS_VERSION = "7.4"


def pymssql_tds_version(cfg: Mapping[str, Any] | None = None) -> str:
    """The TDS version a pymssql login uses: a declared 7.x one, else 7.4.

    A declared ``auto`` or 5.0 is not honoured: neither reaches SQL Server, and
    ``auto`` brings back the second dial that doubles the login wait.
    """
    declared = str((cfg or {}).get("tds_version") or "").strip()
    return declared if declared.startswith("7.") else PYMSSQL_TDS_VERSION
