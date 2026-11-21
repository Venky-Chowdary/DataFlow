"""D187 (QA batch 1 note): planning into Snowflake took 103 s on the live account.

Profiled on fakesnow: a plan into Snowflake (new table and existing table,
31 columns) opens one Snowflake connection and issues five metadata
statements in all — session context, DESC TABLE, SHOW TERSE TABLES, two SHOW
GRANTS — with no per-column query and no INFORMATION_SCHEMA scan, in about
0.6 s (new) / 1.4 s (existing) locally. No N+1 exists on this path today; this
test pins that budget so one cannot creep in. The live 103 s needs the real
account's QUERY_HISTORY to attribute (login, warehouse resume, SHOW GRANTS on
a large account) — see the track report.
"""

from __future__ import annotations

import re

import pytest

pytest.importorskip("fakesnow")

from tests.test_d72_d73_d77_snowflake_mcp_e2e import (  # noqa: E402,F401
    _run,
    _source,
    _tool,
    route,
    snowflake_semantics,
)

#: Statements a plan may send Snowflake, whatever the column count.
STATEMENT_BUDGET = 8


def test_plan_into_snowflake_metadata_is_constant_in_columns(route, monkeypatch):
    import fakesnow.conn as fconn
    from fakesnow.cursor import FakeSnowflakeCursor

    cols = ", ".join(f"c{i} text" for i in range(30))
    src = _source(route, f"id int PRIMARY KEY, {cols}",
                  [tuple([1] + ["x"] * 30), tuple([2] + ["y"] * 30)], "d187")
    existing = "D187_OLD_" + route["sfx"].upper()
    _run({"source_connector_id": route["src"], "source_table": src,
          "dest_connector_id": route["dst"], "dest_table": existing,
          "sync_mode": "full_refresh_overwrite"})

    seen: list[str] = []
    opens: list[int] = []
    strict = FakeSnowflakeCursor.execute
    init = fconn.FakeSnowflakeConnection.__init__

    def execute(self, command, *a, **k):
        seen.append(re.sub(r"\s+", " ", command or ""))
        return strict(self, command, *a, **k)

    def connect(self, *a, **k):
        opens.append(1)
        return init(self, *a, **k)

    monkeypatch.setattr(FakeSnowflakeCursor, "execute", execute)
    monkeypatch.setattr(fconn.FakeSnowflakeConnection, "__init__", connect)
    for dest in ("D187_NEW_" + route["sfx"].upper(), existing):
        seen.clear()
        opens.clear()
        plan = _tool("plan_transfer", {
            "source_connector_id": route["src"], "source_table": src,
            "dest_connector_id": route["dst"], "dest_table": dest,
            "sync_mode": "full_refresh_overwrite",
        })
        assert plan.success, plan.error
        assert len(opens) <= 1, (dest, len(opens))
        assert len(seen) <= STATEMENT_BUDGET, (dest, seen)
        upper = " ".join(seen).upper()
        assert "INFORMATION_SCHEMA" not in upper, seen
        assert not [s for s in seen if re.search(r"\bC\d+\b", s)], "no per-column statement"
