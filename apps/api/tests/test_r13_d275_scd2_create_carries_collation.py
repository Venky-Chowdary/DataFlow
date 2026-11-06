"""D275 (SCD2) — run 2 of an SCD2 history into the SQL Server table run 1 created.

The SCD2 history table is built by ``scd2_engine._build_scd_table``, which
never received the source catalog, and the CREATE owner dropped the
``COLLATE`` the Map stamp already spelled. A MySQL ``utf8mb4_0900_ai_ci``
source therefore got ``SQL_Latin1_General_CP1_CI_AS`` (server default) on
run 1 and run 2 refused DataFlow's own table as a fidelity collapse — the
mirror half was fixed earlier, this is the same family through the other
create path. Keys are never carried onto the versioned table.

Live MySQL 8 -> SQL Server 2022, SCD2, preflight ON, strict, unicode / '' /
NULL text, a changed row between the runs.
"""

from __future__ import annotations

import pytest

from tests import r13_mapping_harness as h
from tests.r13_mapping_harness import isolated  # noqa: F401 - fixture

pytestmark = pytest.mark.timeout(300)

_ROWS = [(1, "Zoë 東京 🚀"), (2, ""), (3, None), (4, "plain")]


def _scd2(fake, table: str, out: str):
    return h.execute(
        fake, h.endpoint("mysql", table), h.endpoint("sqlserver", out),
        sync_mode="scd2",
        stream_contracts=[{"name": table, "sync_mode": "scd2", "primary_key": "id", "selected": True}],
    )


def test_scd2_run_two_into_the_created_sqlserver_table(isolated):  # noqa: F811
    table = f"d275s_{h.token()}"
    out = f"{table}_h"
    try:
        h.run_sql(
            "mysql",
            f"CREATE TABLE `{table}` (id INT PRIMARY KEY, "
            "name VARCHAR(100) COLLATE utf8mb4_0900_ai_ci) CHARACTER SET utf8mb4",
        )
        h.run_sql("mysql", f"INSERT INTO `{table}` VALUES (%s, %s)", params=_ROWS)  # nosec B608
        first = _scd2(isolated, table, out)
        assert first.success, first.error
        collation = h.run_sql(
            "sqlserver",
            "SELECT collation_name FROM sys.columns "  # nosec B608
            f"WHERE object_id = OBJECT_ID('{out}') AND name = 'name'",
        )[0][0]
        assert collation == "Latin1_General_CI_AI", collation
        # A versioned table carries no unique business key.
        assert not h.run_sql(
            "sqlserver",
            "SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID("  # nosec B608
            f"'{out}') AND (is_primary_key = 1 OR is_unique = 1)",
        )
        h.run_sql("mysql", f"UPDATE `{table}` SET name = 'changed' WHERE id = 4")  # nosec B608
        second = _scd2(isolated, table, out)
        assert second.success, second.error
        versions = h.run_sql(
            "sqlserver", f"SELECT id, name FROM [{out}] WHERE id = 4 ORDER BY valid_from"  # nosec B608
        )
        assert [r[1] for r in versions] == ["plain", "changed"], versions
    finally:
        h.drop("mysql", table)
        h.drop("sqlserver", out)
