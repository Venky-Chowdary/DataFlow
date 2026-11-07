"""D275 — mirror run 2 into the SQL Server table DataFlow created in run 1.

Mirror (and SCD2) take the buffered write path (D250). That path never handed
the writer the source catalog, so its CREATE TABLE got the server default
collation (``SQL_Latin1_General_CP1_CI_AS``) instead of the one the plan
stamped for a MySQL ``utf8mb4_0900_ai_ci`` source (``Latin1_General_CI_AI``).
Run 1 passed; run 2 re-read the table and refused it as a fidelity collapse:
"name VARCHAR(400) COLLATE UTF8MB4_0900_AI_CI -> NVARCHAR(400) COLLATE
SQL_LATIN1_GENERAL_CP1_CI_AS". The streaming path (overwrite) was right.

Live MySQL 8 -> SQL Server 2022, mirror with hard deletes, preflight ON,
strict, unicode / '' / NULL text.
"""

from __future__ import annotations

import pytest

from tests import r13_mapping_harness as h
from tests.r13_mapping_harness import isolated  # noqa: F401 - fixture

pytestmark = pytest.mark.timeout(300)

_ROWS = [
    (i, f"Zoë {i} 東京 🚀", "x" * 40, None if i % 3 == 0 else ("" if i % 2 else "  "))
    for i in range(1, 11)
]


def _mirror(fake, table: str, out: str):
    return h.execute(
        fake, h.endpoint("mysql", table), h.endpoint("sqlserver", out),
        sync_mode="mirror",
        stream_contracts=[{
            "name": table, "sync_mode": "mirror", "primary_key": "id",
            "delete_mode": "hard", "selected": True,
        }],
    )


def test_mirror_run_two_into_the_created_sqlserver_table(isolated):  # noqa: F811
    table = f"d275_{h.token()}"
    out = f"{table}_out"
    try:
        h.run_sql(
            "mysql",
            f"CREATE TABLE `{table}` (id INT PRIMARY KEY, "
            "name VARCHAR(400) COLLATE utf8mb4_0900_ai_ci, "
            "long_txt TEXT COLLATE utf8mb4_0900_ai_ci, "
            "note VARCHAR(100) COLLATE utf8mb4_0900_ai_ci) CHARACTER SET utf8mb4",
        )
        h.run_sql("mysql", f"INSERT INTO `{table}` VALUES (%s, %s, %s, %s)", params=_ROWS)  # nosec B608
        first = _mirror(isolated, table, out)
        assert first.success, first.error
        collations = {
            str(name): str(coll)
            for name, coll in h.run_sql(
                "sqlserver",
                "SELECT name, collation_name FROM sys.columns "  # nosec B608
                f"WHERE object_id = OBJECT_ID('{out}') AND collation_name IS NOT NULL",
            )
        }
        # The CREATE carried the accent/case rule the plan stamped.
        assert set(collations.values()) == {"Latin1_General_CI_AI"}, collations

        h.run_sql("mysql", f"DELETE FROM `{table}` WHERE id = 7")  # nosec B608
        second = _mirror(isolated, table, out)
        assert second.success, second.error
        got = h.run_sql("sqlserver", f"SELECT id, name, long_txt, note FROM [{out}] ORDER BY id")  # nosec B608
        assert [tuple(r) for r in got] == [r for r in _ROWS if r[0] != 7]
    finally:
        h.drop("mysql", table)
        h.drop("sqlserver", out)
