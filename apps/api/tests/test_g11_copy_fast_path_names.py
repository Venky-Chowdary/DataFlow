"""G11: the server-to-server COPY fast paths name targets like the row path.

The COPY routes quoted Map targets with their own rules: MySQL destinations
verbatim (``First Name``), BigQuery verbatim up to 1,024 characters, while
the row writers name them with the destination's rule (``First_Name`` on
MySQL, case-kept sanitizing on BigQuery). The same Map then created a
different column depending on which path ran, and the Gate-8 read-back
(which asks for the row path's spelling) missed it.

``_try_copy_fast_path`` now hands every COPY route the Map with targets named
by ``destination_naming.writer_identifier`` for its destination (an adopted
legacy column is already the stored spelling), so both paths create and read
the same column. The routes need ``LOAD DATA LOCAL INFILE`` / cloud services
that are not available here, so each route is checked at the dispatcher with
its fast-path function recording what it was handed.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

TARGETS = ["id", "First Name", "x" * 70]


def _handed(src: str, dest: str, func: str) -> list[str]:
    import src.transfer.copy_route as route
    from src.transfer.models import EndpointConfig

    seen: list[list[str]] = []

    def record(**kwargs):
        seen.append([str(m.get("target")) for m in kwargs["mappings"]])
        return None

    with patch.object(route, func, side_effect=record), \
            patch("services.dest_precount.destination_table_exists", return_value=False):
        route._try_copy_fast_path(
            source=EndpointConfig(kind="database", format=src, table="s"),
            destination=EndpointConfig(kind="database", format=dest, table="d"),
            mappings=[{"source": f"c{i}", "target": t} for i, t in enumerate(TARGETS)],
            schema={f"c{i}": "INTEGER" for i in range(len(TARGETS))},
            src_type=src, dest_type=dest, src_cfg={"table": "s"}, dest_cfg={"table": "d"},
            effective_sync="full_refresh_overwrite", incremental=False, source_filter=None,
            limit=0, checkpoint=None,
        )
    assert seen, f"{func} was not reached"
    return seen[0]


@pytest.mark.parametrize(
    ("src", "dest", "func", "expected"),
    [
        ("postgresql", "mysql", "_try_pg_mysql_copy_fast_path",
         ["id", "First_Name", "x" * 64]),
        ("sqlite", "mysql", "_try_sqlite_mysql_copy_fast_path",
         ["id", "First_Name", "x" * 64]),
        ("postgresql", "sqlite", "_try_pg_sqlite_copy_fast_path",
         ["id", "First_Name", "x" * 70]),
        ("bigquery", "bigquery", "_try_bigquery_bigquery_copy_fast_path",
         ["id", "First_Name", "x" * 70]),
        ("sqlite", "mongodb", "_try_sqlite_mongo_copy_fast_path",
         ["id", "First Name", "x" * 70]),
    ],
)
def test_copy_route_gets_the_row_path_spelling(src, dest, func, expected):
    assert _handed(src, dest, func) == expected
