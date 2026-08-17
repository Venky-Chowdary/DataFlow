"""Oracle/DB2 reachability and row-window syntax on the shared SQL read path.

Two defects found by the live Oracle 23ai / SQL Server 2022 migration matrix:

1. the SQLAlchemy URL was built as ``oracle+oracledb://u:p@host:port/NAME``,
   which is a *SID* DSN — every pluggable database, RAC service and Autonomous
   instance is addressed by service name, so connects died with ORA-12505;
2. the fallback table reader emitted ``LIMIT n OFFSET m``, which Oracle and DB2
   reject (ORA-03047), making those sources unreadable entirely.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from connectors.generic_sql import _build_url
from services.dialect_profiles import (
    page_clause,
    uses_fetch_first_pagination,
    zero_row_probe_sql,
)


def test_oracle_url_uses_service_name_not_sid() -> None:
    url = _build_url(
        {
            "type": "oracle",
            "host": "db.example",
            "port": 1521,
            "database": "FREEPDB1",
            "username": "u",
            "password": "p",
        }
    )
    assert url.query.get("service_name") == "FREEPDB1"
    assert not url.database


def test_oracle_url_honours_explicit_sid() -> None:
    url = _build_url(
        {
            "type": "oracle",
            "host": "db.example",
            "database": "ORCL",
            "sid": "ORCL",
            "username": "u",
            "password": "p",
        }
    )
    assert url.database == "ORCL"
    assert "service_name" not in url.query


def test_explicit_service_name_wins_over_database() -> None:
    url = _build_url(
        {
            "type": "oracle",
            "host": "db.example",
            "database": "ignored",
            "service_name": "svc.example.com",
            "username": "u",
            "password": "p",
        }
    )
    assert url.query.get("service_name") == "svc.example.com"


@pytest.mark.parametrize(
    "dialect",
    ["oracle", "amazon_rds_oracle", "autonomous_database", "db2", "mssql", "azure_sql_database"],
)
def test_fetch_first_dialects_never_emit_limit(dialect: str) -> None:
    assert uses_fetch_first_pagination(dialect)
    clause = page_clause(dialect, 100, 50)
    assert "LIMIT" not in clause.upper()
    assert clause == "OFFSET 100 ROWS FETCH NEXT 50 ROWS ONLY"


@pytest.mark.parametrize("dialect", ["postgresql", "mysql", "sqlite", "duckdb"])
def test_limit_dialects_keep_limit_offset(dialect: str) -> None:
    assert not uses_fetch_first_pagination(dialect)
    assert page_clause(dialect, 100, 50) == "LIMIT 50 OFFSET 100"


def test_zero_row_probe_is_valid_per_dialect() -> None:
    assert "TOP 0" in zero_row_probe_sql("mssql", '"t"')
    # ``LIMIT 0`` is a syntax error on Oracle/DB2.
    oracle_probe = zero_row_probe_sql("oracle", '"T"')
    assert "LIMIT" not in oracle_probe.upper()
    assert "WHERE 1=0" in oracle_probe
    assert zero_row_probe_sql("postgresql", '"t"').endswith("LIMIT 0")


def test_full_population_owners_never_emit_limit_offset_windows() -> None:
    """SCD2/mirror/staging full scans stream one SELECT. OFFSET is not this kernel."""
    api = Path(__file__).resolve().parents[1]
    forbidden = "LIMIT {batch_size} OFFSET {offset}"
    scd2 = (api / "services" / "scd2_engine.py").read_text(encoding="utf-8")
    mirror = (api / "services" / "mirror_engine.py").read_text(encoding="utf-8")
    stream = (api / "src" / "transfer" / "stream.py").read_text(encoding="utf-8")
    # The SCD2/mirror staging scan lives in ``stream_scd2`` since the F8 split.
    stream_scd2 = (api / "src" / "transfer" / "stream_scd2.py").read_text(
        encoding="utf-8"
    )
    assert forbidden not in scd2
    assert forbidden not in mirror
    assert forbidden not in stream
    assert forbidden not in stream_scd2
    assert "stream_select_checksum" in scd2
    assert "stream_select_checksum" in mirror
    assert "iter_select_row_dicts" in stream_scd2


def test_stream_select_checksum_empty_is_blank_not_sha_of_empty() -> None:
    from services.reconciliation_api import stream_select_checksum

    class _Result:
        def keys(self):
            return ["id"]

        def partitions(self, _n):
            if False:
                yield []

    class _Conn:
        def execution_options(self, **_kw):
            return self

        def execute(self, _statement):
            return _Result()

    count, digest = stream_select_checksum(_Conn(), "SELECT id FROM t", ["id"])
    assert count == 0
    assert digest == ""


def test_stream_select_checksum_matches_buffered_canonical() -> None:
    from services.reconciliation_api import canonical_checksum, stream_select_checksum

    rows = [{"id": "1", "name": "A"}, {"id": "2", "name": "B"}]

    class _Row:
        def __init__(self, mapping):
            self._mapping = mapping

    class _Result:
        def keys(self):
            return ["id", "name"]

        def partitions(self, n):
            batch = [_Row(r) for r in rows]
            yield batch[:n]
            if n < len(batch):
                yield batch[n:]

    class _Conn:
        def execution_options(self, **_kw):
            return self

        def execute(self, statement):
            self.sql = str(statement)
            return _Result()

    conn = _Conn()
    count, digest = stream_select_checksum(conn, "SELECT id, name FROM t WHERE is_current", ["id", "name"], itersize=1)
    assert count == 2
    assert digest == canonical_checksum(rows, ["id", "name"])
    assert "LIMIT" not in conn.sql.upper()
    assert "OFFSET" not in conn.sql.upper()


def test_inferred_deletes_via_staging_emits_numeric_bool_on_mssql() -> None:
    from services.mirror_engine import apply_inferred_deletes_via_staging

    class _Conn:
        def __init__(self):
            self.dialect = type("D", (), {"name": "mssql"})()
            self.sql: list[str] = []

        def execute(self, stmt, params=None):  # noqa: ARG002
            self.sql.append(str(stmt))

            class _R:
                rowcount = 0

                def fetchone(self):
                    return (0,)

            return _R()

        def commit(self):
            return None

        def rollback(self):
            return None

    conn = _Conn()
    apply_inferred_deletes_via_staging(
        conn, '"dst"', '"stg"', ["id"], dialect="mssql"
    )
    joined = "\n".join(conn.sql).upper()
    assert "FALSE" not in joined
    assert "TRUE" not in joined
    assert "SELECT COUNT(*)" in joined
    assert "SET" in joined and "= 0" in joined
    assert "= 1" in joined
