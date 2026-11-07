"""``STRING`` is a physical type on some engines and an alias everywhere else.

Regression: CDC stamps ``_df_lsn`` with the logical carrier ``string``, which
passed through as CREATE DDL and emitted ``"_df_lsn" string`` on PostgreSQL.
The engine rejected it, the transaction aborted, and the whole CDC run failed
with ``current transaction is aborted`` — a green-looking pipeline that could
never write a row.
"""

from __future__ import annotations

import pytest

from connectors.writer_common import DF_LSN_COL
from services.type_system import ddl_type, materialize_dest_ddl

_SQL_STRING_DESTS = [
    "postgresql",
    "mysql",
    "mariadb",
    "sqlserver",
    "oracle",
    "sqlite",
    "redshift",
    "snowflake",
    "duckdb",
]
# Engines whose own string wire *is* a STRING token (``String`` on ClickHouse,
# lowercase ``string`` on Iceberg) — pass-through is correct there.
_STRING_TOKEN_DESTS = ["bigquery", "databricks", "spanner", "iceberg", "clickhouse"]


@pytest.mark.parametrize("dest", _SQL_STRING_DESTS)
def test_string_alias_never_reaches_create_ddl(dest: str) -> None:
    for carrier in ("string", "STRING"):
        ddl = materialize_dest_ddl(dest, carrier)
        assert ddl.upper().split("(", 1)[0].strip() != "STRING", (dest, carrier, ddl)
        assert ddl.strip()


@pytest.mark.parametrize("dest", _STRING_TOKEN_DESTS)
def test_string_stays_native_where_it_is_the_wire(dest: str) -> None:
    assert materialize_dest_ddl(dest, "STRING").upper().startswith("STRING")


@pytest.mark.parametrize("dest", _SQL_STRING_DESTS)
def test_widthed_string_alias_keeps_width_in_dialect_spelling(dest: str) -> None:
    """``STRING(36)`` (a UUID exact wire) must not paste STRING into CREATE."""
    ddl = ddl_type(dest, "STRING(36)")
    assert ddl.upper().split("(", 1)[0].strip() != "STRING", (dest, ddl)
    if dest not in {"sqlite", "duckdb"}:
        # Engines with a bounded string wire keep the declared 36-char contract.
        assert "36" in ddl, (dest, ddl)


def test_cdc_lsn_column_ddl_is_legal_on_postgres() -> None:
    """The CDC bookkeeping column is what regressed the whole run."""
    assert DF_LSN_COL == "_df_lsn"
    assert materialize_dest_ddl("postgresql", "string") == "TEXT"
