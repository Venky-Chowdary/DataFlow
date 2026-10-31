"""One answer to "which engine is this connector, really?".

A connector's ``type`` is a catalog tile id: ``alloydb``,
``amazon_rds_postgresql``, ``azure_database_for_mysql``, ``sql_server``,
``amazon_rds_oracle``. Execution already dials the right driver through
:func:`resolve_driver_type`. Preflight and the type system used to dispatch on
the raw tile id through about five private alias lists and dozens of literal
sets (``{"postgresql", "timescaledb", "pgvector"}``), so a tile one list forgot
was "not implemented" at Validate while the same tile transferred fine
(QA round 11: every hosted PostgreSQL tile blocked at preflight).

:func:`engine_family` is built on the two resolvers execution uses:

* :func:`resolve_driver_type` — catalog tile id -> implemented driver;
* :func:`source_read_driver` — driver whose wire protocol it speaks
  (``pgvector`` reads and writes rows over PostgreSQL).

Every dispatch site normalises with it once, at its entry. The raw tile id is
kept only for labels and certification.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Final

#: Engines with a first-class duplex SQL driver. A tile that resolves to one of
#: these is that engine on the wire, whatever its catalog name.
POSTGRESQL: Final = "postgresql"
MYSQL: Final = "mysql"
SQLSERVER: Final = "sqlserver"
ORACLE: Final = "oracle"
SQL_DUPLEX_FAMILIES: Final[frozenset[str]] = frozenset({POSTGRESQL, MYSQL, SQLSERVER, ORACLE})

# Spellings people and older rows type that are not catalog tile ids. The
# resolver's substring fallback would turn ``pg`` into an unknown base token.
_SPELLINGS: Final[dict[str, str]] = {
    "pg": POSTGRESQL,
    "postgres": POSTGRESQL,
    "pgsql": POSTGRESQL,
    "maria": MYSQL,
    "mssql": SQLSERVER,
    "tsql": SQLSERVER,
}


def _resolved_driver(raw: str) -> str:
    from src.transfer.connector_capabilities import (
        _DRIVER_CAPS,
        _FILE_CAPS,
        resolve_driver_type,
        source_read_driver,
    )

    driver = resolve_driver_type(raw)
    if driver not in _DRIVER_CAPS and driver not in _FILE_CAPS and driver != "generic_sql":
        # The resolver's last resort returns the first ``_`` token even when no
        # driver owns it (``file_export`` -> ``file``). That is not an answer.
        return ""
    return source_read_driver(driver)


@lru_cache(maxsize=1024)
def _engine_family(raw: str) -> str:
    if raw in _SPELLINGS:
        return _SPELLINGS[raw]
    driver = _resolved_driver(raw)
    if not driver or driver == "generic_sql":
        return raw
    return driver


def engine_family(kind: str | None) -> str:
    """Engine family a connector kind dispatches on.

    * a duplex SQL tile -> ``postgresql`` / ``mysql`` / ``sqlserver`` /
      ``oracle`` (``pgvector`` and ``timescaledb`` included);
    * any other implemented driver -> that driver (``mongodb``, ``redshift``,
      ``snowflake``, ``s3`` ...);
    * a SQLAlchemy dialect served by the ``generic_sql`` driver -> the dialect
      itself (``duckdb``, ``clickhouse``): it has no shared engine to fold to;
    * anything the resolver does not know -> the lower-cased kind unchanged.
    """
    raw = (kind or "").strip().lower()
    return _engine_family(raw) if raw else ""


def sql_duplex_family(kind: str | None) -> str:
    """``engine_family`` when it is a duplex SQL engine, else ``""``."""
    family = engine_family(kind)
    return family if family in SQL_DUPLEX_FAMILIES else ""


def is_postgresql_family(kind: str | None) -> bool:
    return engine_family(kind) == POSTGRESQL


def is_mysql_family(kind: str | None) -> bool:
    return engine_family(kind) == MYSQL


def is_sqlserver_family(kind: str | None) -> bool:
    return engine_family(kind) == SQLSERVER


def is_oracle_family(kind: str | None) -> bool:
    return engine_family(kind) == ORACLE


# Distinct engines that speak a duplex family's wire through its driver. They
# share the family's dispatch (probe, reader, writer) but not every catalog
# feature: MariaDB has no TABLE_CONSTRAINTS.ENFORCED, CockroachDB no
# pg_index.indisvalid. Hosted twins (RDS, Aurora, Azure, Cloud SQL, AlloyDB,
# Neon, Supabase, TimescaleDB) are the engine itself and have no flavour.
_DISTINCT_ENGINE_TOKENS: Final[tuple[tuple[str, str], ...]] = (
    ("maria", "mariadb"),
    ("singlestore", "singlestore"),
    ("tidb", "tidb"),
    ("vitess", "vitess"),
    ("planetscale", "planetscale"),
    ("oceanbase", "oceanbase"),
    ("polardb", "polardb"),
    ("gaussdb", "gaussdb"),
    ("goldendb", "goldendb"),
    ("cockroach", "cockroachdb"),
    ("yugabyte", "yugabytedb"),
)


def engine_flavour(kind: str | None) -> str:
    """The concrete engine behind a duplex tile, for catalog-feature checks.

    ``amazon_rds_mysql`` -> ``mysql``; ``mariadb`` -> ``mariadb``;
    ``alloydb`` -> ``postgresql``; ``cockroachdb`` -> ``cockroachdb``. A kind
    outside the duplex families is returned as :func:`engine_family` gives it.
    Dispatch (which probe, reader or writer runs) uses :func:`engine_family`;
    only a check of one engine's catalog columns needs the flavour.
    """
    family = engine_family(kind)
    if family not in SQL_DUPLEX_FAMILIES:
        return family
    raw = (kind or "").strip().lower()
    for token, flavour in _DISTINCT_ENGINE_TOKENS:
        if token in raw:
            return flavour
    return family


def family_or_kind(kind: str | None) -> str:
    """Duplex family for a SQL tile, else the lower-cased kind as given.

    For dispatch tables keyed on driver names *and* on concrete non-duplex
    kinds (``redshift``, ``snowflake``, ``duckdb``): a hosted twin folds onto
    its engine, every other key is looked up exactly as before.
    """
    return sql_duplex_family(kind) or (kind or "").strip().lower()
