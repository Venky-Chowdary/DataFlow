"""D246 — MariaDB 11 -> MySQL 8 copied ``utf8mb4_uca1400_ai_ci`` into CREATE.

MariaDB 11's default collation is not a MySQL collation. The create-new stamp
copied any ``UTF8``-named CI/AI source collation verbatim, so preflight was
green and MySQL refused the DDL (1273 Unknown collation) with 0 rows written.
The stamp must only carry a name the destination engine accepts and otherwise
spell the same equality class (case- and accent-insensitive) natively.
"""

from __future__ import annotations

import pytest

from services.collation_carry import apply_dest_native_collation
from services.dest_dialect_facts import _collation_compatible_with_dest


@pytest.mark.parametrize(
    "src_type",
    [
        "VARCHAR(400) COLLATE utf8mb4_uca1400_ai_ci",
        "LONGTEXT COLLATE utf8mb4_uca1400_ai_ci",
        "VARCHAR(40) COLLATE utf8mb4_uca1400_as_ci",
    ],
)
def test_mariadb_only_collation_never_reaches_mysql_ddl(src_type):
    base = src_type.split(" COLLATE")[0]
    out = apply_dest_native_collation(
        base, dest_dialect="mysql", source_dialect="mariadb", source_type=src_type
    )
    assert "uca1400" not in out.lower(), out
    m = out.upper().split("COLLATE ")
    if len(m) > 1:
        assert _collation_compatible_with_dest("mysql", m[1].split()[0])


def test_mariadb_ai_ci_lands_as_mysql_ci_ai_equivalent():
    out = apply_dest_native_collation(
        "VARCHAR(400)",
        dest_dialect="mysql",
        source_dialect="mariadb",
        source_type="VARCHAR(400) COLLATE utf8mb4_uca1400_ai_ci",
    )
    assert out == "VARCHAR(400) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"


def test_mariadb_to_mariadb_keeps_its_own_collation():
    out = apply_dest_native_collation(
        "VARCHAR(400)",
        dest_dialect="mariadb",
        source_dialect="mariadb",
        source_type="VARCHAR(400) COLLATE utf8mb4_uca1400_ai_ci",
    )
    assert out.endswith("COLLATE utf8mb4_uca1400_ai_ci")


def test_mysql_named_ci_ai_still_carried_verbatim():
    out = apply_dest_native_collation(
        "VARCHAR(40)",
        dest_dialect="mysql",
        source_dialect="mariadb",
        source_type="VARCHAR(40) COLLATE utf8mb4_unicode_ci",
    )
    assert out.endswith("COLLATE utf8mb4_unicode_ci")


def test_mariadb_catalog_labelled_mysql_does_not_carry_uca1400_verbatim():
    """The MariaDB source catalog reads as ``mysql``; the same-engine branch
    copied the name into MySQL CREATE (D246 live repro: 1273, 0 rows)."""
    out = apply_dest_native_collation(
        "VARCHAR(400)",
        dest_dialect="mysql",
        source_dialect="mysql",
        source_type="VARCHAR(400) COLLATE utf8mb4_uca1400_ai_ci",
    )
    assert out == "VARCHAR(400) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
