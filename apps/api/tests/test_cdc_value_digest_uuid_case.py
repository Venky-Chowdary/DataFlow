"""The CDC value proof compares a UUID column as a UUID, whatever its case.

PR 424 CI, cdc-sqlserver (pyodbc), run 43442158829 job 131942398189:
test_d196 change tracking → MySQL failed. "CDC verification found 100 rows ...
whose values do not match the source image", although every cell matched.
SQL Server's two product read paths spell one GUID differently: pyodbc
returns '6F9619FF-…', while the batch reader the value proof scans with
returns '6f9619ff-…'. MySQL has no UUID type, so the destination column is
CHAR(36) holding the writer's spelling. The proof fingerprinted it as exact
text, and the same GUID read as drift. A UUID-declared column now compares
by UUID value in the CDC value proof. Other text stays exact.
"""

from __future__ import annotations

import uuid

from services.cdc_value_digest import _fingerprint_row, compare_row_sets

LOWER = "6f9619ff-8b86-d011-b42d-00c04fc964ff"


def test_a_uuid_column_matches_across_case_and_native_uuid() -> None:
    for engine in ("mysql", "mariadb", "postgresql", "sqlserver", "sqlite"):
        for dest_type in ("UUID", "UNIQUEIDENTIFIER", "uuid"):
            types = {"g": dest_type}
            proof = compare_row_sets(
                [{"g": LOWER}, {"g": uuid.UUID(LOWER)}],
                [{"g": LOWER.upper()}],
                ["g"],
                engine=engine,
                dest_types=types,
            )
            assert proof.missing == 0, (engine, dest_type)


def test_a_different_uuid_is_still_a_difference() -> None:
    proof = compare_row_sets(
        [{"g": LOWER}], [{"g": LOWER.replace("6f", "7f", 1).upper()}], ["g"],
        engine="mysql", dest_types={"g": "UUID"},
    )
    assert proof.missing == 1


def test_text_that_is_not_declared_uuid_stays_exact() -> None:
    a = _fingerprint_row({"g": LOWER}, ["g"], engine="mysql", dest_types={"g": "VARCHAR(36)"})
    b = _fingerprint_row({"g": LOWER.upper()}, ["g"], engine="mysql", dest_types={"g": "VARCHAR(36)"})
    assert a != b


def test_a_mysql_char36_column_mapped_from_a_uuid_compares_as_uuid() -> None:
    # What the value proof really receives for d196 → MySQL: the destination
    # catalog type (CHAR(36)) and the mapping's source type (UUID).
    from services.cdc_value_digest import _uuid_columns

    mappings = [{"source": "g", "target": "g", "source_type": "UUID", "target_type": "CHAR(36)"}]
    cols = _uuid_columns(["id", "g"], {"id": "BIGINT", "g": "CHAR(36)"}, mappings)
    assert cols == ["g"]
    proof = compare_row_sets(
        [{"g": LOWER}], [{"g": LOWER.upper()}], ["g"],
        engine="mysql", dest_types={"g": "CHAR(36)"}, uuid_columns=cols,
    )
    assert proof.missing == 0
    assert _uuid_columns(["g"], {"g": "CHAR(36)"}, [{"source": "g", "source_type": "VARCHAR(36)"}]) == []
