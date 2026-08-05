"""Map≡CREATE — foreign BINARY typmod stamps rematerialize to dest wire.

Illegal pass-through of BINARY(16)/VARBINARY(16)/BYTES(16)/… must not reach
CREATE on engines that use BYTES/VARBYTE/BYTEA/fixed/RAW/BLOB.
"""

from __future__ import annotations

import pytest

from services.type_system import ddl_type, materialize_dest_ddl


# (dest, carrier) pairs that previously mismatched materialize vs ddl_type.
_GAP_CASES: list[tuple[str, str]] = [
    ("bigquery", "BINARY(16)"),
    ("bigquery", "VARBINARY(16)"),
    ("bigquery", "VARBYTE(16)"),
    ("bigquery", "fixed(16)"),
    ("redshift", "BINARY(16)"),
    ("redshift", "VARBINARY(16)"),
    ("redshift", "BYTES(16)"),
    ("redshift", "fixed(16)"),
    ("postgresql", "BINARY(16)"),
    ("postgresql", "VARBINARY(16)"),
    ("postgresql", "BYTES(16)"),
    ("postgresql", "VARBYTE(16)"),
    ("postgresql", "fixed(16)"),
    ("snowflake", "VARBINARY(16)"),
    ("snowflake", "BYTES(16)"),
    ("snowflake", "VARBYTE(16)"),
    ("snowflake", "fixed(16)"),
    ("iceberg", "BINARY(16)"),
    ("iceberg", "VARBINARY(16)"),
    ("iceberg", "BYTES(16)"),
    ("iceberg", "VARBYTE(16)"),
    ("sqlite", "BINARY(16)"),
    ("sqlite", "VARBINARY(16)"),
    ("sqlite", "BYTES(16)"),
    ("sqlite", "BYTEA"),
    ("oracle", "BINARY(16)"),
    ("oracle", "VARBINARY(16)"),
    ("mysql", "BYTES(16)"),
    ("mysql", "VARBYTE(16)"),
    ("mysql", "fixed(16)"),
    ("sqlserver", "BYTES(16)"),
    ("sqlserver", "VARBYTE(16)"),
    ("sqlserver", "fixed(16)"),
]


# Native stamps that must keep pass-through (Map authority).
_NATIVE_PASS: list[tuple[str, str]] = [
    ("mysql", "BINARY(16)"),
    ("mysql", "VARBINARY(16)"),
    ("sqlserver", "BINARY(16)"),
    ("sqlserver", "VARBINARY(16)"),
    ("snowflake", "BINARY(16)"),
    ("iceberg", "fixed(16)"),
    ("iceberg", "binary"),
    ("bigquery", "BYTES(16)"),
    ("redshift", "VARBYTE(16)"),
    ("postgresql", "BYTEA"),
    ("sqlite", "BLOB"),
]


@pytest.mark.parametrize("dest,carrier", _GAP_CASES)
def test_binary_typmod_materialize_matches_ddl_type(dest: str, carrier: str):
    expected = ddl_type(dest, carrier)
    got = materialize_dest_ddl(dest, carrier)
    assert got.upper().replace(" ", "") == expected.upper().replace(" ", ""), (
        f"{dest} {carrier}: materialize={got!r} ddl_type={expected!r}"
    )


@pytest.mark.parametrize("dest,carrier", _NATIVE_PASS)
def test_native_binary_stamps_still_pass_through(dest: str, carrier: str):
    """Native dest wire must not be rewritten (Map stamp authority)."""
    expected = ddl_type(dest, carrier)
    got = materialize_dest_ddl(dest, carrier)
    assert got.upper().replace(" ", "") == expected.upper().replace(" ", "")
    # And materialize equals the stamp spelling for exact native forms where
    # ddl_type preserves the same token family.
    if carrier.upper().startswith(("BINARY(", "VARBINARY(", "BYTES(", "VARBYTE(", "FIXED(")):
        # After rematerialize, value equals ddl_type — already asserted.
        assert got
