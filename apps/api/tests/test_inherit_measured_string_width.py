"""Widthless Map VARCHAR inherits measured source (n) — Airbyte TEXT cliff.

CREATE-new must not land a unique/PK column as MySQL TEXT just because Studio
stamped bare VARCHAR. Bounded Map stamps and explicit LOB stamps stay Map≡CREATE.
"""

from __future__ import annotations

from connectors.writer_common import (
    resolve_mapping_dest_types,
    resolve_target_columns,
)
from services.conversion_contract import ddl_identity_columns
from services.decision_kernel import inherit_measured_string_width, materialize_dest_ddl


def test_inherit_mysql_bare_varchar_takes_source_width():
    assert (
        inherit_measured_string_width("VARCHAR", "VARCHAR(255)", dest_db="mysql")
        == "VARCHAR(255)"
    )
    assert (
        inherit_measured_string_width("varchar", "VARCHAR(32)", dest_db="mysql")
        == "VARCHAR(32)"
    )


def test_inherit_explicit_lob_stays_unbounded():
    assert inherit_measured_string_width("TEXT", "VARCHAR(255)", dest_db="mysql") == "TEXT"
    assert inherit_measured_string_width("CLOB", "VARCHAR(255)", dest_db="oracle") == "CLOB"
    assert (
        inherit_measured_string_width("LONGTEXT", "VARCHAR(32)", dest_db="mysql")
        == "LONGTEXT"
    )
    assert (
        inherit_measured_string_width("VARCHAR(MAX)", "VARCHAR(255)", dest_db="sqlserver")
        == "VARCHAR(MAX)"
    )


def test_inherit_bounded_map_stamp_is_ceiling():
    assert (
        inherit_measured_string_width("VARCHAR(10)", "VARCHAR(255)", dest_db="mysql")
        == "VARCHAR(10)"
    )


def test_inherit_over_cap_promotes_not_clamp():
    """Over-cap source width becomes LONGTEXT/CLOB — never silent min(n, cap)."""
    assert (
        inherit_measured_string_width("VARCHAR", "VARCHAR(20000)", dest_db="mysql")
        == "LONGTEXT"
    )
    assert (
        inherit_measured_string_width("VARCHAR", "VARCHAR(5000)", dest_db="oracle")
        == "CLOB"
    )


def test_inherit_preserves_map_national_family():
    assert (
        inherit_measured_string_width("NVARCHAR", "VARCHAR(255)", dest_db="mysql")
        == "NVARCHAR(255)"
    )


def test_inherit_widthless_char_is_string_alias_unless_source_is_char():
    assert (
        inherit_measured_string_width("CHAR", "VARCHAR(255)", dest_db="mysql")
        == "VARCHAR(255)"
    )
    assert (
        inherit_measured_string_width("CHAR", "CHAR(10)", dest_db="mysql") == "CHAR(10)"
    )


def test_inherit_oracle_projects_varchar2_and_length_unit():
    got = inherit_measured_string_width("VARCHAR", "VARCHAR(255)", dest_db="oracle")
    assert got.upper().startswith("VARCHAR2")
    assert "255" in got
    char_unit = inherit_measured_string_width(
        "VARCHAR", "VARCHAR2(64 CHAR)", dest_db="oracle"
    )
    assert "64" in char_unit
    assert "CHAR" in char_unit.upper()


def test_inherit_keeps_mysql_collate_clause():
    got = inherit_measured_string_width(
        "VARCHAR COLLATE utf8mb4_bin", "VARCHAR(32)", dest_db="mysql"
    )
    assert got.upper().startswith("VARCHAR(32)")
    assert "utf8mb4_bin" in got.lower()


def test_materialize_dest_ddl_inherits_when_source_type_passed():
    assert (
        materialize_dest_ddl("mysql", "VARCHAR", source_type="VARCHAR(255)")
        == "VARCHAR(255)"
    )
    assert materialize_dest_ddl("mysql", "TEXT", source_type="VARCHAR(255)") == "TEXT"
    # No source_type: inherit must not invent a width from nowhere.
    bare = materialize_dest_ddl("mysql", "VARCHAR")
    assert "255" not in bare


def test_resolve_mapping_dest_types_inherits_source_width():
    out = resolve_mapping_dest_types(
        ["email", "note"],
        [
            {
                "source": "email",
                "target": "email",
                "target_type": "VARCHAR",
                "source_type": "VARCHAR(255)",
            },
            {
                "source": "note",
                "target": "note",
                "target_type": "TEXT",
                "source_type": "TEXT",
            },
        ],
        {"email": "VARCHAR(255)", "note": "TEXT"},
        dest_db="mysql",
    )
    assert out["email"] == "VARCHAR(255)"
    assert out["note"] == "TEXT"


def test_resolve_target_columns_create_new_inherits_width():
    cols, types = resolve_target_columns(
        [
            {
                "source": "email",
                "target": "email",
                "target_type": "VARCHAR",
                "source_type": "VARCHAR(255)",
            }
        ],
        {"email": "VARCHAR(255)"},
        table_exists=False,
        dest_db="mysql",
    )
    assert dict(zip(cols, types))["email"] == "VARCHAR(255)"


def test_ddl_identity_bare_varchar_matches_inherited_create():
    maps = [
        {
            "source": "email",
            "target": "email",
            "source_type": "VARCHAR(255)",
            "target_type": "VARCHAR",
        }
    ]
    cols = ddl_identity_columns(maps, dest_db="mysql")
    assert cols[0]["materialized_ddl"] == "VARCHAR(255)"


def test_boolean_map_stamp_is_not_string_inherit():
    assert inherit_measured_string_width("BOOLEAN", "VARCHAR(255)", dest_db="mysql") == (
        "BOOLEAN"
    )
    cols, types = resolve_target_columns(
        [{"source": "flag", "target": "flag", "target_type": "BOOLEAN"}],
        {"flag": "VARCHAR(255)"},
        table_exists=False,
        dest_db="mysql",
    )
    assert dict(zip(cols, types))["flag"] == "BOOLEAN"
