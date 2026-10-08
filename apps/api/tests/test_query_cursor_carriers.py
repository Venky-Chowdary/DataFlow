"""Query-mode types come from the cursor, not from a 50-row guess.

DEF-B-019: MariaDB puts a display width in the precision slot, so DATE
became DECIMAL(10,0), DATETIME DECIMAL(26,6) and TINYINT DECIMAL(1,0).
DEF-C-026: PostgreSQL typmod -1 arrives as precision 65535 and was created
as DECIMAL(65535,65535), which cannot exist and collapsed to TEXT.
DEF-B-017: a declared DECIMAL(5,2) or CAST(... AS DECIMAL(10,2)) must not
shrink to the sample envelope.
DEF-C-045 / DEF-C-008: a TEXT, BYTEA or UUID column keeps that carrier
even when a peeked value looks like an interval or a string.
"""

from __future__ import annotations

from services.decimal_observe import cursor_declared_carriers, cursor_declared_numeric_types
from services.procedure_source import _overlay_declared_numerics, peek_callable_schema
from services.source_engine_scope import bind_source_engine
from services.type_system import create_new_mapping_target_type, is_lossy_coercion


def _col(name, type_code, precision, scale):
    return (name, type_code, None, None, precision, scale, True)


def test_mariadb_display_widths_stay_their_carriers():
    description = (
        _col("name", 253, 400, 39),
        _col("is_active", 1, 1, 0),
        _col("d", 10, 10, 0),
        _col("ts", 12, 26, 6),
        _col("updated_at", 7, 26, 6),
    )
    headers = [c[0] for c in description]
    declared = cursor_declared_carriers(headers, description)
    assert declared == {
        "name": "VARCHAR",
        "is_active": "INTEGER",
        "d": "DATE",
        "ts": "DATETIME",
        "updated_at": "TIMESTAMP",
    }
    assert "DECIMAL" not in cursor_declared_numeric_types(headers, description).values()


def test_unconstrained_numeric_typmod_is_numeric_not_65535():
    description = (_col("amt_x2", 1700, 65535, 65535),)
    assert cursor_declared_numeric_types(["amt_x2"], description) == {"amt_x2": "NUMERIC"}
    with bind_source_engine("postgresql"):
        target = create_new_mapping_target_type(
            "NUMERIC", "mysql", source_db="postgresql"
        )
    assert "TEXT" not in target.upper()
    assert is_lossy_coercion("NUMERIC", target, dest_db="mysql") is False
    wide = cursor_declared_numeric_types(["amt"], (_col("amt", 1700, 100, 2),))
    assert wide == {"amt": "DECIMAL(100,2)"}


def test_declared_decimal_beats_the_sample_envelope():
    description = (
        _col("amount", 1700, 5, 2),
        _col("cast_amount", 1700, 10, 2),
        _col("big_val", 20, 19, 0),
    )
    headers = ["amount", "cast_amount", "big_val"]
    sample = {"amount": "DECIMAL(3,2)", "cast_amount": "DECIMAL(3,2)", "big_val": "INTEGER"}
    merged = _overlay_declared_numerics(headers, description, sample)
    assert merged["amount"] == "DECIMAL(5,2)"
    assert merged["cast_amount"] == "DECIMAL(10,2)"
    assert merged["big_val"] == "BIGINT"


def test_text_bytea_and_uuid_are_not_guessed_from_the_peek():
    description = (
        _col("label", 25, None, None),
        _col("payload", 17, None, None),
        _col("uid", 2950, None, None),
        _col("span", 1186, None, None),
    )
    headers = [c[0] for c in description]
    schema, _intel = peek_callable_schema(
        headers,
        [["ambiguous 01:30 EDT (first)", "abc", "not-a-uuid", "01:30"]],
    )
    merged = _overlay_declared_numerics(headers, description, schema)
    assert merged["label"] == "VARCHAR"
    assert merged["payload"] == "BYTEA"
    assert merged["uid"] == "UUID"
    assert merged["span"] == "INTERVAL"
    # A value that really is an interval still cannot retype a text column.
    forced = _overlay_declared_numerics(
        ["label"],
        (_col("label", 25, None, None),),
        {"label": "INTERVAL"},
    )
    assert forced["label"] == "VARCHAR"
