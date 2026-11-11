"""Audit findings 2 + 7 — Gate-8 fingerprint rules, version 2.

The cell and row fingerprint decide whether two populations are "the same".
Version 1 folded several different value classes onto one fingerprint, so two
real tables that differ cell by cell digested alike (audit C2 / H5):

* national / case-insensitive text types (NVARCHAR, NCHAR, CITEXT) were not in
  the text family and were folded as numbers (``'01234' == '1234'``);
* a declared text column folded booleans, dates and JSON spelling;
* exact decimals with 16-17 significant digits were rounded to 15;
* ``+Infinity``, ``-Infinity`` and ``NaN`` shared one token;
* text that spells the NULL marker equalled SQL NULL;
* bytes that happen to be valid base64 equalled their decoded bytes;
* the row encoding let a cell imitate a column boundary;
* a per-scan memo made ``Decimal('1.0')`` vs ``Decimal('1.00')`` depend on
  which the scan met first.

Equality per declared destination type is documented in
``services/fingerprint_rules.py``.
"""

from __future__ import annotations

import base64
import datetime as dt
import sqlite3
from decimal import Decimal

import pytest

from services.reconciliation import (
    _iter_fingerprints,
    canonical_checksum,
    fingerprint_for_reconcile as fp,
    normalize_cell,
)


def _digest(engine: str, ddl: str, value: object) -> str:
    return canonical_checksum(
        [{"c": value}], ["c"], dest_db_type=engine, dest_types={"c": ddl}
    )


# ── text family comes from the logical type ─────────────────────────────────

_NATIONAL_TEXT = [
    ("sqlserver", "NVARCHAR(10)"),
    ("sqlserver", "NCHAR(10)"),
    ("sqlserver", "NVARCHAR(MAX)"),
    ("oracle", "NVARCHAR2(10)"),
    ("postgresql", "CITEXT"),
    ("postgresql", "NATIONAL CHARACTER VARYING(10)"),
    ("mysql", "NATIONAL CHAR(10)"),
    ("postgresql", "VARCHAR(10)"),
    ("mysql", "VARCHAR(10)"),
    ("sqlite", "TEXT"),
]


@pytest.mark.parametrize("engine,ddl", _NATIONAL_TEXT)
@pytest.mark.parametrize(
    "left,right",
    [("01234", "1234"), ("1.50", "1.5"), ("1e3", "1000"), ("+1", "1")],
)
def test_text_family_is_never_folded_as_a_number(engine, ddl, left, right):
    assert _digest(engine, ddl, left) != _digest(engine, ddl, right)


@pytest.mark.parametrize("engine,ddl", _NATIONAL_TEXT)
@pytest.mark.parametrize(
    "left,right",
    [
        ("true", "1"),
        ("TRUE", "t"),
        ("false", "0"),
        ('{"a": 1, "b":2}', '{"b":2,"a":1}'),
        ('{"a": 1.10}', '{"a": 1.1}'),
        ("[1e2]", "[100]"),
    ],
)
def test_declared_text_column_compares_as_text(engine, ddl, left, right):
    assert _digest(engine, ddl, left) != _digest(engine, ddl, right)


@pytest.mark.parametrize("engine,ddl", [e for e in _NATIONAL_TEXT if e[0] != "sqlite"])
@pytest.mark.parametrize(
    "left,right",
    [
        ("2024-06-01 00:00:00", "2024-06-01"),
        ("2024-06-01 00:00:00", "2024-06-01T00:00:00"),
        ("2024-06-01T12:00:00+02:00", "2024-06-01 10:00:00"),
    ],
)
def test_declared_text_column_does_not_read_text_as_a_date(engine, ddl, left, right):
    assert _digest(engine, ddl, left) != _digest(engine, ddl, right)


def test_sqlite_text_is_the_temporal_carrier():
    """SQLite has no temporal storage class; its TEXT holds the timestamp.

    DataFlow's SQLite reader reads date-like text as an instant, so the two
    separator spellings are one value on that engine — and only there.
    """
    left = fp("2024-06-01 10:11:12.5", ddl_type="TEXT", engine="sqlite")
    assert left == fp("2024-06-01T10:11:12.5", ddl_type="TEXT", engine="sqlite")
    assert left != fp("2024-06-01 10:11:13.5", ddl_type="TEXT", engine="sqlite")
    assert fp("01234", ddl_type="TEXT", engine="sqlite") != fp(
        "1234", ddl_type="TEXT", engine="sqlite"
    )


@pytest.mark.parametrize("engine,ddl", _NATIONAL_TEXT)
def test_json_document_in_text_ignores_only_insignificant_whitespace(engine, ddl):
    """The writer stores a JSON document bound for a text carrier compacted."""
    assert _digest(engine, ddl, '{"a": [1, 2], "b": "x y"}') == _digest(
        engine, ddl, '{"a":[1,2],"b":"x y"}'
    )
    assert _digest(engine, ddl, '{"b": "x y"}') != _digest(engine, ddl, '{"b": "xy"}')
    # Not a JSON document: plain text, spaces and all.
    assert _digest(engine, ddl, "{a: 1}") != _digest(engine, ddl, "{a:1}")


def test_typed_value_into_text_column_matches_the_wire_text_the_writer_binds():
    """A typed cell is written through ``cell_to_string``; that text is the value."""
    from services.value_serializer import cell_to_string

    for value in (
        True,
        False,
        dt.datetime(2024, 6, 1, 10, 11, 12, 500000),  # noqa: DTZ001 — a naive wall clock is the case
        dt.date(2024, 6, 1),
        {"b": 1, "a": [1, 2]},
        Decimal("1.50"),
        5,
        b"\x01\x02",
    ):
        wire = cell_to_string(value, preserve_sql_null=True)
        assert fp(value, ddl_type="TEXT", engine="postgresql") == fp(
            wire, ddl_type="TEXT", engine="postgresql"
        ), value


def test_unknown_type_name_is_not_promoted_to_exact_text():
    """A domain / unrecognised type keeps the value-level compare (no false fail)."""
    assert fp("1.50", ddl_type="MY_MONEY_DOMAIN", engine="postgresql") == fp(
        Decimal("1.5"), ddl_type="MY_MONEY_DOMAIN", engine="postgresql"
    )


def test_collation_and_blank_pad_folds_still_follow_the_destination():
    ci = "VARCHAR(50) COLLATE Latin1_General_CI_AI"
    assert fp("McDonald", ddl_type=ci, engine="sqlserver") == fp(
        "MCDONALD", ddl_type=ci, engine="sqlserver"
    )
    assert fp("a    ", ddl_type="NCHAR(5)", engine="sqlserver") == fp(
        "a", ddl_type="NCHAR(5)", engine="sqlserver"
    )
    assert fp("a ", ddl_type="NVARCHAR(5)", engine="sqlserver") != fp(
        "a", ddl_type="NVARCHAR(5)", engine="sqlserver"
    )


# ── exact decimals ──────────────────────────────────────────────────────────

_DECIMAL_CARRIERS = [
    ("postgresql", "NUMERIC(38,18)"),
    ("mysql", "DECIMAL(38,18)"),
    ("oracle", "NUMBER(38,18)"),
    ("sqlserver", "DECIMAL(38,18)"),
    ("postgresql", "NUMERIC"),
    ("sqlite", "NUMERIC"),
]


@pytest.mark.parametrize("engine,ddl", _DECIMAL_CARRIERS)
@pytest.mark.parametrize(
    "left,right",
    [
        (Decimal("1.000000000000001"), Decimal("1")),
        (Decimal("0.30000000000000004"), Decimal("0.3")),
        (Decimal("0.1234567890123457"), Decimal("0.123456789012346")),
        (Decimal("106.60000000000001"), Decimal("106.6")),
        ("1.000000000000001", "1"),
        ("0.1234567890123457", "0.123456789012346"),
    ],
)
def test_fingerprint_exact_decimal_16_17_digits_distinct(engine, ddl, left, right):
    assert fp(left, ddl_type=ddl, engine=engine) != fp(right, ddl_type=ddl, engine=engine)


@pytest.mark.parametrize("engine,ddl", _DECIMAL_CARRIERS)
def test_exact_decimal_compares_by_value_not_by_spelling(engine, ddl):
    """Trailing fractional zeros are not significant on a numeric column."""
    assert fp(Decimal("1.50"), ddl_type=ddl, engine=engine) == fp(
        Decimal("1.5"), ddl_type=ddl, engine=engine
    )
    assert fp("1.50", ddl_type=ddl, engine=engine) == fp(
        Decimal("1.500"), ddl_type=ddl, engine=engine
    )


def test_float_residue_still_folds_on_a_float_carrier():
    """A binary double is approximate: 15 significant digits stay the compare."""
    assert fp(106.60000000000001, ddl_type="DOUBLE PRECISION", engine="postgresql") == fp(
        106.6, ddl_type="DOUBLE PRECISION", engine="postgresql"
    )


# ── non-finite numbers ──────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "engine,ddl",
    [("postgresql", "DOUBLE PRECISION"), ("postgresql", "NUMERIC"), ("sqlite", "REAL"), ("", "")],
)
def test_fingerprint_nonfinite_distinct(engine, ddl):
    pos = fp(float("inf"), ddl_type=ddl, engine=engine)
    neg = fp(float("-inf"), ddl_type=ddl, engine=engine)
    nan = fp(float("nan"), ddl_type=ddl, engine=engine)
    null = fp(None, ddl_type=ddl, engine=engine)
    assert len({pos, neg, nan, null}) == 4
    # One token per value whichever way the driver spelled it.
    assert fp(Decimal("Infinity"), ddl_type=ddl, engine=engine) == pos
    assert fp(Decimal("-Infinity"), ddl_type=ddl, engine=engine) == neg
    assert fp(Decimal("NaN"), ddl_type=ddl, engine=engine) == nan
    assert fp("Infinity", ddl_type=ddl, engine=engine) == pos
    assert fp("-Infinity", ddl_type=ddl, engine=engine) == neg
    assert fp("NaN", ddl_type=ddl, engine=engine) == nan


def test_nonfinite_spelling_stays_text_on_a_text_column():
    assert fp("Infinity", ddl_type="TEXT", engine="postgresql") != fp(
        "inf", ddl_type="TEXT", engine="postgresql"
    )


# ── SQL NULL is out of band ─────────────────────────────────────────────────


def test_text_spelling_the_null_marker_is_not_null():
    from services.value_serializer import WireLiteral

    null = normalize_cell(None)
    assert normalize_cell("\x00NULL\x00") != null
    assert normalize_cell("\x00NULL\x00", ddl_type="TEXT", engine="postgresql") != null
    assert fp(WireLiteral("__DF_SQL_NULL__"), ddl_type="TEXT", engine="postgresql") != fp(
        None, ddl_type="TEXT", engine="postgresql"
    )
    # Distinct texts that start with the marker byte stay distinct.
    assert normalize_cell("\x00NULL\x00") != normalize_cell("\x00TEXT\x00\x00NULL\x00")


def test_destination_holding_the_sentinel_text_is_not_a_null(tmp_path):
    """Real read-back: a stored literal ``__DF_SQL_NULL__`` must not digest as NULL."""
    from services.reconciliation import verify_sqlite_table

    digests = []
    for name, value in (("lit", "__DF_SQL_NULL__"), ("nul", None)):
        path = tmp_path / f"{name}.db"
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        conn.execute("INSERT INTO t VALUES (1, ?)", (value,))
        conn.commit()
        conn.close()
        rows, checksum = verify_sqlite_table(
            connection_string="", database=str(path), table_name="t",
            target_columns=["id", "v"], limit=0,
            dest_types={"id": "INTEGER", "v": "TEXT"},
        )
        assert rows == 1
        digests.append(checksum)
    assert digests[0] != digests[1]


# ── bytes are bytes ─────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "engine,ddl", [("postgresql", "BYTEA"), ("sqlserver", "VARBINARY(100)"), ("sqlite", "BLOB")]
)
@pytest.mark.parametrize("text", [b"abcd", b"DEADBEEF", b"1234", b"aGVsbG8="])
def test_bytes_that_spell_base64_are_not_their_decoded_bytes(engine, ddl, text):
    decoded = base64.b64decode(text)
    assert fp(text, ddl_type=ddl, engine=engine) != fp(decoded, ddl_type=ddl, engine=engine)


# ── the row encoding is injective ───────────────────────────────────────────


@pytest.mark.parametrize(
    "kwargs",
    [{}, {"dest_db_type": "postgresql", "dest_types": {"a": "TEXT", "b": "TEXT"}}],
)
def test_row_encoding_injective(kwargs):
    left = [{"a": "1\x1fb=2", "b": "3"}]
    right = [{"a": "1", "b": "2\x1fb=3"}]
    assert canonical_checksum(left, ["a", "b"], **kwargs) != canonical_checksum(
        right, ["a", "b"], **kwargs
    )
    # The dict-keys path (no column list) encodes the same way.
    assert canonical_checksum(left, **kwargs) != canonical_checksum(right, **kwargs)


def test_row_encoding_a_cell_cannot_imitate_a_length_prefix():
    left = [{"a": "1:x\x1fb=1:y", "b": "z"}]
    right = [{"a": "x", "b": "y"}]
    assert canonical_checksum(left, ["a", "b"]) != canonical_checksum(right, ["a", "b"])


def test_row_encoding_a_column_name_cannot_imitate_a_boundary():
    left = [{"a=1:x\x1fb": "y"}]
    right = [{"a": "x", "b": "y"}]
    assert canonical_checksum(left) != canonical_checksum(right)


# ── the memo does not alias equal-but-different values ──────────────────────


def test_memo_does_not_alias_decimal_scale():
    types = {"k": "INTEGER", "v": "TEXT"}
    rows = [{"k": 1, "v": Decimal("1.0")}, {"k": 2, "v": Decimal("1.00")}]

    def by_key(scan):
        return dict(
            _iter_fingerprints(
                scan, ["k", "v"], sort_key="k", dest_db_type="postgresql", dest_types=types
            )
        )

    forward, backward = by_key(rows), by_key(list(reversed(rows)))
    assert forward == backward
    # On a text carrier the bound text is the value: the two scales differ.
    assert forward["1"] != forward["2"].replace("k=1:2", "k=1:1")


def test_memo_does_not_alias_negative_zero_on_text():
    types = {"k": "INTEGER", "v": "TEXT"}
    rows = [{"k": 1, "v": 0.0}, {"k": 2, "v": -0.0}]
    forward = dict(
        _iter_fingerprints(rows, ["k", "v"], sort_key="k", dest_db_type="sqlite", dest_types=types)
    )
    backward = dict(
        _iter_fingerprints(
            list(reversed(rows)), ["k", "v"], sort_key="k", dest_db_type="sqlite", dest_types=types
        )
    )
    assert forward == backward


def test_numeric_column_scale_is_scan_order_independent_and_equal_by_value():
    types = {"k": "INTEGER", "v": "NUMERIC(10,2)"}
    rows = [{"k": 1, "v": Decimal("1.0")}, {"k": 1, "v": Decimal("1.00")}]
    fps = [
        f
        for _k, f in _iter_fingerprints(
            rows, ["k", "v"], dest_db_type="postgresql", dest_types=types
        )
    ]
    assert fps[0] == fps[1]


# ── version stamp ───────────────────────────────────────────────────────────


def test_fingerprint_version_is_stamped_on_the_reconciliation_report():
    from services.fingerprint_rules import FINGERPRINT_VERSION, stored_fingerprint_version
    from services.reconciliation import reconcile

    assert FINGERPRINT_VERSION == 2
    report = reconcile(
        source_rows=1, target_rows=1, source_checksum="a" * 64, target_checksum="a" * 64
    ).to_dict()
    assert report["fingerprint_version"] == FINGERPRINT_VERSION
    # A report written before the stamp existed is version 1.
    assert stored_fingerprint_version({"passed": True}) == 1
    assert stored_fingerprint_version(report) == FINGERPRINT_VERSION
    assert stored_fingerprint_version(None) == 1


# ── two real PostgreSQL tables that differ must not digest alike (p20) ──────


def test_two_real_postgres_tables_that_differ_do_not_digest_alike():
    from tests.helpers.live_env import pg_creds, pg_up

    if not pg_up():
        pytest.skip("PostgreSQL is not reachable with the configured credentials")
    import psycopg2

    from services.verification_ladder import read_postgres_rows

    creds = pg_creds()
    schema = "audit02_fp"
    conn = psycopg2.connect(
        host=creds["host"], port=creds["port"], dbname=creds["database"],
        user=creds["username"], password=creds["password"],
    )
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    cur.execute(f'CREATE SCHEMA "{schema}"')
    try:
        cur.execute("CREATE EXTENSION IF NOT EXISTS citext")
        ddl = (
            "(id int primary key, amt numeric(38,18), code citext, "
            "payload bytea, big double precision)"
        )
        cur.execute(f"CREATE TABLE {schema}.a {ddl}")
        cur.execute(f"CREATE TABLE {schema}.b {ddl}")
        a = [
            (1, "1.000000000000001", "01234", b"abcd", float("inf")),
            (2, "0.1234567890123457", "1.50", b"DEADBEEF", float("-inf")),
            (3, "106.60000000000001", "1e3", b"1234", float("nan")),
        ]
        b = [
            (1, "1", "1234", base64.b64decode("abcd"), float("-inf")),
            (2, "0.123456789012346", "1.5", base64.b64decode("DEADBEEF"), float("nan")),
            (3, "106.6", "1000", base64.b64decode("1234"), float("inf")),
        ]
        cur.executemany(f"INSERT INTO {schema}.a VALUES (%s,%s,%s,%s,%s)", a)
        cur.executemany(f"INSERT INTO {schema}.b VALUES (%s,%s,%s,%s,%s)", b)
        cols = ["id", "amt", "code", "payload", "big"]
        kw = dict(
            host=creds["host"], port=int(creds["port"]), database=creds["database"],
            username=creds["username"], password=creds["password"], schema=schema,
        )
        rows_a = read_postgres_rows(table="a", columns=cols, **kw)
        rows_b = read_postgres_rows(table="b", columns=cols, **kw)
        types = {
            "id": "INTEGER", "amt": "NUMERIC(38,18)", "code": "CITEXT",
            "payload": "BYTEA", "big": "DOUBLE PRECISION",
        }
        for col in ("amt", "code", "payload", "big"):
            left = canonical_checksum(
                rows_a, ["id", col], dest_db_type="postgresql", dest_types=types
            )
            right = canonical_checksum(
                rows_b, ["id", col], dest_db_type="postgresql", dest_types=types
            )
            assert left != right, f"column {col} differs in every row but digests alike"
        # A table still equals itself.
        assert canonical_checksum(
            rows_a, cols, dest_db_type="postgresql", dest_types=types
        ) == canonical_checksum(
            read_postgres_rows(table="a", columns=cols, **kw), cols,
            dest_db_type="postgresql", dest_types=types,
        )
    finally:
        cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()
