"""The per-column fast paths give exactly the full path's answer.

``services.cell_fastpath`` answers ``fingerprint_for_reconcile`` and
``normalize_cell`` for ordinary cells of planned columns. These tests run both
paths — the public functions, and the same functions with the fast path
switched off for the thread — over generated corpora (edge values, random
values, adversarial text) for every plan kind on PostgreSQL and MySQL, every
transform the write pass uses, every number / date locale pin and both strict
modes. Any difference fails, including a difference in result type.
"""

from __future__ import annotations

import os
import random
import string
from datetime import date, datetime, timedelta
from decimal import Decimal

import pytest

from services import cell_fastpath
from services.reconciliation import fingerprint_for_reconcile, normalize_cell
from services.transform_engine import (
    reset_active_date_locale,
    reset_active_number_locale,
    reset_strict_number_reading,
    set_active_date_locale,
    set_active_number_locale,
    set_strict_number_reading,
)

COLUMNS = [
    ("BIGINT", "postgresql"), ("INTEGER", "postgresql"), ("SMALLINT", "postgresql"),
    ("NUMERIC(18,4)", "postgresql"), ("NUMERIC(12,2)", "postgresql"), ("NUMERIC(38,10)", "postgresql"),
    ("NUMERIC(10,0)", "postgresql"), ("DOUBLE PRECISION", "postgresql"),
    ("TIMESTAMP", "postgresql"), ("TIMESTAMP(6)", "postgresql"), ("TIMESTAMP(3)", "postgresql"),
    ("TIMESTAMP(0)", "postgresql"), ("DATE", "postgresql"), ("BOOLEAN", "postgresql"),
    ("TEXT", "postgresql"), ("VARCHAR(64)", "postgresql"), ("VARCHAR(255)", "postgresql"),
    ("BIGINT", "mysql"), ("BIGINT(20)", "mysql"), ("INT", "mysql"), ("DECIMAL(18,4)", "mysql"),
    ("DECIMAL(10,2)", "mysql"), ("DOUBLE", "mysql"), ("DATETIME", "mysql"), ("DATETIME(6)", "mysql"),
    ("DATETIME(3)", "mysql"), ("DATE", "mysql"), ("BOOLEAN", "mysql"), ("LONGTEXT", "mysql"),
    ("VARCHAR(64)", "mysql"), ("VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin", "mysql"),
    ("TEXT", "mariadb"), ("DECIMAL(18,4)", "mariadb"), ("DATETIME(6)", "mariadb"),
    # Columns that must get no plan (they still go through both paths here).
    ("JSONB", "postgresql"), ("TIMESTAMPTZ", "postgresql"), ("REAL", "postgresql"),
    ("FLOAT", "mysql"), ("TINYINT(1)", "mysql"), ("TIMESTAMP", "mysql"), ("CHAR(10)", "postgresql"),
    ("VARCHAR(64)", "sqlserver"), ("BIGINT", "oracle"), ("TEXT", "sqlite"),
    ("TEXT CARRYING SOURCE_BOOLEAN", "postgresql"), ("UUID", "postgresql"),
    ("VARCHAR(64) COLLATE utf8mb4_general_ci", "mysql"),
]
TRANSFORMS = [None, "", "none", "integer", "decimal", "date", "datetime", "boolean", "json", "upper"]
NUMBER_LOCALES = ["", "US", "EU", "WIRE"]
#: (number locale, date locale, strict number reading) pins the write pass runs under.
PINS = [("", "", False), ("EU", "DMY", True), ("US", "MDY", False), ("WIRE", "", False),
        ("EU", "", False), ("", "MDY", True)]
#: ``CELL_FASTPATH_EQUIVALENCE=full`` runs the large corpora (about 11 minutes);
#: the default sizes keep this module to a few minutes in a CI shard.
FULL = os.environ.get("CELL_FASTPATH_EQUIVALENCE", "").strip().lower() == "full"
SCALE = 1.0 if FULL else 0.35


def _corpus(seed: int, n: int) -> list:
    rng = random.Random(seed)
    values: list = [
        None, True, False, 0, 1, -1, 2, 42, -42081, 2**31 - 1, -(2**31), 2**53 + 1, 2**63 - 1,
        -(2**63), 2**63, 10**30,
        Decimal("0"), Decimal("-0"), Decimal("0.0000"), Decimal("-0.0000"), Decimal("1"),
        Decimal("20.9458"), Decimal("100.00"), Decimal("1E+2"), Decimal("1.23456"),
        Decimal("-0.0001"), Decimal("99999999999999.9999"), Decimal("123456789012345678.1234"),
        Decimal("NaN"), Decimal("Infinity"), Decimal("-Infinity"), Decimal("1.5E-8"),
        0.0, -0.0, 0.125, -2.5, 1e-7, 1e22, 1e300, 0.1, 106.60000000000001, float("nan"),
        float("inf"), float("-inf"), 2.0**53, 3.0,
        datetime(2020, 1, 1, 0, 0, 37), datetime(2020, 1, 1, 0, 0, 37, 120000),
        datetime(2020, 1, 1, 0, 0, 37, 999999), datetime(999, 1, 1), datetime(1, 1, 1),
        datetime(9999, 12, 31, 23, 59, 59, 999999), datetime(1970, 1, 1),
        date(2000, 1, 2), date(999, 5, 6), date(1, 1, 1), date(9999, 12, 31), date(2024, 2, 29),
        "", " ", "0", "-0", "00", "01", "1", "-1", "+1", "1.0", "1.00", "1.000", "1.0000",
        "1,234", "1.234", "1.234,56", "$12.50", "12.50", "-12.5", "1e5", "1E5", "1e-07",
        "0.375", "0.1", "-0.0", "NaN", "Infinity", "-inf", "nan", "true", "false", "TRUE",
        "t", "f", "yes", "no", "1 ", " 1", "2024-01-01", "2024-02-30", "2024-13-01",
        "0999-01-01", "2024-01-01T00:00:00", "2024-01-01 00:00:00", "2024-01-01T25:00:00",
        "2024-01-01T00:00:00Z", "2024-01-01T00:00:00+02:00", "2024-01-01T00:00:00.5",
        "01/02/2024", "20240101", "12:00:00.250000", "03:04:05.25", "{}", "[]", '{"a": 1.10}',
        " {\"a\":1}", "[1, 2]", "__DF_SQL_NULL__", "__df_ddb_null__", "__DF_MISSING__",
        "__DF_ESC__x", "__DF_ESC____DF_SQL_NULL__", "a__b", "\x00lead", "nul\x00mid",
        "null", "NULL", "None", "n/a", "-", "label-1", "Δοκιμή", "🙂 emoji", "café",
        "tab\tand\nnewline\\back", "trail  ", "  lead", "Tōkyō", "ＡＢＣ", "１２３",
        "e", "E", "x" * 300, "1" * 40, "-" + "9" * 19, "9" * 18, "-" + "9" * 18,
    ]
    alphabet = string.ascii_letters + string.digits + " .,-_:/{}[]\"'$€%()\\\t\n" + "éΔ🙂０"
    for _ in range(n):
        pick = rng.randrange(10)
        if pick == 0:
            values.append(rng.randrange(-(2**64), 2**64))
        elif pick == 1:
            scale = rng.randrange(0, 12)
            values.append(Decimal(rng.randrange(-(10**20), 10**20)).scaleb(-scale))
        elif pick == 2:
            values.append(rng.uniform(-1e9, 1e9) * rng.choice([1, 1e-12, 1e12]))
        elif pick == 3:
            base = datetime(1900, 1, 1) + timedelta(seconds=rng.randrange(0, 250 * 365 * 86400))
            values.append(base.replace(microsecond=rng.choice([0, 0, rng.randrange(10**6)])))
        elif pick == 4:
            values.append(date(1900, 1, 1) + timedelta(days=rng.randrange(0, 365 * 300)))
        elif pick == 5:
            v = values[rng.randrange(len(values))]
            values.append(str(v) if not isinstance(v, (datetime, date)) else v.isoformat())
        elif pick == 6:
            values.append(str(rng.randrange(-(10**19), 10**19)))
        elif pick == 7:
            whole = rng.randrange(0, 10**rng.randrange(1, 16))
            frac = "".join(rng.choice(string.digits) for _ in range(rng.randrange(0, 8)))
            sign = rng.choice(["", "-"])
            values.append(f"{sign}{whole}" + (f".{frac}" if frac else ""))
        elif pick == 8:
            values.append(repr(rng.uniform(-1e6, 1e6)))
        else:
            values.append("".join(rng.choice(alphabet) for _ in range(rng.randrange(0, 20))))
    return values


def _both(fn, value, **kw):
    try:
        fast = ("ok", fn(value, **kw))
    except Exception as exc:  # noqa: BLE001 - both paths must agree on raising too
        fast = ("raise", type(exc).__name__)
    with cell_fastpath.full_path_only():
        try:
            slow = ("ok", fn(value, **kw))
        except Exception as exc:  # noqa: BLE001
            slow = ("raise", type(exc).__name__)
    return fast, slow


def _assert_same(fast, slow, context):
    assert fast == slow and type(fast[1]) is type(slow[1]), (context, fast, slow)


@pytest.mark.parametrize("ddl,engine", COLUMNS)
def test_fingerprint_fast_path_matches_full_path(ddl, engine):
    corpus = _corpus(sum(map(ord, ddl + engine)), int(500 * SCALE))
    hits = 0
    for number_locale, date_locale, strict in PINS:
        tn = set_active_number_locale(number_locale)
        td = set_active_date_locale(date_locale)
        ts = set_strict_number_reading(strict)
        try:
            for transform in TRANSFORMS:
                for source_locale in ("", "EU"):
                    for value in corpus:
                        fast, slow = _both(
                            fingerprint_for_reconcile, value, ddl_type=ddl,
                            engine=engine, transform=transform,
                            source_locale=source_locale,
                        )
                        _assert_same(fast, slow, (ddl, engine, transform, source_locale,
                                                  number_locale, date_locale, strict,
                                                  repr(value)))
                        hits += 1
        finally:
            reset_strict_number_reading(ts)
            reset_active_date_locale(td)
            reset_active_number_locale(tn)
    assert hits
    assert not cell_fastpath.self_check_failures


@pytest.mark.parametrize("ddl,engine", COLUMNS + [("", ""), ("", "postgresql")])
def test_normalize_cell_fast_path_matches_full_path(ddl, engine):
    corpus = _corpus(sum(map(ord, ddl + engine)) + 1, int(3000 * SCALE))
    for number_locale in NUMBER_LOCALES:
        tn = set_active_number_locale(number_locale)
        try:
            for value in corpus:
                fast, slow = _both(normalize_cell, value, ddl_type=ddl, engine=engine)
                _assert_same(fast, slow, (ddl, engine, number_locale, repr(value)))
        finally:
            reset_active_number_locale(tn)


def test_planned_columns_are_the_ordinary_ones():
    plan = cell_fastpath.column_plan
    assert plan("BIGINT", "postgresql").kind == "int"
    assert plan("NUMERIC(18,4)", "postgresql").kind == "decimal"
    assert plan("DOUBLE PRECISION", "postgresql").kind == "double"
    assert plan("TIMESTAMP(6)", "postgresql").kind == "timestamp"
    assert plan("DATETIME(6)", "mysql").kind == "timestamp"
    assert plan("DATE", "mysql").kind == "date"
    assert plan("BOOLEAN", "postgresql").kind == "bool"
    assert plan("TEXT", "postgresql").kind == "text"
    for ddl, engine in [("JSONB", "postgresql"), ("TIMESTAMPTZ", "postgresql"), ("REAL", "postgresql"),
                        ("FLOAT", "mysql"), ("TINYINT(1)", "mysql"), ("TIMESTAMP", "mysql"),
                        ("VARCHAR(64)", "sqlserver"), ("BIGINT", "oracle"), ("TEXT", "sqlite"),
                        ("TEXT CARRYING SOURCE_BOOLEAN", "postgresql"),
                        ("VARCHAR(64) COLLATE utf8mb4_general_ci", "mysql"),
                        ("CHAR(10)", "postgresql")]:
        assert plan(ddl, engine) is None, (ddl, engine)


def test_switch_turns_the_fast_paths_off(monkeypatch):
    monkeypatch.setenv("DATAFLOW_CELL_FASTPATH", "0")
    cell_fastpath.reset_plans()
    try:
        assert cell_fastpath.column_plan("BIGINT", "postgresql") is None
    finally:
        monkeypatch.delenv("DATAFLOW_CELL_FASTPATH")
        cell_fastpath.reset_plans()
    assert cell_fastpath.column_plan("BIGINT", "postgresql") is not None


@pytest.mark.parametrize("ddl,engine", COLUMNS)
def test_bind_fast_path_matches_full_path(ddl, engine):
    """What the writer binds — the value handed to the driver — is unchanged."""
    from connectors.sql_bind import normalize_sql_bind_value

    corpus = _corpus(sum(map(ord, ddl + engine)) + 2, int(3000 * SCALE))
    for number_locale, date_locale, strict in PINS:
        tn = set_active_number_locale(number_locale)
        td = set_active_date_locale(date_locale)
        ts = set_strict_number_reading(strict)
        try:
            for value in corpus:
                try:
                    fast = ("ok", normalize_sql_bind_value(value, ddl, engine=engine))
                except Exception as exc:  # noqa: BLE001
                    fast = ("raise", type(exc).__name__)
                with cell_fastpath.full_path_only():
                    try:
                        slow = ("ok", normalize_sql_bind_value(value, ddl, engine=engine))
                    except Exception as exc:  # noqa: BLE001
                        slow = ("raise", type(exc).__name__)
                same = fast[0] == slow[0] and type(fast[1]) is type(slow[1]) and (
                    fast[1] == slow[1] or (fast[1] != fast[1] and slow[1] != slow[1])
                )
                assert same, (ddl, engine, number_locale, date_locale, strict, repr(value),
                              repr(fast), repr(slow))
        finally:
            reset_strict_number_reading(ts)
            reset_active_date_locale(td)
            reset_active_number_locale(tn)


@pytest.mark.parametrize("transform", ["integer", "decimal", "date", "datetime", "boolean",
                                       "INTEGER", "int", "none", None, "json", "upper"])
def test_transform_fast_path_matches_full_path(transform):
    """What the writer converts each source cell to is unchanged."""
    from services.transform_engine import apply_transform

    corpus = [v for v in _corpus(sum(map(ord, str(transform))), int(4000 * SCALE))]
    corpus += [str(v) for v in corpus if not isinstance(v, str)]
    for number_locale, date_locale, strict in PINS:
        tn = set_active_number_locale(number_locale)
        td = set_active_date_locale(date_locale)
        ts = set_strict_number_reading(strict)
        try:
            for informal in (False, True):
                for value in corpus:
                    fast, slow = _both(apply_transform, value, transform=transform,
                                       accept_informal_boolean=informal)
                    same = fast[0] == slow[0] and type(fast[1]) is type(slow[1]) and (
                        fast[1] == slow[1] or repr(fast[1]) == repr(slow[1])
                    )
                    assert same and repr(fast) == repr(slow), (
                        transform, number_locale, date_locale, strict, informal, repr(value),
                        fast, slow)
        finally:
            reset_strict_number_reading(ts)
            reset_active_date_locale(td)
            reset_active_number_locale(tn)


@pytest.mark.parametrize("ddl,engine", COLUMNS)
def test_temporal_fit_fast_path_matches_full_path(ddl, engine):
    """The writer holds out exactly the same temporal cells."""
    from connectors.writer_quarantine_unfit import temporal_cell_unfit_reason

    corpus = _corpus(sum(map(ord, ddl + engine)) + 3, int(3000 * SCALE))
    for value in corpus:
        fast, slow = _both(temporal_cell_unfit_reason, value, typ=ddl, dest_db=engine)
        assert fast == slow, (ddl, engine, repr(value), fast, slow)


def test_decimal_parse_fast_reject_matches_full_path():
    """``decimal_wire_value`` refuses exactly the same text."""
    from services.transform_engine import decimal_wire_value

    corpus = _corpus(77, int(20000 * SCALE))
    corpus += [str(v) for v in corpus if not isinstance(v, str)]
    corpus += ["USD", "US$5", "5USD", "Rs.5", "kr", "EUR 1.234,50", "1_000", "e5", "1e5", "E",
               "2024-01-01", "1-", "-1", "1-2", "(1,234)", "12%", "1:2", "{1}", "Infinity",
               "label-1", "x1", "0x10", "١٢٣", "１２３", "1 000", "1 000",
               "note abc 12", "12 USD", "USD 12", "usd 12", "Rs. 5", "Rs 5", "kr 5", "5 kr",
               "CHF 5.50", "Fr. 5", "SFr 5", "1 000 EUR", "label 1", "1 e 5", "US$ 5", "A$ 5",
               "\u039e5", "\u0141 5", "T\u014dky\u014d", "\u0394\u03bf\u03ba\u03b9\u03bc\u03ae",
               "caf\u00e9 5", "\uff15 \uff45", "\uff15\uff45\uff13", "\u216b", "1\u20442", "\u00b2",
               "\u20ac 5", "5 \u20ac", "lei 5", "z\u0142 5", "5\u3000kr", "DOGE 1", "1INCH 5",
               "e 5", "E5", "K\u010d 5", "5K\u010d", "\u043b\u0432 5"]
    for number_locale, date_locale, strict in PINS:
        tn = set_active_number_locale(number_locale)
        ts = set_strict_number_reading(strict)
        try:
            for value in corpus:
                fast, slow = _both(decimal_wire_value, value)
                assert repr(fast) == repr(slow), (number_locale, strict, repr(value), fast, slow)
        finally:
            reset_strict_number_reading(ts)
            reset_active_number_locale(tn)


@pytest.mark.parametrize("ddl,engine", COLUMNS)
def test_float_coerce_fast_path_matches_full_path(ddl, engine):
    """The writer's float fit check (``coerce_float_wire``) is unchanged."""
    from connectors.sql_bind import coerce_float_wire

    corpus = _corpus(sum(map(ord, ddl + engine)) + 4, int(2000 * SCALE))
    for number_locale, date_locale, strict in PINS:
        tn = set_active_number_locale(number_locale)
        ts = set_strict_number_reading(strict)
        try:
            for value in corpus:
                fast, slow = _both(coerce_float_wire, value, ddl_type=ddl, engine=engine)
                assert repr(fast) == repr(slow), (ddl, engine, number_locale, repr(value),
                                                  fast, slow)
        finally:
            reset_strict_number_reading(ts)
            reset_active_number_locale(tn)


def _reference_parse_date_worker(value, with_time, date_locale):
    """``transform_engine._parse_date_worker`` as it was before the ISO-stamp shortcut."""
    import re as _re
    from datetime import date as _date, datetime as _datetime

    from services import transform_engine as te

    text = value.strip()
    if not text:
        return None
    if te._ISO_DATE_RE.match(text):
        try:
            _date.fromisoformat(text)
        except ValueError:
            return None
        return text
    if not te._DATE_LIKE_RE.search(text):
        return None
    if text.lower() in te.NULL_SENTINELS:
        return None
    if te._is_ambiguous_mdy_dmy(text, date_locale):
        return None
    if _re.match(r"^\d{8}$", text):
        try:
            return _datetime.strptime(text, "%Y%m%d").strftime("%Y-%m-%d")
        except ValueError:
            pass
    patterns = te._reorder_date_patterns(text, te.DATE_PATTERNS, date_locale)
    if with_time:
        patterns += te._reorder_date_patterns(text, te.DATE_WITH_TIME_PATTERNS, date_locale)
    for fmt in patterns:
        try:
            return _datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def test_iso_stamp_date_parse_matches_the_pattern_sweep():
    from services.transform_engine import _parse_date_worker

    rng = random.Random(5)
    values = ["2024-01-01 00:00:00", "2024-01-01T00:00:00", "2024-02-30 10:00:00",
              "2024-02-29T23:59:59.5", "2024-12-31 23:59:60", "2024-12-31 24:00:00",
              "0999-01-01 00:00:00", "0000-01-01 00:00:00", "2024-1-01 00:00:00",
              "2024-01-01 00:00:00.1234567", " 2024-01-01 00:00:00 ", "2024-01-01T10:61:00"]
    for _ in range(3000):
        values.append(
            f"{rng.randrange(0, 10000):04d}-{rng.randrange(0, 14):02d}-{rng.randrange(0, 33):02d}"
            f"{rng.choice([' ', 'T'])}{rng.randrange(0, 26):02d}:{rng.randrange(0, 62):02d}:"
            f"{rng.randrange(0, 62):02d}" + rng.choice(["", "", ".5", ".123456", ".1234567"])
        )
    for value in values:
        for with_time in (False, True):
            for locale in ("", "DMY", "MDY"):
                _parse_date_worker.cache_clear()
                assert _parse_date_worker(value, with_time, locale) == (
                    _reference_parse_date_worker(value, with_time, locale)
                ), (value, with_time, locale)
