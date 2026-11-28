"""Accuracy Track A item 8: Gate-8 cell fingerprint properties (deterministic).

Promoted from the research differential probe (gate8_diff_probe.py). For every
destination carrier below, with a fixed seed:

* **no false proven**: one unambiguous change to a value (a digit, the sign, a
  scale step, a microsecond, a day, a letter, an appended or leading
  character, a flipped boolean) must change the fingerprint, or Gate-8 would
  call a real difference verified;
* **no false unexplained**: another driver spelling of the same destination
  value (str/Decimal/float/int, ISO date or datetime, 1/'true') must keep the
  fingerprint, or Gate-8 would raise a mismatch on equal data;
* NULL never fingerprints like an empty string, zero or false;
* text is compared as stored: a change of case, a leading or trailing space,
  a no-break space or a tab for a space is a difference, except where the
  carrier itself makes them equal (``CITEXT`` ignores case, ``CHAR(n)`` pads
  trailing spaces), and those equalities must hold.

Runs offline in well under 60 s (CI job ai-offline-correctness).
"""

from __future__ import annotations

import datetime as dt
import random
import time
import zlib
from decimal import Decimal

import pytest

from services.reconciliation import fingerprint_for_reconcile as fp

SEED = 20261121
N = 400
CARRIERS = {
    "int": [("postgresql", "BIGINT"), ("mysql", "INT"), ("sqlserver", "INT"), ("oracle", "NUMBER(19)"),
            ("sqlite", "INTEGER")],
    "dec": [("postgresql", "NUMERIC(38,18)"), ("mysql", "DECIMAL(38,10)"), ("sqlserver", "DECIMAL(38,10)"),
            ("oracle", "NUMBER(38,10)"), ("snowflake", "NUMBER(38,10)")],
    "float": [("postgresql", "DOUBLE PRECISION"), ("mysql", "DOUBLE"), ("sqlserver", "FLOAT")],
    "text": [("postgresql", "TEXT"), ("postgresql", "VARCHAR(50)"), ("sqlserver", "NVARCHAR(50)"),
             ("oracle", "VARCHAR2(50)"), ("mysql", "VARCHAR(50)"), ("postgresql", "CITEXT"),
             ("snowflake", "VARCHAR(50)")],
    "char": [("postgresql", "CHAR(10)"), ("sqlserver", "CHAR(10)"), ("mysql", "CHAR(10)"), ("oracle", "CHAR(10)")],
    "ts": [("postgresql", "TIMESTAMP(6)"), ("mysql", "DATETIME(6)"), ("sqlserver", "DATETIME2(7)"),
           ("snowflake", "TIMESTAMP_NTZ(9)")],
    "date": [("postgresql", "DATE"), ("mysql", "DATE"), ("sqlserver", "DATE")],
    "bool": [("postgresql", "BOOLEAN"), ("mysql", "TINYINT(1)"), ("sqlserver", "BIT")],
}
CASES = [(kind, eng, ddl) for kind, rows in CARRIERS.items() for eng, ddl in rows]


def _rng(kind: str, eng: str, ddl: str) -> random.Random:
    return random.Random(SEED ^ zlib.crc32(f"{kind}|{eng}|{ddl}".encode()))


def _gen(R: random.Random, kind: str):
    if kind == "int":
        return R.choice([R.randint(-10**6, 10**6), R.randint(-2**62, 2**62)])
    if kind == "dec":
        v = Decimal(R.randint(1, 10 ** R.randint(1, 20))) / (Decimal(10) ** R.randint(0, 10))
        return v if R.random() < .8 else -v
    if kind == "float":
        return R.uniform(-1e6, 1e6)
    if kind in ("text", "char"):
        alpha = "abcdefghijklmnopqrstuvwxyzABCXYZ0123456789 -_"
        n = 9 if kind == "char" else 20
        return "".join(R.choice(alpha) for _ in range(R.randint(1, n))).strip() or "x"
    if kind in ("ts", "date"):
        t = dt.datetime(2000, 1, 1) + dt.timedelta(seconds=R.randint(0, 10**9), microseconds=R.randint(0, 999999))
        return t if kind == "ts" else t.date()
    return R.random() < .5


def _mutate(R: random.Random, kind: str, v, ddl: str):
    if kind == "int":
        return v + R.choice([1, -1, 10]) if R.random() < .8 else (-v if v else 1)
    if kind == "dec":
        scale = int(ddl.split(",")[1].rstrip(")")) if "," in ddl else 10
        return v + Decimal(1).scaleb(-R.randint(0, min(scale, 18))) if R.random() < .8 else (-v if v else Decimal(1))
    if kind == "float":
        return v * (1 + 1e-9) + 1e-6 if R.random() < .5 else (-v if v else 1.0)
    if kind in ("text", "char"):
        i = R.randrange(len(v))
        alts = [x for x in "qwrtyp7531" if x.lower() != v[i].lower()]
        c = R.random()
        if c < .3:
            return v[:i] + R.choice(alts) + v[i + 1:]
        if c < .45:
            return v + R.choice(alts)
        if c < .55:
            return "0" + v  # a leading zero stays significant on text
        if c < .65:
            return " " + v  # a leading space is data
        if c < .75:
            return v.replace(" ", "\u00a0", 1) if " " in v else v + "\u00a0"  # no-break space
        if c < .85:
            return v.replace(" ", "\t", 1) if " " in v else "\t" + v
        if "CITEXT" not in ddl.upper() and v.lower() != v.upper():
            return v.swapcase()  # case is data except on CITEXT
        if kind == "text":
            return v + " "  # a trailing space is data except on CHAR(n)
        return "0" + v
    if kind == "ts":
        return v + dt.timedelta(microseconds=R.choice([1, 1000, 10**6]))
    if kind == "date":
        return v + dt.timedelta(days=R.choice([1, -1, 365]))
    return not v


def _respell(R: random.Random, kind: str, v):
    if kind == "int":
        return R.choice([str(v), Decimal(v), float(v) if abs(v) < 2**53 else v])
    if kind == "dec":
        f = format(v, "f")
        return R.choice([f, Decimal(f + "0") if "." in f else Decimal(f + ".0"), v.normalize()])
    if kind == "float":
        return R.choice([repr(v), Decimal(repr(v))])
    if kind == "text":
        if "CITEXT" in _DDL.get(id(R), ""):
            return R.choice([v.upper(), v.lower(), v.swapcase()])  # CITEXT ignores case
        return str(v)
    if kind == "char":
        return v + " " * R.randint(0, 3)  # CHAR(n) pads trailing spaces
    if kind == "ts":
        return R.choice([v.isoformat(), v.isoformat(sep=" ")])
    if kind == "date":
        return R.choice([v.isoformat(), dt.datetime(v.year, v.month, v.day)])
    return R.choice([int(v), "true" if v else "false", "1" if v else "0"])


_DDL: dict[int, str] = {}


def probe(kind: str, eng: str, ddl: str, fingerprint=fp) -> dict[str, list[str]]:
    R = _rng(kind, eng, ddl)
    _DDL[id(R)] = ddl.upper()
    out: dict[str, list[str]] = {"false_proven": [], "false_unexplained": []}
    for _ in range(N):
        a = _gen(R, kind)
        b = _mutate(R, kind, a, ddl)
        c = _respell(R, kind, a)
        if a == b:
            continue
        fa = fingerprint(a, ddl_type=ddl, engine=eng)
        if fa == fingerprint(b, ddl_type=ddl, engine=eng):
            out["false_proven"].append(f"{a!r} vs {b!r}")
        if fa != fingerprint(c, ddl_type=ddl, engine=eng):
            out["false_unexplained"].append(f"{a!r} vs {c!r}")
    return out


@pytest.mark.parametrize("kind,eng,ddl", CASES, ids=[f"{e}:{d}" for _k, e, d in CASES])
def test_single_cell_change_is_never_proven_and_respelling_never_flagged(kind, eng, ddl) -> None:
    found = probe(kind, eng, ddl)
    assert found["false_proven"] == [], found["false_proven"][:5]
    assert found["false_unexplained"] == [], found["false_unexplained"][:5]


@pytest.mark.parametrize("kind,eng,ddl", CASES, ids=[f"{e}:{d}" for _k, e, d in CASES])
def test_null_is_not_an_empty_zero_or_false_value(kind, eng, ddl) -> None:
    null = fp(None, ddl_type=ddl, engine=eng)
    blanks = {"text": [""], "char": [""], "int": [0, "0"], "dec": [Decimal(0), "0"], "float": [0.0],
              "bool": [False, 0], "ts": [], "date": []}[kind]
    if eng == "oracle" and kind in ("text", "char"):
        blanks = []  # Oracle stores '' as NULL: the same destination value (D210).
    for v in blanks:
        assert fp(v, ddl_type=ddl, engine=eng) != null, (v, ddl)


def test_the_property_catches_a_lossy_fingerprint() -> None:
    """Self-check: a fingerprint that drops information must fail the property."""

    def truncating(value, *, ddl_type="", engine=""):
        return fp(value, ddl_type=ddl_type, engine=engine)[:6]

    def unstable(value, *, ddl_type="", engine=""):
        return f"{type(value).__name__}:{fp(value, ddl_type=ddl_type, engine=engine)}"

    def casefolding(value, *, ddl_type="", engine=""):
        v = value.casefold() if isinstance(value, str) else value
        return fp(v, ddl_type=ddl_type, engine=engine)

    def stripping(value, *, ddl_type="", engine=""):
        v = " ".join(value.split()) if isinstance(value, str) else value
        return fp(v, ddl_type=ddl_type, engine=engine)

    assert probe("text", "postgresql", "TEXT", truncating)["false_proven"]
    assert probe("int", "postgresql", "BIGINT", unstable)["false_unexplained"]
    # Review finding 13: a case-folding or whitespace-stripping fingerprint fails.
    for kind, eng, ddl in (("text", "postgresql", "TEXT"), ("text", "sqlserver", "NVARCHAR(50)"),
                           ("char", "postgresql", "CHAR(10)")):
        assert probe(kind, eng, ddl, casefolding)["false_proven"], ddl
        assert probe(kind, eng, ddl, stripping)["false_proven"], ddl


def test_property_is_deterministic_and_bounded() -> None:
    start = time.perf_counter()
    first = [probe(k, e, d) for k, e, d in CASES[:3]]
    assert first == [probe(k, e, d) for k, e, d in CASES[:3]]
    # The full matrix (34 carriers x 400 pairs) must stay far inside 60 s.
    assert (time.perf_counter() - start) * len(CASES) / 6 < 60
