"""Property tests: CE1 is never looser than Gate-8's fingerprint (design §5.2).

Soundness invariant: whenever two values encode equal under CE1, the Gate-8
fingerprint (``reconciliation._fingerprint_with_ddl`` under the destination's
canonical type — the call Gate-8's digest makes) calls them equal too. CE1 may
be stricter; these tests never fail on that direction.

For approved lossy casts the oracle is the real writer: the projection must
replay ``map_rows_for_fingerprint`` value by value (held out / NULL / written),
and a SQLite write round trip must store exactly what the projection says.

Examples per property: ``DATAFLOW_PARITY_CE_EXAMPLES`` (default 400; the CE
matrix job runs 2,000+). The seed is fixed (``derandomize``).
"""

from __future__ import annotations

import os
import sqlite3
import unicodedata
import uuid
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from typing import Any

import pytest

hypothesis = pytest.importorskip("hypothesis")
from hypothesis import HealthCheck, given, settings  # noqa: E402
from hypothesis import strategies as st  # noqa: E402

from services.parity_scale import ce1  # noqa: E402
from services.parity_scale.ce1 import DecimalFit, FieldSpec  # noqa: E402

EXAMPLES = int(os.environ.get("DATAFLOW_PARITY_CE_EXAMPLES") or 400)
PROPERTY = settings(
    max_examples=EXAMPLES,
    derandomize=True,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large],
)


def gate8_fp(value: Any, ddl: str, engine: str) -> str:
    """Gate-8's cell fingerprint: canonical destination type, then the v2 rules."""
    from services.reconciliation import _canonical_fingerprint_ddl, _fingerprint_with_ddl

    return _fingerprint_with_ddl(value, _canonical_fingerprint_ddl(engine, ddl), engine)


def assert_sound(a: Any, b: Any, spec: FieldSpec, *, source_a: bool = False) -> bool:
    """CE1(a) == CE1(b) ⇒ fp(a) == fp(b). Returns whether CE1 called them equal."""
    left = ce1.encode_source_value(a, spec) if source_a else ce1.encode_value(a, spec)
    right = ce1.encode_value(b, spec)
    if left is ce1.HELD or left != right:
        return False
    fa = gate8_fp(a, spec.dest_type, spec.dest_engine)
    fb = gate8_fp(b, spec.dest_type, spec.dest_engine)
    assert fa == fb, (
        f"CE1 looser than fingerprint v2 for {spec.kind} {spec.dest_type!r}: "
        f"{a!r} and {b!r} encode {left!r} but fingerprint {fa!r} vs {fb!r}"
    )
    return True


# ── value pools and same-value variants ─────────────────────────────────────

INTS = st.integers(min_value=-(2**63), max_value=2**63 - 1)
FOREIGN = st.one_of(st.text(max_size=6), st.floats(allow_nan=False), st.binary(max_size=6))


def _decimal_variants(d: Decimal) -> st.SearchStrategy[Any]:
    options: list[Any] = [d, -d, d + Decimal("0.000"), d.normalize() if d.is_finite() else d]
    if d.is_finite():
        options.append(d + Decimal("1E-30"))
        if d == d.to_integral_value():
            options.append(int(d))
    return st.sampled_from(options)


DECIMALS = st.one_of(
    st.decimals(allow_nan=False, allow_infinity=False, places=None, min_value=-(10**20), max_value=10**20),
    st.decimals(allow_nan=False, allow_infinity=False, places=4, min_value=-(10**8), max_value=10**8),
    st.sampled_from([Decimal("NaN"), Decimal("Infinity"), Decimal("-Infinity"), Decimal("-0"),
                     Decimal("0E-10"), Decimal("1E+5"), Decimal("12.3450")]),
)
TEXT = st.text(alphabet=st.characters(codec="utf-8"), max_size=12)


def _text_variants(s: str) -> st.SearchStrategy[Any]:
    return st.sampled_from([
        s, s + " ", " " + s, s.upper(), s.lower(), s.casefold(),
        unicodedata.normalize("NFC", s), unicodedata.normalize("NFD", s), s.strip(), s + "\x00",
    ])


OFFSETS = st.integers(min_value=-14 * 60, max_value=14 * 60).map(lambda m: timezone(timedelta(minutes=m)))
NAIVE = st.datetimes(min_value=datetime(1, 1, 2), max_value=datetime(9999, 12, 30))
AWARE = st.datetimes(min_value=datetime(1, 1, 2), max_value=datetime(9999, 12, 30), timezones=OFFSETS)


def _instant_variants(v: datetime) -> st.SearchStrategy[Any]:
    options: list[Any] = [v, v + timedelta(microseconds=1), v.replace(microsecond=0),
                          v.replace(microsecond=(v.microsecond // 1000) * 1000)]
    if v.tzinfo is not None:
        utc = v.astimezone(timezone.utc)
        options += [utc, utc.replace(tzinfo=None), v.astimezone(timezone(timedelta(hours=5)))]
    else:
        options += [v.replace(tzinfo=timezone.utc), v.isoformat(), v.isoformat(" ")]
    return st.sampled_from(options)


def _pair(pool: st.SearchStrategy[Any], variants) -> st.SearchStrategy[tuple[Any, Any]]:
    @st.composite
    def build(draw):
        a = draw(pool)
        b = draw(st.one_of(variants(a), pool, FOREIGN, st.none()))
        return a, b

    return build()


PG = "postgresql"

# ── one property per (kind, destination type) ───────────────────────────────


@PROPERTY
@given(pair=_pair(st.one_of(INTS, st.booleans()), lambda a: st.sampled_from([a, int(a), str(a), Decimal(int(a))])),
       ddl=st.sampled_from([("bigint", PG), ("integer", PG), ("INTEGER", "sqlite")]))
def test_integer_is_sound(pair, ddl) -> None:
    assert_sound(pair[0], pair[1], FieldSpec(kind="integer", dest_type=ddl[0], dest_engine=ddl[1]))


@PROPERTY
@given(pair=_pair(st.one_of(st.booleans(), st.integers(-3, 3)),
                  lambda a: st.sampled_from([a, bool(a), int(a), "true", "1", "t"])),
       ddl=st.sampled_from([("boolean", PG), ("BOOLEAN", "sqlite")]))
def test_boolean_is_sound(pair, ddl) -> None:
    assert_sound(pair[0], pair[1], FieldSpec(kind="boolean", dest_type=ddl[0], dest_engine=ddl[1]))


@PROPERTY
@given(pair=_pair(DECIMALS, _decimal_variants),
       ddl=st.sampled_from([("numeric(12,4)", PG), ("numeric", PG), ("numeric(38,10)", PG),
                            ("NUMERIC(10,2)", "sqlite")]))
def test_decimal_is_sound(pair, ddl) -> None:
    assert_sound(pair[0], pair[1], FieldSpec(kind="decimal", dest_type=ddl[0], dest_engine=ddl[1]))


@PROPERTY
@given(d=st.decimals(allow_nan=False, allow_infinity=False, places=2, min_value=-(10**8), max_value=10**8),
       bump=st.sampled_from([0.0, 1e-9, -1e-9, 0.005]))
def test_sqlite_real_against_an_exact_decimal_is_sound(d, bump) -> None:
    """SQLite NUMERIC stores REAL: the projection compares it to an exact source by value."""
    spec = FieldSpec(kind="decimal", dest_type="NUMERIC(10,2)", dest_engine="sqlite")
    stored = float(d) + bump
    assert_sound(d, stored, spec)
    assert_sound(float(d), stored, spec)
    if bump == 0.0 and Decimal(repr(float(d))) == d:
        # The faithful case is not a false difference.
        assert ce1.encode_value(d, spec) == ce1.encode_value(stored, spec)


@PROPERTY
@given(pair=_pair(TEXT, _text_variants),
       ddl=st.sampled_from([("varchar(20)", PG), ("text", PG), ("TEXT", "sqlite"),
                            ("VARCHAR(20) COLLATE utf8mb4_0900_ai_ci", "mysql"), ("citext", PG),
                            ("NVARCHAR(20) COLLATE Latin1_General_CI_AS", "sqlserver")]),
       width=st.sampled_from(["inline", "digest"]))
def test_text_is_sound(pair, ddl, width) -> None:
    assert_sound(pair[0], pair[1], FieldSpec(kind="text", dest_type=ddl[0], dest_engine=ddl[1], width=width))


@PROPERTY
@given(pair=_pair(TEXT, _text_variants), ddl=st.sampled_from([("char(10)", PG), ("CHAR(10)", "sqlserver")]))
def test_fixed_char_is_sound(pair, ddl) -> None:
    assert_sound(pair[0], pair[1], FieldSpec(kind="char", dest_type=ddl[0], dest_engine=ddl[1]))


@PROPERTY
@given(pair=_pair(TEXT, _text_variants))
def test_oracle_empty_string_rule_is_sound(pair) -> None:
    assert_sound(pair[0], pair[1], FieldSpec(kind="text", dest_type="VARCHAR2(20)", dest_engine="oracle",
                                             empty_is_null=True))


@PROPERTY
@given(pair=_pair(st.dates(), lambda d: st.sampled_from([d, datetime.combine(d, time.min), d.isoformat(),
                                                           d + timedelta(days=1) if d < date.max else d])))
def test_date_is_sound(pair) -> None:
    assert_sound(pair[0], pair[1], FieldSpec(kind="date", dest_type="date", dest_engine=PG))


@PROPERTY
@given(pair=_pair(st.one_of(NAIVE, AWARE), _instant_variants),
       target=st.sampled_from([("timestamp(6)", "timestamp", 6), ("timestamp(3)", "timestamp", 3),
                               ("timestamp(0)", "timestamp", 0),
                               ("timestamp with time zone", "timestamptz", 6),
                               ("timestamp(3) with time zone", "timestamptz", 3)]))
def test_timestamp_is_sound(pair, target) -> None:
    ddl, kind, digits = target
    spec = FieldSpec(kind=kind, dest_type=ddl, dest_engine=PG, digits=digits)
    assert_sound(pair[0], pair[1], spec)


@PROPERTY
@given(data=st.data(), bump=st.integers(-1500, 1500),
       target=st.sampled_from([("timestamp(3)", "timestamp", 3), ("timestamp(0)", "timestamp", 0),
                               ("timestamp(6)", "timestamp", 6), ("timestamp(3) with time zone", "timestamptz", 3)]))
def test_projected_source_instant_is_sound(data, bump, target) -> None:
    """A source instant projected to the carrier vs what the destination holds."""
    from services.carrier_instant import quantize_instant_for_carrier

    ddl, kind, digits = target
    # A naive carrier's source is naive and an aware one's is aware (other
    # pairings are streamed by the plan); the other class is also tried below.
    v = data.draw(AWARE if kind == "timestamptz" else NAIVE)
    spec = FieldSpec(kind=kind, dest_type=ddl, dest_engine=PG, digits=digits)
    stored = quantize_instant_for_carrier(v, ddl_type=ddl, engine=PG)
    try:
        candidate = stored + timedelta(microseconds=bump * (10 ** (6 - digits)) // 1000)
    except OverflowError:
        candidate = stored
    other = data.draw(NAIVE if kind == "timestamptz" else AWARE)
    for dest_value in (stored, candidate, other):
        assert_sound(v, dest_value, spec, source_a=True)
        assert_sound(other, dest_value, spec, source_a=True)
    # The writer's stored value always matches its own source (no false difference).
    assert ce1.encode_source_value(v, spec) == ce1.encode_value(stored, spec)


@PROPERTY
@given(pair=_pair(st.times(), lambda t: st.sampled_from([t, t.replace(microsecond=0), t.isoformat(),
                                                          timedelta(hours=t.hour, minutes=t.minute,
                                                                    seconds=t.second, microseconds=t.microsecond)])),
       target=st.sampled_from([("time", 6), ("time(3)", 3), ("time(0)", 0)]))
def test_time_is_sound(pair, target) -> None:
    assert_sound(pair[0], pair[1], FieldSpec(kind="time", dest_type=target[0], dest_engine=PG, digits=target[1]))


@PROPERTY
@given(pair=_pair(st.uuids(), lambda u: st.sampled_from([u, str(u), str(u).upper(), "{" + str(u) + "}", u.hex])))
def test_uuid_is_sound(pair) -> None:
    assert_sound(pair[0], pair[1], FieldSpec(kind="uuid", dest_type="uuid", dest_engine=PG))


@PROPERTY
@given(pair=_pair(st.binary(max_size=40), lambda b: st.sampled_from([b, bytearray(b), memoryview(b), b + b"\x00"])))
def test_binary_is_sound(pair) -> None:
    assert_sound(pair[0], pair[1], FieldSpec(kind="binary", dest_type="bytea", dest_engine=PG, width="digest"))


@PROPERTY
@given(pair=_pair(st.one_of(st.floats(), INTS, TEXT),
                  lambda f: st.sampled_from([f, -f if isinstance(f, (int, float)) else f, str(f)])))
def test_sqlite_native_rendering_is_sound(pair) -> None:
    assert_sound(pair[0], pair[1], FieldSpec(kind="native", dest_type="REAL", dest_engine="sqlite"))


def test_d144_json_null_follows_the_file_rule() -> None:
    from services.value_serializer import JSON_NULL

    scalar = FieldSpec(kind="text", dest_type="varchar(20)", dest_engine="s3")
    document = FieldSpec(kind="native", dest_type="json", dest_engine="s3")
    assert ce1.encode_value(JSON_NULL, scalar) == ce1.encode_value(None, scalar) == b"N"
    assert assert_sound(JSON_NULL, None, scalar)
    # On a document column the JSON value null is not SQL NULL.
    assert ce1.encode_value(JSON_NULL, document) != ce1.encode_value(None, document)


def test_known_representation_pairs_collide_and_agree() -> None:
    """The folds CE1 *does* make are exercised (the properties are not vacuous)."""
    dec = FieldSpec(kind="decimal", dest_type="numeric(12,4)", dest_engine=PG)
    assert assert_sound(Decimal("12.3400"), Decimal("12.34"), dec)
    assert assert_sound(Decimal("-0.00"), Decimal("0"), dec)
    assert assert_sound(7, Decimal("7.000"), dec)
    tz = FieldSpec(kind="timestamptz", dest_type="timestamp with time zone", dest_engine=PG)
    assert assert_sound(datetime(2024, 1, 1, 2, tzinfo=timezone(timedelta(hours=2))),
                        datetime(2024, 1, 1, 0, tzinfo=timezone.utc), tz)
    u = uuid.uuid4()
    assert assert_sound(u, str(u).upper(), FieldSpec(kind="uuid", dest_type="uuid", dest_engine=PG))
    assert assert_sound(True, 1, FieldSpec(kind="boolean", dest_type="boolean", dest_engine=PG))
    assert assert_sound("ab   ", "ab", FieldSpec(kind="char", dest_type="char(5)", dest_engine=PG))


def test_stricter_cases_are_the_judges() -> None:
    """Folds left to the judge: CE1 differs where fingerprint v2 is equal."""
    ci = FieldSpec(kind="text", dest_type="citext", dest_engine=PG)
    assert ce1.encode_value("ABC", ci) != ce1.encode_value("abc", ci)
    assert gate8_fp("ABC", ci.dest_type, PG) == gate8_fp("abc", ci.dest_type, PG)
    tz = FieldSpec(kind="timestamptz", dest_type="timestamp with time zone", dest_engine=PG)
    aware, naive = datetime(2024, 1, 1, 0, tzinfo=timezone.utc), datetime(2024, 1, 1)
    assert ce1.encode_value(aware, tz) != ce1.encode_value(naive, tz)
    real = FieldSpec(kind="decimal", dest_type="NUMERIC(10,2)", dest_engine="sqlite")
    # A REAL that is not the exact decimal stays apart (0.1 + 0.2 is not 0.3).
    assert ce1.encode_value(0.1 + 0.2, real) != ce1.encode_value(Decimal("0.3"), real)
    assert ce1.encode_value(12.34, real) == ce1.encode_value(Decimal("12.34"), real)


# ── approved lossy casts: the projection replays the real writer ─────────────

FIT_TYPES = [("numeric(12,4)", "numeric(10,2)"), ("numeric(8,4)", "numeric(4,4)"), ("numeric(20,6)", "numeric(6,0)")]
FIT_VALUES = st.one_of(
    st.decimals(allow_nan=False, allow_infinity=False, places=4, min_value=-(10**9), max_value=10**9),
    st.decimals(allow_nan=False, allow_infinity=False, places=6, min_value=-2, max_value=2),
    st.sampled_from([Decimal("12.3400"), Decimal("12.345"), Decimal("99999999.99"), Decimal("99999999.995"),
                     Decimal("100000000"), Decimal("-0.00"), Decimal("0"), Decimal("0.0001"),
                     Decimal("0.9999"), Decimal("1"), Decimal("NaN")]),
)


def _contract_mapping(src_type: str, dst_type: str, quarantine_policy: str | None) -> dict[str, Any]:
    from services.migration_risk_contract import create_migration_risk_contract

    extra = {"quarantine_policy": quarantine_policy} if quarantine_policy else {}
    contract = create_migration_risk_contract(
        column="amt", source_type=src_type, destination_type=dst_type, approved_by="qa@example.com",
        reason="accepted narrowing", execution_policy="CAST_AND_CONTINUE", **extra,
    )
    return {"source": "amt", "target": "amt", "risk_contract": contract.to_dict()}


def _writer_output(value: Decimal, mapping: dict[str, Any], dst_type: str, engine: str) -> tuple[bool, Any]:
    """``(written, value written)`` from the writer Gate-8 replays (WIRE locale)."""
    from connectors.writer_common import map_rows_for_fingerprint
    from services.transform_engine import reset_active_number_locale, set_active_number_locale

    token = set_active_number_locale("WIRE")
    try:
        mapped, _rejected = map_rows_for_fingerprint(
            headers=["amt"], data_rows=[[value]], mappings=[mapping], target_cols=["amt"],
            dest_types={"amt": dst_type}, dest_kind=engine, error_policy="quarantine",
        )
    finally:
        reset_active_number_locale(token)
    return (bool(mapped), mapped[0][0] if mapped else None)


_FIT_CACHE: dict[tuple[str, str, str | None, str], tuple[dict[str, Any], DecimalFit]] = {}


def _fit(src: str, dst: str, policy: str | None, engine: str) -> tuple[dict[str, Any], DecimalFit]:
    from services.parity_scale.plan import probe_decimal_fit

    key = (src, dst, policy, engine)
    if key not in _FIT_CACHE:
        mapping = _contract_mapping(src, dst, policy)
        fit = probe_decimal_fit(mapping, dst, engine, number_locale="WIRE")
        assert fit is not None
        _FIT_CACHE[key] = (mapping, fit)
    return _FIT_CACHE[key]


@PROPERTY
@given(value=FIT_VALUES, types=st.sampled_from(FIT_TYPES), policy=st.sampled_from(["COERCE_NULL", None]),
       engine=st.sampled_from([PG, "sqlite"]))
def test_decimal_projection_replays_the_writer(value, types, policy, engine) -> None:
    mapping, fit = _fit(types[0], types[1], policy, engine)
    spec = FieldSpec(kind="decimal", dest_type=types[1], dest_engine=engine, decimal_fit=fit)
    written, out = _writer_output(value, mapping, types[1], engine)
    projected = ce1.project_source_value(value, spec)
    assert (projected is ce1.HELD) == (not written), (value, projected, written, out)
    if not written:
        return
    assert (projected is None) == (out is None), (value, projected, out)
    # The projection encodes exactly what the writer wrote, and Gate-8 agrees.
    assert ce1.encode_source_value(value, spec) == ce1.encode_value(out, spec)
    assert gate8_fp(projected, types[1], engine) == gate8_fp(out, types[1], engine)


@settings(max_examples=max(20, EXAMPLES // 20), derandomize=True, deadline=None,
          suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture])
@given(values=st.lists(FIT_VALUES, min_size=1, max_size=40), policy=st.sampled_from(["COERCE_NULL", None]))
def test_decimal_projection_matches_a_sqlite_write_round_trip(values, policy, tmp_path, monkeypatch) -> None:
    """Write through the real SQLite writer, read back, and check every row."""
    from connectors.sqlite_writer import write_mapped_rows
    from services.transform_engine import reset_active_number_locale, set_active_number_locale

    monkeypatch.setenv("DATAFLOW_SQLITE_ROOT", str(tmp_path))
    src_type, dst_type = "NUMERIC(12,4)", "NUMERIC(10,2)"
    mapping, fit = _fit(src_type, dst_type, policy, "sqlite")
    path = tmp_path / f"rt-{uuid.uuid4().hex[:8]}.db"
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, amt NUMERIC(10,2))")
    conn.commit()
    conn.close()
    token = set_active_number_locale("WIRE")
    try:
        result = write_mapped_rows(
            host="", port=0, database=str(path), username="", password="", schema="",
            connection_string="", ssl=False, table_name="t", headers=["id", "amt"],
            data_rows=[[i, v] for i, v in enumerate(values)],
            mappings=[{"source": "id", "target": "id"}, mapping],
            column_types={"id": "INTEGER", "amt": dst_type}, create_table=False, error_policy="quarantine",
        )
    finally:
        reset_active_number_locale(token)
    assert result.ok, result.error
    conn = sqlite3.connect(path)
    stored = dict(conn.execute("SELECT id, amt FROM t").fetchall())
    conn.close()
    spec = FieldSpec(kind="decimal", dest_type=dst_type, dest_engine="sqlite", decimal_fit=fit)
    for i, value in enumerate(values):
        projected = ce1.project_source_value(value, spec)
        if projected is ce1.HELD and value.is_nan() and stored.get(i, "absent") is None:
            # Known divergence in the product, not in CE1: Gate-8's source
            # replay (map_rows_for_fingerprint) holds a NaN out of a SQLite
            # NUMERIC, while the SQLite writer itself stores the row with NULL.
            # CE1 follows Gate-8's replay, so the scan reports that row as an
            # extra destination row — the honest direction (never a pass).
            continue
        if projected is ce1.HELD:
            assert i not in stored, (value, stored.get(i))
            continue
        assert i in stored, value
        if projected is None:
            assert stored[i] is None, (value, stored[i])
            continue
        # SQLite keeps the number as INTEGER/REAL: CE1 is stricter there (the
        # judge rules); Gate-8 itself must call the stored value the written one.
        assert gate8_fp(stored[i], dst_type, "sqlite") == gate8_fp(projected, dst_type, "sqlite"), (value, stored[i])
        assert_sound(value, stored[i], spec, source_a=True)
