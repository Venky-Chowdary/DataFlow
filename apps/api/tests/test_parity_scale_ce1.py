"""CE1 reference encoder: golden byte vectors, injectivity, words, buckets, plan.

The golden vectors (``tests/data/ce1_vectors.json``) were written by hand —
their digests computed from hand-written canonical bytes — so a change to
``services/parity_scale/ce1.py`` that moves a byte fails here first. The same
vectors feed the live PostgreSQL rendering test.
"""

from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from services.parity_scale import ce1
from services.parity_scale.ce1 import DecimalFit, FieldSpec

VECTORS = Path(__file__).resolve().parent / "data" / "ce1_vectors.json"


def decode(value: Any) -> Any:
    from services.parity_scale.store import decode_value
    from services.value_serializer import JSON_NULL

    if isinstance(value, dict) and value.get("$json_null"):
        return JSON_NULL
    return decode_value(value)


def spec_of(case: dict[str, Any]) -> FieldSpec:
    raw = dict(case["spec"])
    fit = raw.pop("decimal_fit", None)
    return FieldSpec(kind=case["kind"], decimal_fit=DecimalFit(**fit) if fit else None, **raw)


def load_vectors() -> dict[str, Any]:
    return json.loads(VECTORS.read_text(encoding="utf-8"))


FIELD_CASES = load_vectors()["fields"]


@pytest.mark.parametrize("case", FIELD_CASES, ids=[c["name"] for c in FIELD_CASES])
def test_golden_field_vectors(case: dict[str, Any]) -> None:
    spec = spec_of(case)
    value = decode(case["value"])
    if case["side"] == "source":
        got = ce1.encode_source_value(value, spec)
    else:
        got = ce1.encode_value(value, spec)
    if case["expected"] == "HELD":
        assert got is ce1.HELD
    else:
        assert got == case["expected"].encode("utf-8")


def test_golden_row_vectors() -> None:
    for row in load_vectors()["rows"]:
        specs = [FieldSpec(kind=k, dest_engine="postgresql") for k in row["kinds"]]
        encoded = ce1.encode_row([decode(v) for v in row["values"]], specs, source=False)
        assert encoded == row["expected"].encode("utf-8"), row["name"]
        h, a, b = ce1.hash_words(encoded)  # type: ignore[arg-type]
        assert h.hex() == row["sha256"]
        assert (a, b) == (int(row["a"]), int(row["b"]))


def test_vectors_cover_every_kind() -> None:
    kinds = {c["kind"] for c in FIELD_CASES}
    assert kinds == set(ce1.KINDS)


def test_words_are_the_design_hex_slices() -> None:
    """``a`` is hex chars 2–16 and ``b`` 18–32 of the digest (design §1.4)."""
    h = hashlib.sha256(b"anything").digest()
    hexd = h.hex()
    a, b = ce1.words(h)
    assert a == int(hexd[1:16], 16)
    assert b == int(hexd[17:32], 16)
    assert a < 2**60 and b < 2**60


def test_buckets_are_disjoint_bit_ranges() -> None:
    key_hash = bytes.fromhex("ffc00000000000000000000000000000" * 2)
    assert ce1.key_bucket(key_hash, 0) == 1023
    assert ce1.key_bucket(key_hash, 1) == 0
    row_hash = bytes(16) + bytes.fromhex("003ff00000000000") + bytes(8)
    assert ce1.row_bucket(row_hash, 0) == 0
    assert ce1.row_bucket(row_hash, 1) == 1023


def test_length_prefix_makes_rows_injective() -> None:
    specs = [FieldSpec(kind="text"), FieldSpec(kind="text")]
    left = ce1.encode_row(["ab", "c"], specs, source=False)
    right = ce1.encode_row(["a", "bc"], specs, source=False)
    assert left != right
    assert ce1.encode_row(["", None], specs, source=False) != ce1.encode_row([None, ""], specs, source=False)


def test_storage_class_is_kept_in_the_bytes() -> None:
    text = FieldSpec(kind="text")
    assert ce1.encode_value(5, text) != ce1.encode_value("5", text)
    assert ce1.encode_value(1.5, text) != ce1.encode_value("1.5", text)
    # A text value cannot imitate a foreign field either.
    assert ce1.encode_value("Xinteger:1:5", text) != ce1.encode_value(5, text)


def test_decimal_text_matches_postgres_trim_scale_rules() -> None:
    cases = {
        "0": "0", "0.000": "0", "-0": "0", "1.10": "1.1", "100": "100", "1E+3": "1000",
        "123.4500": "123.45", "-0.0001": "-0.0001", "1E-7": "0.0000001",
    }
    for raw, want in cases.items():
        assert ce1.decimal_text(Decimal(raw)) == want


def test_projection_never_rounds_a_decimal() -> None:
    fit = DecimalFit(precision=10, scale=2, dest_db="postgresql", actions={"scale": "null", "int": "hold"})
    spec = FieldSpec(kind="decimal", decimal_fit=fit)
    assert ce1.project_source_value(Decimal("12.345"), spec) is None
    assert ce1.project_source_value(Decimal("12.3400"), spec) == Decimal("12.3400")
    assert ce1.project_source_value(Decimal("100000000"), spec) is ce1.HELD
    assert ce1.encode_row([Decimal("100000000")], [spec], source=True) is ce1.HELD


def test_plan_classifies_mapping_shapes() -> None:
    from services.parity_scale.plan import build_plan

    types = {
        "id": "bigint", "name": "varchar(20)", "note": "text", "f": "double precision",
        "j": "jsonb", "t": "text", "gone": "text", "ts": "timestamp(6)",
    }
    dest = {**types, "ts": "timestamp(3)"}
    mappings = [
        {"source": "id", "target": "id"},
        {"source": "name", "target": "name"},
        {"source": "note", "target": "note"},
        {"source": "f", "target": "f"},
        {"source": "j", "target": "j"},
        {"source": "t", "target": "t", "transform": "upper"},
        {"source": "gone", "target": "gone", "intentional_omit": True},
        {"source": "ts", "target": "ts"},
    ]
    plan = build_plan(mappings, source_types=types, dest_types=dest, source_engine="postgresql",
                      dest_engine="postgresql", key_columns=["id"])
    by = {c.source: c for c in plan.columns}
    assert (by["id"].cls, by["id"].kind) == ("engine", "integer")
    assert (by["name"].cls, by["name"].width) == ("engine", "inline")
    assert (by["note"].cls, by["note"].width) == ("engine", "digest")
    assert (by["f"].cls, by["f"].kind) == ("engine", "native")
    assert (by["j"].cls, by["j"].kind) == ("engine", "native")
    assert by["t"].cls == "stream"
    assert by["gone"].cls == "excluded"
    assert (by["ts"].kind, by["ts"].digits, by["ts"].projection) == ("timestamp", 3, "instant(3)")
    assert plan.order_portable
    # Without same-engine native rendering, floats and JSON are streamed.
    plan2 = build_plan(mappings, source_types=types, dest_types=dest, source_engine="postgresql",
                       dest_engine="postgresql", key_columns=["id"], allow_native=False)
    by2 = {c.source: c for c in plan2.columns}
    assert by2["f"].cls == by2["j"].cls == "stream"
    assert plan2.plan_hash != plan.plan_hash


def test_plan_text_key_needs_binary_collation_for_mode_r() -> None:
    from services.parity_scale.plan import build_plan

    types = {"code": "varchar(10)", "v": "integer"}
    m = [{"source": "code", "target": "code"}, {"source": "v", "target": "v"}]
    plain = build_plan(m, source_types=types, dest_types=types, source_engine="postgresql",
                       dest_engine="postgresql", key_columns=["code"])
    assert not plain.order_portable
    binary = build_plan(m, source_types=types, dest_types=types, source_engine="postgresql",
                        dest_engine="postgresql", key_columns=["code"], key_collations={"code": "C"})
    assert binary.order_portable


def test_plan_refuses_uncertified_pairs_but_names_the_first_wave() -> None:
    from services.parity_scale.plan import PairNotCertified, build_plan, require_pair

    types = {"id": "int"}
    plan = build_plan([{"source": "id", "target": "id"}], source_types=types, dest_types=types,
                      source_engine="mysql", dest_engine="postgresql", key_columns=["id"])
    with pytest.raises(PairNotCertified, match="first certification wave"):
        require_pair(plan)
