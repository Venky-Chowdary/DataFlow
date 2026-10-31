"""QA round-2 transform overrides: keep the operator cast and its vocabulary.

R2-047 boolean ``yes`` stayed text because the resolver dropped ``boolean``
on a non-boolean stamp. R2-048/049/050 turned a valid IBAN, id, and time
into NULL the same way, or by re-parsing a masked cell. R2-051 hash_pii
must name the missing key and must not emit a digest. R2-053/054/055 NULL
sentinels are SQL NULL before email/phone/postal validation.
"""

from __future__ import annotations

from services.transform_engine import apply_transform, canonical_boolean_as_number
from services.transform_resolver import resolve_transform
from services.value_serializer import SQL_NULL_SENTINEL


def _mapping(transform: str, *, target_type: str, override: bool = False) -> dict:
    mapping = {
        "source": "col",
        "target": "col",
        "transform": transform,
        "target_type": target_type,
    }
    if override:
        mapping["transform_override"] = True
    return mapping


def test_operator_boolean_accepts_yes_and_canonical_parser_does_not():
    refused, err = apply_transform("yes", "boolean")
    assert refused is None and err and "Invalid boolean" in err
    accepted, accepted_err = apply_transform(
        "yes", "boolean", accept_informal_boolean=True
    )
    assert accepted_err is None and accepted is True
    off, off_err = apply_transform("off", "boolean", accept_informal_boolean=True)
    assert off_err is None and off is False
    # Integer columns must not turn the word yes into 1.
    assert canonical_boolean_as_number("yes") is None


def test_replay_override_writes_yes_as_boolean():
    from connectors.writer_common import build_mapped_rows_with_details

    rows, errors, rejected = build_mapped_rows_with_details(
        headers=["flag"],
        data_rows=[["yes"], ["no"]],
        mappings=[
            {
                "source": "flag",
                "target": "flag",
                "transform": "boolean",
                "target_type": "VARCHAR",
                "transform_override": True,
            }
        ],
        target_cols=["flag"],
        column_types={"flag": "VARCHAR"},
    )
    assert errors == []
    assert rejected == []
    assert rows == [(True,), (False,)]


def test_transform_override_keeps_iban_time_and_trim_id():
    assert resolve_transform(_mapping("iban", target_type="INTEGER")) != "iban"
    assert resolve_transform(_mapping("time", target_type="VARCHAR")) == "time"
    assert resolve_transform(_mapping("trim_id", target_type="INTEGER")) != "trim_id"
    assert (
        resolve_transform(_mapping("iban", target_type="INTEGER", override=True))
        == "iban"
    )
    assert (
        resolve_transform(_mapping("time", target_type="VARCHAR", override=True))
        == "time"
    )
    assert (
        resolve_transform(_mapping("trim_id", target_type="INTEGER", override=True))
        == "trim_id"
    )
    assert (
        resolve_transform(_mapping("boolean", target_type="VARCHAR", override=True))
        == "boolean"
    )

    iban, iban_err = apply_transform("DE89370400440532013000", "iban")
    assert iban_err is None and iban == "DE89370400440532013000"
    clock, clock_err = apply_transform("10:30:00", "time")
    assert clock_err is None and clock == "10:30:00"
    ident, ident_err = apply_transform("ID-7", "trim_id")
    assert ident_err is None and ident == "ID-7"


def test_null_sentinel_is_sql_null_before_semantic_validation():
    for transform in ("email", "phone", "postal", "iban", "time", "trim_id", "boolean"):
        value, err = apply_transform(SQL_NULL_SENTINEL, transform)
        assert value is None and err is None, transform


def test_hash_pii_names_the_missing_key_and_writes_nothing(monkeypatch):
    monkeypatch.delenv("DATAFLOW_PII_HASH_KEY", raising=False)
    monkeypatch.delenv("DATAFLOW_SECRET", raising=False)
    # Round 8: keys are per-workspace and auto-provisioned; "no key" now means
    # the key store cannot answer, and the message names no infrastructure.
    from services import pii_hash_keys

    def _unavailable(_workspace_id):
        raise pii_hash_keys.PiiHashKeyUnavailable("store down")

    monkeypatch.setattr(pii_hash_keys, "workspace_key", _unavailable)
    value, err = apply_transform("ada@example.com", "hash_pii")
    assert value is None
    assert err and "hashing key" in err and "PII_HASH_KEY" not in err
    assert "no row was hashed" in err
