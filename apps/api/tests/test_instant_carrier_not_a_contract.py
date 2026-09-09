"""An instant landing in an instant carrier is not a Risk Contract.

Two routes were refusing to move without a signed continue-policy contract for a
loss the destination does not have:

* PostgreSQL ``TIMESTAMPTZ`` → **create-new** MySQL. The mapper stamps
  ``TIMESTAMP(6)`` (MySQL's own instant carrier), but the create-new risk stamp
  then re-invented that stamp as if it were a *source* token with no dialect,
  and a dialect-less ``TIMESTAMP(6)`` invents to ``DATETIME(6)`` — a wall-clock
  column, so the route graded itself as a fidelity collapse.
* PostgreSQL ``TIMESTAMPTZ`` → Redis. Redis has one carrier, text, and its JSON
  wire writes RFC 3339 *with the offset*, so the text is the instant.

The collapses these rules exist for must still fire: an explicit MySQL
``DATETIME(6)`` target drops the instant, and TEXT on a typed engine is still an
open-text collapse.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.create_new_risk_stamp import apply_create_new_risk_stamps  # noqa: E402
from services.type_system import (  # noqa: E402
    is_lossy_coercion,
    is_precision_collapse_coercion,
    is_dest_instant_carrier_spelling,
    keyspace_instant_text_wire_preserved,
    reinvent_would_drop_dest_instant_carrier,
    timezone_aware_would_collapse_to_string,
)
from services.value_serializer import json_default, sanitize_json_value  # noqa: E402


def test_reinvent_guard_protects_only_the_destination_s_own_spelling() -> None:
    # MySQL TIMESTAMP(6) is MySQL's instant carrier; re-invent reads it as a
    # dialect-less source and returns the wall-clock DATETIME(6).
    assert is_dest_instant_carrier_spelling("TIMESTAMP(6)", dest_db="mysql") is True
    assert (
        reinvent_would_drop_dest_instant_carrier(
            "TIMESTAMP(6)", "DATETIME(6)", dest_db="mysql"
        )
        is True
    )
    # Same polarity on both sides, or a non-temporal stamp, is not a drop.
    assert (
        reinvent_would_drop_dest_instant_carrier(
            "TIMESTAMP(6)", "TIMESTAMP(6)", dest_db="mysql"
        )
        is False
    )
    assert (
        reinvent_would_drop_dest_instant_carrier("VARCHAR(64)", "TEXT", dest_db="mysql")
        is False
    )
    # A *source* spelling on the way into SQL Server must still be re-invented
    # into DATETIMEOFFSET — that stamp is the pipeline doing its job.
    assert is_dest_instant_carrier_spelling("TIMESTAMPTZ", dest_db="mssql") is False
    assert (
        reinvent_would_drop_dest_instant_carrier(
            "TIMESTAMPTZ", "DATETIMEOFFSET", dest_db="mssql"
        )
        is False
    )


def test_create_new_mysql_keeps_the_instant_carrier() -> None:
    mappings = [
        {
            "source": "ts_utc",
            "target": "ts_utc",
            "source_type": "TIMESTAMPTZ",
            "target_type": "TIMESTAMP(6)",
            "create_new": True,
        }
    ]
    stamped = apply_create_new_risk_stamps(
        mappings,
        "mysql",
        dest_table_exists=False,
    )
    assert stamped[0]["target_type"].upper().startswith("TIMESTAMP(6)"), stamped[0]


def test_mysql_catalog_timestamptz_is_the_dest_instant_spelling() -> None:
    """Live ``timestamp(6)`` introspects as ``TIMESTAMPTZ(6)`` — still an instant."""
    assert is_dest_instant_carrier_spelling("TIMESTAMPTZ(6)", dest_db="mysql") is True
    assert is_dest_instant_carrier_spelling("TIMESTAMPTZ", dest_db="mariadb") is True
    assert is_dest_instant_carrier_spelling("TIMESTAMPTZ", dest_db="mssql") is False
    assert (
        reinvent_would_drop_dest_instant_carrier(
            "TIMESTAMPTZ(6)", "DATETIME(6)", dest_db="mysql"
        )
        is True
    )


def test_create_new_mysql_legalizes_catalog_timestamptz_to_timestamp() -> None:
    stamped = apply_create_new_risk_stamps(
        [
            {
                "source": "ts_utc",
                "target": "ts_utc",
                "source_type": "TIMESTAMPTZ",
                "target_type": "TIMESTAMPTZ(6)",
                "create_new": True,
            }
        ],
        "mysql",
        dest_table_exists=False,
    )
    assert stamped[0]["target_type"].upper().startswith("TIMESTAMP(6)"), stamped[0]
    assert not stamped[0].get("requires_risk_contract")


def test_dialect_less_timestamp_is_still_a_wall_clock() -> None:
    """Bare TIMESTAMP without dest_db stays NTZ — do not invent MySQL semantics."""
    assert is_lossy_coercion("TIMESTAMPTZ", "TIMESTAMP(6)") is True


def test_explicit_wall_clock_target_is_still_a_collapse() -> None:
    assert is_lossy_coercion("TIMESTAMPTZ", "DATETIME(6)", dest_db="mysql") is True


def test_redis_json_wire_writes_the_offset() -> None:
    """The exemption is about the wire — prove the wire before trusting it."""
    aware = datetime(
        2024, 12, 31, 23, 59, 59, 123456, tzinfo=timezone(timedelta(hours=5, minutes=30))
    )
    wire = json.dumps(sanitize_json_value({"ts": aware}), default=json_default)
    round_tripped = datetime.fromisoformat(json.loads(wire)["ts"])
    assert round_tripped.utcoffset() == aware.utcoffset()
    assert round_tripped == aware


def test_timestamptz_into_redis_text_needs_no_contract() -> None:
    for target in ("string", "TEXT", "VARCHAR"):
        assert (
            keyspace_instant_text_wire_preserved(
                "TIMESTAMPTZ", target, dest_db="redis"
            )
            is True
        )
        assert is_lossy_coercion("TIMESTAMPTZ", target, dest_db="redis") is False
        assert (
            is_precision_collapse_coercion("TIMESTAMPTZ", target, dest_db="redis")
            is False
        )
    # TIMETZ carries an offset too, and the same text wire keeps it.
    assert is_lossy_coercion("TIMETZ", "string", dest_db="redis") is False


def test_open_text_on_a_typed_engine_is_still_a_collapse() -> None:
    assert timezone_aware_would_collapse_to_string("TIMESTAMPTZ", "TEXT") is True
    assert (
        timezone_aware_would_collapse_to_string("TIMESTAMPTZ", "TEXT", dest_db="mysql")
        is True
    )
    assert is_lossy_coercion("TIMESTAMPTZ", "TEXT", dest_db="postgresql") is True
    # A typed carrier on the keyspace engine is not the text wire this exempts.
    assert (
        keyspace_instant_text_wire_preserved(
            "TIMESTAMPTZ", "DATETIME(6)", dest_db="redis"
        )
        is False
    )


def test_overwrite_create_new_mysql_stamps_timestamp_without_a_contract() -> None:
    from types import SimpleNamespace

    from services.mapping_proof import mapping_fidelity
    from services.overwrite_typed_mapping import build_overwrite_create_new_mappings
    from services.type_coercion_validator import validate_mapping_coercions

    request = SimpleNamespace(
        source=SimpleNamespace(format="postgresql", kind="database"),
        destination=SimpleNamespace(format="mysql"),
        validation_mode="strict",
        schema_policy="manual_review",
    )
    mappings = build_overwrite_create_new_mappings(
        columns=["ts_utc"],
        schema={"ts_utc": "TIMESTAMPTZ"},
        sample_rows=[{"ts_utc": "2024-12-31T23:59:59+00:00"}],
        request=request,
    )
    row = next(m for m in mappings if m.get("source") == "ts_utc")
    assert str(row.get("target_type") or "").upper().startswith("TIMESTAMP(6)"), row
    assert not row.get("requires_risk_contract")
    issues = validate_mapping_coercions(
        mappings,
        source_types={"ts_utc": "TIMESTAMPTZ"},
        target_types={"ts_utc": str(row["target_type"])},
        dest_db_type="mysql",
        dest_table_exists=False,
        validation_mode="strict",
    )
    blockers = [
        i
        for i in issues
        if i.get("severity") == "block" and i.get("source") == "ts_utc"
    ]
    assert blockers == [], blockers
    verdict = mapping_fidelity(
        row, destination_db_type="mysql", dest_table_exists=False
    )
    assert verdict["requires_risk_contract"] is False
    assert verdict["verdict"] != "lossy_cast"


def test_overwrite_create_new_redis_stamps_text_without_a_contract() -> None:
    from types import SimpleNamespace

    from services.mapping_proof import mapping_fidelity, _mapping_risks
    from services.overwrite_typed_mapping import build_overwrite_create_new_mappings
    from services.type_coercion_validator import validate_mapping_coercions
    from services.timezone_policy import POLICY_OFFSET_TEXT, resolve_timezone_policy

    request = SimpleNamespace(
        source=SimpleNamespace(format="postgresql", kind="database"),
        destination=SimpleNamespace(format="redis"),
        validation_mode="strict",
        schema_policy="manual_review",
    )
    mappings = build_overwrite_create_new_mappings(
        columns=["ts_utc"],
        schema={"ts_utc": "TIMESTAMPTZ"},
        sample_rows=[{"ts_utc": "2024-12-31T23:59:59+05:30"}],
        request=request,
    )
    row = next(m for m in mappings if m.get("source") == "ts_utc")
    tgt = str(row.get("target_type") or "")
    assert not row.get("requires_risk_contract"), row
    policy = resolve_timezone_policy("TIMESTAMPTZ", tgt, dest_db="redis")
    assert policy is not None
    assert policy.policy == POLICY_OFFSET_TEXT
    assert policy.requires_contract is False
    issues = validate_mapping_coercions(
        mappings,
        source_types={"ts_utc": "TIMESTAMPTZ"},
        target_types={"ts_utc": tgt},
        dest_db_type="redis",
        dest_table_exists=False,
        validation_mode="strict",
    )
    blockers = [
        i
        for i in issues
        if i.get("severity") == "block" and i.get("source") == "ts_utc"
    ]
    assert blockers == [], blockers
    verdict = mapping_fidelity(
        row, destination_db_type="redis", dest_table_exists=False
    )
    assert verdict["requires_risk_contract"] is False
    risks = _mapping_risks(
        {
            "source": "ts_utc",
            "target": "ts_utc",
            "source_type": "TIMESTAMPTZ",
            "target_type": tgt or "string",
            "transform": "none",
        },
        dest_mode="create_new",
        destination_db_type="redis",
    )
    tz_chips = [r for r in risks if r.get("code") == "timezone_policy"]
    assert tz_chips, risks
    assert tz_chips[0]["severity"] == "info"
    assert tz_chips[0]["timezone_policy"]["requires_contract"] is False


def test_mysql_instant_carrier_does_not_quarantine_offset_wires() -> None:
    from connectors.writer_common import quarantine_unfit_temporals

    aware = datetime(
        2024, 12, 31, 23, 59, 59, tzinfo=timezone(timedelta(hours=5, minutes=30))
    )
    for dest_type in ("TIMESTAMP(6)", "timestamp(6)", "TIMESTAMPTZ(6)"):
        details: list = []
        out = quarantine_unfit_temporals(
            [(aware,)],
            ["ts_utc"],
            [dest_type],
            details,
            "quarantine",
            dest_db="mysql",
        )
        assert details == [], (dest_type, details)
        assert len(out) == 1

    wall: list = []
    quarantine_unfit_temporals(
        [(aware,)],
        ["ts_utc"],
        ["DATETIME(6)"],
        wall,
        "quarantine",
        dest_db="mysql",
    )
    assert wall, "DATETIME(6) must still hold out offset-bearing wires"
