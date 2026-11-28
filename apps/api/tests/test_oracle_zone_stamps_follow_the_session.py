"""Map's fidelity stamp on an Oracle zone pair follows the run's session at Validate.

Map stamps a mapping's verdict with no destination session bound, so
TIMESTAMPTZ into an Oracle TIMESTAMP WITH [LOCAL] TIME ZONE column always came
out ``lossy_cast`` with ``requires_risk_contract``. G4 trusts the stamp, so a
route whose Oracle session read back UTC was refused for a Risk Contract the
grade no longer asks for (oracle-live, first run: family_b run 1 into Oracle,
r11 amazon_rds_oracle). Validate (and Execute, which binds the pin itself)
re-judge those stamps on the session: confirmed UTC needs no contract,
anything else still does.
"""

from __future__ import annotations

from typing import Any

import pytest

import services.oracle_session_scope as scope
from services.mapping_proof import mapping_fidelity
from services.oracle_session_scope import bind_oracle_utc_pin
from services.preflight_service import run_file_preflight

_TS = {"source": "ts", "target": "ts", "confidence": 0.95, "source_type": "TIMESTAMPTZ",
       "target_type": "TIMESTAMP WITH LOCAL TIME ZONE"}


def _map_stamped() -> dict[str, Any]:
    """The ts mapping as Map leaves it: judged with no Oracle session bound."""
    v = mapping_fidelity(dict(_TS), destination_db_type="oracle", dest_table_exists=False)
    assert v["verdict"] == "lossy_cast" and v["requires_risk_contract"] is True
    return {**_TS, "fidelity": v["verdict"], "fidelity_reason": v["reason"],
            "requires_risk_contract": v["requires_risk_contract"]}


def _preflight(stamped: dict[str, Any] | None = None) -> dict[str, Any]:
    stamped = stamped or _map_stamped()
    return run_file_preflight(
        columns=["id", "ts"], column_types={"id": "BIGINT", "ts": "TIMESTAMPTZ"}, row_count=2,
        mappings=[{"source": "id", "target": "id", "confidence": 0.95, "source_type": "BIGINT",
                   "target_type": "NUMBER(19)"}, stamped],
        sample_rows=[{"id": 1, "ts": "2026-01-01T00:00:00+00:00"}, {"id": 2, "ts": "2026-01-02T00:00:00+00:00"}],
        destination_connected=True, destination_config={"host": "ora", "type": "oracle"},
        destination_db_type="oracle", destination_table="T", destination_table_exists=False,
        destination_can_create=True, source_kind="database", source_format="postgresql",
        sync_mode="full_refresh_overwrite", validation_mode="strict",
    )


def _risk_blocks(result: dict[str, Any]) -> list[str]:
    return [str(g.get("message")) for g in result.get("gates") or []
            if g.get("status") in ("block", "fail") and "Risk Contract" in str(g.get("message"))]


@pytest.fixture(autouse=True)
def _no_advisory(monkeypatch):
    monkeypatch.setattr(scope, "_jobs_in_window", lambda store=None: [])


def test_validate_on_a_session_that_read_back_utc_needs_no_contract(monkeypatch) -> None:
    monkeypatch.setattr(scope, "probe_oracle_session", lambda cfg: (True, "session time zone read back as +00:00", "UTC"))
    result = _preflight()
    assert _risk_blocks(result) == [], result.get("blockers")
    assert not result.get("blockers"), result.get("blockers")


def test_validate_on_a_session_that_is_not_utc_still_needs_the_contract(monkeypatch) -> None:
    monkeypatch.setattr(scope, "probe_oracle_session",
                        lambda cfg: (False, "session time zone read back as -03:30, not UTC", "-03:30"))
    assert _risk_blocks(_preflight())


def test_execute_judges_the_stamp_on_the_pin_it_bound() -> None:
    stamped = _map_stamped()  # Map ran before the run bound its pin
    with bind_oracle_utc_pin(True, "session time zone read back as +00:00"):
        assert _risk_blocks(_preflight(stamped)) == []
    with bind_oracle_utc_pin(False, "ALTER SESSION SET TIME_ZONE was refused (DatabaseError)"):
        assert _risk_blocks(_preflight(stamped))
