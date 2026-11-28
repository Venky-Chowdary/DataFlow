"""Map grades an Oracle zone pair on the destination's session, as Validate and Execute do.

oracle-live (run 46103213250): PostgreSQL TIMESTAMPTZ into a new Oracle table
was mapped with no destination session bound, so the pair read as lossy at
0.70. Validate then judged the session (it read back UTC) and re-graded the
stamp, but the 0.70 stayed under the 0.75 Map floor: every first run was
blocked by g4_mapping_confidence (test_family_b_run2_own_table_e2e[oracle-*],
test_r11_engine_family_alias_e2e[amazon_rds_oracle]). Pilot's plan and
Studio's /transfer/map now bind the probed pin around Map; a session that is
not confirmed UTC keeps the pair lossy, as before.
"""

from __future__ import annotations

from typing import Any

import pytest

import services.oracle_session_scope as scope

_UTC = (True, "session time zone read back as +00:00", "UTC")
_NOT_UTC = (False, "session time zone read back as -03:30, not UTC", "-03:30")


# --------------------------------------------------------------------------- #
# Studio: /transfer/map
# --------------------------------------------------------------------------- #


def _map_body() -> dict[str, Any]:
    return {
        "source_columns": ["id", "ts"],
        "source_schema": {"id": "BIGINT", "ts": "TIMESTAMPTZ"},
        "source_samples": {"id": ["1", "2"], "ts": ["2026-11-24T10:00:00+00:00", "2026-11-24T11:00:00+00:00"]},
        "target_columns": [],
        "destination_db_type": "oracle",
        "source_db_type": "postgresql",
        "source_kind": "database",
        "destination_table_exists": False,
        "sync_mode": "full_refresh_overwrite",
        "validation_mode": "strict",
        "use_llm": False,
        "destination": {
            "kind": "database", "format": "oracle", "host": "127.0.0.1", "port": 1521,
            "database": "FREEPDB1", "schema": "DATAFLOW", "table": "T_NEW",
            "username": "dataflow", "password": "x",
        },
    }


def _ts(mappings: list[dict]) -> dict:
    return next(m for m in mappings if m.get("source") == "ts")


@pytest.fixture()
def studio(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient

    import services.config  # noqa: F401 - settings parse before the harness blanks env

    from tests import r4_route_harness as h

    h.isolate(tmp_path, monkeypatch)
    from src.main import app

    with TestClient(app) as client:
        yield client


def test_studio_map_on_a_utc_session_maps_the_instant_above_the_floor(studio, monkeypatch) -> None:
    seen: list[Any] = []
    monkeypatch.setattr(scope, "probe_oracle_session", lambda cfg: (seen.append(cfg), _UTC)[1])
    res = studio.post("/api/v1/transfer/map", json=_map_body())
    assert res.status_code == 200, res.text
    ts = _ts(res.json()["mappings"])
    assert seen and seen[0].get("host") == "127.0.0.1"
    assert ts["fidelity"] == "preserve", ts
    assert ts["confidence"] >= 0.75, ts


def test_studio_map_on_a_session_that_is_not_utc_keeps_the_pair_lossy(studio, monkeypatch) -> None:
    monkeypatch.setattr(scope, "probe_oracle_session", lambda cfg: _NOT_UTC)
    res = studio.post("/api/v1/transfer/map", json=_map_body())
    assert res.status_code == 200, res.text
    ts = _ts(res.json()["mappings"])
    assert ts["fidelity"] == "lossy_cast", ts
    assert ts["confidence"] < 0.75, ts


# --------------------------------------------------------------------------- #
# Pilot: plan_transfer (MCP plan_transfer / start_transfer)
# --------------------------------------------------------------------------- #


class _StopAfterMap(Exception):
    pass


def _plan_pin(monkeypatch, probe: tuple[bool, str, str]) -> tuple[Any, list[Any]]:
    import services.mapping_pipeline as mp
    import src.ai.copilot.transfer_tools as tt

    probed: list[Any] = []
    monkeypatch.setattr(scope, "probe_oracle_session", lambda cfg: (probed.append(cfg), probe)[1])
    pins: list[Any] = []

    def _spy(*args: Any, **kwargs: Any) -> dict:
        pins.append(scope._PIN.get())
        raise _StopAfterMap

    def _connector(cid, name, tool):
        role = "source" if "PG" in (name or "") else "destination"
        kind = "postgresql" if role == "source" else "oracle"
        return {"id": f"{role}-1", "name": name, "type": kind}, None

    ora_cfg = {"type": "oracle", "host": "127.0.0.1", "port": 1521, "database": "FREEPDB1"}

    def _introspect(conn, table, purpose="", execute_shape=False):
        if purpose == "source":
            return {"ok": True, "error": "", "db_type": "postgresql", "config": {"type": "postgresql"},
                    "columns": [{"name": "id", "inferred_type": "BIGINT"},
                                {"name": "ts", "inferred_type": "TIMESTAMPTZ"}],
                    "table_exists": True, "raw": {}}
        # A table this run creates reads back not-found, with its connection.
        return {"ok": False, "error": "table not found", "db_type": "oracle", "config": ora_cfg,
                "columns": [], "table_exists": False, "raw": {}}

    monkeypatch.setattr(tt, "_safe_connector", _connector)
    monkeypatch.setattr(tt, "_introspect", _introspect)
    monkeypatch.setattr(tt, "_sample_rows", lambda conn, table: [{"id": 1, "ts": "2026-11-24T10:00:00+00:00"}])
    monkeypatch.setattr(mp, "run_mapping_pipeline", _spy)
    with pytest.raises(_StopAfterMap):
        tt.plan_transfer(
            source_connector_name="QA PG", source_table="t",
            dest_connector_name="QA Oracle", dest_table="T_NEW",
            sync_mode="full_refresh_overwrite",
        )
    return pins, probed


def test_pilot_plan_maps_on_the_destination_session_it_probed(monkeypatch) -> None:
    pins, probed = _plan_pin(monkeypatch, _UTC)
    assert probed and probed[0].get("host") == "127.0.0.1"
    assert pins == [(True, _UTC[1])]


def test_pilot_plan_on_a_session_that_is_not_utc_maps_unconfirmed(monkeypatch) -> None:
    pins, _ = _plan_pin(monkeypatch, _NOT_UTC)
    assert pins == [(False, _NOT_UTC[1])]
