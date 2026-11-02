"""D112 — Validate must surface the destination's own live constraints.

A route that declares no relationships but points at an existing table
carrying real FOREIGN KEYs must not report "referential integrity was not
asked": the destination catalog is probed and the live FKs are returned,
which both feeds G22 and arms the source-population orphan probe.
"""

from __future__ import annotations

import src.transfer.endpoint_intelligence as ei
import services.schema_introspect as si
from services.preflight_destination import inspect_destination_for_preflight

LIVE_FKS = [
    {
        "name": "hq2207_fd_pid_fkey",
        "columns": ["pid"],
        "referenced_schema": "public",
        "referenced_table": "hq2207_par",
        "referenced_columns": ["id"],
    }
]


def _endpoint_info(**over):
    info = {
        "connected": True,
        "objects": [],
        "columns": ["id", "pid"],
        "schema": {"id": "INT4", "pid": "INT4"},
        "schema_nullability": {},
        "schema_defaults": {},
        "row_estimate": 0,
        "auto_create": [],
        "table_exists": True,
        "message": "Connected",
    }
    info.update(over)
    return info


def _call_preflight():
    return inspect_destination_for_preflight(
        dest_type="postgresql",
        dest_host="localhost",
        dest_port=5432,
        dest_database="df",
        dest_table="hq2207_fd",
        dest_schema="public",
        dest_username="u",
        dest_password="p",
    )


def test_dest_live_fks_surfaced_when_route_declares_none(monkeypatch):
    calls = []

    def fake_introspect_schema(db_type, **kwargs):
        calls.append(kwargs)
        assert kwargs.get("table") == "hq2207_fd"
        return {
            "connected": True,
            "foreign_keys": LIVE_FKS,
            "foreign_keys_meta": {"measured": True},
            "check_constraints": [{"name": "hq2207_cd_amt_check", "sqltext": "amt >= 0"}],
        }

    monkeypatch.setattr(
        ei, "introspect_endpoint", lambda *a, **k: _endpoint_info()
    )
    monkeypatch.setattr(si, "introspect_schema", fake_introspect_schema)

    out = _call_preflight()
    assert calls, "destination constraint catalog probe did not run"
    assert out["foreign_keys"] == LIVE_FKS
    assert out["foreign_keys_meta"] == {"measured": True}
    assert out["check_constraints"] == [
        {"name": "hq2207_cd_amt_check", "sqltext": "amt >= 0"}
    ]


def test_dest_check_constraints_from_meta_items(monkeypatch):
    """Real introspect_schema reports CHECKs under
    check_constraints_meta.items (no top-level 'check_constraints' key).
    Reading only the flat key left the preflight CHECK probe starved —
    prod pf_7a5786c90049 showed safe_to_start with no warning although a
    violating source row existed."""
    monkeypatch.setattr(
        ei, "introspect_endpoint", lambda *a, **k: _endpoint_info()
    )
    monkeypatch.setattr(
        si,
        "introspect_schema",
        lambda db_type, **kw: {
            "connected": True,
            "foreign_keys": [],
            "foreign_keys_meta": {"measured": True},
            "check_constraints_meta": {
                "dialect": "postgresql",
                "status": "ok",
                "items": [
                    {
                        "name": "hq1604_cd_amt_check",
                        "predicate": "(amt >= 0)",
                        "columns": ["amt"],
                    }
                ],
            },
        },
    )
    out = _call_preflight()
    assert out["check_constraints"] == [
        {
            "name": "hq1604_cd_amt_check",
            "predicate": "(amt >= 0)",
            "columns": ["amt"],
        }
    ]


def test_dest_probe_failure_never_fails_validate(monkeypatch):
    monkeypatch.setattr(
        ei, "introspect_endpoint", lambda *a, **k: _endpoint_info()
    )

    def boom(*a, **k):
        raise RuntimeError("catalog down")

    monkeypatch.setattr(si, "introspect_schema", boom)
    out = _call_preflight()
    assert out["connected"] is True
    assert out["foreign_keys"] == []
    assert out["foreign_keys_meta"]["measured"] is False


def test_no_extra_probe_when_introspect_returns_fks(monkeypatch):
    monkeypatch.setattr(
        ei,
        "introspect_endpoint",
        lambda *a, **k: _endpoint_info(foreign_keys=LIVE_FKS),
    )

    def _fail(*a, **k):
        raise AssertionError("must not re-probe when FKs already known")

    monkeypatch.setattr(si, "introspect_schema", _fail)
    out = _call_preflight()
    assert out["foreign_keys"] == LIVE_FKS


def test_no_probe_when_table_missing(monkeypatch):
    monkeypatch.setattr(
        ei,
        "introspect_endpoint",
        lambda *a, **k: _endpoint_info(table_exists=False, columns=[]),
    )

    def _fail(*a, **k):
        raise AssertionError("must not probe a table that does not exist")

    monkeypatch.setattr(si, "introspect_schema", _fail)
    out = _call_preflight()
    assert out["table_exists"] is False
    assert out["foreign_keys"] == []
