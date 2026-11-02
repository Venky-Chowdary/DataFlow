"""QA round 12 (bug 6 family): ``/preflight/run`` ran every Validate as ``strict``.

The route passed ``validation_mode`` to the policy gates and the run record but
not to ``run_file_preflight``, so the mode-dependent preflight gates always
judged as strict. Execute passes the request's mode to the same function
(``src/transfer/engine.py``), so Validate and Execute disagreed for the same
request: Validate blocked a lenient/balanced run that Execute would let through.

The case: an incremental read whose cursor column holds NULLs on a real
PostgreSQL source (D99). Strict blocks (``g_nullable_cursor``); balanced and
lenient warn. Pinned through ``/api/v1/preflight/run`` for each mode, and
checked against what Execute enforces on the same request.
"""

from __future__ import annotations

import uuid

import pytest

from tests import r4_route_harness as h

pytestmark = pytest.mark.timeout(600)


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


@pytest.fixture
def pair(client):
    from services.connector_store import create_connector

    token = uuid.uuid4().hex[:8]
    table = f"r12vm_{token}"
    h.execute("postgresql",
              f'CREATE TABLE "{table}" (id INT PRIMARY KEY, v INT, note TEXT)')
    h.execute("postgresql",
              f"INSERT INTO \"{table}\" VALUES (1, 10, 'a'), (2, NULL, ''), "  # nosec B608
              "(3, 11, '   '), (4, 12, 'Zoë ✓'), (5, 13, NULL)")
    src = create_connector({"name": f"r12vm-s-{token}", "role": "source", "workspace_id": "",
                            "ssl": False, **h.dest_config("postgresql")})
    dst = create_connector({"name": f"r12vm-d-{token}", "role": "destination",
                            "workspace_id": "", "ssl": False, **h.dest_config("postgresql")})
    dests: list[str] = []
    yield {"src": src, "dst": dst, "table": table, "dests": dests}
    for name in [table, *dests]:
        h.drop_table("postgresql", name)


_COLS = ["id", "v", "note"]
_CONTRACT = [{"selected": True, "cursor_column": "v", "primary_key": "id",
              "sync_mode": "incremental_append"}]


def _validate(client, pair, mode: str, dest: str) -> dict:
    resp = client.post("/api/v1/preflight/run", json={
        "dataset_name": pair["table"], "columns": _COLS,
        "column_types": {"id": "integer", "v": "integer", "note": "text"},
        "mappings": [{"source": c, "target": c, "confidence": 1.0} for c in _COLS],
        "source_connector_id": pair["src"].id, "source_kind": "database",
        "source_type": "postgresql", "source_table": pair["table"],
        "connector_id": pair["dst"].id, "dest_kind": "database", "dest_type": "postgresql",
        "dest_table": dest, "sync_mode": "incremental_append",
        "stream_contracts": _CONTRACT, "validation_mode": mode,
    })
    assert resp.status_code == 200, resp.text[:400]
    return resp.json()


def _execute(client, pair, mode: str, dest: str) -> dict:
    resp = client.post("/api/v1/transfer/execute", json={
        "source": {"kind": "database", "format": "postgresql",
                   "connector_id": pair["src"].id, "table": pair["table"]},
        "destination": {"kind": "database", "format": "postgresql",
                        "connector_id": pair["dst"].id, "table": dest},
        "mappings": [{"source": c, "target": c, "confidence": 1.0} for c in _COLS],
        "sync_mode": "incremental_append", "stream_contracts": _CONTRACT,
        "validation_mode": mode, "async_mode": False, "skip_preflight": False,
    })
    return {"status": resp.status_code, "text": resp.text}


def _gate(result: dict, gate_id: str) -> dict | None:
    return next((g for g in result.get("gates") or [] if g.get("id") == gate_id), None)


@pytest.mark.parametrize("mode, sent, expected", [
    ("strict", "strict", "block"),
    ("balanced", " Balanced", "warn"),
    ("lenient", "LENIENT", "warn"),
])
def test_preflight_route_judges_with_the_requested_mode(client, pair, mode, sent, expected):
    dest = f"{pair['table']}_{mode}"
    pair["dests"].append(dest)
    result = _validate(client, pair, sent, dest)
    gate = _gate(result, "g_nullable_cursor")
    assert gate is not None, [g.get("id") for g in result.get("gates") or []]
    assert gate["details"]["null_count"] == 1, gate
    assert gate["status"] == expected, (mode, gate["status"])
    blocking = any(b.get("id") == "g_nullable_cursor" for b in result.get("blockers") or [])
    assert blocking is (expected == "block"), (mode, result.get("blockers"))
    assert result.get("validation_mode") == mode, result.get("validation_mode")


def test_validate_verdict_matches_execute_for_each_mode(client, pair):
    """Execute's own preflight on the same request reaches Validate's verdict.

    strict: both refuse (NULL cursor rows). balanced: both pass and the rows
    land. lenient: the NULL cursor only warns, and both refuse on the cursor
    semantics contract that lenient still enforces (services/cursor_semantics.py).
    """
    verdicts = {}
    for mode in ("strict", "balanced", "lenient"):
        dest = f"{pair['table']}_x{mode[0]}"
        pair["dests"].append(dest)
        result = _validate(client, pair, mode, dest)
        run = _execute(client, pair, mode, dest)
        validate_ok, execute_ok = bool(result.get("passed")), run["status"] == 200
        assert validate_ok is execute_ok, (
            mode, [b.get("id") for b in result.get("blockers") or []],
            run["status"], run["text"][:400])
        verdicts[mode] = validate_ok
        if execute_ok:
            rows = h.query("postgresql", f'SELECT id, v, note FROM "{dest}" ORDER BY id')  # nosec B608
            assert rows == [(1, 10, "a"), (2, None, ""), (3, 11, "   "), (4, 12, "Zoë ✓"),
                            (5, 13, None)], rows
    assert verdicts == {"strict": False, "balanced": True, "lenient": False}, verdicts
