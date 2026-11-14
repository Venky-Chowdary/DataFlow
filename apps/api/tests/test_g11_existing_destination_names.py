"""G11 compatibility: existing destinations keep the names earlier runs gave them.

G11 stopped cutting SQL names at 63 on engines that allow more and stopped
lowercasing BigQuery. A table an earlier run created still holds the old
spelling:

* SQL / BigQuery, existing table: a target whose new spelling is not a column
  but whose legacy spelling (cut at 63, lowercased on BigQuery) is exactly one
  column keeps writing to it; Validate shows an info note
  (``g23d_existing_target_columns``) and does not refuse the name; Execute
  retargets the mapping before any read or write, so Gate-8 reads that column.
  A new table gets the full name; an over-limit name with no matching column
  is still refused.
"""

from __future__ import annotations

import socket
import sqlite3
import sys
import uuid
from pathlib import Path
from unittest.mock import patch

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

LONG_64 = "customer_lifetime_value_after_discounts_and_refunds_in_eur_cents"
CUT_63 = LONG_64[:63]
assert len(LONG_64) == 64


def _maps(*targets: str) -> list[dict]:
    return [{"source": f"c{i}", "target": t, "confidence": 1.0} for i, t in enumerate(targets)]


def _gates(dest_type: str, targets, **kw) -> dict[str, dict]:
    from services.preflight_transfer_policy import run_transfer_policy_gates

    gates = run_transfer_policy_gates(
        sync_mode="full_refresh_append", dest_type=dest_type, source_type="postgresql",
        source_kind="database", mappings=_maps(*targets),
        destination_table=kw.pop("destination_table", "t"), **kw,
    )
    return {g["id"]: g for g in gates if str(g.get("id", "")).startswith("g23")}


# -- SQL: existing legacy columns ----------------------------------------------


def test_validate_existing_mysql_cut_column_is_used_with_an_info_note():
    gates = _gates("mysql", ["id", LONG_64], destination_columns=["id", CUT_63])
    assert "g23c_target_names" not in gates, gates
    note = gates["g23d_existing_target_columns"]
    assert note["status"] == "pass" and note["severity"] == "info"
    assert f"uses the existing column {CUT_63!r} created by an earlier run" in note["message"]
    assert note["details"]["notes"][0] == {
        "code": "target_uses_existing_column", "target": LONG_64, "column": CUT_63,
        "message": note["details"]["notes"][0]["message"],
    }


def test_validate_new_mysql_table_keeps_the_full_name_silently():
    assert _gates("mysql", ["id", LONG_64]) == {}
    assert _gates("mysql", ["id", LONG_64], destination_columns=["id", LONG_64]) == {}


def test_over_limit_name_is_refused_only_without_a_matching_column():
    name = "p" * 70
    assert "g23c_target_names" in _gates("postgresql", [name])
    gates = _gates("postgresql", [name], destination_columns=["p" * 63])
    assert "g23c_target_names" not in gates
    assert gates["g23d_existing_target_columns"]["details"]["notes"][0]["column"] == "p" * 63


def test_two_targets_onto_one_legacy_column_are_refused():
    a, b = CUT_63 + "a", CUT_63 + "b"
    gates = _gates("mysql", [a, b], destination_columns=[CUT_63])
    issues = gates["g23c_target_names"]["details"]["issues"]
    assert [i["code"] for i in issues] == ["target_name_collision"], issues


def test_ambiguous_legacy_match_is_not_guessed():
    from services.target_name_legacy import existing_column_for

    # SQL Server compares names case-insensitively: two columns, no guess.
    assert existing_column_for("sqlserver", "x" * 70, ["x" * 63, "X" * 63]) is None
    assert existing_column_for("sqlserver", "x" * 70, ["x" * 63]) == "x" * 63


def test_bigquery_existing_lowercase_column_matches_mixed_case_target():
    gates = _gates("bigquery", ["CustomerID", "CustomerName"],
                   destination_columns=["customerid", "customername"])
    assert "g23c_target_names" not in gates
    columns = [n["column"] for n in gates["g23d_existing_target_columns"]["details"]["notes"]]
    assert columns == ["customerid", "customername"]
    # A new BigQuery table keeps the case.
    assert _gates("bigquery", ["CustomerID", "CustomerName"]) == {}


# -- Execute: the mapping is retargeted before anything reads or writes --------


def test_execute_retargets_onto_the_existing_column():
    from services.source_filter_gate import request_refusal
    from src.transfer.models import EndpointConfig, TransferRequest

    request = TransferRequest(
        mappings=_maps("id", "p" * 70),
        source=EndpointConfig(kind="database", format="sqlite", database="/nonexistent.db", table="t"),
        destination=EndpointConfig(kind="database", format="postgresql", database="d", table="t"),
        sync_mode="full_refresh_append",
        skip_preflight=True,
    )
    info = {"connected": True, "table_exists": True, "schema": {"id": "INTEGER", "p" * 63: "TEXT"}}
    with patch("src.transfer.endpoint_intelligence.introspect_endpoint", return_value=info):
        assert request_refusal(request) == ""
    assert [m["target"] for m in request.mappings] == ["id", "p" * 63]


def test_execute_still_refuses_an_over_limit_name_on_a_new_table():
    from services.source_filter_gate import request_refusal
    from src.transfer.models import EndpointConfig, TransferRequest

    request = TransferRequest(
        mappings=_maps("id", "p" * 70),
        source=EndpointConfig(kind="database", format="sqlite", database="/nonexistent.db", table="t"),
        destination=EndpointConfig(kind="database", format="postgresql", database="d", table="t"),
        sync_mode="full_refresh_append",
        skip_preflight=True,
    )
    info = {"connected": True, "table_exists": False, "schema": {}}
    with patch("src.transfer.endpoint_intelligence.introspect_endpoint", return_value=info):
        assert "at most 63" in request_refusal(request)


def test_execute_does_not_probe_when_no_spelling_changed():
    from services.source_filter_gate import request_refusal
    from src.transfer.models import EndpointConfig, TransferRequest

    request = TransferRequest(
        mappings=_maps("id", "name"),
        source=EndpointConfig(kind="database", format="sqlite", database="/nonexistent.db", table="t"),
        destination=EndpointConfig(kind="database", format="mysql", database="d", table="t"),
        sync_mode="full_refresh_append",
        skip_preflight=True,
    )
    with patch("src.transfer.endpoint_intelligence.introspect_endpoint",
               side_effect=AssertionError("no probe needed")):
        assert request_refusal(request) == ""


def _mysql_up() -> bool:
    try:
        socket.create_connection(("localhost", 3306), timeout=1).close()
        import pymysql

        pymysql.connect(host="localhost", port=3306, user="dataflow", password="dataflow",
                        database="dataflow").close()
    except Exception:  # noqa: BLE001 - any failure means "not available"
        return False
    return True


def _mysql_columns(table: str) -> list[str]:
    import pymysql

    db = pymysql.connect(host="localhost", port=3306, user="dataflow", password="dataflow",
                         database="dataflow", autocommit=True)
    try:
        with db.cursor() as cur:
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'dataflow' AND table_name = %s ORDER BY ordinal_position",
                (table,),
            )
            return [r[0] for r in cur.fetchall()]
    finally:
        db.close()


@pytest.mark.skipif(not _mysql_up(), reason="no MySQL on :3306")
def test_mysql_existing_cut_column_keeps_receiving_rows(tmp_path: Path):
    import pymysql

    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest
    from tests.typed_fidelity_helpers import drop_mysql_table, mysql_endpoint

    src = tmp_path / "src.sqlite"
    conn = sqlite3.connect(str(src))
    with conn:
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        conn.executemany("INSERT INTO t VALUES (?, ?)", [(1, 10), (2, 20)])
    conn.close()
    table = f"g11e_{uuid.uuid4().hex[:10]}"
    db = pymysql.connect(host="localhost", port=3306, user="dataflow", password="dataflow",
                         database="dataflow", autocommit=True)
    with db.cursor() as cur:  # the table an earlier run created: the 63-character cut
        cur.execute(f"CREATE TABLE `{table}` (id BIGINT, `{CUT_63}` BIGINT)")
        cur.execute(f"INSERT INTO `{table}` VALUES (0, 0)")
    db.close()
    try:
        result = UniversalTransferEngine().execute_tracked(
            TransferRequest(
                source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
                destination=mysql_endpoint(table),
                mappings=[
                    {"source": "id", "target": "id", "confidence": 1.0},
                    {"source": "v", "target": LONG_64, "confidence": 1.0},
                ],
                sync_mode="full_refresh_append",
                skip_preflight=True,
            ),
            "g11e" + uuid.uuid4().hex[:16],
        )
        assert result.success, result.error
        assert (result.reconciliation or {}).get("passed") is True, result.reconciliation
        assert _mysql_columns(table) == ["id", CUT_63]  # no second column
        db = pymysql.connect(host="localhost", port=3306, user="dataflow", password="dataflow",
                             database="dataflow", autocommit=True)
        try:
            with db.cursor() as cur:
                cur.execute(f"SELECT id, `{CUT_63}` FROM `{table}` ORDER BY id")
                rows = cur.fetchall()
        finally:
            db.close()
        assert [tuple(r) for r in rows] == [(0, 0), (1, 10), (2, 20)]
    finally:
        drop_mysql_table(table)


@pytest.fixture
def api(tmp_path, monkeypatch):
    from tests import r4_route_harness as h

    h.isolate(tmp_path, monkeypatch)
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def test_validate_route_notes_the_existing_legacy_column(api):
    """Behaviour, through ``/api/v1/preflight/run`` on live PostgreSQL: the route
    hands the gates the destination's columns, so the existing 63-character
    column is noted (g23d) and neither G6 nor g23c refuses the long target.
    The plan route is covered end to end by ``test_g11_legacy_column_approval``."""
    from tests import r4_route_harness as h

    cfg = h.dest_config("postgresql")
    table = f"g11v_{uuid.uuid4().hex[:8]}"
    long = CUT_63 + "_more"
    h.execute("postgresql", f'CREATE TABLE {table} (id bigint, "{CUT_63}" bigint)')
    try:
        resp = api.post("/api/v1/preflight/run", json={
            "columns": ["id", "v"], "column_types": {"id": "INTEGER", "v": "INTEGER"},
            "row_count": 1, "sample_rows": [{"id": 1, "v": 2}],
            "source_kind": "database", "source_type": "postgresql",
            "dest_kind": "database", "dest_type": "postgresql",
            "dest_host": cfg["host"], "dest_port": cfg["port"],
            "dest_database": cfg["database"], "dest_username": cfg["username"],
            "dest_password": cfg["password"], "dest_schema": "public", "dest_table": table,
            "sync_mode": "full_refresh_append",
            "mappings": [{"source": "id", "target": "id", "confidence": 1.0},
                         {"source": "v", "target": long, "confidence": 1.0}],
        })
        assert resp.status_code == 200, resp.text
        body = resp.json()
        gates = {g.get("id"): g for g in body.get("gates") or []}
        assert CUT_63 in str((gates.get("g23d_existing_target_columns") or {}).get("message"))
        blockers = [b.get("id") or b.get("gate") for b in body.get("blockers") or []]
        assert "g23c_target_names" not in blockers and "g6_target_ddl" not in blockers, blockers
    finally:
        h.drop_table("postgresql", table)


def test_execute_retargets_onto_a_live_existing_column():
    """Execute's rename against live PostgreSQL, nothing patched."""
    from services.source_filter_gate import request_refusal
    from src.transfer.models import EndpointConfig, TransferRequest
    from tests import r4_route_harness as h

    cfg = h.dest_config("postgresql")
    table = f"g11e_{uuid.uuid4().hex[:8]}"
    long = CUT_63 + "_more"
    h.execute("postgresql", f'CREATE TABLE {table} (id bigint, "{CUT_63}" bigint)')
    try:
        def request(tbl: str) -> TransferRequest:
            return TransferRequest(
                mappings=_maps("id", long),
                source=EndpointConfig(kind="database", format="sqlite", database="/x.db",
                                      table="t"),
                destination=EndpointConfig(
                    kind="database", format="postgresql", host=cfg["host"], port=cfg["port"],
                    database=cfg["database"], username=cfg["username"],
                    password=cfg["password"], schema="public", table=tbl),
                sync_mode="full_refresh_append",
            )

        existing = request(table)
        assert request_refusal(existing) == ""
        assert [m["target"] for m in existing.mappings] == ["id", CUT_63]
        new = request(f"g11new_{uuid.uuid4().hex[:8]}")
        assert "at most 63" in request_refusal(new)
    finally:
        h.drop_table("postgresql", table)