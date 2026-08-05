"""Wave 92 — Datawrap Pilot moves data, with real gates and an explicit Confirm.

Three properties are non-negotiable and each has a test here:

1. **Nothing is fabricated.** The plan's mapping, type conversions and gate
   results come from ``run_mapping_pipeline`` and ``PREFLIGHT_GATES``. The old
   ``plan_transfer_route`` invented gate IDs; a test pins the emitted IDs to the
   registry so that cannot come back.
2. **Nothing moves without Confirm.** ``start_transfer`` stages an ack and
   returns; the row counts on both sides must be unchanged until
   ``POST /copilot/confirm`` runs, and a replayed ack must not run it twice.
3. **Blocked means blocked.** When preflight blocks, no ack is minted at all,
   and ``skip_preflight`` can never be set from chat.
"""

from __future__ import annotations

import socket
import time
import uuid

import pytest


def _pg_reachable() -> bool:
    try:
        with socket.create_connection(("localhost", 5432), timeout=1.5):
            return True
    except OSError:
        return False


# --------------------------------------------------------------------------
# Intent parsing — no live services needed
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("message", "table", "src", "dst", "mode", "plan_only"),
    [
        ("transfer orders from Local Postgres to Warehouse",
         "orders", "Local Postgres", "Warehouse", "", False),
        ("move the customers table from Local Postgres into Mongo Prod",
         "customers", "Local Postgres", "Mongo Prod", "", False),
        ("copy orders from pg to wh as upsert",
         "orders", "pg", "wh", "incremental_upsert", False),
        ("sync payments from mysql prod to snowflake with overwrite",
         "payments", "mysql prod", "snowflake", "full_refresh_overwrite", False),
        ("plan a transfer of orders from Local Postgres to Warehouse",
         "orders", "Local Postgres", "Warehouse", "", True),
        ("what would happen if I move orders from pg to wh",
         "orders", "pg", "wh", "", True),
        ("migrate the events collection from Mongo Prod to Local Postgres",
         "events", "Mongo Prod", "Local Postgres", "", False),
        ("load orders from pg to wh, is that safe?",
         "orders", "pg", "wh", "", True),
        ("please transfer all orders from Local Postgres to Warehouse",
         "orders", "Local Postgres", "Warehouse", "", False),
        ("transfer orders from Local Postgres to Warehouse now",
         "orders", "Local Postgres", "Warehouse", "", False),
        ("can you move orders from pg to wh?",
         "orders", "pg", "wh", "", False),
        ("transfer orders from Local Postgres to Warehouse please",
         "orders", "Local Postgres", "Warehouse", "", False),
    ],
)
def test_transfer_intent_parses(message, table, src, dst, mode, plan_only):
    from src.ai.copilot.tools import parse_transfer_intent

    got = parse_transfer_intent(message)
    assert got is not None, message
    assert got["source_table"] == table
    assert got["source_connector_name"] == src
    assert got["dest_connector_name"] == dst
    assert got["sync_mode"] == mode
    assert got["plan_only"] is plan_only


@pytest.mark.parametrize(
    "message",
    [
        "how many rows are in the airports table on Local Postgres",
        "show me orders from Local Postgres",
        "list tables on Local Postgres",
        "what connectors do I have",
        "start a transfer",
    ],
)
def test_non_transfer_messages_do_not_parse_as_transfers(message):
    """A question about data must never be read as an instruction to move it."""
    from src.ai.copilot.tools import parse_transfer_intent

    assert parse_transfer_intent(message) is None


def test_reading_a_plan_never_routes_to_the_mutating_tool():
    from src.ai.copilot.tools import infer_tools_from_message, prune_planned_tools

    planned = dict(prune_planned_tools(infer_tools_from_message(
        "what would happen if I move orders from pg to wh"
    )))
    assert "plan_transfer" in planned
    assert "start_transfer" not in planned


def test_transfer_request_outranks_generic_advice_tools():
    from src.ai.copilot.tools import infer_tools_from_message, prune_planned_tools

    names = {n for n, _ in prune_planned_tools(infer_tools_from_message(
        "transfer orders from Local Postgres to Warehouse with cdc"
    ))}
    assert "start_transfer" in names
    # The old stubs used to pile on beside a concrete request.
    assert not names & {"plan_transfer_route", "recommend_sync_mode", "start_transfer_studio"}


@pytest.mark.parametrize(
    ("spoken", "expected"),
    [
        ("", "full_refresh_append"),
        ("overwrite", "full_refresh_overwrite"),
        ("replace the table", "full_refresh_overwrite"),
        ("truncate and load", "full_refresh_overwrite"),
        ("upsert", "incremental_upsert"),
        ("merge on id", "incremental_upsert"),
        ("cdc", "cdc_incremental"),
        ("append", "full_refresh_append"),
        ("incremental_upsert", "incremental_upsert"),
        ("nonsense words", "full_refresh_append"),
    ],
)
def test_sync_mode_normalizes_to_engine_tokens(spoken, expected):
    from src.ai.copilot.transfer_tools import SYNC_MODES, normalize_sync_mode

    got = normalize_sync_mode(spoken)
    assert got == expected
    assert got in SYNC_MODES


def test_overwrite_is_never_the_default():
    """One ambiguous sentence must not be able to truncate a destination."""
    from src.ai.copilot.transfer_tools import normalize_sync_mode

    for phrase in ("", "please", "move it over", "just do it", "now"):
        assert normalize_sync_mode(phrase) != "full_refresh_overwrite"


def test_generic_route_advice_names_only_real_gates():
    """The stub used to invent gate IDs like 'source_contract'."""
    from preflight.gates import PREFLIGHT_GATES

    from src.ai.copilot.tools import DataPilotTools

    res = DataPilotTools().execute("plan_transfer_route", {"source": "csv", "destination": "pg"})
    assert res.success
    assert res.output["generic"] is True
    real = {gid.value if hasattr(gid, "value") else str(gid) for gid, _ in PREFLIGHT_GATES}
    assert set(res.output["required_gates"]) <= real
    assert res.output["required_gates"], "should still name the real gates"


def test_plan_transfer_requires_a_table():
    from src.ai.copilot.tools import DataPilotTools

    res = DataPilotTools().execute("plan_transfer", {"source_connector_name": "pg"})
    assert not res.success
    assert "table" in res.error.lower()


def test_exact_name_type_is_reachable_for_typed_passthrough():
    """``transform="integer"`` on INTEGER→INTEGER is a cast, not a custom transform.

    Without this, every typed column fell into ``semantic_inference`` and
    stamped ``requires_review``, so G4 blocked transfers between identical
    schemas. Shared by Transfer Studio and the pilot.
    """
    from services.mapping_quality import classify_mapping_confidence

    cls = classify_mapping_confidence({
        "source": "id",
        "target": "id",
        "source_type": "INTEGER",
        "target_type": "INTEGER",
        "transform": "integer",
        "confidence": 0.99,
    })
    assert cls["confidence_class"] == "exact_name_type"


def test_g3_does_not_false_block_not_null_when_source_is_also_not_null():
    """A PRIMARY KEY → PRIMARY KEY copy is a valid transfer, not a coercion issue."""
    from services.preflight_service import run_file_preflight

    pf = run_file_preflight(
        columns=["id", "status"],
        column_types={"id": "INTEGER", "status": "TEXT"},
        column_nullability={"id": False, "status": True},
        row_count=2,
        mappings=[
            {"source": "id", "target": "id", "confidence": 0.99, "transform": "integer"},
            {"source": "status", "target": "status", "confidence": 0.99, "transform": "none"},
        ],
        destination_connected=True,
        sample_rows=[{"id": 1, "status": "open"}, {"id": 2, "status": "closed"}],
        sync_mode="full_refresh_append",
        schema_policy="manual_review",
        validation_mode="balanced",
        destination_column_types={"id": "INTEGER", "status": "TEXT"},
        destination_column_nullability={"id": False, "status": True},
        destination_table_exists=True,
        destination_db_type="postgresql",
        confidence_threshold=0.75,
    )
    g3 = next(g for g in pf["gates"] if g["id"] == "g3_schema_contract")
    assert g3["status"] == "pass", g3
    assert not any("NOT NULL" in (b.get("message") or "") for b in (pf.get("blockers") or []))


def test_transfer_tools_are_registered_and_described():
    from src.ai.copilot.tools import TOOL_DEFINITIONS, DataPilotTools

    names = {t["name"] for t in TOOL_DEFINITIONS}
    assert {"plan_transfer", "start_transfer"} <= names
    # Both dispatch (an unknown tool name would come back as a failure).
    for tool in ("plan_transfer", "start_transfer"):
        res = DataPilotTools().execute(tool, {})
        assert "unknown tool" not in (res.error or "").lower(), tool
    start = next(t for t in TOOL_DEFINITIONS if t["name"] == "start_transfer")
    assert "confirm" in start["description"].lower()


# --------------------------------------------------------------------------
# Live proof against Postgres
# --------------------------------------------------------------------------


def _pg_conn():
    import psycopg2

    return psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="dataflow",
        user="dataflow",
        password="dataflow",
    )


@pytest.fixture
def pg_route():
    """A real source table with data and a real empty destination table."""
    from services.connector_store import create_connector, delete_connector

    suffix = uuid.uuid4().hex[:6]
    src_table = f"w92_src_{suffix}"
    dst_table = f"w92_dst_{suffix}"
    conn = _pg_conn()
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(
            f"CREATE TABLE {src_table} ("
            "id INTEGER PRIMARY KEY, status TEXT, amount NUMERIC(12,2), ordered_at DATE)"
        )
        cur.executemany(
            f"INSERT INTO {src_table} (id, status, amount, ordered_at) VALUES (%s,%s,%s,%s)",
            [
                (1, "open", "150.25", "2024-01-05"),
                (2, "open", "20.00", "2024-02-11"),
                (3, "closed", "300.50", "2024-02-14"),
                (4, "closed", "1.00", "2025-03-03"),
                (5, None, "400.75", "2025-03-21"),
            ],
        )
        cur.execute(
            f"CREATE TABLE {dst_table} ("
            "id INTEGER PRIMARY KEY, status TEXT, amount NUMERIC(12,2), ordered_at DATE)"
        )
    name = f"W92PG{suffix}"
    saved = create_connector({
        "name": name,
        "type": "postgresql",
        "host": "localhost",
        "port": 5432,
        "database": "dataflow",
        "username": "dataflow",
        "password": "dataflow",
        "schema": "public",
    })
    try:
        yield {
            "conn": conn,
            "connector_name": name,
            "connector_id": saved.id,
            "src": src_table,
            "dst": dst_table,
        }
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {src_table}")
            cur.execute(f"DROP TABLE IF EXISTS {dst_table}")
        conn.close()
        delete_connector(saved.id)


def _count(conn, table: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        return int(cur.fetchone()[0])


@pytest.mark.skipif(not _pg_reachable(), reason="Postgres not reachable")
def test_live_plan_reports_real_schema_mapping_and_real_gates(pg_route):
    from preflight.gates import PREFLIGHT_GATES

    from src.ai.copilot.tools import DataPilotTools

    res = DataPilotTools().execute("plan_transfer", {
        "source_connector_name": pg_route["connector_name"],
        "source_table": pg_route["src"],
        "dest_connector_name": pg_route["connector_name"],
        "dest_table": pg_route["dst"],
    })
    assert res.success, res.error
    out = res.output

    # Both ends were actually read.
    assert out["source"]["column_count"] == 4
    assert out["destination"]["table_exists"] is True
    assert out["destination"]["column_count"] == 4

    # Identical schemas must map cleanly with nothing dropped.
    assert out["mapped_count"] == 4
    assert out["unmapped_source_columns"] == []
    assert out["lossy_conversions"] == []
    mapped = {m["source"]: m["target"] for m in out["mappings"]}
    for col in ("id", "status", "amount", "ordered_at"):
        assert mapped.get(col) == col, mapped

    # Gates are the real registry (plus whatever the policy layer appends),
    # never the invented IDs the old stub printed.
    pf = out["preflight"]
    real_ids = {gid.value if hasattr(gid, "value") else str(gid) for gid, _ in PREFLIGHT_GATES}
    emitted = {g["id"] for g in pf["gates"]}
    assert real_ids <= emitted, f"missing real gates: {real_ids - emitted}"
    assert not emitted & {"source_contract", "semantic_mapping", "capacity_check"}
    assert pf["total_gates"] >= len(PREFLIGHT_GATES)
    assert pf["run_id"], "preflight run must be persisted so the operator can cite it"
    assert pf["passed"] is True, f"identical schemas must not block: {pf['blockers']}"

    from services.preflight_run_store import get_preflight_run

    assert get_preflight_run(pf["run_id"]), "run_id must resolve in the store"


@pytest.mark.skipif(not _pg_reachable(), reason="Postgres not reachable")
def test_live_plan_is_read_only(pg_route):
    """Planning must never touch either side."""
    from src.ai.copilot.tools import DataPilotTools

    before_src = _count(pg_route["conn"], pg_route["src"])
    before_dst = _count(pg_route["conn"], pg_route["dst"])
    res = DataPilotTools().execute("plan_transfer", {
        "source_connector_name": pg_route["connector_name"],
        "source_table": pg_route["src"],
        "dest_connector_name": pg_route["connector_name"],
        "dest_table": pg_route["dst"],
    })
    assert res.success, res.error
    assert res.output["risk"] == "safe"
    assert _count(pg_route["conn"], pg_route["src"]) == before_src
    assert _count(pg_route["conn"], pg_route["dst"]) == before_dst == 0


@pytest.mark.skipif(not _pg_reachable(), reason="Postgres not reachable")
def test_live_plan_counts_real_rows_not_the_sample(pg_route):
    """G7 capacity sizes batches from volume — the sample size would lie."""
    from src.ai.copilot.transfer_tools import _exact_row_count

    conn = pg_route["conn"]
    assert _exact_row_count(
        {"id": pg_route["connector_id"], "name": pg_route["connector_name"], "type": "postgresql"},
        pg_route["src"],
    ) == _count(conn, pg_route["src"]) == 5


@pytest.mark.skipif(not _pg_reachable(), reason="Postgres not reachable")
def test_live_start_transfer_stages_but_does_not_move_data(pg_route):
    from src.ai.copilot.ack_ledger import get_ack_ledger
    from src.ai.copilot.tools import DataPilotTools

    res = DataPilotTools().execute("start_transfer", {
        "source_connector_name": pg_route["connector_name"],
        "source_table": pg_route["src"],
        "dest_connector_name": pg_route["connector_name"],
        "dest_table": pg_route["dst"],
    })
    assert res.success, res.error
    out = res.output
    assert out["risk"] == "mutate"
    assert out["requires_confirm"] is True
    assert out["ack_id"]

    # Staging is not running: the destination is still empty.
    assert _count(pg_route["conn"], pg_route["dst"]) == 0

    # The gates can't be turned off from chat, and secrets stay server-side.
    payload, err = get_ack_ledger().get_pending_payload(out["ack_id"])
    assert not err
    assert payload["skip_preflight"] is False
    assert payload["preflight_run_id"]
    assert "password" not in str(out["preview"]).lower()


@pytest.mark.skipif(not _pg_reachable(), reason="Postgres not reachable")
def test_live_confirm_actually_moves_the_rows(pg_route):
    """The whole point: Confirm runs a real job and the rows land."""
    import asyncio

    from src.ai.copilot.tools import DataPilotTools
    from src.routers.copilot_router import ConfirmActionRequest, copilot_confirm

    staged = DataPilotTools().execute("start_transfer", {
        "source_connector_name": pg_route["connector_name"],
        "source_table": pg_route["src"],
        "dest_connector_name": pg_route["connector_name"],
        "dest_table": pg_route["dst"],
    })
    assert staged.success, staged.error
    ack_id = staged.output["ack_id"]

    confirmed = asyncio.get_event_loop().run_until_complete(
        copilot_confirm(ConfirmActionRequest(ack_id=ack_id, actor="wave92-test"))
    )
    assert confirmed["ok"] is True
    assert confirmed["kind"] == "start_transfer"
    job_id = confirmed["job_id"]
    assert job_id

    deadline = time.time() + 120
    moved = 0
    while time.time() < deadline:
        moved = _count(pg_route["conn"], pg_route["dst"])
        if moved >= 5:
            break
        time.sleep(1.0)
    assert moved == 5, f"job {job_id} moved {moved}/5 rows"

    # Values, not just row count — a transfer that mangles types is a failure.
    with pg_route["conn"].cursor() as cur:
        cur.execute(f"SELECT id, status, amount, ordered_at FROM {pg_route['dst']} ORDER BY id")
        rows = cur.fetchall()
    with pg_route["conn"].cursor() as cur:
        cur.execute(f"SELECT id, status, amount, ordered_at FROM {pg_route['src']} ORDER BY id")
        expected = cur.fetchall()
    assert rows == expected

    # Replaying the same approval must not run a second job.
    replay = asyncio.get_event_loop().run_until_complete(
        copilot_confirm(ConfirmActionRequest(ack_id=ack_id, actor="wave92-test"))
    )
    assert replay["idempotent"] is True
    assert replay["job_id"] == job_id
    assert _count(pg_route["conn"], pg_route["dst"]) == 5


@pytest.mark.skipif(not _pg_reachable(), reason="Postgres not reachable")
def test_live_missing_source_table_is_refused_with_guidance(pg_route):
    from src.ai.copilot.tools import DataPilotTools

    res = DataPilotTools().execute("start_transfer", {
        "source_connector_name": pg_route["connector_name"],
        "source_table": "table_that_does_not_exist_w92",
        "dest_connector_name": pg_route["connector_name"],
        "dest_table": pg_route["dst"],
    })
    assert not res.success
    assert "list tables" in res.error.lower() or "could not read" in res.error.lower()
    assert _count(pg_route["conn"], pg_route["dst"]) == 0


@pytest.mark.skipif(not _pg_reachable(), reason="Postgres not reachable")
def test_live_same_table_round_trip_is_refused(pg_route):
    from src.ai.copilot.tools import DataPilotTools

    res = DataPilotTools().execute("plan_transfer", {
        "source_connector_name": pg_route["connector_name"],
        "source_table": pg_route["src"],
        "dest_connector_name": pg_route["connector_name"],
        "dest_table": pg_route["src"],
    })
    assert not res.success
    assert "same table" in res.error.lower()


@pytest.mark.skipif(not _pg_reachable(), reason="Postgres not reachable")
def test_live_blocked_preflight_mints_no_approval(pg_route, monkeypatch):
    """If a gate blocks, there must be nothing for the operator to click."""
    import src.ai.copilot.transfer_tools as tt

    blocked = {
        "run_id": "pf_blocked_w92",
        "passed": False,
        "readiness_score": 33.3,
        "passed_count": 3,
        "total_gates": 9,
        "gates": [],
        "blockers": [{"id": "G6_TARGET_DDL", "message": "destination type is narrower"}],
        "warnings": [],
    }
    monkeypatch.setattr(tt, "_run_preflight", lambda **_kw: blocked)

    res = tt.start_transfer(
        source_connector_name=pg_route["connector_name"],
        source_table=pg_route["src"],
        dest_connector_name=pg_route["connector_name"],
        dest_table=pg_route["dst"],
    )
    assert not res.success
    assert "preflight blocked" in res.error.lower()
    assert "G6_TARGET_DDL" in res.error
    assert "pf_blocked_w92" in res.error
    # No ack means the UI has no Confirm button to press.
    assert "ack_id" not in (res.output or {})
    assert _count(pg_route["conn"], pg_route["dst"]) == 0


@pytest.mark.skipif(not _pg_reachable(), reason="Postgres not reachable")
def test_live_blocked_plan_still_shows_the_gate_evidence(pg_route, monkeypatch):
    import src.ai.copilot.transfer_tools as tt
    from src.ai.copilot.pilot_agent import _render_transfer

    monkeypatch.setattr(tt, "_run_preflight", lambda **_kw: {
        "run_id": "pf_blocked_w92b",
        "passed": False,
        "passed_count": 8,
        "total_gates": 9,
        "readiness_score": 88.9,
        "gates": [{"id": "G6_TARGET_DDL", "status": "block", "message": "narrower type"}],
        "blockers": [{"id": "G6_TARGET_DDL", "message": "narrower type"}],
        "warnings": [],
    })
    res = tt.start_transfer(
        source_connector_name=pg_route["connector_name"],
        source_table=pg_route["src"],
        dest_connector_name=pg_route["connector_name"],
        dest_table=pg_route["dst"],
    )
    text = _render_transfer("plan_transfer", res.output or {})
    assert "BLOCK G6_TARGET_DDL" in text
    assert "pf_blocked_w92b" in text


@pytest.mark.skipif(not _pg_reachable(), reason="Postgres not reachable")
def test_live_answer_names_the_route_casts_and_gates(pg_route):
    from src.ai.copilot.pilot_agent import _render_transfer
    from src.ai.copilot.tools import DataPilotTools

    res = DataPilotTools().execute("start_transfer", {
        "source_connector_name": pg_route["connector_name"],
        "source_table": pg_route["src"],
        "dest_connector_name": pg_route["connector_name"],
        "dest_table": pg_route["dst"],
    })
    assert res.success, res.error
    text = _render_transfer("start_transfer", res.output)
    assert pg_route["src"] in text and pg_route["dst"] in text
    assert "full_refresh_append" in text
    assert "Preflight" in text
    assert "nothing moves until you do" in text


@pytest.mark.skipif(not _pg_reachable(), reason="Postgres not reachable")
def test_live_overwrite_answer_warns_before_confirm(pg_route):
    from src.ai.copilot.pilot_agent import _render_transfer
    from src.ai.copilot.tools import DataPilotTools

    res = DataPilotTools().execute("start_transfer", {
        "source_connector_name": pg_route["connector_name"],
        "source_table": pg_route["src"],
        "dest_connector_name": pg_route["connector_name"],
        "dest_table": pg_route["dst"],
        "sync_mode": "overwrite",
    })
    assert res.success, res.error
    assert res.output["destructive"] is True
    assert "overwrites the destination" in _render_transfer("start_transfer", res.output)
