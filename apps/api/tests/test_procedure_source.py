"""Stored-procedure / custom-SQL extract — named fixture, not marketing."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from services.preflight_service import run_transfer_policy_gates
from services.procedure_source import (
    ProcedureSourceError,
    assert_callable_sync_allowed,
    compile_callable_sql,
    is_callable_source,
    map_callable_result,
    parse_callable_source,
    source_read_mode_of,
)


FIXTURE = Path(__file__).resolve().parent / "fixtures" / "procedure_source_matrix.json"
PROOF_DIR = Path(__file__).resolve().parents[1] / "data" / "proofs"


def _load() -> dict:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def test_parse_accept_matrix() -> None:
    data = _load()
    rows = []
    ok = 0
    for case in data["parse_accept"]:
        spec = parse_callable_source(
            case["text"],
            dialect=case["dialect"],
            mode=case["mode"],
            params=case.get("params") or {},
        )
        sql, binds = compile_callable_sql(spec)
        passed = bool(spec.identifier) and bool(sql)
        if case["id"] == "call_bound":
            passed = passed and "since" in binds and ":since" in sql
        if case["id"] == "bare_pg":
            passed = passed and sql.upper().startswith("SELECT")
        if case["id"] == "bare_mssql":
            passed = passed and sql.upper().startswith("EXEC")
        ok += int(passed)
        rows.append({"id": case["id"], "ok": passed, "sql": sql, "ident": spec.identifier})
    assert ok == len(data["parse_accept"]), rows


def test_parse_reject_matrix() -> None:
    data = _load()
    refused = 0
    for case in data["parse_reject"]:
        with pytest.raises(ProcedureSourceError):
            parse_callable_source(
                case["text"],
                dialect=case["dialect"],
                mode=case["mode"],
                params=case.get("params") or {},
            )
        refused += 1
    assert refused == len(data["parse_reject"])


def test_map_callable_result_matrix() -> None:
    data = _load()
    rows = []
    correct = 0
    for case in data["map_cases"]:
        result = map_callable_result(
            case["source"],
            case["dest"],
            source_types=case.get("source_types"),
            dest_types=case.get("dest_types"),
        )
        contract = result["shape_contract"]
        mappings = result["mappings"]
        passed = True
        if "expect_shape" in case:
            passed = passed and contract["shape"] == case["expect_shape"]
        if "expect_bound" in case:
            bound = {
                str(m.get("source"))
                for m in mappings
                if m.get("target") in case["expect_bound"]
                and not m.get("create_new")
            }
            passed = passed and set(case["expect_bound"]).issubset(bound | set(case["dest"]))
            for name in case["expect_bound"]:
                hit = next((m for m in mappings if m.get("source") == name), None)
                passed = passed and hit is not None and str(hit.get("target") or "") == name
        if "expect_extra_min" in case:
            extra = result["extra_source_columns"]
            unaccounted = contract.get("unaccounted_sources") or []
            add_n = int((contract.get("counts") or {}).get("add_proposed") or 0)
            passed = passed and (len(extra) + len(unaccounted) + add_n) >= case["expect_extra_min"]
        if "expect_review_kinds" in case:
            kinds = {str(m.get("review_kind") or "") for m in mappings}
            passed = passed and set(case["expect_review_kinds"]).issubset(kinds)
        correct += int(passed)
        rows.append(
            {
                "id": case["id"],
                "ok": passed,
                "shape": contract.get("shape"),
                "extra": result.get("extra_source_columns"),
                "review": [m.get("review_kind") for m in mappings],
            }
        )
    PROOF_DIR.mkdir(parents=True, exist_ok=True)
    (PROOF_DIR / "procedure_source_map.json").write_text(
        json.dumps({"correct": correct, "total": len(data["map_cases"]), "rows": rows}, indent=2),
        encoding="utf-8",
    )
    assert correct == len(data["map_cases"]), rows


def test_cdc_refused_on_procedure_source() -> None:
    src = {"source_read_mode": "procedure", "procedure_call": "CALL get_orders()"}
    assert is_callable_source(src)
    with pytest.raises(ProcedureSourceError):
        assert_callable_sync_allowed("cdc", src)
    gates = run_transfer_policy_gates(
        sync_mode="cdc",
        source_kind="database",
        source_type="postgresql",
        source_read_mode="procedure",
        stream_contracts=[
            {
                "name": "get_orders",
                "selected": True,
                "cursor_field": "updated_at",
                "primary_key": "order_id",
            }
        ],
    )
    blockers = [g for g in gates if g["status"] == "block" and g["id"] == "g9_sync_contract"]
    assert blockers
    assert "snapshot" in str(blockers[0]["details"]).lower() or "CALL" in str(blockers[0]["details"])


def test_source_read_mode_from_extra() -> None:
    assert source_read_mode_of({"extra": {"source_read_mode": "procedure"}}) == "procedure"
    assert source_read_mode_of({"table": "orders"}) == "table"
    assert source_read_mode_of({"procedure_call": "CALL x()"}) == "procedure"


def test_endpoint_extra_roundtrip_preserves_procedure_fields() -> None:
    from src.transfer.models import EndpointConfig, endpoint_to_dict

    ep = EndpointConfig.from_dict(
        "database",
        {
            "format": "postgresql",
            "source_read_mode": "procedure",
            "procedure_call": "CALL get_orders(:since)",
            "procedure_params": {"since": "2024-01-01"},
        },
    )
    assert source_read_mode_of(ep) == "procedure"
    assert ep.extra.get("procedure_call") == "CALL get_orders(:since)"
    assert ep.extra.get("procedure_params") == {"since": "2024-01-01"}
    back = EndpointConfig.from_dict("database", endpoint_to_dict(ep))
    assert source_read_mode_of(back) == "procedure"
    assert back.extra.get("procedure_params") == {"since": "2024-01-01"}


def test_write_ready_mappings_drop_pending_extras() -> None:
    from services.shape_contract import write_ready_mappings

    ready = write_ready_mappings(
        [
            {"source": "id", "target": "id"},
            {"source": "loyalty_tier", "target": "loyalty_tier", "assignment_strategy": "pending_dest_schema"},
            {"source": "notes", "target": "notes", "intentional_omit": True},
        ]
    )
    assert [m["source"] for m in ready] == ["id"]


def test_query_mode_sqlite_select_roundtrip(tmp_path: Path) -> None:
    """Query mode (not CALL) against SQLite — procedure mode stays refused."""
    import sqlite3

    db = tmp_path / "proc.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE customers (id INTEGER, email TEXT)")
    conn.execute("INSERT INTO customers VALUES (1, 'a@example.com')")
    conn.execute("INSERT INTO customers VALUES (2, 'b@example.com')")
    conn.commit()
    conn.close()

    with pytest.raises(ProcedureSourceError):
        parse_callable_source("CALL get_orders()", dialect="sqlite", mode="procedure")

    spec = parse_callable_source(
        "SELECT id, email FROM customers",
        dialect="sqlite",
        mode="query",
    )
    assert spec.mode == "query"
    from services.procedure_source import read_callable_batch

    cfg = {
        "type": "sqlite",
        "database": str(db),
        "source_read_mode": "query",
        "source_query": "SELECT id, email FROM customers ORDER BY id",
        "host": "",
        "port": 0,
        "username": "",
        "password": "",
        "schema": "",
        "connection_string": f"sqlite:///{db}",
        "ssl": False,
    }
    batch = read_callable_batch(cfg, offset=0, limit=10, peek=True)
    assert batch.headers == ["id", "email"]
    assert len(batch.rows) == 2
    assert str(batch.rows[0][0]) == "1"

    page0 = read_callable_batch(cfg, offset=0, limit=1, peek=False)
    page1 = read_callable_batch(cfg, offset=1, limit=1, peek=False)
    assert page0.total_rows == 2
    assert len(page0.rows) == 1
    assert str(page0.rows[0][0]) == "1"
    assert len(page1.rows) == 1
    assert str(page1.rows[0][0]) == "2"
    from services.procedure_source import close_callable_spool

    close_callable_spool()


def test_callable_source_skips_fk_catalog_probe() -> None:
    from services.preflight_source_catalog import load_source_foreign_keys

    fks = load_source_foreign_keys(
        source_config={
            "type": "postgresql",
            "source_read_mode": "procedure",
            "procedure_call": "CALL get_orders()",
            "extra": {"source_read_mode": "procedure"},
        },
        source_table="get_orders",
    )
    assert fks == []


def test_mapped_rows_skip_pending_dest_schema() -> None:
    from connectors.writer_common import build_mapped_rows_with_details

    rows, errors, details = build_mapped_rows_with_details(
        headers=["id", "loyalty_tier"],
        data_rows=[["1", "gold"]],
        mappings=[
            {"source": "id", "target": "id", "transform": "none"},
            {
                "source": "loyalty_tier",
                "target": "loyalty_tier",
                "assignment_strategy": "pending_dest_schema",
                "transform": "none",
            },
        ],
        target_cols=["id"],
        column_types={"id": "INTEGER", "loyalty_tier": "VARCHAR"},
    )
    assert errors == []
    assert details == []
    assert len(rows) == 1
    assert rows[0] == (1,) or rows[0] == ("1",) or list(rows[0]) == [1] or list(rows[0]) == ["1"]
    assert len(rows[0]) == 1
