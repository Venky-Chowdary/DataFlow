"""Exit rehearsal: regressions from the second adversarial review (review 2) and the web integration.

Each test names what it pins: N9 the definition hash, gap 3 the rehearsals
list and a running summary, N10 masked control totals, N5 / N8 the typed digest
and actionable reasons, N4 the verification lease, gap 1 PII review, and the
Low items.
"""

from __future__ import annotations

from tests.exit_background_guard import _no_exit_background_work_outlives_a_test  # noqa: F401

import os

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from r8_ent_harness import ent  # noqa: E402,F401
from tests.test_exit_rehearsal_e2e import _run, _system, world  # noqa: E402,F401


# --------------------------------------------- N9: what the definition hash tracks


def _definition(**over):
    from services.exit_inventory import definition_sha256

    system = {"id": "exs_1", "workspace_id": "ws1", "source": {"connector_id": "c1"},
              "tables": [{"name": "orders", "schema": None, "key_columns": []}], "exit_window_hours": 24,
              "extract_format": "jsonl"}
    system.update(over)
    return definition_sha256(system)


def test_n9_the_extract_format_is_part_of_the_definition(world):  # noqa: F811
    assert _definition() != _definition(extract_format="csv")


def test_n9_an_edit_to_the_connector_behind_the_same_id_changes_the_definition(world, monkeypatch):  # noqa: F811
    from services import schedule_runner

    conn = {"name": "ledger", "type": "postgresql", "host": "db1.internal", "port": 5432, "database": "ledger",
            "username": "svc", "password": "s3cret"}
    monkeypatch.setattr(schedule_runner, "_resolve_connector_for_schedule", lambda _cid, _sched: dict(conn))
    before = _definition()
    conn["password"] = "rotated"  # a secret is never part of the fingerprint
    assert _definition() == before
    conn["host"] = "db2.internal"
    moved = _definition()
    assert moved != before
    conn["database"] = "ledger_v2"
    assert _definition() != moved


def test_n9_changing_the_extract_format_makes_a_proven_system_stale(world):  # noqa: F811
    from services.exit_inventory import get_system, readiness, update

    system = _system(tables=[{"name": "events"}])
    assert _run(system)["verdict"] == "proven"
    assert readiness(get_system(system["id"]))["state"] == "proven"
    update(system["id"], {"extract_format": "csv"}, actor="owner@example.com", expected_version=None)
    ready = readiness(get_system(system["id"]))
    assert ready["stale"] is True and ready["state"] == "not_proven"
    assert "format" in ready["stale_reason"]


# ------------------------------- gap 3: the rehearsals list and a running rehearsal's summary


def test_gap3_the_rehearsals_list_says_whether_each_rehearsal_is_current(world):  # noqa: F811
    from services.exit_inventory import get_system, update
    from services.exit_inventory import rehearsal_history

    system = _system(tables=[{"name": "events"}])
    assert _run(system)["verdict"] == "proven"
    listed = rehearsal_history(get_system(system["id"]), limit=20)
    assert [r["current"] for r in listed["rehearsals"]] == [True]
    update(system["id"], {"exit_window_hours": 48}, actor="owner@example.com", expected_version=None)
    listed = rehearsal_history(get_system(system["id"]), limit=20)
    assert [r["current"] for r in listed["rehearsals"]] == [False]
    assert listed["trend"]["points"]


def test_gap3_a_running_rehearsal_summary_reads_its_window_and_tables_from_its_definition(world):  # noqa: F811
    from services.exit_inventory import get_system, readiness, rehearsal_summary
    from services.exit_rehearsal import start

    system = _system(tables=[{"name": "events"}, {"name": "orders"}], exit_window_hours=6)
    running = start(system, actor="owner@example.com")
    summary = rehearsal_summary(running, get_system(system["id"]))
    assert summary["verdict"] == "running"
    assert summary["window_seconds"] == 6 * 3600 and summary["tables_declared"] == 2
    assert readiness(get_system(system["id"]))["last_rehearsal"]["tables_declared"] == 2


def test_gap3_the_list_route_reads_current_over_rest(ent):  # noqa: F811
    import sqlite3
    import time

    (ent.tmp / "sqlite").mkdir(exist_ok=True)
    db = ent.tmp / "sqlite" / "gap3.sqlite"
    conn = sqlite3.connect(str(db))
    conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, amount NUMERIC)")
    conn.executemany("INSERT INTO orders VALUES (?, ?)", [(1, 1.5), (2, 2.5)])
    conn.commit()
    conn.close()
    ws = ent.workspace("Exit gap 3")
    editor = ent.member(email="exit-gap3@example.com", workspace_id=ws, role="editor")
    connector = editor.post("/api/v1/connectors/saved", json={"name": "ledger", "type": "sqlite", "role": "source",
                                                               "database": str(db)}).json()["id"]
    sid = editor.post("/api/v1/exit/systems", json={
        "name": "Gap 3 ledger", "source": {"connector_id": connector},
        "tables": [{"name": "orders", "key_columns": ["id"]}], "exit_window_hours": 24}).json()["id"]
    started = editor.post(f"/api/v1/exit/systems/{sid}/rehearsals").json()
    running = editor.get(f"/api/v1/exit/systems/{sid}/rehearsals").json()["rehearsals"][0]
    assert running["tables_declared"] == 1 and running["window_seconds"] == 24 * 3600
    deadline, body = time.monotonic() + 120, started
    while body["verdict"] == "running" and time.monotonic() < deadline:
        time.sleep(0.5)
        body = editor.get(f"/api/v1/exit/rehearsals/{started['id']}").json()
    listed = editor.get(f"/api/v1/exit/systems/{sid}/rehearsals").json()["rehearsals"]
    assert listed[0]["id"] == started["id"] and listed[0]["current"] is True


# -------------------------------- N10: control totals are masked unless classified non-sensitive


def _totals(types, rows):
    from services.exit_completeness import judge_table, profile_rows

    src = profile_rows(rows, list(types), [], None, types=types, engine="postgresql")
    ext = profile_rows(rows, list(types), [], sorted(src.totals), types=types, engine="postgresql")
    out = judge_table(name="t", source=src, extract=ext, key_columns=[], seal_ok=True,
                      parity={"check": "count_only", "verdict": "unverifiable", "unexplained": 0}, not_measured=[])
    return {c["column"]: c for c in out["control_totals"]}, out


def test_n10_identifier_and_personal_figures_are_masked_by_default():
    import json
    from decimal import Decimal

    types = {"national_id": "BIGINT", "tax_id": "BIGINT", "tin": "BIGINT", "salary": "NUMERIC(12,2)",
             "income": "NUMERIC(12,2)", "n": "INTEGER", "card": "NUMERIC(20,2)", "id": "BIGINT"}
    rows = [{"national_id": 123456789, "tax_id": 223456789, "tin": 323456789, "salary": Decimal("91000.00"),
             "income": Decimal("88000.00"), "n": 423456789, "card": Decimal("4111111111111111.00"), "id": 1}]
    totals, out = _totals(types, rows)
    for col in types:
        assert totals[col]["masked"] is True, col
        assert totals[col]["source"] == {"sum": None, "min": None, "max": None}, col
        assert totals[col]["match"] is True, col
    blob = json.dumps(out)
    for secret in ("123456789", "91000", "88000", "423456789", "4111111111111111"):
        assert secret not in blob, secret


def test_n10_a_measure_the_classifier_clears_shows_its_totals():
    from decimal import Decimal

    types = {"amount": "NUMERIC(12,2)", "qty": "INTEGER"}
    totals, _ = _totals(types, [{"amount": Decimal("2.50"), "qty": 3}, {"amount": Decimal("1.25"), "qty": 4}])
    assert totals["amount"]["masked"] is False and totals["amount"]["source"]["max"] == "2.50"
    assert totals["qty"]["masked"] is False and totals["qty"]["source"]["sum"] == "7"
    # A measure whose values the classifier reads as PII stays masked.
    card, _ = _totals({"amount": "NUMERIC(20,0)"}, [{"amount": Decimal("4111111111111111")}])
    assert card["amount"]["masked"] is True


# ------------------------------------------------------------- Low: NaN in a numeric column


def test_low_a_nan_in_a_numeric_column_is_profiled_not_reported_as_unreadable():
    from decimal import Decimal

    from services.exit_completeness import profile_rows

    types = {"id": "BIGINT", "ratio": "DOUBLE PRECISION", "amount": "NUMERIC(12,2)"}
    rows = [{"id": 1, "ratio": float("nan"), "amount": Decimal("NaN")},
            {"id": 2, "ratio": 0.5, "amount": Decimal("2.50")}]
    src = profile_rows(rows, list(types), ["id"], None, types=types, engine="postgresql")
    assert src.rows == 2 and src.totals["ratio"]["max"] == Decimal("0.5")
    same = profile_rows([{"id": 1, "ratio": "NaN", "amount": "NaN"}, {"id": 2, "ratio": "0.5", "amount": "2.50"}],
                        list(types), ["id"], sorted(src.totals), types=types, engine="postgresql")
    other = profile_rows([{"id": 1, "ratio": "0", "amount": "0"}, {"id": 2, "ratio": "0.5", "amount": "2.50"}],
                         list(types), ["id"], sorted(src.totals), types=types, engine="postgresql")
    totals, _ = _totals_of(src, same)
    assert totals["ratio"]["match"] is True and totals["amount"]["match"] is True
    totals, _ = _totals_of(src, other)
    assert totals["ratio"]["match"] is False and totals["amount"]["match"] is False


def _totals_of(src, ext):
    from services.exit_completeness import judge_table

    out = judge_table(name="t", source=src, extract=ext, key_columns=["id"], seal_ok=True,
                      parity={"check": "count_only", "verdict": "unverifiable", "unexplained": 0}, not_measured=[])
    return {c["column"]: c for c in out["control_totals"]}, out


# ---------------------- N5 / N8: the typed digest compares every value exactly, and timetz matches itself


def _same(source, extract, ddl, engine):
    from services.exit_completeness import profile_rows

    a = profile_rows([{"c": source}], ["c"], [], None, types={"c": ddl}, engine=engine)
    b = profile_rows([{"c": extract}], ["c"], [], [], types={"c": ddl}, engine=engine)
    return a.row_set_sha256 == b.row_set_sha256


def _tz(hours):
    from datetime import timedelta, timezone

    return timezone(timedelta(hours=hours))


@pytest.mark.parametrize("source,extract,ddl,engine", [
    (0.1 + 0.2, 0.3, "double precision", "postgresql"),
    (1.0000000000000002, 1.0, "double precision", "postgresql"),
    (123456789.12345679, 123456789.1234568, "double precision", "postgresql"),
    (0.1 + 0.2, "0.3", "float8", "postgresql"),
    ("__dt__2024-01-01T13:05:00", "2024-01-01", "DATE", "oracle"),
    ("__dt__2024-01-01T10:00:00+02:00", "2024-01-01T08:00:00+00:00", "datetimeoffset", "sqlserver"),
    ("__dt__2024-01-01T10:00:00+02:00", "2024-01-01T08:00:00+00:00", "TIMESTAMP WITH TIME ZONE", "oracle"),
    ("__dt__2024-01-01T10:00:00+00:00", "2024-01-01T10:00:00", "timestamptz", "postgresql"),
    ("__dec__1.50", "1.5", "numeric", "postgresql"),
    ("__dec__1.50", 1.5, "numeric", "postgresql"),
    ("__time__10:00:00+02:00", "10:00:00+00:00", "timetz", "postgresql"),
])
def test_n5_values_that_differ_within_their_type_are_told_apart(source, extract, ddl, engine):
    assert not _same(_value(source), extract, ddl, engine)


@pytest.mark.parametrize("source,extract,ddl,engine", [
    (0.1 + 0.2, 0.30000000000000004, "double precision", "postgresql"),
    (0.1 + 0.2, "0.30000000000000004", "double precision", "postgresql"),
    (-0.0, "-0.0", "double precision", "postgresql"),
    (float("nan"), "NaN", "double precision", "postgresql"),
    ("__dt__2024-01-01T13:05:00", "2024-01-01T13:05:00", "DATE", "oracle"),
    ("__dt__2024-01-01T13:05:00", "2024-01-01 13:05:00", "DATE", "oracle"),
    ("__dt__2024-01-01T10:00:00+02:00", "2024-01-01T10:00:00+02:00", "datetimeoffset", "sqlserver"),
    ("__dt__2024-01-01T10:00:00+00:00", "2024-01-01 12:00:00+02:00", "timestamptz", "postgresql"),
    ("__dec__1.50", "1.50", "numeric", "postgresql"),
    ("__dec__1.50", "__frac__1.50", "numeric", "postgresql"),
    ("__dec__100", 100, "numeric", "postgresql"),
    ("__dec__12345678901234567890.123456789", "12345678901234567890.123456789", "numeric", "postgresql"),
    ("__dec__1.5000", 1.5, "numeric(12,4)", "postgresql"),  # a fixed scale still compares by value
    ("__time__10:00:00+02:00", "10:00:00+02:00", "timetz", "postgresql"),
    ("__time__10:00:00+02:00", "10:00:00+02", "time with time zone", "postgresql"),
])
def test_n5_a_value_and_its_exact_extract_rendering_still_match(source, extract, ddl, engine):
    assert _same(_value(source), _value(extract), ddl, engine)


def _value(spec):
    """Driver-shaped source values from a short spelling (``__dt__`` ISO, ``__dec__``, ``__time__``)."""
    from datetime import datetime, time
    from decimal import Decimal

    if not isinstance(spec, str):
        return spec
    if spec.startswith("__dt__"):
        return datetime.fromisoformat(spec[6:])
    if spec.startswith("__dec__"):
        return Decimal(spec[7:])
    if spec.startswith("__time__"):
        return time.fromisoformat(spec[8:])
    if spec.startswith("__frac__"):
        from services.value_serializer import json_loads_exact

        return json_loads_exact(spec[8:], keep_literals=True)
    return spec


@pytest.mark.parametrize("fmt", ["jsonl", "csv"])
def test_n5_a_clean_extract_of_exact_floats_is_still_proven_e2e(world, fmt):  # noqa: F811
    import sqlite3

    conn = sqlite3.connect(str(world["src"]))
    with conn:
        conn.execute("CREATE TABLE ratios (id INTEGER PRIMARY KEY, r REAL, note TEXT)")
        conn.executemany("INSERT INTO ratios VALUES (?, ?, ?)", [
            (1, 0.1 + 0.2, "a"), (2, 1.0000000000000002, "b"), (3, -0.0, "c"), (4, 123456789.12345679, None),
            (5, 1e-300, "e"), (6, None, "f")])
    conn.close()
    out = _run(_system(name=f"Ratios {fmt}", tables=[{"name": "ratios"}], extract_format=fmt))
    assert out["verdict"] == "proven", out["tables"][0]["reason"]


# ------------------------- N8: a failed extract's reason names the job, a reference and the engine's finding


_SECRET = "alice@example.com"


def _reason(job):
    from services.exit_job_reasons import failed_reason

    return failed_reason(job, "job_abc123")


def test_n8_a_writer_cell_finding_names_the_job_the_count_and_the_column_never_the_value():
    job = {"status": "failed",
           "error": (f"file_export rejected 1 cell finding(s) across 1 row(s); strict error policy blocks partial "
                     f"write — v (row 1): value is not a valid interval wire payload: '{_SECRET}' — quarantined"),
           "rejected_details": [{"column": "v", "row": 1, "reason": f"value is not valid: '{_SECRET}'"}]}
    reason = _reason(job)
    assert "job_abc123" in reason and "Reference " in reason
    assert "rejected 1 cell finding(s) across 1 row(s)" in reason and "column(s) v" in reason
    assert _SECRET not in reason and "see the server log" not in reason


def test_n8_a_preflight_block_names_the_root_cause_and_gate_never_the_blocker_text():
    job = {"status": "failed", "error": f"Blocking issues detected: value '{_SECRET}' does not fit",
           "error_details": {"reason": "Preflight blocked transfer",
                             "blockers": [f"value '{_SECRET}' does not fit"],
                             "guidance": [{"gate": "rc-fidelity-collapse-5009bdaaed43",
                                           "message": f"Lossy: '{_SECRET}'"}]}}
    reason = _reason(job)
    assert "job_abc123" in reason and "Reference " in reason
    assert "preflight" in reason and "fidelity collapse" in reason and "rc-fidelity-collapse-5009bdaaed43" in reason
    assert _SECRET not in reason


def test_n8_an_unknown_error_still_names_the_job_and_a_reference():
    reason = _reason({"status": "failed", "error": f"boom near '{_SECRET}'"})
    assert "job_abc123" in reason and "Reference " in reason and _SECRET not in reason
    assert "see the server log" in reason
    assert "job_abc123" in _reason({"status": "cancelled"})


def test_n8_a_failed_extract_s_rehearsal_reason_names_its_job_and_reference_e2e(world):  # noqa: F811
    out = _run(_system(name="Missing", tables=[{"name": "no_such_table"}]))
    table = out["tables"][0]
    assert out["verdict"] == "failed" and table["status"] == "failed"
    assert table["job_id"] and table["job_id"] in table["reason"], table["reason"]
    assert "Reference " in table["reason"]


# ------------------------------------- N4: a hung verifier loses its claim; the verification is capped


def _poll(rid, seconds):
    import time

    from services.exit_rehearsal import settle
    from services.exit_store import REHEARSALS, get

    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline and get(REHEARSALS, rid)["verdict"] == "running":
        settle(rid, inline=False)  # page poll / scheduler beat
        time.sleep(0.2)
    return get(REHEARSALS, rid)


def test_n4_a_verifier_that_reads_nothing_stops_refreshing_and_the_cap_closes_the_rehearsal(world, monkeypatch):  # noqa: F811,E501
    import threading
    import time
    from datetime import timedelta

    from services import exit_rehearsal, exit_verify_guard
    from services.exit_store import REHEARSALS, get
    from tests.test_exit_rehearsal_e2e import _wait

    hang = threading.Event()
    runs: list[float] = []

    def hung_verify(doc, jobs, **kw):
        if kw.get("timed_out_after") is not None:
            return real_verify(doc, jobs, **kw)
        runs.append(time.monotonic())
        hang.wait(20)  # a source read stuck on a dead connection: no row is ever read
        return real_verify(doc, jobs, timed_out_after=3.0)  # released late: its close finds the claim taken

    real_verify = exit_rehearsal.verify
    monkeypatch.setattr(exit_rehearsal, "VERIFY_STALE", timedelta(seconds=1))
    monkeypatch.setattr(exit_verify_guard, "max_verify_seconds", lambda: 3.0)
    monkeypatch.setattr(exit_rehearsal, "verify", hung_verify)
    monkeypatch.setattr(exit_rehearsal, "settle_quietly", lambda rid: exit_rehearsal.settle(rid))
    system = _system(tables=[{"name": "events"}])
    first = exit_rehearsal.start(system, actor="a@example.com")
    _wait(first)
    try:
        exit_rehearsal.settle(first["id"], inline=False)
        time.sleep(0.6)
        claimed = get(REHEARSALS, first["id"])["_verify"]
        time.sleep(1.5)  # > the stale window, with no row read: the claim is not refreshed
        assert get(REHEARSALS, first["id"])["_verify"]["at"] == claimed["at"]
        doc = _poll(first["id"], 8)
    finally:
        hang.set()
    assert doc["verdict"] == "not_proven", doc["verdict_reason"]
    assert "did not finish" in doc["tables"][0]["reason"]
    assert len(runs) >= 1
    # The system is free again: a new rehearsal starts.
    assert exit_rehearsal.start(system, actor="a@example.com")["verdict"] == "running"


def test_n4_a_verifier_that_keeps_reading_keeps_its_claim(world, monkeypatch):  # noqa: F811
    import time
    from datetime import timedelta

    from services import exit_rehearsal, exit_verify_guard
    from tests.test_exit_rehearsal_e2e import _wait

    real_verify = exit_rehearsal.verify
    runs: list[float] = []

    def reading_verify(doc, jobs, **kw):
        runs.append(time.monotonic())
        for _ in range(15):  # 1.5 s of rows being read: longer than the stale window
            exit_verify_guard.tick(100)
            time.sleep(0.1)
        return real_verify(doc, jobs, **kw)

    monkeypatch.setattr(exit_rehearsal, "VERIFY_STALE", timedelta(seconds=1))
    monkeypatch.setattr(exit_rehearsal, "verify", reading_verify)
    monkeypatch.setattr(exit_rehearsal, "settle_quietly", lambda rid: exit_rehearsal.settle(rid))
    first = exit_rehearsal.start(_system(tables=[{"name": "events"}]), actor="a@example.com")
    _wait(first)
    doc = _poll(first["id"], 8)
    assert doc["verdict"] == "proven", doc["verdict_reason"]
    assert len(runs) == 1


def test_n4_the_source_re_read_runs_under_a_statement_timeout(world, monkeypatch):  # noqa: F811
    from services import exit_completeness
    from src.ai.copilot.tool_deadline import statement_timeout_seconds

    seen: list[float | None] = []
    real = exit_completeness.source_columns_and_rows

    def spy(endpoint, **kw):
        seen.append(statement_timeout_seconds())
        return real(endpoint, **kw)

    monkeypatch.setattr(exit_completeness, "source_columns_and_rows", spy)
    assert _run(_system(tables=[{"name": "events"}]))["verdict"] == "proven"
    assert seen and seen[0] is not None and 0 < seen[0] <= 6 * 3600
