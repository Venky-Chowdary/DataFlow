"""Reversibility gate: third adversarial review (C1, M3, M4, M6 and the gate Lows).

Real engine runs (SQLite files, in-process job store) wherever the finding was
confirmed on the engine; the rest at the function the finding names.
"""

from __future__ import annotations

import ast
import os
import sqlite3
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

API = Path(__file__).resolve().parent.parent


def _exec(path: Path, sql: str, rows=None) -> None:
    conn = sqlite3.connect(str(path))
    try:
        with conn:
            conn.executemany(sql, rows) if rows is not None else conn.execute(sql)
    finally:
        conn.close()


def _q(path: Path, sql: str):
    conn = sqlite3.connect(str(path))
    try:
        return conn.execute(sql).fetchall()
    finally:
        conn.close()


@pytest.fixture()
def fake(monkeypatch, tmp_path):
    import src.transfer.engine as engine_mod
    from services import audit_log
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    class _Store(_FakeMongo):
        def update_job_fields(self, job_id, fields, **_kw):
            self.jobs.setdefault(job_id, {}).update(fields)
            return True

    store = _Store()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: store)
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    return store


def _run(fake, request):
    from src.transfer.engine import UniversalTransferEngine

    job_id = "rev" + uuid.uuid4().hex[:12]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


def _sqlite_request(src: Path, dst: Path, table: str = "people", **kw):
    from src.transfer.models import EndpointConfig, TransferRequest

    base = dict(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table=table),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table=table),
        sync_mode="full_refresh_overwrite",
        stream_contracts=[{"name": table, "primary_key": ["id"], "selected": True}],
        validation_mode="strict", skip_preflight=False, require_reversibility=True,
        compliance_acknowledged=True, acknowledgment_actor="owner@example.com",
        acknowledgment_reason="reviewed run of the reversibility gate",
    )
    base.update(kw)
    return TransferRequest(**base)


def _g24(result) -> bool:
    blob = str(result.error or "") + str(getattr(result, "error_details", "") or "")
    return "must be reversible" in blob or "g24" in blob


# ------------------------------------------------------------------- M6


def _gate_calls(tree: ast.AST) -> list[ast.Call]:
    return [n for n in ast.walk(tree) if isinstance(n, ast.Call)
            and isinstance(n.func, ast.Attribute) and n.func.attr == "_execute_policy_gates_for_request"]


def test_m6_only_the_streaming_path_hands_planned_to_the_millisecond_gate():
    """``planned=`` switches on g3_millisecond_fit; the in-memory and file paths never passed it before."""
    tree = ast.parse((API / "src/transfer/engine.py").read_text(encoding="utf-8"))
    keywords = [sorted(k.arg for k in call.keywords if k.arg) for call in _gate_calls(tree)]
    assert len(keywords) == 3
    assert sum("planned" in k for k in keywords) == 1, keywords
    assert all("planned" in k or "reversibility" in k for k in keywords), keywords


def test_m6_reversibility_inputs_do_not_run_the_millisecond_gate(monkeypatch, tmp_path):
    """Flag off: a datetime onto a millisecond carrier runs no source aggregate on the in-memory / file paths."""
    from services import millisecond_fit as mf
    from src.transfer.engine_preflight import _execute_policy_gates_for_request
    from src.transfer.models import EndpointConfig, TransferRequest

    db = tmp_path / "s.sqlite"
    _exec(db, "CREATE TABLE ev (id INTEGER PRIMARY KEY, at TIMESTAMP)")
    _exec(db, "INSERT INTO ev VALUES (1, '2026-01-01 10:00:00.123456')")
    calls: list[int] = []
    real = mf.source_sub_millisecond_counts
    monkeypatch.setattr(mf, "source_sub_millisecond_counts", lambda *a, **k: (calls.append(1), real(*a, **k))[1])
    maps = [{"source": "id", "target": "id", "source_type": "INTEGER", "target_type": "INTEGER"},
            {"source": "at", "target": "at", "source_type": "TIMESTAMP(6)", "target_type": "date"}]
    types, dest_types = {"id": "INTEGER", "at": "TIMESTAMP(6)"}, {"id": "INTEGER", "at": "date"}
    inputs = (maps, types, dest_types, [{"id": 1, "at": "2026-01-01 10:00:00.123456"}], 1)

    def gates(require: bool, **kw):
        request = TransferRequest(
            source=EndpointConfig(kind="database", format="sqlite", database=str(db), table="ev"),
            destination=EndpointConfig(kind="database", format="elasticsearch", database="x", table="ev"),
            sync_mode="full_refresh_overwrite", require_reversibility=require)
        return [g["id"] for g in _execute_policy_gates_for_request(request, source_columns=["id", "at"], **kw)]

    baseline = gates(False)
    assert gates(False, reversibility=inputs) == baseline and calls == []
    # The gate still gets its inputs when the run must be reversible - and still no millisecond aggregate.
    assert gates(True, reversibility=inputs) == [*baseline, "g24_reversibility"] and calls == []
    # The streaming path's own ``planned`` keeps its millisecond answer (D148), unchanged.
    assert "g3_millisecond_fit" in gates(False, planned=inputs) and calls == [1]


# ------------------------------------------------------------------- C1


_ID = {"source": "id", "target": "id", "transform": "none", "confidence": 0.99, "source_type": "INTEGER",
       "target_type": "INTEGER"}


def _note(transform: str = "none") -> dict:
    return {"source": "note", "target": "note", "transform": transform, "confidence": 0.99, "source_type": "TEXT",
            "target_type": "TEXT"}


def _two_tables(tmp_path: Path, orders: int = 10) -> tuple[Path, Path]:
    src, dst = tmp_path / "s.sqlite", tmp_path / "d.sqlite"
    _exec(src, "CREATE TABLE people (id INTEGER PRIMARY KEY, email TEXT)")
    _exec(src, "INSERT INTO people VALUES (?, ?)", [(i, f"u{i}@example.com") for i in range(1, 11)])
    _exec(src, "CREATE TABLE orders (id INTEGER PRIMARY KEY, note TEXT)")
    _exec(src, "INSERT INTO orders VALUES (?, ?)", [(i, f"Note {i}") for i in range(1, orders + 1)])
    return src, dst


def _contracts(**orders) -> list[dict]:
    return [{"name": "people", "primary_key": ["id"], "selected": True},
            {"name": "orders", "primary_key": ["id"], "selected": True, **orders}]


def _tables(dst: Path) -> set[str]:
    if not dst.exists():
        return set()
    return {r[0] for r in _q(dst, "SELECT name FROM sqlite_master WHERE type='table'")}


@pytest.mark.parametrize("orders,named", [
    # the reviewer's four multi-stream cases: each was success=True / gate pass with only `people` analysed
    ({"mappings": [_ID, _note("uppercase")]}, "orders.note"),
    ({"mappings": [_ID]}, "orders.note"),
    ({"source_read_mode": "query", "source_query": "SELECT id, note FROM orders WHERE id <= 3"},
     "orders: source query"),
    ({"source_read_mode": "query", "source_query": "SELECT id, note FROM orders",
      "mappings": [_ID, _note("hash_pii")]}, "orders: source query; orders.note"),
])
def test_c1_execute_analyses_every_selected_stream_and_writes_nothing(tmp_path, fake, orders, named):
    src, dst = _two_tables(tmp_path)
    result = _run(fake, _sqlite_request(src, dst, mappings=[], stream_contracts=_contracts(**orders)))
    assert not result.success and _g24(result), result.error
    assert named in str(result.error), result.error
    assert "people." not in str(result.error), "the reversible first table is not blamed"
    assert not _tables(dst) & {"people", "orders"}, "the gate blocks before any table is written"


def test_c1_two_reversible_streams_run_and_a_stream_that_cannot_be_read_blocks_by_name(tmp_path, fake):
    src, dst = _two_tables(tmp_path)
    ok = _run(fake, _sqlite_request(src, dst, mappings=[],
                                    stream_contracts=_contracts(mappings=[dict(_ID), _note()])))
    assert ok.success, ok.error
    assert _q(dst, "SELECT count(*) FROM people") == [(10,)] and _q(dst, "SELECT note FROM orders WHERE id = 2") == [
        ("Note 2",)]
    missing = [{"name": "people", "primary_key": ["id"], "selected": True},
               {"name": "ghosts", "primary_key": ["id"], "selected": True}]
    blocked = _run(fake, _sqlite_request(src, tmp_path / "d2.sqlite", mappings=[], stream_contracts=missing))
    assert not blocked.success and _g24(blocked) and "ghosts: not analysed" in str(blocked.error), blocked.error


def test_c1_a_second_streams_conditional_column_is_enforced_on_its_own_rows(tmp_path, fake):
    """Its sample (the first page) fits; row 150 does not: the writer stops, naming orders.note."""
    src, dst = _two_tables(tmp_path, orders=0)
    _exec(src, "INSERT INTO orders VALUES (?, ?)",
          [(i, f"NOTE {i}") for i in range(1, 150)] + [(150, "Mixed 150")])
    contracts = _contracts(mappings=[dict(_ID), _note("uppercase")])
    result = _run(fake, _sqlite_request(src, dst, mappings=[], stream_contracts=contracts))
    assert not result.success and not _g24(result), result.error
    assert "orders.note would change under its transform (upper)" in str(result.error), result.error
    assert "Mixed" not in str(result.error)


def _plan(contracts, mappings, **policies):
    from types import SimpleNamespace

    rev = SimpleNamespace(mappings=mappings)
    return SimpleNamespace(
        id="p1", active_revision=lambda: rev,
        policies={"require_reversibility": True, "stream_contracts": contracts, **policies},
        sample_rows=[{"id": i, "email": f"u{i}@x.com"} for i in range(1, 4)], row_count_estimate=3,
        source={"format": "postgresql", "table": "people"}, destination={"format": "postgresql", "table": "people"},
        source_columns=["id", "email"], source_schema={"id": "INTEGER", "email": "TEXT"},
        target_schema={"id": "INTEGER", "email": "TEXT"})


_PEOPLE = [dict(_ID), {"source": "email", "target": "email", "transform": "none", "source_type": "TEXT",
                       "target_type": "TEXT"}]


def test_c1_the_plan_reports_one_table_per_selected_stream():
    from services.reversibility_sources import plan_report, preflight_gate

    lossy = _contracts(source_read_mode="query", source_query="SELECT * FROM orders WHERE id < 3",
                       mappings=[dict(_ID), _note("hash_pii")])
    report = plan_report(_plan(lossy, _PEOPLE))
    assert [t["name"] for t in report["tables"]] == ["people", "orders"]
    assert report["tables"][0]["verdict"] == "reversible" and report["tables"][1]["verdict"] != "reversible"
    gate = preflight_gate(report)
    assert gate["status"] == "block" and "orders: source query" in gate["message"] and "orders.note" in gate["message"]
    assert {(c["table"], c["column"], c["reason_code"]) for c in report["lossy_columns"]} >= {
        ("orders", "note", "hash_pii"), ("orders", "(rows)", "row_change")}
    # A stream with no mappings of its own cannot be analysed before Execute reads it: named, never assumed.
    inherited = plan_report(_plan(_contracts(), _PEOPLE))
    assert inherited["gate"]["status"] == "block" and "orders: not analysed" in inherited["gate"]["message"]
    assert "declares no mappings of its own" in inherited["tables"][1]["reason"]
    # An unselected stream is not part of the run.
    single = _contracts()
    single[1]["selected"] = False
    assert [t["name"] for t in plan_report(_plan(single, _PEOPLE))["tables"]] == ["people"]


def test_c1_direct_validate_and_the_job_view_report_every_stream():
    from types import SimpleNamespace

    from services.reversibility import AT_EXECUTE
    from services.reversibility_sources import job_report, request_report

    contracts = _contracts(mappings=[dict(_ID)])
    request = SimpleNamespace(
        source={"format": "postgresql", "table": "people"}, destination={"format": "postgresql", "table": "people"},
        mappings=_PEOPLE, shape_recipe=None, limit=0, stream_contracts=contracts, source_filter=None, job_id="",
        read_options={})
    types = {"id": "INTEGER", "email": "TEXT"}
    report = request_report(request, _PEOPLE, types, types, sample_rows=[], enforce=AT_EXECUTE)
    assert [t["name"] for t in report["tables"]] == ["people", "orders"]
    assert report["gate"]["status"] == "block" and "orders: not analysed" in report["gate"]["message"]
    job = {"transfer_request": {"source": request.source, "destination": request.destination, "mappings": _PEOPLE,
                                "column_types": types, "stream_contracts": _contracts(
                                    source_read_mode="query", source_query="SELECT 1", mappings=[dict(_ID)]),
                                "require_reversibility": True}}
    stored = job_report(job, "job1")
    assert [t["name"] for t in stored["tables"]] == ["people", "orders"]
    assert any(c["reason_code"] == "row_change" for c in stored["tables"][1]["columns"])


# ------------------------------------------------------------------- M3


_UP = [dict(_ID), {"source": "email", "target": "email", "transform": "uppercase", "confidence": 0.99,
                   "source_type": "TEXT", "target_type": "TEXT"}]


def _people(tmp_path: Path, n: int, bad_last: bool = False) -> tuple[Path, Path]:
    src, dst = tmp_path / "s.sqlite", tmp_path / "d.sqlite"
    _exec(src, "CREATE TABLE people (id INTEGER PRIMARY KEY, email TEXT)")
    rows = [(i, f"U{i}@EXAMPLE.COM") for i in range(1, n + 1)]
    if bad_last:
        rows[-1] = (n, f"mixed{n}@Example.com")
    _exec(src, "INSERT INTO people VALUES (?, ?)", rows)
    return src, dst


def test_m3_an_honest_table_larger_than_the_sample_runs_and_records_what_was_checked(tmp_path, fake):
    from services.reversibility_sources import job_report

    src, dst = _people(tmp_path, 450)
    request = _sqlite_request(src, dst, mappings=[dict(m) for m in _UP])
    result = _run(fake, request)
    assert result.success, result.error
    assert _q(dst, "SELECT count(*) FROM people") == [(450,)]
    enforcement = result.destination_summary["reversibility_enforcement"]
    assert enforcement == {"enforced": True, "columns": [{"table": "people", "column": "email", "rows_checked": 450}]}
    stored = {"transfer_request": {"source": {"format": "sqlite", "table": "people"},
                                   "destination": {"format": "sqlite", "table": "people"}, "mappings": _UP,
                                   "column_types": {"id": "INTEGER", "email": "TEXT"},
                                   "require_reversibility": True},
              "destination_summary": result.destination_summary}
    report = job_report(stored, "job1")
    assert report["gate"]["status"] == "pass" and "enforced while writing" in report["gate"]["message"]
    assert report["data_checked"]["enforcement"] == enforcement
    assert report["tables"][0]["columns"][1]["check"]["enforcement"] == "while_writing"


def test_m3_a_row_committed_after_the_gate_is_checked_like_any_other(tmp_path, fake, monkeypatch):
    """The reviewer's N2 race: the count and the sample are a point in time; the writer is not."""
    import src.transfer.engine_preflight as engine_preflight

    src, dst = _people(tmp_path, 10)
    real = engine_preflight._execute_policy_gates_for_request

    def after_gate(*args, **kwargs):
        gates = real(*args, **kwargs)
        _exec(src, "INSERT INTO people VALUES (11, 'late@Example.com')")
        _exec(src, "UPDATE people SET email = 'changed@Example.com' WHERE id = 5")
        return gates

    monkeypatch.setattr(engine_preflight, "_execute_policy_gates_for_request", after_gate)
    result = _run(fake, _sqlite_request(src, dst, mappings=[dict(m) for m in _UP]))
    # Either the reader saw the late rows and the writer stopped on them, or it did not read them at all:
    # a value the transform changed is never in the destination of a run that must be reversible.
    if result.success:
        written = [r[0] for r in _q(dst, "SELECT email FROM people")]
        assert "LATE@EXAMPLE.COM" not in written and "CHANGED@EXAMPLE.COM" not in written, written
    else:
        assert "people.email would change under its transform (upper)" in str(result.error), result.error
        assert "late@" not in str(result.error) and "Example.com" not in str(result.error)


def test_m3_the_file_streaming_path_enforces_too(tmp_path, fake):
    from src.transfer.models import EndpointConfig, TransferRequest

    source = tmp_path / "people.csv"
    source.write_text("id,email\n" + "".join(f"{i},U{i}@EXAMPLE.COM\n" for i in range(1, 400))
                      + "400,mixed400@Example.com\n", encoding="utf-8")
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"), source_filename="people.csv",
        source_content=source.read_bytes(),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(tmp_path / "d.sqlite"),
                                   table="people"),
        sync_mode="full_refresh_overwrite", validation_mode="strict", skip_preflight=False,
        stream_contracts=[{"name": "people", "primary_key": ["id"], "selected": True}],
        mappings=[dict(m) for m in _UP], require_reversibility=True, compliance_acknowledged=True,
        acknowledgment_actor="owner@example.com", acknowledgment_reason="reviewed run of the reversibility gate")
    result = _run(fake, request)
    assert not result.success, "the file path wrote a changed value on a run that must be reversible"
    assert "would change under its transform (upper)" in str(result.error), result.error
    assert "mixed400" not in str(result.error)


def test_m3_enforcement_is_off_entirely_when_the_flag_is_off(tmp_path, fake, monkeypatch):
    from connectors import writer_common
    from services import reversibility_enforce

    calls: list[str] = []
    monkeypatch.setattr(writer_common, "refuse_changed_value", lambda *a, **k: calls.append("check"))
    monkeypatch.setattr(reversibility_enforce, "stamp", lambda *a, **k: calls.append("stamp"))
    src, dst = _people(tmp_path, 150, bad_last=True)
    maps = [dict(m) for m in _UP]
    result = _run(fake, _sqlite_request(src, dst, mappings=maps, require_reversibility=False))
    assert result.success, result.error
    assert calls == [] and all(reversibility_enforce.STAMP not in m for m in maps)
    assert "reversibility_enforcement" not in (result.destination_summary or {})
    assert _q(dst, "SELECT email FROM people WHERE id = 150") == [("MIXED150@EXAMPLE.COM",)]


def test_m3_the_check_names_the_column_and_the_rows_checked_never_the_value():
    from connectors.writer_common import build_mapped_rows_with_details
    from services.reversibility_enforce import ReversibilityViolation, stamp, summary

    maps = [{"source": "id", "target": "id"}, {"source": "code", "target": "code", "transform": "trim",
                                                 "user_override": True}]
    stamp(maps[1], table="t", column="code", code="normalize", transform="trim", job_id="job-m3")
    kwargs = dict(headers=["id", "code"], mappings=maps, target_cols=["id", "code"], error_policy="quarantine")
    rows, _errors, rejected = build_mapped_rows_with_details(data_rows=[["1", "AB"], ["2", None], ["3", "CD"]],
                                                             **kwargs)
    assert len(rows) == 3 and not rejected
    with pytest.raises(ReversibilityViolation) as caught:  # even a quarantine policy fails closed
        build_mapped_rows_with_details(data_rows=[["4", "EF"], ["5", " secret-value "]], **kwargs)
    text = str(caught.value)
    assert "t.code would change under its transform (trim)" in text and "first 3 checked row(s)" in text
    assert "secret-value" not in text and "blocks partial write" in text
    assert summary("job-m3") == {"enforced": True, "columns": [{"table": "t", "column": "code", "rows_checked": 3}]}
    assert summary("job-m3") is None, "the tally is forgotten once reported"


def test_m3_a_type_condition_is_enforced_value_by_value():
    from services import reversibility_enforce
    from services.reversibility_enforce import changes
    from services.reversibility_types import TYPE_CHECKED

    assert reversibility_enforce._TYPE_CODES == TYPE_CHECKED

    pad = {"code": "blank_pad", "target_type": "CHAR(5)"}
    assert changes(pad, "ab", "ab") is False and changes(pad, "ab ", "ab ") is True
    assert changes({"code": "cast"}, "007", 7) is True and changes({"code": "cast"}, "7", 7) is False
    assert changes({"code": "default"}, None, "n/a") is True and changes({"code": "default"}, None, None) is False
    assert changes({"code": "normalize"}, "a", None, "could not convert") is True


def test_m3_cdc_events_are_enforced_one_by_one():
    from connectors.cdc_eos_sa import _apply_mapped_transforms
    from services.reversibility_enforce import ReversibilityViolation, stamp

    maps = [{"source": "id", "target": "id"}, {"source": "email", "target": "email", "transform": "uppercase"}]
    stamp(maps[1], table="people", column="email", code="normalize", transform="upper", job_id="job-cdc")
    types = {"id": "INTEGER", "email": "TEXT"}
    assert _apply_mapped_transforms({"id": 1, "email": "A@X.COM"}, maps, types)["email"] == "A@X.COM"
    with pytest.raises(ReversibilityViolation, match="people.email would change"):
        _apply_mapped_transforms({"id": 2, "email": "b@x.com"}, maps, types)


def test_m3_a_server_side_copy_is_declined_for_an_enforced_column():
    from services.reversibility_enforce import enforced_columns, stamp

    maps = [{"source": "id", "target": "id"}, {"source": "email", "target": "email"}]
    assert enforced_columns(maps) == []
    stamp(maps[1], table="people", column="email", code="charset", job_id="job-copy")
    assert enforced_columns(maps) == ["email"]
    source = (API / "src/transfer/copy_route.py").read_text(encoding="utf-8")
    body = source[source.index("def _try_copy_fast_path("):source.index("def _try_pg_mysql_copy_fast_path(")]
    assert "enforced_columns(mappings)" in body and body.index("enforced_columns(mappings)") < body.index(
        "# Studio may set source.table")


def test_m3_a_column_without_a_value_rule_cannot_be_enforced_and_still_blocks():
    from services.reversibility import WHILE_WRITING, analyze_table, build_report

    table = analyze_table(table="t", mappings=[{"source": "v", "target": "v"}], source_columns=["v"],
                          source_types={"v": "TEXT"}, target_types={}, sample_rows=[{"v": "a"}],
                          enforce=WHILE_WRITING)
    column = table["columns"][0]
    assert column["reason_code"] == "type_unknown" and "enforcement" not in column["check"]
    assert build_report([table], scope="job", subject_id="j", require=True, data_rows=1,
                        data_source="preflight_sample")["gate"]["status"] == "block"


# ------------------------------------------------------------------- M4 / Low


def test_m4_a_client_estimate_never_proves_coverage_at_validate():
    from services.reversibility_sources import plan_report

    for estimate in (0, 3, 10, 5_000_000):
        plan = _plan([{"name": "people", "primary_key": ["id"]}], [dict(m) for m in _UP])
        plan.policies.pop("require_reversibility")
        plan.sample_rows = [{"id": i, "email": f"U{i}@X.COM"} for i in range(1, 26)]
        plan.row_count_estimate = estimate
        report = plan_report(plan)
        check = report["tables"][0]["columns"][1]["check"]
        assert check["status"] == "fits" and check["covers_population"] is False, estimate
        assert report["data_checked"]["population_rows"] is None and report["verdict"] == "partly_reversible"


def test_low_a_header_taken_from_a_lower_row_is_a_row_change():
    from services.reversibility import gate_for
    from services.reversibility_sources import request_report

    def report(options):
        return request_report(_sqlite_request(Path("s"), Path("d"), read_options=options), _PEOPLE,
                              {"id": "INTEGER", "email": "TEXT"}, {"id": "INTEGER", "email": "TEXT"})

    lowered = report({"header_row": 3})
    entry = next(c for c in lowered["tables"][0]["columns"] if c["reason_code"] == "row_change")
    assert entry["transforms"] == ["header_row"] and "the 2 row(s) above it are not written" in entry["reason"]
    assert "people: rows above the header" in gate_for(lowered)["message"]
    for first in ({"header_row": 1}, {"header_row": 0}, {}):
        assert report(first)["verdict"] == "reversible"
