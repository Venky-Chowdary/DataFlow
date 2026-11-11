"""Reversibility gate: the invertibility table, per-table verdicts and the g24 gate.

Pins the plan's guarantees: every lossy mapping is named (hash_pii, mask_pii,
redact, omit, dropped, narrowing), a conditional column is checked against the
observed values (never assumed), a sample that fits is disclosed as a sample,
verdicts are deterministic, and a transfer that requires reversibility is
blocked at preflight (plan Validate and Execute) until it is reversible.
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from services.reversibility import (  # noqa: E402
    CONDITIONAL,
    INVERTIBLE,
    LOSSY,
    NOT_REVERSIBLE,
    PARTLY,
    REVERSIBLE,
    analyze_table,
    build_report,
    classify_mapping,
)


def _m(source, target=None, transform="none", stype="", ttype=""):
    return {"source": source, "target": target or source, "transform": transform,
            "source_type": stype, "target_type": ttype}


def _report(table, require=False, rows=0):
    return build_report([table], scope="plan", subject_id="p1", require=require,
                        data_rows=rows, data_source="plan_sample")


@pytest.mark.parametrize("transform,code", [
    ("hash_pii", "hash_pii"), ("mask_pii", "mask_pii"), ("redact", "redact"), ("omit", "omit"),
])
def test_protecting_transforms_are_lossy_and_named(transform, code):
    col = classify_mapping(_m("email", transform=transform, stype="TEXT", ttype="TEXT"))
    assert col["invertibility"] == LOSSY
    assert col["reason_code"] == code
    assert "email" in col["reason"]


def test_every_lossy_column_is_listed_including_a_dropped_one():
    table = analyze_table(
        table="customers",
        mappings=[_m("id", stype="INTEGER", ttype="INTEGER"),
                  _m("email", transform="hash_pii", stype="TEXT", ttype="TEXT")],
        source_columns=["id", "email", "notes"],
    )
    report = _report(table)
    named = {(c["column"], c["reason_code"]) for c in report["lossy_columns"]}
    assert named == {("email", "hash_pii"), ("notes", "dropped")}
    assert table["verdict"] == PARTLY
    assert "hash_pii" in table["reason"] and "notes" in table["reason"]
    assert table["rollback"] == "keep_source_as_fallback"


def test_lossless_mapping_is_reversible_with_a_documented_rollback():
    table = analyze_table(
        table="orders",
        mappings=[_m("id", stype="INTEGER", ttype="BIGINT"),
                  _m("cust", "customer_name", stype="VARCHAR(50)", ttype="VARCHAR(200)"),
                  _m("placed", stype="DATE", ttype="TIMESTAMP"),
                  _m("total", stype="NUMERIC(12,2)", ttype="NUMERIC(14,4)")],
        source_columns=["id", "cust", "placed", "total"],
        dest_db="postgresql", source_db="postgresql",
    )
    assert table["verdict"] == REVERSIBLE, table
    assert {c["invertibility"] for c in table["columns"]} == {INVERTIBLE}
    assert {c["reason_code"] for c in table["columns"]} == {"widen", "lossless_cast"}
    assert "renamed to customer_name" in table["columns"][1]["reason"]
    assert table["rollback"] == "restore_from_destination"
    assert "does not undo" in table["rollback_note"]
    assert "one-click" not in table["rollback_note"].lower()


def test_narrowing_is_lossy_and_counts_the_observed_rows_that_lose_digits():
    rows = [{"amount": v} for v in ("1.2345", "2.50", "3.1", "4.0001", "5.12", "6.999", "7.1234")]
    table = analyze_table(
        table="payments", mappings=[_m("amount", stype="NUMERIC(12,4)", ttype="NUMERIC(12,2)")],
        source_columns=["amount"], sample_rows=rows, dest_db="postgresql",
    )
    col = table["columns"][0]
    assert col["invertibility"] == LOSSY and col["reason_code"] == "narrowing"
    assert col["check"] == {"status": "does_not_fit", "rows_checked": 7, "rows_not_fitting": 4}
    assert "4 of 7" in col["reason"]


def test_narrowing_stays_lossy_when_the_sample_happens_to_fit():
    table = analyze_table(
        table="t", mappings=[_m("name", stype="VARCHAR(255)", ttype="VARCHAR(20)")],
        source_columns=["name"], sample_rows=[{"name": "short"}],
    )
    assert table["columns"][0]["invertibility"] == LOSSY
    assert table["verdict"] != REVERSIBLE


@pytest.mark.parametrize("stype,ttype,code", [
    ("TIMESTAMP", "DATE", "truncation"),
    ("NUMERIC(10,2)", "INTEGER", "rounding"),
    ("DOUBLE PRECISION", "NUMERIC(20,6)", "rounding"),
])
def test_truncating_and_rounding_casts_are_lossy(stype, ttype, code):
    col = classify_mapping(_m("c", stype=stype, ttype=ttype), dest_db="postgresql")
    assert (col["invertibility"], col["reason_code"]) == (LOSSY, code)


def test_a_cast_out_of_text_is_checked_against_the_observed_values():
    fits = classify_mapping(_m("code", stype="TEXT", ttype="INTEGER"), observed=["42", "7", None])
    assert fits["invertibility"] == CONDITIONAL
    assert fits["check"] == {"status": "fits", "rows_checked": 2, "rows_not_fitting": 0}
    assert "rows not sampled are not proven" in fits["reason"]
    loses = classify_mapping(_m("code", stype="TEXT", ttype="INTEGER"), observed=["42", "007"])
    assert loses["check"]["status"] == "does_not_fit" and loses["check"]["rows_not_fitting"] == 1


def test_a_normalising_transform_fits_only_when_it_changes_nothing():
    clean = classify_mapping(_m("name", transform="trim", stype="TEXT", ttype="TEXT"), observed=["a", "b"])
    dirty = classify_mapping(_m("name", transform="trim", stype="TEXT", ttype="TEXT"), observed=["a", " b"])
    unseen = classify_mapping(_m("name", transform="upper", stype="TEXT", ttype="TEXT"), observed=[])
    assert clean["check"]["status"] == "fits"
    assert dirty["check"]["status"] == "does_not_fit"
    assert unseen["check"]["status"] == "not_checked"
    # Review 2 (N2): a check that fits counts only when it covered the whole table.
    table_clean = analyze_table(table="t", mappings=[_m("name", transform="trim", stype="TEXT", ttype="TEXT")],
                                source_columns=["name"], sample_rows=[{"name": "a"}], population_rows=1)
    assert table_clean["verdict"] == REVERSIBLE
    table_unseen = analyze_table(table="t", mappings=[_m("name", transform="upper", stype="TEXT", ttype="TEXT")],
                                 source_columns=["name"])
    assert table_unseen["verdict"] == PARTLY


def test_an_undeclared_type_is_never_assumed_reversible():
    col = classify_mapping(_m("x"))
    assert col["invertibility"] == CONDITIONAL and col["reason_code"] == "type_unknown"
    assert col["check"]["status"] == "not_checked"


def test_engine_numeric_bind_on_a_same_family_column_is_invertible():
    col = classify_mapping(_m("n", transform="integer", stype="INTEGER", ttype="BIGINT"))
    assert col["invertibility"] == INVERTIBLE and col["reason_code"] == "type_preserving_bind"


def test_a_lossy_key_makes_the_table_not_reversible():
    table = analyze_table(
        table="people",
        mappings=[_m("ssn", transform="hash_pii", stype="TEXT", ttype="TEXT"),
                  _m("name", stype="TEXT", ttype="TEXT")],
        source_columns=["ssn", "name"], key_columns=["ssn"],
    )
    assert table["verdict"] == NOT_REVERSIBLE
    assert "key ssn" in table["reason"]


def test_recipe_steps_that_filter_rows_or_round_are_lossy():
    table = analyze_table(
        table="t", mappings=[_m("id", stype="INTEGER", ttype="INTEGER")], source_columns=["id"],
        recipe_steps=[{"op": "filter_rows", "options": {"expression": "id > 3"}},
                      {"op": "round_number", "column": "id", "options": {"places": 0}},
                      {"op": "rename_column", "column": "id", "enabled": False}],
    )
    codes = [(c["invertibility"], c["reason_code"]) for c in table["columns"][1:]]
    assert codes == [(LOSSY, "row_change"), (LOSSY, "recipe_lossy")]
    assert table["verdict"] == PARTLY


def test_verdicts_are_deterministic():
    kwargs = dict(table="t", mappings=[_m("a", transform="trim", stype="TEXT", ttype="TEXT"),
                                       _m("b", transform="hash_pii", stype="TEXT", ttype="TEXT")],
                  source_columns=["a", "b", "c"], sample_rows=[{"a": " x"}])
    first, second = _report(analyze_table(**kwargs)), _report(analyze_table(**kwargs))
    first.pop("generated_at"), second.pop("generated_at")
    assert first == second


def test_gate_skips_unless_required_and_blocks_until_reversible():
    partly = analyze_table(table="t", mappings=[_m("e", transform="mask_pii", stype="TEXT", ttype="TEXT")],
                           source_columns=["e"])
    assert _report(partly)["gate"]["status"] == "skip"
    blocked = _report(partly, require=True)["gate"]
    assert blocked["id"] == "g24_reversibility" and blocked["status"] == "block"
    assert "t.e" in blocked["message"]
    ok = analyze_table(table="t", mappings=[_m("e", stype="TEXT", ttype="TEXT")], source_columns=["e"])
    assert _report(ok, require=True)["gate"]["status"] == "pass"


def test_g24_is_a_hard_gate_with_remediation():
    from services.preflight_rules import HARD_GATE_IDS, PREFLIGHT_GATE_RULES

    assert "g24_reversibility" in HARD_GATE_IDS
    assert PREFLIGHT_GATE_RULES["g24_reversibility"]["fix"]


def test_execute_policy_gates_add_g24_only_when_required():
    from src.transfer.engine_preflight import _execute_policy_gates_for_request
    from src.transfer.models import EndpointConfig, TransferRequest

    req = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=":memory:", table="t"),
        destination=EndpointConfig(kind="database", format="sqlite", database=":memory:", table="t"),
        mappings=[_m("email", transform="hash_pii", stype="TEXT", ttype="TEXT")],
    )
    planned = (req.mappings, {"email": "TEXT"}, {"email": "TEXT"})
    assert "g24_reversibility" not in {g["id"] for g in _execute_policy_gates_for_request(req, planned=planned)}
    req.require_reversibility = True
    gates = {g["id"]: g for g in _execute_policy_gates_for_request(req, planned=planned)}
    assert gates["g24_reversibility"]["status"] == "block"
    assert gates["g24_reversibility"]["details"]["lossy_columns"][0]["reason_code"] == "hash_pii"


def test_request_flag_round_trips_through_the_stored_request():
    from src.transfer.models import (
        EndpointConfig,
        TransferRequest,
        transfer_request_from_dict,
        transfer_request_to_dict,
    )

    req = TransferRequest(source=EndpointConfig(), destination=EndpointConfig(), require_reversibility=True)
    assert transfer_request_from_dict(transfer_request_to_dict(req)).require_reversibility is True


def test_plan_policy_stamps_execute_and_never_clears_it():
    from types import SimpleNamespace

    from services.reversibility_sources import stamp_plan_requirement

    req = SimpleNamespace(require_reversibility=False)
    stamp_plan_requirement(req, {"require_reversibility": True})
    assert req.require_reversibility is True
    stamp_plan_requirement(req, {})
    assert req.require_reversibility is True


def test_plan_report_reads_the_active_revision_and_the_plan_sample():
    from services.reversibility_sources import plan_report
    from services.transfer_plan_store import PlanRevision, TransferPlanRecord

    plan = TransferPlanRecord(
        id="plan-1", name="p", status="mapped",
        source={"format": "postgresql", "table": "payments"},
        destination={"format": "postgresql", "table": "payments"},
        source_columns=["id", "amount"], source_schema={"id": "INTEGER", "amount": "NUMERIC(12,4)"},
        target_columns=["id", "amount"], target_schema={"id": "INTEGER", "amount": "NUMERIC(12,2)"},
        sample_rows=[{"id": 1, "amount": "1.2345"}, {"id": 2, "amount": "2.5"}],
        policies={"require_reversibility": True},
        revisions=[PlanRevision(version=1, mappings=[{"source": "id", "target": "id"},
                                                     {"source": "amount", "target": "amount"}],
                                transforms=[], validation={}, mapping_hash="h", source_schema_hash="s",
                                target_schema_hash="t", agents_used=[])],
        active_version=1,
    )
    report = plan_report(plan)
    assert report["scope"] == "plan" and report["require_reversibility"] is True
    assert report["data_checked"] == {"rows": 2, "source": "plan_sample", "population_rows": None}
    assert report["verdict"] == PARTLY
    assert report["gate"]["status"] == "block"
    amount = next(c for c in report["tables"][0]["columns"] if c["source"] == "amount")
    assert amount["check"]["rows_not_fitting"] == 1


# ----------------------------------------------------- in-process engine e2e


def _exec(path: Path, sql: str, rows=None) -> None:
    conn = sqlite3.connect(str(path))
    try:
        with conn:
            conn.executemany(sql, rows) if rows is not None else conn.execute(sql)
    finally:
        conn.close()


@pytest.fixture()
def fake_store(monkeypatch):
    import src.transfer.engine as engine_mod
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    class _Store(_FakeMongo):
        def update_job_fields(self, job_id, fields, **_kwargs):
            return super().update_job_fields(job_id, fields)

    fake = _Store()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    return fake


def _run(fake, src: Path, dst: Path, *, require: bool, transform: str):
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    req = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="people"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="people"),
        mappings=[{"source": "id", "target": "id", "transform": "none", "confidence": 0.99,
                   "source_type": "INTEGER", "target_type": "INTEGER"},
                  {"source": "email", "target": "email", "transform": transform, "confidence": 0.99,
                   "source_type": "TEXT", "target_type": "TEXT"}],
        sync_mode="full_refresh_overwrite",
        stream_contracts=[{"name": "people", "primary_key": ["id"], "selected": True}],
        validation_mode="strict", skip_preflight=False, require_reversibility=require,
        compliance_acknowledged=True,
    )
    job_id = "rev" + uuid.uuid4().hex[:12]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(req, job_id)


def test_engine_blocks_a_required_reversible_run_with_a_hashed_column(tmp_path, fake_store):
    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    _exec(src, "CREATE TABLE people (id INTEGER PRIMARY KEY, email TEXT)")
    _exec(src, "INSERT INTO people VALUES (?, ?)", [(i, f"u{i}@example.com") for i in range(1, 6)])

    blocked = _run(fake_store, src, dst, require=True, transform="hash_pii")
    assert not blocked.success
    assert "g24_reversibility" in str(blocked.error_details), blocked.error
    # The gate keeps its own headline: it names the lossy column, never "fidelity collapse".
    assert "people.email" in blocked.error and "fidelity collapse" not in blocked.error.lower()
    assert not dst.exists() or not sqlite3.connect(str(dst)).execute(
        "SELECT name FROM sqlite_master WHERE name='people'").fetchall()

    allowed = _run(fake_store, src, dst, require=False, transform="hash_pii")
    assert allowed.success, allowed.error


def test_job_report_reads_the_types_the_run_recorded():
    from services.reversibility_sources import job_report

    job = {"transfer_request": {
        "source": {"format": "postgresql", "table": "t"}, "destination": {"format": "postgresql", "table": "t"},
        "column_types": {"id": "INTEGER", "email": "TEXT"},
        "mappings": [{"source": "id", "target": "id", "target_type": "BIGINT"},
                     {"source": "email", "target": "email", "target_type": "TEXT"}],
        "require_reversibility": True,
    }}
    report = job_report(job, "job-1")
    assert report["scope"] == "job" and report["verdict"] == REVERSIBLE
    assert report["gate"]["status"] == "pass" and report["data_checked"]["source"] == "none"
