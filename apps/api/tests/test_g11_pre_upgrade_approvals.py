"""G11 re-review H1: approvals stamped before the upgrade still hold.

Schedules and saved jobs carry ``approved_ddl_identity_hash`` /
``approved_decision_artifact_hash`` stamped by a pre-G11 Validate over the
operator's own Map targets. G11's Execute adopts an existing table's legacy
column (``customer_..._cents`` cut at 63, BigQuery ``customername``) before it
checks the approval, so the first beat after the upgrade failed "DDL identity
mismatch ... Re-validate" for exactly the jobs adoption exists for.

Adoption is deterministic from the live columns, so the approval is accepted
when it matches the operator's contract (``operator_rows``: the rows before
adoption); the run evidence names the adopted columns. A Map that really
changed is still refused.

Final review B1: the fallback trusted any row carrying ``map_target``, a key
clients could send. Execute then wrote ``v_evil`` under an approval for
``v_ok``. Now ``map_target`` is stripped from every inbound request, and the
fallback only uses an adoption the server made in this run: Execute re-runs
``adopt_existing_columns`` over the live columns it read and requires the same
rows (``engine_identity.verified_identity_maps``).
"""

from __future__ import annotations

import uuid

import pytest

from tests import g11_studio_flow as flow
from tests import r4_route_harness as h

pytestmark = pytest.mark.timeout(600)

LONG = "customer_lifetime_value_after_discounts_and_refunds_in_euro_cents_x"  # 68
CUT = LONG[:63]


def _operator_and_adopted(dest_db: str, targets: list[str], existing: list[str]):
    from services.target_name_legacy import adopt_existing_columns
    from src.transfer.engine_identity import verified_identity_maps

    operator = [{"source": f"s{i}", "target": t, "source_type": "INTEGER",
                 "target_type": "BIGINT" if dest_db != "bigquery" else "INT64"}
                for i, t in enumerate(targets)]
    adopted, notes = adopt_existing_columns(dest_db, operator, existing)
    assert notes, "the fixture must adopt a legacy column"
    # What Execute hands the identity checks: the rows, with the adoption it
    # proved against the live columns it read.
    return operator, verified_identity_maps(adopted, {"dest": dest_db, "live": existing})


@pytest.mark.parametrize(
    ("dest_db", "targets", "existing"),
    [("postgresql", ["id", LONG], ["id", CUT]),
     ("bigquery", ["CustomerID", "CustomerName"], ["customerid", "customername"])],
)
def test_pre_upgrade_ddl_identity_stamp_still_holds(dest_db, targets, existing):
    from services.decision_kernel import ddl_identity_report
    from src.transfer.engine_identity import _enforce_ddl_identity, verified_identity_maps

    operator, adopted = _operator_and_adopted(dest_db, targets, existing)
    stamp = ddl_identity_report(operator, dest_db=dest_db)["ddl_identity_hash"]  # pre-G11 Validate
    pf: dict = {"warnings": []}
    assert _enforce_ddl_identity(pf, adopted, dest_db=dest_db,
                                 approved_ddl_identity_hash=stamp) is None
    evidence = " ".join(str(w) for w in pf["warnings"])
    assert "operator's Map" in evidence and existing[-1] in evidence, pf["warnings"]

    changed = [dict(r) for r in adopted]
    changed[-1]["source"] = "other_column"
    changed = verified_identity_maps(changed, {"dest": dest_db, "live": existing})
    assert "DDL identity mismatch" in str(
        _enforce_ddl_identity({}, changed, dest_db=dest_db, approved_ddl_identity_hash=stamp))


@pytest.mark.parametrize(
    ("dest_db", "targets", "existing"),
    [("postgresql", ["id", LONG], ["id", CUT]),
     ("bigquery", ["CustomerID", "CustomerName"], ["customerid", "customername"])],
)
def test_pre_upgrade_decision_artifact_stamp_still_holds(dest_db, targets, existing):
    from services.decision_kernel import build_artifact_from_mappings
    from src.transfer.engine_identity import _enforce_decision_artifact

    operator, adopted = _operator_and_adopted(dest_db, targets, existing)
    art = build_artifact_from_mappings(operator, dest_db=dest_db, route_id=f"validate:{dest_db}",
                                       sync_mode="full_refresh_append").to_dict()
    err, accepted = _enforce_decision_artifact(
        {"proof_bundle": {"decision_artifact": art}}, adopted, dest_db=dest_db,
        approved_decision_artifact_hash=art["content_hash"], decision_artifact=art,
        sync_mode="full_refresh_append",
    )
    assert err is None and accepted is not None, err

    changed = [dict(r) for r in adopted]
    changed[-1]["source"] = "other_column"
    err, accepted = _enforce_decision_artifact(
        {"proof_bundle": {"decision_artifact": art}}, changed, dest_db=dest_db,
        approved_decision_artifact_hash=art["content_hash"], decision_artifact=art,
        sync_mode="full_refresh_append",
    )
    assert err is not None and accepted is None


APPROVED = [{"source": "id", "target": "id", "source_type": "INTEGER", "target_type": "BIGINT"},
            {"source": "ssn", "target": "ssn_secure", "source_type": "TEXT", "target_type": "TEXT"}]
CRAFTED = [dict(APPROVED[0]),
           {"source": "ssn", "target": "public_notes", "map_target": "ssn_secure",
            "source_type": "TEXT", "target_type": "TEXT"}]


def test_a_client_supplied_map_target_does_not_pass_the_approval():
    """The reviewer's unit probe: a crafted row is not an adoption."""
    from services.decision_kernel import build_artifact_from_mappings, ddl_identity_report
    from src.transfer.engine_identity import (
        _enforce_ddl_identity,
        _enforce_decision_artifact,
        verified_identity_maps,
    )

    stamp = ddl_identity_report(APPROVED, dest_db="postgresql")["ddl_identity_hash"]
    crafted = [dict(r) for r in CRAFTED]
    assert "DDL identity mismatch" in str(_enforce_ddl_identity(
        {"warnings": []}, crafted, dest_db="postgresql", approved_ddl_identity_hash=stamp))
    # Even with the live columns, re-running adoption does not produce it.
    proved = verified_identity_maps(
        crafted, {"dest": "postgresql", "live": ["id", "ssn_secure", "public_notes"]})
    assert proved.verified_adoption is None
    assert _enforce_ddl_identity({"warnings": []}, proved, dest_db="postgresql",
                                 approved_ddl_identity_hash=stamp)
    art = build_artifact_from_mappings(APPROVED, dest_db="postgresql",
                                       route_id="validate:postgresql",
                                       sync_mode="full_refresh_append").to_dict()
    err, accepted = _enforce_decision_artifact(
        {"proof_bundle": {"decision_artifact": art}}, crafted, dest_db="postgresql",
        approved_decision_artifact_hash=art["content_hash"], decision_artifact=art,
        sync_mode="full_refresh_append")
    assert err is not None and accepted is None


def test_inbound_map_target_is_stripped():
    from src.transfer.models import EndpointConfig, TransferRequest, transfer_request_from_dict

    req = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database="/x.db", table="t"),
        destination=EndpointConfig(kind="database", format="postgresql", table="d"),
        mappings=[dict(r) for r in CRAFTED])
    assert all("map_target" not in m for m in req.mappings)
    assert req.mappings[1]["target"] == "public_notes"
    again = transfer_request_from_dict({"source": {"kind": "file"}, "destination": {},
                                        "mappings": [dict(r) for r in CRAFTED]})
    assert all("map_target" not in m for m in again.mappings)


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def test_a_job_stamped_before_the_upgrade_runs_green_live(client, monkeypatch):
    """Live PostgreSQL: Validate with adoption switched off (the pre-G11
    stamp, over the operator's targets), then Execute with adoption on."""
    import services.target_name_legacy as legacy

    token = uuid.uuid4().hex[:8]
    src_table, dest_table = f"g11u_src_{token}", f"g11u_dst_{token}"
    h.execute("postgresql", f"CREATE TABLE {src_table} (id int PRIMARY KEY, v int)")  # nosec B608
    h.execute("postgresql", f"INSERT INTO {src_table} VALUES (1, 10)")  # nosec B608
    h.execute("postgresql", f'CREATE TABLE {dest_table} (id bigint, "{CUT}" bigint)')
    try:
        src, dst = flow.connectors(token, "postgresql", "postgresql")
        source = {"kind": "database", "format": "postgresql", "connector_id": src.id,
                  "database": h.PG["database"], "table": src_table}
        destination = {"kind": "database", "format": "postgresql", "connector_id": dst.id,
                       "database": h.PG["database"], "table": dest_table}
        mappings = [
            {"source": "id", "target": "id", "confidence": 1.0, "source_type": "INTEGER",
             "target_type": "BIGINT"},
            {"source": "v", "target": LONG, "confidence": 1.0, "source_type": "INTEGER",
             "target_type": "BIGINT"},
        ]
        real = legacy.adopt_existing_columns
        with monkeypatch.context() as m:  # the pre-G11 Validate: no adoption, no g23 gates
            m.setattr(legacy, "adopt_existing_columns", lambda d, rows, e: (
                [dict(legacy._as_dict(r)) for r in (rows or [])], []))
            m.setattr(legacy, "target_name_gates", lambda *a, **k: [])
            plan_id, pf = flow.validate(
                client, source=source, destination=destination, columns=["id", "v"],
                types={"id": "INTEGER", "v": "INTEGER"}, mappings=mappings,
                sync_mode="full_refresh_append", schema_policy="propagate_all",
            )
        assert legacy.adopt_existing_columns is real
        assert (pf.get("proof_bundle") or {}).get("ddl_identity"), sorted(pf)
        run = flow.execute(client, plan_id=plan_id, pf=pf, source=source,
                           destination=destination, mappings=mappings,
                           sync_mode="full_refresh_append", schema_policy="propagate_all",
                           expect_status=None)
        assert run.get("success") is True, (run.get("error"), run.get("error_details"))
        import json

        evidence = json.dumps(run, default=str)
        assert "approval matched the operator's Map" in evidence and CUT in evidence
        rows = h.query("postgresql", f'SELECT id, "{CUT}" FROM {dest_table}')  # nosec B608
        assert [tuple(int(x) for x in r) for r in rows] == [(1, 10)]
    finally:
        h.drop_table("postgresql", src_table)
        h.drop_table("postgresql", dest_table)


def test_crafted_map_target_on_the_real_routes_is_refused(client):
    """The reviewer's live probe: Validate approves v -> v_ok; Execute sends
    {target: v_evil, map_target: v_ok}. Nothing may be written to v_evil."""
    token = uuid.uuid4().hex[:8]
    src_table, dest_table = f"g11b_src_{token}", f"g11b_dst_{token}"
    h.execute("postgresql", f"CREATE TABLE {src_table} (id int PRIMARY KEY, v int)")  # nosec B608
    h.execute("postgresql", f"INSERT INTO {src_table} VALUES (1, 10)")  # nosec B608
    h.execute("postgresql", f"CREATE TABLE {dest_table} (id bigint, v_ok bigint, v_evil bigint)")
    try:
        src, dst = flow.connectors(token, "postgresql", "postgresql")
        source = {"kind": "database", "format": "postgresql", "connector_id": src.id,
                  "database": h.PG["database"], "table": src_table}
        destination = {"kind": "database", "format": "postgresql", "connector_id": dst.id,
                       "database": h.PG["database"], "table": dest_table}
        mappings = [
            {"source": "id", "target": "id", "confidence": 1.0, "source_type": "INTEGER",
             "target_type": "BIGINT"},
            {"source": "v", "target": "v_ok", "confidence": 1.0, "source_type": "INTEGER",
             "target_type": "BIGINT"},
        ]
        plan_id, pf = flow.validate(
            client, source=source, destination=destination, columns=["id", "v"],
            types={"id": "INTEGER", "v": "INTEGER"}, mappings=mappings,
            sync_mode="full_refresh_append", schema_policy="propagate_all")
        crafted = [dict(m) for m in (pf.get("mappings") or mappings)]
        for m in crafted:
            if m.get("target") == "v_ok":
                m["target"], m["map_target"] = "v_evil", "v_ok"
        run = flow.execute(client, plan_id=plan_id, pf={**pf, "mappings": crafted},
                           source=source, destination=destination, mappings=crafted,
                           sync_mode="full_refresh_append", schema_policy="propagate_all",
                           expect_status=None)
        assert run.get("success") is not True, run
        assert "identity" in str(run.get("error") or "").lower(), run.get("error")
        rows = h.query("postgresql", f"SELECT id, v_ok, v_evil FROM {dest_table}")  # nosec B608
        assert rows == [], rows
    finally:
        h.drop_table("postgresql", src_table)
        h.drop_table("postgresql", dest_table)
