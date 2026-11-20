"""D325 on the Mongo pair: the transform proof compares at the accepted carrier.

QA job 6afbb18f: PostgreSQL → a new MongoDB collection, upsert, pipelines on
``name``/``note`` and a CAST_AND_CONTINUE contract accepting millisecond
truncation of ``ts`` and ``updated_at``. The write truncated as accepted; the
proof compared the microsecond source with the millisecond BSON date, failed
with "Destination drift on column ts, updated_at is not explained by any recipe
step", and the collection was dropped.

Rule R3 / R9: compare at the precision the operator accepted, and only there.
A column whose narrowing was not accepted is compared at full precision, and a
tampered value still fails and is named.

Live: PostgreSQL source, MongoDB destination, through the engine.
"""

from __future__ import annotations

import datetime as dt
import os
import socket
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

pytestmark = pytest.mark.timeout(600)

_COLS = ["id", "name", "note", "ts", "updated_at"]
_UTC = dt.timezone.utc
_ROWS = [
    (1, "  ada ", "x", dt.datetime(2026, 1, 2, 3, 4, 5, 123456, _UTC), dt.datetime(2026, 1, 2, 3, 4, 5, 987654)),
    (2, "bob", "", dt.datetime(2026, 1, 2, 3, 4, 6, 1, _UTC), dt.datetime(2026, 1, 2, 3, 4, 6, 500999)),
    (3, None, None, None, dt.datetime(2026, 1, 2, 3, 4, 7, 250000)),
]


def _up(port: int) -> bool:
    try:
        socket.create_connection(("127.0.0.1", port), timeout=1).close()
        return True
    except OSError:
        return False


@pytest.fixture()
def fake_jobs(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


@pytest.fixture()
def pg_source():
    from tests import r4_route_harness as h

    table = "d325m_" + uuid.uuid4().hex[:8]
    conn = h.pg_conn()  # skips when PostgreSQL is absent
    try:
        cur = conn.cursor()
        cur.execute(
            f'CREATE TABLE public."{table}" (id integer PRIMARY KEY, name text, note text, '  # nosec B608
            "ts timestamptz(6), updated_at timestamp(6))"
        )
        cur.executemany(f'INSERT INTO public."{table}" VALUES (%s, %s, %s, %s, %s)', _ROWS)  # nosec B608
        conn.commit()
    finally:
        conn.close()
    try:
        yield table
    finally:
        h.drop_table("postgresql", table)


@pytest.fixture()
def mongo():
    pymongo = pytest.importorskip("pymongo")
    if not _up(27017):
        pytest.skip("MongoDB not reachable")
    client = pymongo.MongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=3000)
    made: list[str] = []
    try:
        yield client, made
    finally:
        for name in made:
            client["dataflow_test"][name].drop()
        client.close()


def _contract(column: str, source_type: str) -> dict:
    from services.migration_risk_contract import create_migration_risk_contract

    return create_migration_risk_contract(
        column=column,
        source_type=source_type,
        destination_type="date",
        approved_by="risk-approver@example.com",
        reason="Accept millisecond truncation",
        execution_policy="CAST_AND_CONTINUE",
        expected_precision_loss=True,
    ).to_dict()


def _request(table: str, collection: str, *, accept: tuple[str, ...]) -> TransferRequest:
    from services.column_transform_chain import compile_column_chains, split_column_transforms
    from services.shape_models import ShapeRecipe
    from tests.typed_fidelity_helpers import pg_endpoint

    singles, chains = split_column_transforms({"name": ["trim", "upper"], "note": ["trim", "lower"]})
    recipe, refusal = compile_column_chains(chains, _COLS)
    assert not refusal, refusal
    source_types = {"ts": "TIMESTAMPTZ(6)", "updated_at": "TIMESTAMP(6)"}
    mappings = []
    for c in _COLS:
        m = {"source": c, "target": c, "approved": True, "confidence": 0.99}
        if c in singles:
            m.update(transform=singles[c], transform_override=True)
        if c in accept:
            m.update(risk_acknowledged=True, risk_contract=_contract(c, source_types[c]))
        mappings.append(m)
    return TransferRequest(
        source=pg_endpoint(table),
        destination=EndpointConfig(
            kind="database", format="mongodb", host="127.0.0.1", port=27017,
            database="dataflow_test", collection=collection, table=collection,
        ),
        mappings=mappings,
        sync_mode="incremental_upsert",
        validation_mode="strict",
        shape_recipe=recipe,
        approved_shape_recipe_hash=ShapeRecipe.parse(recipe).recipe_hash,
        stream_contracts=[
            {"name": collection, "primary_key": ["id"], "cursor_field": "updated_at",
             "cursor_semantics": "modification_timestamp",
             "sync_mode": "incremental_upsert", "selected": True}
        ],
        triggered_by="risk-approver@example.com",
    )


def _run(fake, request: TransferRequest):
    job_id = "d325m" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


def _ms(value):
    if value is None:
        return None
    value = value if value.tzinfo else value.replace(tzinfo=_UTC)
    return value.replace(microsecond=value.microsecond - value.microsecond % 1000)


def test_accepted_millisecond_truncation_verifies_and_keeps_the_collection(
    fake_jobs, pg_source, mongo
):
    client, made = mongo
    collection = pg_source + "_out"
    made.append(collection)
    result = _run(fake_jobs, _request(pg_source, collection, accept=("ts", "updated_at")))

    assert result.success, result.error
    proof = (result.reconciliation or {}).get("transform_applied") or {}
    assert proof.get("status") == "verified", proof
    assert "not explained by any recipe step" not in str(result.error or "")
    docs = list(client["dataflow_test"][collection].find({}, {"_id": 0}).sort("id", 1))
    got = [
        (d["id"], d.get("name"), d.get("note"), _ms(d.get("ts")), _ms(d.get("updated_at")))
        for d in docs
    ]
    assert got == [
        (1, "ADA", "x", _ms(_ROWS[0][3]), _ms(_ROWS[0][4])),
        (2, "BOB", "", _ms(_ROWS[1][3]), _ms(_ROWS[1][4])),
        (3, None, None, None, _ms(_ROWS[2][4])),
    ]


def test_a_tampered_value_still_fails_the_proof_and_is_named(
    fake_jobs, pg_source, mongo, monkeypatch
):
    import src.transfer.reconcile_step as rs

    client, made = mongo
    collection = pg_source + "_tamper"
    made.append(collection)
    original = rs._transform_applied_proof

    def _tamper_then_prove(**kwargs):
        client["dataflow_test"][collection].update_one({"id": 2}, {"$set": {"name": "EVE"}})
        return original(**kwargs)

    monkeypatch.setattr(rs, "_transform_applied_proof", _tamper_then_prove)
    result = _run(fake_jobs, _request(pg_source, collection, accept=("ts", "updated_at")))

    assert not result.success
    proof = (result.reconciliation or {}).get("transform_applied") or {}
    assert proof.get("status") == "failed", proof
    assert proof.get("culprit_columns") == ["name"], proof
    assert (proof.get("culprit_keys") or {}).get("changed") == ["2"], proof


def test_an_unaccepted_narrowing_is_refused_by_the_write_not_blamed_on_a_recipe(
    fake_jobs, pg_source, mongo
):
    """AC-3: only the accepted column is narrowed; the other keeps full precision."""
    client, made = mongo
    collection = pg_source + "_partial"
    made.append(collection)
    result = _run(fake_jobs, _request(pg_source, collection, accept=("ts",)))

    assert not result.success
    # Refused up front (preflight answers before the write guard would), with
    # the unaccepted column named; never a proof "drift" after the write.
    assert result.records_transferred == 0
    assert "updated_at" in str(result.error or ""), result.error
    assert "not explained by any recipe step" not in str(result.error or "")
    assert client["dataflow_test"][collection].count_documents({}) == 0


def test_text_instants_truncated_under_the_contract_verify(fake_jobs, mongo, tmp_path):
    """A text source holding ISO instants, cast to a BSON date under a contract.

    The writer truncates every acknowledged temporal column it actually
    truncated; the proof must compare that column at the same carrier, not
    only the columns whose *source declaration* is a datetime.
    """
    import sqlite3

    from services.column_transform_chain import compile_column_chains, split_column_transforms
    from services.migration_risk_contract import create_migration_risk_contract
    from services.shape_models import ShapeRecipe

    client, made = mongo
    collection = "d325t_" + uuid.uuid4().hex[:8]
    made.append(collection)
    src = tmp_path / "src.sqlite"
    conn = sqlite3.connect(str(src))
    with conn:
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, ts TEXT)")
        conn.executemany(
            "INSERT INTO t VALUES (?, ?, ?)",
            [(1, " ada ", "2026-01-02T03:04:05.123456+00:00"), (2, "bob", "2026-01-02T03:04:06.000001+00:00")],
        )
    conn.close()
    singles, chains = split_column_transforms({"name": ["trim", "upper"]})
    recipe, refusal = compile_column_chains(chains, ["id", "name", "ts"])
    assert not refusal, refusal
    contract = create_migration_risk_contract(
        column="ts", source_type="TEXT", destination_type="date",
        approved_by="risk-approver@example.com", reason="Accept millisecond truncation",
        execution_policy="CAST_AND_CONTINUE", expected_precision_loss=True,
    ).to_dict()
    mappings = [
        {"source": "id", "target": "id", "approved": True, "confidence": 0.99},
        {"source": "name", "target": "name", "approved": True, "confidence": 0.99},
        {"source": "ts", "target": "ts", "approved": True, "confidence": 0.99,
         "target_type": "date", "transform": "datetime", "transform_override": True,
         "risk_acknowledged": True, "risk_contract": contract},
    ]
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=EndpointConfig(
            kind="database", format="mongodb", host="127.0.0.1", port=27017,
            database="dataflow_test", collection=collection, table=collection,
        ),
        mappings=mappings,
        sync_mode="full_refresh_overwrite",
        validation_mode="strict",
        shape_recipe=recipe,
        approved_shape_recipe_hash=ShapeRecipe.parse(recipe).recipe_hash,
        triggered_by="risk-approver@example.com",
    )
    result = _run(fake_jobs, request)

    assert result.success, result.error
    proof = (result.reconciliation or {}).get("transform_applied") or {}
    assert proof.get("status") == "verified", proof
    docs = list(client["dataflow_test"][collection].find({}, {"_id": 0}).sort("id", 1))
    assert [(d["id"], d["name"], _ms(d["ts"])) for d in docs] == [
        (1, "ADA", dt.datetime(2026, 1, 2, 3, 4, 5, 123000, _UTC)),
        (2, "BOB", dt.datetime(2026, 1, 2, 3, 4, 6, 0, _UTC)),
    ]
