"""G11 / N-F1: DynamoDB, Redis, Kafka and SaaS writers keep the mapped names.

These writers named every target with SQL identifier rules (cut at 63,
``First Name`` -> ``First_Name``, ``address.city`` -> ``address_city``):
a property Airtable / Notion / HubSpot does not have, and a renamed attribute
in a key-value store or a Kafka message. They now store names exactly as
mapped (``destination_naming.EXACT_NAME_STORES``), and the Gate-8 read-back
asks for the same names.

Existing destinations keep working:

* DynamoDB / Redis records holding an earlier run's renamed field: Execute
  (and Validate g23e) refuse a non-overwrite run that would leave it stale,
  with the remedies and the ``legacy_field_policy=write_new`` option - the
  MongoDB / Elasticsearch rule;
* a SaaS object whose field has the old spelling (and not the new one):
  the target adopts that existing field, as an existing SQL table's column.

Live: Redis (Studio routes: plan Validate -> approved hashes ->
``/api/v1/transfer/run``). DynamoDB, Kafka and the SaaS APIs are not
available here: their writers are driven up to name resolution, and their
existing-field reads through fakes.
"""

from __future__ import annotations

import json
import shutil
import socket
import sqlite3
import tempfile
import uuid
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from tests import g11_studio_flow as flow
from tests import r4_route_harness as h

pytestmark = pytest.mark.timeout(600)


@pytest.fixture(autouse=True)
def _written_before_g11(monkeypatch):
    """These collections / indexes / prefixes / tables were written by runs
    before the G11 upgrade, so the old-field check applies to them
    (``naming_history.object_history``; final review M-d)."""
    import services.naming_history as naming_history

    monkeypatch.setattr(naming_history, "object_history",
                        lambda *a, **k: naming_history.LEGACY)

TARGETS = ["First Name", "address.city", "x" * 70]


class _Stop(RuntimeError):
    pass


@pytest.mark.parametrize(
    ("module", "host"),
    [("dynamodb", "h"), ("redis", "h"), ("kafka", "h"), ("hubspot", "h"),
     ("zendesk", "acme.zendesk.com"), ("airtable", "h"), ("notion", "h"),
     ("stripe", "h"), ("shopify", "acme.myshopify.com")],
)
def test_writer_resolves_the_mapped_names_exactly(module, host):
    import importlib

    mod = importlib.import_module(f"connectors.{module}_writer")
    seen: list[list[str]] = []
    real = mod.resolve_target_columns

    def spy(*args, **kwargs):
        cols, _types = real(*args, **kwargs)
        seen.append(list(cols))
        raise _Stop  # before any network call

    with patch.object(mod, "resolve_target_columns", side_effect=spy):
        try:
            mod.write_mapped_rows(
                host=host, port=1, database="d", username="u", password="p", schema="s",
                connection_string="", ssl=False, table_name="t", headers=["a", "b", "c"],
                data_rows=[["1", "2", "3"]],
                mappings=[{"source": s, "target": t} for s, t in zip("abc", TARGETS)],
                column_types={"a": "TEXT", "b": "TEXT", "c": "TEXT"}, create_table=True,
            )
        except _Stop:
            pass
    assert seen == [TARGETS], seen


def test_validate_accepts_exact_names_for_these_destinations():
    from connectors.destination_naming import target_name_issues

    for dest in ("dynamodb", "redis", "kafka", "airtable", "notion", "hubspot", "salesforce"):
        assert target_name_issues(dest, TARGETS) == [], dest


# -- live Redis: names land exactly and Gate-8 reads them ---------------------


def _redis_up() -> bool:
    try:
        socket.create_connection(("localhost", 6379), timeout=1).close()
        import redis

        redis.Redis(host="localhost", port=6379, socket_timeout=2).ping()
    except Exception:  # noqa: BLE001 - any failure means "not available"
        return False
    return True


live_redis = pytest.mark.skipif(not _redis_up(), reason="no Redis on :6379")


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


@pytest.fixture
def src():
    path = Path(tempfile.mkdtemp(prefix="g11k"))
    db = path / "src.sqlite"
    conn = sqlite3.connect(str(db))
    with conn:
        conn.execute("CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT, city TEXT)")
        conn.executemany("INSERT INTO people VALUES (?, ?, ?)",
                         [(1, "Ada", "Paris"), (2, "Bob", "Oslo")])
    conn.close()
    try:
        yield db
    finally:
        shutil.rmtree(path, ignore_errors=True)


@pytest.fixture
def prefix():
    import redis

    name = f"g11k_{uuid.uuid4().hex[:8]}"
    r = redis.Redis(host="localhost", port=6379)
    try:
        yield name
    finally:
        for key in r.scan_iter(match=f"{name}:*"):
            r.delete(key)
        r.close()


MAPPINGS = [
    {"source": "id", "target": "id", "confidence": 1.0, "target_type": "INTEGER"},
    {"source": "name", "target": "First Name", "confidence": 1.0, "target_type": "VARCHAR"},
    {"source": "city", "target": "address.city", "confidence": 1.0, "target_type": "VARCHAR"},
]


def _redis_route(prefix_name: str, src: Path):
    from services.connector_store import create_connector

    dst = create_connector({
        "name": f"g11k-{uuid.uuid4().hex[:6]}", "role": "destination", "workspace_id": "",
        "type": "redis", "host": "localhost", "port": 6379, "database": "0", "ssl": False,
    })
    source = {"kind": "database", "format": "sqlite", "database": str(src), "table": "people"}
    destination = {"kind": "database", "format": "redis", "connector_id": dst.id,
                   "database": "0", "table": prefix_name}
    return source, destination


@live_redis
def test_redis_stores_exact_field_names_and_gate8_reads_them(client, src, prefix):
    import redis

    source, destination = _redis_route(prefix, src)
    contracts = [{"name": "people", "sync_mode": "full_refresh_overwrite",
                  "primary_key": "id", "selected": True}]
    plan_id, pf = flow.validate(
        client, source=source, destination=destination, columns=["id", "name", "city"],
        types={"id": "INTEGER", "name": "TEXT", "city": "TEXT"}, mappings=MAPPINGS,
        sync_mode="full_refresh_overwrite", stream_contracts=contracts,
    )
    assert pf.get("passed") is True, flow.blockers(pf)
    run = flow.execute(client, plan_id=plan_id, pf=pf, source=source, destination=destination,
                       mappings=MAPPINGS, sync_mode="full_refresh_overwrite",
                       stream_contracts=contracts)
    assert run.get("success") is True, run.get("error")
    assert (run.get("reconciliation") or {}).get("passed") is True, run.get("reconciliation")
    r = redis.Redis(host="localhost", port=6379)
    try:
        docs = [json.loads(r.get(k)) for k in sorted(r.scan_iter(match=f"{prefix}:*"))]
    finally:
        r.close()
    assert docs and all({"First Name", "address.city"} <= set(d) for d in docs), docs
    assert not any("First_Name" in d or "address_city" in d for d in docs), docs


@live_redis
def test_redis_existing_old_field_is_refused_unless_overwrite(prefix):
    import redis

    from services.source_filter_gate import request_refusal
    from src.transfer.models import EndpointConfig, TransferRequest

    r = redis.Redis(host="localhost", port=6379)
    try:
        r.set(f"{prefix}:9", json.dumps({"id": 9, "First_Name": "Old"}))
    finally:
        r.close()

    def request(sync_mode: str, extra: dict | None = None) -> TransferRequest:
        return TransferRequest(
            mappings=[dict(m) for m in MAPPINGS],
            source=EndpointConfig(kind="database", format="sqlite", database="/x.db",
                                  table="people"),
            destination=EndpointConfig(kind="database", format="redis", host="localhost",
                                       port=6379, database="0", table=prefix,
                                       extra=dict(extra or {})),
            sync_mode=sync_mode,
        )

    refused = request_refusal(request("upsert"))
    assert "'First_Name'" in refused and "'First Name'" in refused, refused
    assert request_refusal(request("full_refresh_overwrite")) == ""
    acked = request("upsert", {"legacy_field_policy": "write_new"})
    assert request_refusal(acked) == ""
    assert acked.destination.extra["legacy_field_ack"]


def test_dynamodb_existing_old_attribute_is_refused():
    from services.source_filter_gate import request_refusal
    from src.transfer.models import EndpointConfig, TransferRequest

    client = MagicMock()
    client.scan.return_value = {"Items": [{"First_Name": {"S": "Old"}}]}
    request = TransferRequest(
        mappings=[dict(m) for m in MAPPINGS],
        source=EndpointConfig(kind="database", format="sqlite", database="/x.db", table="p"),
        destination=EndpointConfig(kind="database", format="dynamodb", host="us-east-1",
                                   table="people"),
        sync_mode="upsert",
    )
    with patch("connectors.aws_common.boto3_client", return_value=client):
        refused = request_refusal(request)
    assert "'First_Name'" in refused and "UpdateItem" in refused, refused
    names = client.scan.call_args.kwargs["ExpressionAttributeNames"]
    assert set(names.values()) == {"First_Name", "address_city"}


def test_saas_target_adopts_the_existing_old_spelling():
    """Airtable field ``First_Name`` (the spelling earlier runs sent) and no
    ``First Name``: the run keeps writing ``First_Name``; Validate notes it."""
    from services.preflight_transfer_policy import run_transfer_policy_gates
    from services.source_filter_gate import request_refusal
    from src.transfer.models import EndpointConfig, TransferRequest

    gates = {g["id"]: g for g in run_transfer_policy_gates(
        sync_mode="full_refresh_append", dest_type="airtable", source_type="sqlite",
        source_kind="database", mappings=[dict(m) for m in MAPPINGS], destination_table="tbl",
        destination_columns=["id", "First_Name", "address.city"],
    ) if str(g.get("id", "")).startswith("g23")}
    assert "'First_Name'" in gates["g23d_existing_target_columns"]["message"], gates

    request = TransferRequest(
        mappings=[dict(m) for m in MAPPINGS],
        source=EndpointConfig(kind="database", format="sqlite", database="/x.db", table="p"),
        destination=EndpointConfig(kind="database", format="airtable", host="h", table="tbl"),
        sync_mode="full_refresh_append",
    )
    info = {"connected": True, "table_exists": True,
            "schema": {"id": "INTEGER", "First_Name": "VARCHAR", "address.city": "VARCHAR"}}
    with patch("src.transfer.endpoint_intelligence.introspect_endpoint", return_value=info):
        assert request_refusal(request) == ""
    assert [m["target"] for m in request.mappings] == ["id", "First_Name", "address.city"]
