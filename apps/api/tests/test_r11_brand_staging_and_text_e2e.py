"""B1 branding (QA handover, build 9c75ada4): ``*_df_staging`` and server names in text.

QA M10: write_via_staging left ``qa_*_df_staging`` tables behind and "the
internal df name shows" — the destination's object listing showed them.
Root cause: ``services/pre_ingestion_staging.staging_table_name`` hard-codes
``{table}_df_staging`` and that artifact is missing from
``services.brand.ARTIFACTS``, so ``is_internal_object_name`` (which every
operator-facing listing filters through) did not know it, and
``DATAWRAP_BRAND_OBJECT_NAMES`` could not switch it.

Contract, through ``POST /api/v1/transfer/run`` into a real PostgreSQL and
``POST /api/v1/transfer/introspect`` (the Destination step's listing):

* the staging table is the product's own object under either spelling and is
  hidden from the listing;
* flag off (default): ``{table}_df_staging`` exactly as before;
* flag on: a new run stages into ``{table}_dw_staging`` (audited), the old
  spelling's leftover is replaced, and the second run into the table the first
  run created writes the same rows (NULL, '', whitespace, unicode kept);
* flag off again: back to the legacy name, still working.

Also: the egress refusal and the BigQuery default dataset (switchable through
``name_for_new``, off by default).
"""

from __future__ import annotations

import json
import uuid

import pytest

from tests import r4_route_harness as h

pytestmark = pytest.mark.timeout(600)

ROWS = [(1, "a"), (2, None), (3, ""), (4, " "), (5, "Łukasz 山田 ✓")]


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.dest_config("postgresql")  # skip without PostgreSQL
    h.isolate(tmp_path, monkeypatch)
    monkeypatch.delenv("DATAWRAP_BRAND_OBJECT_NAMES", raising=False)
    monkeypatch.delenv("DATAFLOW_BRAND_OBJECT_NAMES", raising=False)
    from services import audit_log, brand

    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: None)
    brand.reset_resolution_cache()
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c
    brand.reset_resolution_cache()


def _exists(table: str, schema: str) -> bool:
    return bool(h.query(
        "postgresql",
        "SELECT 1 FROM information_schema.tables "  # nosec B608
        f"WHERE table_schema = '{schema}' AND table_name = '{table}'",
    ))


def _run(client, src_id, dst_id, src_table, dest_table) -> dict:
    mappings = [{"source": c, "target": c, "confidence": 1.0} for c in ("id", "note")]
    resp = client.post("/api/v1/transfer/run", data={
        "source_kind": "database", "source_format": "postgresql",
        "source_connector_id": src_id, "source_table": src_table,
        "dest_kind": "database", "dest_format": "postgresql",
        "dest_connector_id": dst_id, "dest_table": dest_table,
        "dest_database": h.PG["database"],
        "mappings_json": json.dumps(mappings), "sync_mode": "full_refresh_overwrite",
        "validation_mode": "balanced", "write_via_staging": "true",
        "skip_preflight": "true", "async_mode": "false",
    })
    body = resp.json()
    assert resp.status_code == 200 and body.get("success") is True, body
    from services.mongodb_service import get_mongodb_service

    job = get_mongodb_service().get_job(str(body["job_id"])) or {}
    return (job.get("destination_summary") or {})


def _listed(client, dst_id) -> set[str]:
    res = client.post("/api/v1/transfer/introspect", json={
        "source": {"kind": "database", "format": "postgresql", "connector_id": dst_id},
        "destination": {"kind": "database", "format": "postgresql", "connector_id": dst_id},
    })
    assert res.status_code == 200, res.text
    objs = res.json()["destination"].get("objects") or []
    return {str(o.get("name")).rsplit(".", 1)[-1] for o in objs if isinstance(o, dict)}


def _audit(action: str) -> list[dict]:
    from pathlib import Path

    from services import audit_log

    path = Path(audit_log.STORE_PATH)
    if not path.exists():
        return []
    rows = [json.loads(x) for x in path.read_text(encoding="utf-8").splitlines() if x]
    return [r for r in rows if r.get("action") == action]


def test_staging_table_is_internal_hidden_and_switches_with_the_flag(client, monkeypatch):
    from services.connector_store import create_connector

    token = uuid.uuid4().hex[:8]
    src_table, dest_table = f"r11b_src_{token}", f"r11b_dst_{token}"
    legacy, new = f"{dest_table}_df_staging", f"{dest_table}_dw_staging"
    schema = f"r11b_{token}"  # its own schema, so the listing is not a bounded page
    h.execute("postgresql", f"CREATE SCHEMA {schema}")
    h.execute("postgresql", f"CREATE TABLE {src_table} (id int PRIMARY KEY, note text)")
    h.execute(
        "postgresql",
        f"INSERT INTO {src_table} VALUES (1, 'a'), (2, NULL), (3, ''), (4, ' '), (5, 'Łukasz 山田 ✓')",
    )
    try:
        src = create_connector({"name": f"r11b-s-{token}", "role": "source", "workspace_id": "",
                                "ssl": False, **h.dest_config("postgresql")})
        dst = create_connector({"name": f"r11b-d-{token}", "role": "destination", "workspace_id": "",
                                "ssl": False, **h.dest_config("postgresql"), "schema": schema})

        # Run 1, flag off: the legacy name, exactly as before.
        from services import brand

        art = brand.artifact("pre_ingestion_staging_table")
        summary = _run(client, src.id, dst.id, src_table, dest_table)
        # The stage name comes from the job and the brand artifact. A clean
        # promote drops the stage (claude/r11-pg-writer), so it is not looked
        # for in the database; nothing of ours is left behind under either name.
        assert summary.get("staging_table") == legacy == f"{dest_table}{art.legacy}"
        assert not _exists(legacy, schema) and not _exists(new, schema)
        listed = _listed(client, dst.id)
        assert dest_table in listed
        assert legacy not in listed, "the product's staging table is shown to the operator"

        # Run 2, flag on, into the table run 1 created.
        monkeypatch.setenv("DATAWRAP_BRAND_OBJECT_NAMES", "1")
        summary = _run(client, src.id, dst.id, src_table, dest_table)
        assert summary.get("staging_table") == new == f"{dest_table}{brand.name_for_new('pre_ingestion_staging_table')}"
        assert not _exists(new, schema) and not _exists(legacy, schema)
        assert sorted(h.query("postgresql", f"SELECT id, note FROM {schema}.{dest_table} ORDER BY id")) == ROWS
        created = [e for e in _audit("brand.object_name.created")
                   if e["details"].get("kind") == "pre_ingestion_staging_table"]
        assert created and created[0]["details"]["name"] == "_dw_staging"
        assert new not in _listed(client, dst.id)

        # Run 3, flag off again: still works, legacy name again.
        monkeypatch.setenv("DATAWRAP_BRAND_OBJECT_NAMES", "0")
        summary = _run(client, src.id, dst.id, src_table, dest_table)
        assert summary.get("staging_table") == legacy
        assert not _exists(new, schema) and not _exists(legacy, schema)
        assert sorted(h.query("postgresql", f"SELECT id, note FROM {schema}.{dest_table} ORDER BY id")) == ROWS
    finally:
        h.drop_table("postgresql", src_table)
        h.execute("postgresql", f"DROP SCHEMA IF EXISTS {schema} CASCADE")


def test_staging_artifact_is_in_the_brand_inventory():
    from services import brand

    art = brand.artifact("pre_ingestion_staging_table")
    assert (art.legacy, art.new, art.shape, art.switchable) == (
        "_df_staging", "_dw_staging", "table_suffix", True,
    )
    for name in ("orders_df_staging", "PUBLIC.ORDERS_DF_STAGING", "orders_dw_staging"):
        assert brand.is_internal_object_name(name), name
    assert not brand.is_internal_object_name("df_staging")
    assert not brand.is_internal_object_name("orders_staging")


def test_bigquery_default_dataset_switches_through_name_for_new(monkeypatch):
    from services import brand
    from services.dialect_profiles import default_schema_for

    monkeypatch.delenv("DATAWRAP_BRAND_OBJECT_NAMES", raising=False)
    monkeypatch.delenv("DATAFLOW_BRAND_OBJECT_NAMES", raising=False)
    assert brand.artifact("bigquery_default_dataset").switchable is True
    assert brand.bigquery_default_dataset() == "dataflow"
    assert default_schema_for("bigquery") == "dataflow"
    monkeypatch.setenv("DATAWRAP_BRAND_OBJECT_NAMES", "1")
    assert brand.bigquery_default_dataset() == "datawrap"
    assert default_schema_for("bigquery") == "datawrap"
    # A dataset the connector names is never replaced.
    from services.brand import bigquery_dataset_of

    assert bigquery_dataset_of({"schema": "analytics"}) == "analytics"
    assert bigquery_dataset_of({"schema": "  "}) == "datawrap"
    assert bigquery_dataset_of({}) == "datawrap"


def test_egress_refusal_names_no_server_variable(client):
    res = client.post("/api/v1/connectors/test", json={
        "type": "postgresql", "host": "10.20.30.40", "port": 5432,
        "database": "d", "username": "u", "password": "p",
    })
    text = res.text
    assert "10.20.30.40" in text or res.status_code in (400, 403, 422), text
    assert "DATAFLOW_" not in text and "DATAWRAP_" not in text, text
