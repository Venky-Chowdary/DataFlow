"""Studio's real route sequence for G11 tests: Validate -> approved hashes -> Execute.

``/api/v1/transfer/plans`` + ``/plans/{id}/mappings`` + ``/plans/{id}/preflight``
(Validate, as the web app calls it) stamps the Decision Artifact and the
Map->DDL identity; ``/api/v1/transfer/run`` with ``skip_preflight=false``
executes under those approved hashes. No ``skip_preflight=True`` shortcut.
"""

from __future__ import annotations

import json
from typing import Any

from tests import r4_route_harness as h


def connectors(token: str, src_kind: str, dst_kind: str) -> tuple[Any, Any]:
    from services.connector_store import create_connector

    src = create_connector({
        "name": f"g11-src-{token}", "role": "source", "workspace_id": "", "ssl": False,
        **h.dest_config(src_kind),
    })
    dst = create_connector({
        "name": f"g11-dst-{token}", "role": "destination", "workspace_id": "", "ssl": False,
        **h.dest_config(dst_kind),
    })
    return src, dst


def validate(
    client,
    *,
    source: dict[str, Any],
    destination: dict[str, Any],
    columns: list[str],
    types: dict[str, str],
    mappings: list[dict[str, Any]],
    sync_mode: str,
    stream_contracts: list[dict[str, Any]] | None = None,
    schema_policy: str = "manual_review",
) -> tuple[str, dict[str, Any]]:
    # Preview, as Studio does: the plan carries the destination as it is now.
    resp = client.post("/api/v1/transfer/introspect",
                       json={"source": source, "destination": destination})
    assert resp.status_code == 200, resp.text
    dest_info = resp.json().get("destination") or {}
    exists = bool(dest_info.get("table_exists"))
    dest_cols = list(dest_info.get("columns") or []) if exists else []
    dest_types = dict(dest_info.get("schema") or {}) if exists else {}
    policies = {"sync_mode": sync_mode, "schema_policy": schema_policy,
                "validation_mode": "strict", "stream_contracts": stream_contracts or []}
    resp = client.post("/api/v1/transfer/plans", json={
        "name": source.get("table") or "g11", "source": source, "destination": destination,
        "source_columns": columns, "source_schema": types, "target_columns": dest_cols,
        "target_schema": dest_types, "row_count_estimate": 3, "sample_rows": [],
        "policies": policies,
    })
    assert resp.status_code == 200, resp.text
    plan_id = resp.json()["plan"]["id"]
    resp = client.post(f"/api/v1/transfer/plans/{plan_id}/mappings", json={"mappings": mappings})
    assert resp.status_code == 200, resp.text
    resp = client.post(f"/api/v1/transfer/plans/{plan_id}/preflight", json={})
    assert resp.status_code == 200, resp.text
    return plan_id, resp.json()


def gates_of(pf: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for key in ("gates", "blockers", "warnings"):
        for g in pf.get(key) or []:
            if isinstance(g, dict) and (g.get("id") or g.get("gate")):
                out.setdefault(str(g.get("id") or g.get("gate")), g)
    return out


def execute(
    client,
    *,
    plan_id: str,
    pf: dict[str, Any],
    source: dict[str, Any],
    destination: dict[str, Any],
    mappings: list[dict[str, Any]],
    sync_mode: str,
    stream_contracts: list[dict[str, Any]] | None = None,
    extra_form: dict[str, str] | None = None,
    schema_policy: str = "manual_review",
    expect_status: int | None = 200,
) -> dict[str, Any]:
    """``/api/v1/transfer/run`` under the hashes Validate approved."""
    proof = pf.get("proof_bundle") or {}
    artifact = proof.get("decision_artifact") or {}
    assert artifact.get("content_hash"), sorted(proof)
    dest_table = str(destination.get("table") or destination.get("collection") or "")
    form = {
        "source_kind": "database", "source_format": source["format"],
        "source_connector_id": source.get("connector_id") or "",
        "source_database": source.get("database") or "",
        "source_table": source.get("table") or "",
        "dest_kind": "database", "dest_format": destination["format"],
        "dest_connector_id": destination.get("connector_id") or "",
        "dest_database": destination.get("database") or "",
        "dest_table": dest_table, "dest_collection": dest_table,
        "skip_preflight": "false", "async_mode": "false",
        **({"plan_id": plan_id} if plan_id else {}),
        # Studio runs the Map Validate approved (its effective rows).
        "mappings_json": json.dumps(pf.get("mappings") or mappings), "sync_mode": sync_mode,
        "validation_mode": "strict", "schema_policy": schema_policy,
        "approved_decision_artifact_hash":
            proof.get("decision_artifact_hash") or artifact["content_hash"],
        "approved_ddl_identity_hash":
            (proof.get("ddl_identity") or {}).get("ddl_identity_hash", ""),
        "decision_artifact_json": json.dumps(artifact),
        **({"stream_contracts_json": json.dumps(stream_contracts)} if stream_contracts else {}),
        **(extra_form or {}),
    }
    resp = client.post("/api/v1/transfer/run", data=form)
    if expect_status is not None:
        assert resp.status_code == expect_status, resp.text
    body = resp.json()
    return body.get("detail") if isinstance(body.get("detail"), dict) else body


def blockers(pf: dict[str, Any]) -> list[tuple[Any, Any]]:
    return [(b.get("id") or b.get("gate"), b.get("message")) for b in pf.get("blockers") or []]
