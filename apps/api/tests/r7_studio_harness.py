"""The REST calls Studio's TransferPage makes, in its order (apps/web/src).

* ``POST /transfer/introspect`` -- source + destination schema
  (``introspectTransferEndpoints``);
* ``POST /transfer/map`` -- Map against an existing destination
  (``mapTransferColumns``; the direct path TransferPage takes when the
  destination's columns are in hand), or ``POST /transfer/plans/{id}/map``;
* Validate -- ``PATCH``/``POST /transfer/plans`` (``ensurePersistedPlan``),
  ``POST /transfer/plans/{id}/mappings`` (``syncTransferPlanMappings``),
  ``POST /transfer/plans/{id}/preflight`` (``preflightTransferPlan``) and
  ``/approve`` when the decision is approve;
* Execute -- ``POST /transfer/plans/{id}/mappings`` then the multipart
  ``POST /transfer/run`` (``runUniversalTransfer``).
"""

from __future__ import annotations

import json
from typing import Any

from tests import r4_route_harness as h


class Studio:
    def __init__(self, client, *, source: dict, destination: dict, policies: dict) -> None:
        self.client = client
        self.source = dict(source)
        self.destination = dict(destination)
        self.policies = dict(policies)
        self.plan_id = ""

    # -- Source step -------------------------------------------------------
    def introspect(self) -> tuple[dict, dict]:
        resp = self.client.post(
            "/api/v1/transfer/introspect",
            json={"source": self.source, "destination": self.destination},
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        return body["source"], body["destination"]

    @staticmethod
    def columns(intro: dict) -> tuple[list[str], dict[str, str]]:
        cols = [c.get("name") if isinstance(c, dict) else c for c in intro.get("columns") or []]
        schema = {str(k): str(v) for k, v in (intro.get("schema") or {}).items()}
        return [str(c) for c in cols], schema

    # -- Map step ----------------------------------------------------------
    def map(self, src_cols, src_schema, dst_cols, dst_schema, *, table_exists, prior=None) -> list[dict]:
        resp = self.client.post("/api/v1/transfer/map", json={
            "source_columns": src_cols, "source_schema": src_schema,
            "target_columns": dst_cols, "target_schema": dst_schema,
            "validation_mode": self.policies.get("validation_mode", "strict"),
            "use_llm": False,
            "destination_db_type": self.destination.get("format", ""),
            "sync_mode": self.policies.get("sync_mode", ""),
            "schema_policy": self.policies.get("schema_policy", "manual_review"),
            "destination_table_exists": table_exists,
            "source_kind": "database",
            "source_db_type": self.source.get("format", ""),
            "prior_mappings": list(prior or []),
        })
        assert resp.status_code == 200, resp.text
        return list(resp.json().get("mappings") or [])

    # -- Validate ----------------------------------------------------------
    def _persist(self, src_cols, src_schema, dst_cols, dst_schema) -> str:
        payload = {
            "name": str(self.source.get("table") or "studio"),
            "source": self.source, "destination": self.destination,
            "source_columns": src_cols, "source_schema": src_schema,
            "target_columns": dst_cols, "target_schema": dst_schema,
            "policies": self.policies,
        }
        if self.plan_id:
            resp = self.client.patch(f"/api/v1/transfer/plans/{self.plan_id}", json=payload)
            assert resp.status_code == 200, resp.text
        else:
            resp = self.client.post("/api/v1/transfer/plans", json=payload)
            assert resp.status_code == 200, resp.text
            self.plan_id = resp.json()["plan"]["id"]
        return self.plan_id

    def validate(self, src_cols, src_schema, dst_cols, dst_schema, mappings, **ack) -> dict:
        plan_id = self._persist(src_cols, src_schema, dst_cols, dst_schema)
        resp = self.client.post(
            f"/api/v1/transfer/plans/{plan_id}/mappings", json={"mappings": mappings}
        )
        assert resp.status_code == 200, resp.text
        resp = self.client.post(f"/api/v1/transfer/plans/{plan_id}/preflight", json=ack)
        assert resp.status_code == 200, resp.text
        pf = resp.json()
        decision = ((pf.get("proof_bundle") or {}).get("transfer_decision") or {}).get("decision")
        if pf.get("passed") and decision == "approve":
            self.client.post(f"/api/v1/transfer/plans/{plan_id}/approve")
        return pf

    # -- Execute -----------------------------------------------------------
    def execute(self, mappings, **extra) -> dict:
        if self.plan_id:
            self.client.post(
                f"/api/v1/transfer/plans/{self.plan_id}/mappings", json={"mappings": mappings}
            )
        data = {
            "source_kind": "database",
            "source_format": self.source.get("format", ""),
            "source_connector_id": self.source.get("connector_id", ""),
            "source_table": self.source.get("table", ""),
            "dest_kind": "database",
            "dest_format": self.destination.get("format", ""),
            "dest_connector_id": self.destination.get("connector_id", ""),
            "dest_table": self.destination.get("table", ""),
            "dest_collection": self.destination.get("table", ""),
            "skip_preflight": "false",
            "async_mode": "false",
            "mappings_json": json.dumps(mappings),
            "sync_mode": self.policies.get("sync_mode", "full_refresh_append"),
            "schema_policy": self.policies.get("schema_policy", "manual_review"),
            "validation_mode": self.policies.get("validation_mode", "strict"),
            **({"plan_id": self.plan_id} if self.plan_id else {}),
            **{k: (json.dumps(v) if isinstance(v, (list, dict)) else str(v)) for k, v in extra.items()},
        }
        if self.source.get("database"):
            data["source_database"] = self.source["database"]
        resp = self.client.post("/api/v1/transfer/run", data=data)
        assert resp.status_code == 200, resp.text
        body = resp.json()
        job_id = str(body.get("job_id") or (body.get("job") or {}).get("id") or "")
        job = h.wait_job(job_id) if job_id else body
        return job


def preflight_mappings(
    mappings: list[dict], *, approve: bool = False, dest_columns: list[str] | None = None
) -> list[dict]:
    """``buildPreflightMappings`` (apps/web/src/lib/mapping.ts).

    ``user_override`` only after an explicit Approve; a target the destination
    does not have is create-new (``create_new`` + ``create_compatible_new``).
    """
    known = {str(c).lower() for c in (dest_columns or [])}
    out = []
    for m in mappings:
        target = str(m.get("target") or "")
        create_new = bool(m.get("create_new")) or (
            dest_columns is not None and target.lower() not in known
        )
        conf = float(m.get("confidence") or 0)
        row = {
            "source": m.get("source"), "target": target,
            "confidence": min(conf, 0.93) if create_new else conf,
            "reason": m.get("reason") or "User reviewed",
            "user_override": bool(approve),
            "transform": m.get("transform") or "none",
            "create_new": create_new,
            "requires_review": bool(m.get("requires_review")) and not approve,
            "source_type": m.get("source_type") or m.get("inferred_type"),
        }
        if create_new:
            row["assignment_strategy"] = "create_compatible_new"
        ttype = m.get("target_type") or m.get("dest_type")
        if ttype:
            row["target_type"] = ttype
        out.append(row)
    return out


def blockers(pf: dict) -> list[tuple[str, str]]:
    return [
        (str(b.get("id") or ""), str(b.get("message") or ""))
        for b in pf.get("blockers") or [] if isinstance(b, dict)
    ]
