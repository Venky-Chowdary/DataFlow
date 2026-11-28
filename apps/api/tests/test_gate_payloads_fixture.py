"""Real gate payloads for the web checklist (review round 3, item 3).

The Validate checklist words each gate's findings from its ``details``. A
hand-written payload let the web drift from what the engine sends: G3 sends
both ``issues`` (lines) and ``issues_detail`` (entries, including warn-level
and risk-acknowledged ones), and G2 sends its privilege / staging probes. This
test runs the preflight package on fixed plans and keeps the results in
``apps/web/src/lib/__fixtures__/gate_payloads.json``; the web test
``gateChecklist.payloads.test.ts`` reads that file.

Regenerate with UPDATE_GATE_PAYLOAD_FIXTURE=1.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[3]
_PREFLIGHT = REPO / "packages" / "preflight" / "src"
if str(_PREFLIGHT) not in sys.path:
    sys.path.insert(0, str(_PREFLIGHT))

from preflight import (  # noqa: E402
    ColumnMapping,
    ColumnSchema,
    DestinationConfig,
    PreflightContext,
    PreflightEngine,
    SourceConfig,
    TransferPlan,
)
from preflight.gates import gate_g3_schema_contract  # noqa: E402
from preflight.models import GateResult  # noqa: E402
from preflight.risk_contract import make_clearing_risk_contract  # noqa: E402

FIXTURE = REPO / "apps" / "web" / "src" / "lib" / "__fixtures__" / "gate_payloads.json"


def _plan(source_types: dict[str, str], dest_types: dict[str, str]) -> TransferPlan:
    return TransferPlan(
        source=SourceConfig(
            kind="file",
            connected=True,
            parseable=True,
            columns=[ColumnSchema(name=s, inferred_type=t) for s, t in source_types.items()],
            row_count_estimate=10,
        ),
        destination=DestinationConfig(
            kind="database",
            connected=True,
            can_write=True,
            can_create_table=True,
            target_columns=[ColumnSchema(name=t, inferred_type=dt) for t, dt in dest_types.items()],
        ),
        mappings=[ColumnMapping(source=s, target=s, confidence=0.95) for s in source_types],
    )


def _sign(mapping: ColumnMapping, source_type: str, destination_type: str, **kwargs: Any) -> None:
    mapping.risk_acknowledged = True
    mapping.risk_contract = make_clearing_risk_contract(
        column=mapping.source, source_type=source_type, destination_type=destination_type, **kwargs
    )


_PROBE = {
    "sampled_rows": 3,
    "by_source": {
        "qty": {"severity": "block", "sampled": 3, "failed": 1, "sentinel_nulls": 0,
                "sample_failures": ["row 2: cannot cast 'x' to INTEGER"]},
        "price": {"severity": "block", "sampled": 3, "failed": 2, "sentinel_nulls": 0,
                  "sample_failures": ["row 1: cannot cast 'n/a' to INTEGER"]},
        "code": {"severity": "warn", "sampled": 3, "failed": 0, "sentinel_nulls": 1, "sample_failures": []},
    },
}


class _ProbedContext(PreflightContext):
    def coercion_report(self) -> dict[str, Any]:
        return _PROBE


def _g3_probed_with_signed_column() -> GateResult:
    """Two blocking columns and one signed (warn, risk-acknowledged) column."""
    plan = _plan({"qty": "VARCHAR", "price": "VARCHAR", "code": "VARCHAR(20)"},
                 {"qty": "INTEGER", "price": "INTEGER", "code": "VARCHAR(10)"})
    _sign(plan.mappings[1], "VARCHAR", "INTEGER", execution_policy="CAST_AND_CONTINUE")
    return gate_g3_schema_contract(_ProbedContext(plan=plan))


def _g3_schemaless_with_signed_column() -> GateResult:
    """A document store: the signed column stays severity "block" with risk_acknowledged."""
    plan = _plan({"owner_id": "ObjectId", "team_id": "ObjectId"},
                 {"owner_id": "INTEGER", "team_id": "INTEGER"})
    plan.destination.db_type = "mongodb"
    _sign(plan.mappings[1], "ObjectId", "INTEGER")
    return gate_g3_schema_contract(PreflightContext(plan=plan))


def _g3_declared_without_samples() -> GateResult:
    """No samples: two blocking lines, but an entry for only one of them."""
    plan = _plan({"qty": "VARCHAR", "seen_at": "TIMESTAMP"}, {"qty": "INTEGER", "seen_at": "DATE"})
    return gate_g3_schema_contract(PreflightContext(plan=plan))


def _g2(**probes: Any) -> GateResult:
    plan = _plan({"id": "INTEGER"}, {"id": "INTEGER"})
    for key, value in probes.items():
        setattr(plan.destination, key, value)
    result = PreflightEngine().run(PreflightContext(plan=plan))
    return next(g for g in result.gates if g.gate_id.value == "g2_destination")


def _g2_privilege_probe_unavailable() -> GateResult:
    return _g2(table_exists=False, privilege_probe={
        "status": "unavailable", "detail": "ACL describe unavailable", "method": "has_table_privilege",
        "engine": "postgresql",
    })


def _g2_redshift_staging_denied() -> GateResult:
    return _g2(db_type="redshift", table_exists=True, redshift_staging_probe={
        "status": "denied", "method": "GetBucketAcl", "bucket": "dw-stage",
        "detail": "Staging bucket `dw-stage` ACL does not grant write for COPY FROM S3",
    })


_CASES = {
    "g3_probed_with_signed_column": _g3_probed_with_signed_column,
    "g3_schemaless_with_signed_column": _g3_schemaless_with_signed_column,
    "g3_declared_without_samples": _g3_declared_without_samples,
    "g2_privilege_probe_unavailable": _g2_privilege_probe_unavailable,
    "g2_redshift_staging_denied": _g2_redshift_staging_denied,
}


def _payloads() -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for name, build in _CASES.items():
        gate = build()
        out[name] = {
            "id": gate.gate_id.value,
            "status": gate.status.value,
            "message": gate.message,
            "details": json.loads(json.dumps(gate.details, default=str)),
        }
    return out


def test_payloads_are_the_shapes_the_web_test_relies_on():
    p = _payloads()
    g3 = p["g3_probed_with_signed_column"]
    assert g3["status"] == "block"
    detail = g3["details"]["issues_detail"]
    # Both lists are sent; the entries include a warn-level, signed column.
    assert g3["details"]["issues"] and detail
    assert any(e.get("severity") == "warn" and e.get("risk_acknowledged") for e in detail)
    assert any(not e.get("message") for e in detail)
    doc = p["g3_schemaless_with_signed_column"]["details"]["issues_detail"]
    assert any(e.get("severity") == "block" and e.get("risk_acknowledged") for e in doc)
    bare = p["g3_declared_without_samples"]["details"]
    blocking = [e for e in bare["issues_detail"] if e.get("severity") == "block"]
    assert len(bare["issues"]) == 2 and len(blocking) == 1
    assert p["g2_privilege_probe_unavailable"]["status"] == "block"
    assert p["g2_privilege_probe_unavailable"]["details"]["privilege_probe"]["status"] == "unavailable"
    assert p["g2_redshift_staging_denied"]["status"] == "block"
    assert p["g2_redshift_staging_denied"]["details"]["redshift_staging_probe"]["status"] == "denied"


def test_the_web_fixture_holds_the_engine_payloads():
    text = json.dumps({"generated_by": "apps/api/tests/test_gate_payloads_fixture.py", "payloads": _payloads()},
                      indent=1, sort_keys=True, ensure_ascii=False) + "\n"
    if os.environ.get("UPDATE_GATE_PAYLOAD_FIXTURE"):
        FIXTURE.write_text(text, encoding="utf-8")
    assert FIXTURE.read_text(encoding="utf-8") == text, "stale: rerun with UPDATE_GATE_PAYLOAD_FIXTURE=1"
