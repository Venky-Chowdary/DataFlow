"""The Map step's holds equal the server gates' verdicts on the same inputs (review M6).

``apps/web/src/lib/__fixtures__/map_hold_parity.json`` holds each case's
destination reading, the catalog the API builds from it, the mappings, and the
holds expected. The web suite (``destColumnCatalog.parity.test.ts``) asserts
``mapTargetIssues`` returns exactly those holds; this test asserts the catalog
is what the API builds today and that G14 + the DDL gate block on exactly the
same conditions. Either side drifting fails a test.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_API = Path(__file__).resolve().parents[1]
if str(_API) not in sys.path:
    sys.path.insert(0, str(_API))

from services.ddl_compatibility import evaluate_ddl_compatibility  # noqa: E402
from services.destination_requirements_gate import build_destination_requirements_gate  # noqa: E402
from services.mapping_dest_catalog import build_destination_column_catalog  # noqa: E402

FIXTURE = _API.parent / "web" / "src" / "lib" / "__fixtures__" / "map_hold_parity.json"
CASES = json.loads(FIXTURE.read_text(encoding="utf-8"))["cases"]


def _server_holds(case: dict) -> set[str]:
    info = case["info"]
    known = {c.lower() for c in info["columns"]}
    mappings = [
        {**m, "confidence": 0.99, "approved": True,
         **({"create_new": True} if m["target"].lower() not in known else {})}
        for m in case["mappings"]
    ]
    holds: set[str] = set()
    g14 = build_destination_requirements_gate(
        destination_table_exists=True,
        column_nullability=info["schema_nullability"],
        column_defaults=info["schema_defaults"],
        identity_columns=info["identity_columns"],
        generated_columns=info["generated_columns"],
        mappings=mappings,
        dest_columns=info["columns"],
        dest_recreated=case["table_recreated"],
    )
    if g14 and g14["status"] == "block":
        holds.add("generated_target" if g14["details"].get("generated_targets") else "required_unmapped")
    if not case["table_recreated"]:
        _ok, issues = evaluate_ddl_compatibility(
            mappings=mappings,
            source_schema={m["source"]: "VARCHAR" for m in mappings},
            target_schema=info["schema"],
            table_exists=True,
            dest_connected=True,
            dest_db_type="postgresql",
            schema_policy=case["schema_policy"],
            sync_mode="full_refresh_append",
        )
        if any("Duplicate target column" in i for i in issues):
            holds.add("target_conflict")
        if any("does not exist in destination table" in i for i in issues):
            holds.add("new_target_refused")
    return holds


@pytest.mark.parametrize("case", CASES, ids=[c["name"] for c in CASES])
def test_fixture_catalog_is_what_the_api_builds(case):
    built = build_destination_column_catalog(
        case["info"], dest_type="postgresql", schema_policy=case["schema_policy"]
    )
    assert built == case["catalog"]


@pytest.mark.parametrize("case", CASES, ids=[c["name"] for c in CASES])
def test_server_gates_hold_exactly_what_the_map_step_holds(case):
    assert _server_holds(case) == set(case["expected"])
