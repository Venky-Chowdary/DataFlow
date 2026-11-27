"""Re-review B: every gate that can answer "skip" says why in a structured field.

Validate's checklist classifies a skipped gate from its details, never its
message (apps/web/src/lib/gateChecklist.ts gateOutcome): ``unmeasured`` (or
``remediation_kind: retry_validate``) is "Could not run", ``applicable: false``
/ ``declared: false`` / ``schemaless`` / a G19 run that recreates nothing is
"Not applicable", a ``.post_write`` rule is "Checked after the run", and any
other skip falls to "Could not run". A skip site without a structured field
therefore shows "Could not run" with a Re-run that never clears (G15 on every
create-new table did exactly that).

This test walks the source (services/, src/, packages/preflight) with the AST,
finds every gate result built with status "skip" (a literal, an if-expression
that can be "skip", a variable assigned "skip", GateStatus.SKIP, or a status
looked up with skip as the default for an unknown one), and
asserts its details literal carries one of those fields. It also keeps
apps/web/src/lib/__fixtures__/gate_skip_sites.json in step: the web test runs
gateOutcome over every site and checks the outcome the field means.
Regenerate the fixture with UPDATE_GATE_SKIP_FIXTURE=1.
"""

from __future__ import annotations

import ast
import functools
import json
import os
from pathlib import Path

API = Path(__file__).resolve().parents[1]
REPO = API.parents[1]
ROOTS = [API / "services", API / "src", REPO / "packages" / "preflight" / "src"]
FIXTURE = REPO / "apps" / "web" / "src" / "lib" / "__fixtures__" / "gate_skip_sites.json"
STRUCTURED = {"unmeasured", "applicable", "declared", "schemaless", "dest_recreated", "remediation_kind", "rule_id"}


def _is_skip(node: ast.AST, names: set[str]) -> bool:
    if isinstance(node, ast.Constant):
        return node.value == "skip"
    if isinstance(node, ast.IfExp):
        return _is_skip(node.body, names) or _is_skip(node.orelse, names)
    if isinstance(node, ast.Attribute):
        return node.attr == "SKIP"
    if isinstance(node, ast.Name):
        return node.id in names
    if isinstance(node, ast.Call) and getattr(node.func, "attr", "") == "get" and len(node.args) == 2:
        # {"pass": ..., "block": ...}.get(raw, GateStatus.SKIP): an unknown status falls to skip.
        return _is_skip(node.args[1], names)
    return False


def _details_dict(node: ast.AST | None) -> ast.Dict | None:
    if isinstance(node, ast.Dict):
        return node
    # _with_scope({...}, scope) and dict({...}): the first argument carries the fields.
    if isinstance(node, ast.Call) and node.args and isinstance(node.args[0], ast.Dict):
        return node.args[0]
    return None


def _value(node: ast.AST) -> object:
    """A field value as the skip branch sees it (expressions: their skip-branch value)."""
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.JoinedStr):
        return "".join(v.value for v in node.values if isinstance(v, ast.Constant) and isinstance(v.value, str))
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):
        return False  # `not ignored`: false exactly when the gate skips
    if isinstance(node, ast.Compare):
        return False  # `status != "skip"`
    if isinstance(node, ast.Call) and getattr(node.func, "id", "") == "bool":
        return False  # G19: bool(dest_recreated) is false on its skip branch
    return None


def _skip_sites() -> list[dict]:
    sites = []
    for root in ROOTS:
        for path in sorted(root.rglob("*.py")):
            if "tests" in path.parts:
                continue
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for fn in ast.walk(tree):
                if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                names = {t.id for n in ast.walk(fn) if isinstance(n, ast.Assign) and _is_skip(n.value, set())
                         for t in n.targets if isinstance(t, ast.Name)}
                for node in ast.walk(fn):
                    details = None
                    hit = False
                    if isinstance(node, ast.Dict):
                        keys = [k.value if isinstance(k, ast.Constant) else None for k in node.keys]
                        if "status" in keys and "id" in keys and _is_skip(node.values[keys.index("status")], names):
                            hit = True
                            details = node.values[keys.index("details")] if "details" in keys else None
                    elif isinstance(node, ast.Call) and getattr(node.func, "id", getattr(node.func, "attr", "")) == "GateResult":
                        kw = {k.arg: k.value for k in node.keywords}
                        if "status" in kw and _is_skip(kw["status"], names):
                            hit = True
                            details = kw.get("details")
                    if not hit:
                        continue
                    found = _details_dict(details)
                    fields = {}
                    if found is not None:
                        for k, v in zip(found.keys, found.values):
                            if isinstance(k, ast.Constant) and k.value in STRUCTURED | {"destination_table_exists"}:
                                fields[k.value] = _value(v)
                    sites.append({"site": f"{path.relative_to(REPO).as_posix()}::{fn.name}", "fields": fields})
    return sorted(sites, key=lambda s: (s["site"], json.dumps(s["fields"], sort_keys=True)))


@functools.lru_cache(maxsize=1)
def _scan() -> tuple[str, ...]:
    return tuple(json.dumps(s, sort_keys=True) for s in _skip_sites())


def skip_sites() -> list[dict]:
    return [json.loads(s) for s in _scan()]


def _says_why(fields: dict) -> bool:
    if fields.get("unmeasured") is True or fields.get("remediation_kind") == "retry_validate":
        return True
    if fields.get("applicable") is False or fields.get("declared") is False or fields.get("schemaless") is True:
        return True
    if "dest_recreated" in fields:
        return True
    return str(fields.get("rule_id") or "").endswith(".post_write")


def test_every_skip_site_carries_a_structured_reason():
    sites = skip_sites()
    assert len(sites) >= 15, "the scan found the gate skip sites"
    missing = [s["site"] for s in sites if not _says_why(s["fields"])]
    assert missing == [], f"skip without a structured reason: {missing}"


def test_the_web_fixture_lists_every_skip_site():
    text = json.dumps({"generated_by": "apps/api/tests/test_gate_skip_sites_are_structured.py",
                       "sites": skip_sites()}, indent=1) + "\n"
    if os.environ.get("UPDATE_GATE_SKIP_FIXTURE"):
        FIXTURE.write_text(text, encoding="utf-8")
    assert FIXTURE.read_text(encoding="utf-8") == text, "stale: rerun with UPDATE_GATE_SKIP_FIXTURE=1"


def test_a_hosted_gate_without_a_known_status_is_an_unmeasured_skip():
    """Review round 3 #9: no status / an unknown one proved nothing; the host's details are not passed on."""
    import sys
    import time

    sys.path.insert(0, str(REPO / "packages" / "preflight" / "src"))
    from preflight.gates import _host_gate_to_result
    from preflight.models import GateId, GateStatus

    for payload, reason in (({}, "host_status_missing"),
                            ({"status": "maybe", "details": {"applicable": False}}, "host_status_unknown")):
        result = _host_gate_to_result(GateId.G13_SOURCE_COVERAGE, payload, time.perf_counter())
        assert result.status == GateStatus.SKIP
        assert result.details["unmeasured"] is True
        assert result.details["skip_reason"] == reason
        assert "applicable" not in result.details
        assert result.message.startswith("This check could not run")
    known = _host_gate_to_result(GateId.G13_SOURCE_COVERAGE, {"status": "PASS", "message": "ok"}, time.perf_counter())
    assert known.status == GateStatus.PASS and known.message == "ok"
