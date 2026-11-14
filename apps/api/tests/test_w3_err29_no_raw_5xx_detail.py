"""ERR-29: no server-error response carries an exception's text.

A 5xx built as ``HTTPException(status_code=500, detail=str(e))`` sent the
driver's text (hosts, Mongo topology, SQL) to the browser with no support
reference. Server errors go through ``safe_http_exception`` (a public
sentence plus a reference) or ``classified_http_exception`` (the taxonomy's
message). This scan forbids new ones anywhere in the API.
"""

from __future__ import annotations

import ast
from pathlib import Path

_API = Path(__file__).resolve().parents[1]
_SCANNED = ("src", "services", "connectors")


def _raw_5xx_sites() -> list[str]:
    hits: list[str] = []
    for top in _SCANNED:
        for path in sorted((_API / top).rglob("*.py")):
            if "__pycache__" in path.parts:
                continue
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for handler in ast.walk(tree):
                if not isinstance(handler, ast.ExceptHandler) or not handler.name:
                    continue
                for call in ast.walk(handler):
                    if not isinstance(call, ast.Call):
                        continue
                    func = call.func
                    name = getattr(func, "id", None) or getattr(func, "attr", None)
                    if name != "HTTPException":
                        continue
                    kw = {k.arg: k.value for k in call.keywords}
                    status = kw.get("status_code") or (call.args[0] if call.args else None)
                    if not (isinstance(status, ast.Constant) and isinstance(status.value, int)):
                        continue
                    if status.value < 500:
                        continue
                    detail = kw.get("detail") or (call.args[1] if len(call.args) > 1 else None)
                    if detail is None:
                        continue
                    names = {n.id for n in ast.walk(detail) if isinstance(n, ast.Name)}
                    if handler.name in names:
                        rel = path.relative_to(_API)
                        hits.append(f"{rel}:{call.lineno}: {ast.unparse(detail)[:60]}")
    return hits


def test_no_server_error_detail_is_built_from_an_exception():
    hits = _raw_5xx_sites()
    assert not hits, (
        "5xx HTTPException with the exception's text as detail — use "
        "services.safe_errors.safe_http_exception (or classified_http_exception):\n"
        + "\n".join(hits)
    )


def test_scan_catches_the_pattern():
    """The scanner is live: a planted site is reported."""
    planted = _API / "src" / "_w3err_planted_site.py"
    planted.write_text(
        "from fastapi import HTTPException\n"
        "def f():\n"
        "    try:\n"
        "        pass\n"
        "    except Exception as e:\n"
        "        raise HTTPException(status_code=500, detail=str(e))\n",
        encoding="utf-8",
    )
    try:
        assert any("_w3err_planted_site.py" in h for h in _raw_5xx_sites())
    finally:
        planted.unlink()


def test_safe_http_exception_carries_the_envelope():
    from services.safe_errors import safe_http_exception

    raised = safe_http_exception(
        RuntimeError("could not connect to mongo-internal-7:27017 replicaSet rs0"),
        event="w3err.test",
        public_message="Couldn't list jobs.",
    )
    assert raised.status_code == 500
    assert "mongo-internal" not in str(raised.detail)
    assert "Reference: " in str(raised.detail)
    envelope = raised.envelope
    assert envelope["error"]["code"] == "platform.internal"
    assert envelope["error"]["reference"] and envelope["error"]["reference"] in envelope["detail"]
