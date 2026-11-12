"""Audit M-2 / M-3: every product flag is read through the brand-aware reader.

The product tells operators to prefer ``DATAWRAP_*`` names, and
``services.brand_env.getenv_brand`` reads ``DATAWRAP_X`` then ``DATAFLOW_X``.
Some forty flags were read with ``os.getenv("DATAFLOW_X")`` directly (or
through a small helper that did), so ``DATAWRAP_PUBLIC_RATE_LIMIT=1``,
``DATAWRAP_AUDIT_RETENTION_DAYS=90`` or ``DATAWRAP_HSTS=1`` silently did
nothing. The worker and scheduler entrypoints also forced their role under the
legacy name only, so a ``DATAWRAP_PROCESS_ROLE`` left in a shared environment
won over the entrypoint.

Three layers: each security-relevant flag honours both names (the new one
wins), the entrypoints force both names, and a static scan fails on any new
direct read so the list cannot grow back.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

API_ROOT = Path(__file__).resolve().parents[1]
REPO = API_ROOT.parents[1]


def _only(monkeypatch, suffix: str, *, wrap: str | None = None, flow: str | None = None) -> None:
    for prefix, value in (("DATAWRAP_", wrap), ("DATAFLOW_", flow)):
        if value is None:
            monkeypatch.delenv(prefix + suffix, raising=False)
        else:
            monkeypatch.setenv(prefix + suffix, value)


# --------------------------------------------------------------------------- behaviour


def _public_limiter() -> bool:
    from services.auth_rate_limit import public_rate_limit_enabled

    return public_rate_limit_enabled()


def _auth_limiter() -> bool:
    from services.auth_rate_limit import auth_rate_limit_enabled

    return auth_rate_limit_enabled()


def _mcp_limiter() -> bool:
    from services.mcp_rate_limit import rate_limit_enabled

    return rate_limit_enabled()


def _pilot_limiter() -> bool:
    from services.mcp_rate_limit import check_pilot_rate_limit

    return not check_pilot_rate_limit("m3-probe").get("disabled", False)


def _legal_hold() -> bool:
    from services.audit_log import _legal_hold_enabled

    return _legal_hold_enabled()


# (flag, "on" value, "off" value, reader that returns True when on)
SWITCHES = [
    ("PUBLIC_RATE_LIMIT", "1", "0", _public_limiter),
    ("AUTH_RATE_LIMIT", "1", "0", _auth_limiter),
    ("MCP_RATE_LIMIT", "1", "0", _mcp_limiter),
    ("PILOT_RATE_LIMIT", "1", "0", _pilot_limiter),
    ("AUDIT_LEGAL_HOLD", "1", "0", _legal_hold),
]


@pytest.mark.parametrize("flag,on,off,reader", SWITCHES, ids=[s[0] for s in SWITCHES])
def test_switch_honours_both_names_and_the_new_one_wins(monkeypatch, flag, on, off, reader):
    _only(monkeypatch, flag, wrap=on)
    assert reader() is True, f"DATAWRAP_{flag}={on} was ignored"
    _only(monkeypatch, flag, wrap=off)
    assert reader() is False, f"DATAWRAP_{flag}={off} was ignored"
    _only(monkeypatch, flag, flow=on)
    assert reader() is True, f"DATAFLOW_{flag} (the old name) stopped working"
    _only(monkeypatch, flag, flow=off)
    assert reader() is False
    _only(monkeypatch, flag, wrap=off, flow=on)
    assert reader() is False, "DATAWRAP_ must win when both are set"


def test_audit_retention_days_honours_both_names(monkeypatch):
    from services.audit_log import _retention_days

    _only(monkeypatch, "AUDIT_RETENTION_DAYS")
    assert _retention_days() is None
    _only(monkeypatch, "AUDIT_RETENTION_DAYS", wrap="90")
    assert _retention_days() == 90
    _only(monkeypatch, "AUDIT_RETENTION_DAYS", flow="45")
    assert _retention_days() == 45
    _only(monkeypatch, "AUDIT_RETENTION_DAYS", wrap="120", flow="45")
    assert _retention_days() == 120


def test_rate_limit_tunables_honour_both_names(monkeypatch):
    from services import auth_rate_limit, mcp_rate_limit
    from src.routers import contact_router

    for module, flag in (
        (auth_rate_limit, "AUTH_RATE_BURST"),
        (mcp_rate_limit, "MCP_RATE_BURST"),
        (contact_router, "CONTACT_IP_CAPACITY"),
    ):
        _only(monkeypatch, flag, wrap="7")
        assert module._env_float(f"DATAFLOW_{flag}", 30.0) == 7.0, flag
        _only(monkeypatch, flag, flow="9")
        assert module._env_float(f"DATAFLOW_{flag}", 30.0) == 9.0, flag
    _only(monkeypatch, "AUTH_LOCKOUT_FAILURES", wrap="3")
    assert auth_rate_limit._env_int("DATAFLOW_AUTH_LOCKOUT_FAILURES", 10) == 3
    _only(monkeypatch, "MCP_RATE_MAX_KEYS", wrap="11")
    assert mcp_rate_limit._env_int("DATAFLOW_MCP_RATE_MAX_KEYS", 5000) == 11


def test_hsts_flag_honours_both_names_over_http(monkeypatch):
    """Through the app: a TLS-terminating proxy that does not forward the scheme."""
    from fastapi.testclient import TestClient

    from src.main import app

    client = TestClient(app)
    _only(monkeypatch, "HSTS")
    assert "strict-transport-security" not in client.get("/health").headers
    _only(monkeypatch, "HSTS", wrap="1")
    assert "max-age=" in client.get("/health").headers.get("strict-transport-security", "")
    _only(monkeypatch, "HSTS", flow="1")
    assert "max-age=" in client.get("/health").headers.get("strict-transport-security", "")
    _only(monkeypatch, "HSTS", wrap="0", flow="1")
    assert "strict-transport-security" not in client.get("/health").headers


def test_sftp_host_key_trust_honours_both_names(monkeypatch):
    from connectors.sftp_common import parse_sftp_config

    for flag, attr, value in (
        ("SFTP_HOST_KEY_POLICY", "host_key_policy", "strict"),
        ("SFTP_KNOWN_HOSTS", "known_hosts", "/etc/ssh/known_hosts"),
        ("SFTP_HOST_KEY", "host_key", "ssh-ed25519 AAAA"),
    ):
        _only(monkeypatch, flag, wrap=value)
        assert getattr(parse_sftp_config(host="sftp.example.com", username="u"), attr) == value, flag
        _only(monkeypatch, flag, flow=value)
        assert getattr(parse_sftp_config(host="sftp.example.com", username="u"), attr) == value, flag
        _only(monkeypatch, flag)


def test_query_and_pool_limits_honour_both_names(monkeypatch):
    from services.engine_pool import pool_settings
    from src.routers import query_router

    _only(monkeypatch, "DB_POOL_SIZE", wrap="3")
    assert pool_settings()["pool_size"] == 3
    _only(monkeypatch, "DB_POOL_SIZE", flow="4")
    assert pool_settings()["pool_size"] == 4

    class _Conn:
        def __init__(self):
            self.statements = []

        def execute(self, statement, *_a, **_k):
            self.statements.append(str(statement))

    for names in ({"wrap": "7"}, {"flow": "7"}):
        _only(monkeypatch, "QUERY_STATEMENT_TIMEOUT_SECONDS", **names)
        conn = _Conn()
        query_router._apply_statement_timeout(conn, "postgresql")
        assert conn.statements == ["SET LOCAL statement_timeout = 7000"], names


@pytest.mark.parametrize("module_name,role", [("src.worker_main", "worker"), ("src.scheduler_main", "scheduler")])
def test_entrypoint_role_wins_over_a_leftover_new_name(monkeypatch, module_name, role):
    """``python -m src.worker_main`` is a worker whatever a shared env block says."""
    import importlib

    from services.process_role import process_role, schedule_loop_enabled

    module = importlib.import_module(module_name)
    for flag, stale in (("PROCESS_ROLE", "api"), ("SCHEDULE_LOOP", "1" if role == "worker" else "0"),
                        ("API_CLAIM_LOOP", "1"), ("WORKER_FLEET", "0")):
        monkeypatch.setenv(f"DATAWRAP_{flag}", stale)
        monkeypatch.setenv(f"DATAFLOW_{flag}", stale)
    getattr(module, f"_force_{role}_env")()
    assert process_role() == role
    assert schedule_loop_enabled() is (role == "scheduler")
    import os

    for flag in ("PROCESS_ROLE", "SCHEDULE_LOOP", "API_CLAIM_LOOP", "WORKER_FLEET"):
        assert os.environ[f"DATAWRAP_{flag}"] == os.environ[f"DATAFLOW_{flag}"], flag


def test_setenv_brand_sets_both_names(monkeypatch):
    import os

    from services.brand_env import getenv_brand, setenv_brand

    monkeypatch.setenv("DATAWRAP_M3_PROBE", "stale")
    monkeypatch.delenv("DATAFLOW_M3_PROBE", raising=False)
    setenv_brand("DATAFLOW_M3_PROBE", "fresh")
    assert getenv_brand("M3_PROBE") == "fresh"
    assert os.environ["DATAWRAP_M3_PROBE"] == os.environ["DATAFLOW_M3_PROBE"] == "fresh"
    monkeypatch.delenv("DATAWRAP_M3_PROBE", raising=False)
    monkeypatch.delenv("DATAFLOW_M3_PROBE", raising=False)


# --------------------------------------------------------------------------- static guard

SCANNED = [API_ROOT / "src", API_ROOT / "services", API_ROOT / "connectors", REPO / "apps" / "cli"]
BRAND_READERS = {"getenv_brand", "getenv_brand_str", "getenv_brand_bool", "setenv_brand"}
#: Writing a default under the fallback name is correct: an operator's
#: DATAWRAP_ value still wins on the read.
DEFAULT_WRITERS = {"os.environ.setdefault"}
#: Flags read by the Pilot's own modules (src/ai), which this change does not
#: edit. Listed exactly so a new one fails here. Hand to the Pilot team.
KNOWN_UNCONVERTED = {
    ("src/ai/copilot/tool_deadline.py", "DATAFLOW_TOOL_DEADLINE_SECONDS"),
    ("src/ai/copilot/tool_deadline.py", "DATAFLOW_QUERY_TOOL_DEADLINE_SECONDS"),
    ("src/ai/first_party/engine.py", "DATAFLOW_FP_REWRITE"),
    ("src/ai/first_party/engine.py", "DATAFLOW_FP_NARRATE"),
    ("src/ai/first_party/engine.py", "DATAFLOW_FP_LM"),
}


def _dotted(node: ast.AST) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return f"{_dotted(node.value)}.{node.attr}"
    if isinstance(node, ast.Call):
        return _dotted(node.func)
    return "?"


def _is_flag(value: object) -> bool:
    return (
        isinstance(value, str)
        and value.startswith(("DATAFLOW_", "DATAWRAP_"))
        and len(value) > 9
        and value.upper() == value
    )


def _brand_aware_helpers(tree: ast.AST) -> set[str]:
    """Functions of this module whose body reads through the brand reader."""
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            calls = {_dotted(c.func).rsplit(".", 1)[-1] for c in ast.walk(node) if isinstance(c, ast.Call)}
            if calls & BRAND_READERS:
                names.add(node.name)
    return names


_HELPERS_BY_FILE: dict[Path, set[str]] = {}


def _helpers_defined_in(source: Path) -> set[str]:
    if source not in _HELPERS_BY_FILE:
        _HELPERS_BY_FILE[source] = (
            _brand_aware_helpers(ast.parse(source.read_text(encoding="utf-8"))) if source.is_file() else set()
        )
    return _HELPERS_BY_FILE[source]


def _imported_brand_aware_helpers(tree: ast.AST) -> set[str]:
    """Private helpers this module imports from a module where they read through the brand reader."""
    names: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.ImportFrom) or not node.module:
            continue
        wanted = [alias for alias in node.names if alias.name.startswith("_")]
        if not wanted:
            continue
        defined = _helpers_defined_in(API_ROOT / (node.module.replace(".", "/") + ".py"))
        names |= {alias.asname or alias.name for alias in wanted if alias.name in defined}
    return names


def _direct_flag_reads() -> list[str]:
    offenders: list[str] = []
    for root in SCANNED:
        for path in sorted(root.rglob("*.py")):
            rel = path.relative_to(API_ROOT if API_ROOT in path.parents else REPO).as_posix()
            if "/tests/" in f"/{rel}" or path.name.startswith("test_"):
                continue
            text = path.read_text(encoding="utf-8")
            if "DATAFLOW_" not in text and "DATAWRAP_" not in text:
                continue
            tree = ast.parse(text)
            helpers = _brand_aware_helpers(tree) | _imported_brand_aware_helpers(tree)
            for node in ast.walk(tree):
                flag = callee = None
                if isinstance(node, ast.Call) and node.args and isinstance(node.args[0], ast.Constant):
                    if _is_flag(node.args[0].value):
                        flag, callee = node.args[0].value, _dotted(node.func)
                elif (
                    isinstance(node, ast.Subscript)
                    and isinstance(node.ctx, ast.Load)
                    and _dotted(node.value).endswith("environ")
                    and isinstance(node.slice, ast.Constant)
                    and _is_flag(node.slice.value)
                ):
                    flag, callee = node.slice.value, "os.environ[...]"
                if flag is None:
                    continue
                short = callee.rsplit(".", 1)[-1]
                if short == "get" and not callee.endswith("environ.get"):
                    continue  # a dict that merely has a flag-named key (config, another process's env)
                if short in BRAND_READERS or callee in DEFAULT_WRITERS or short in helpers:
                    continue
                if (rel, flag) in KNOWN_UNCONVERTED:
                    continue
                offenders.append(f"{rel}:{node.lineno}: {callee}({flag!r})")
    return offenders


def test_no_product_flag_is_read_around_the_brand_reader():
    offenders = _direct_flag_reads()
    assert not offenders, (
        "These read a DATAFLOW_/DATAWRAP_ flag without services.brand_env.getenv_brand, so the "
        "other prefix is ignored:\n  " + "\n  ".join(offenders)
    )


def test_cli_reads_both_names(monkeypatch):
    import importlib
    import sys

    cli_root = str(REPO / "apps" / "cli")
    monkeypatch.syspath_prepend(cli_root)
    sys.modules.pop("dataflow_cli.main", None)
    cli = importlib.import_module("dataflow_cli.main")
    _only(monkeypatch, "API_BASE", wrap="https://new.example/api/v1")
    _only(monkeypatch, "API_TOKEN", flow="legacy-token")
    args = cli.build_parser().parse_args(["plan", "-f", "pipeline.yaml"])
    assert args.api == "https://new.example/api/v1"
    assert args.token == "legacy-token"
