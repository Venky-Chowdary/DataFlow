"""Snowflake automated-save repair — locked P1.

Gate (user-declared):
- create AND update MCP paths accept Snowflake warehouse + account role
  distinctly from topology role, persisted and returned (never echo secret).
- Credential encrypt->store->decrypt round-trip proven via the MCP
  create/update path; secret passing must length/hash-check server-side —
  a truncated secret fails loudly at stage time.
- Regression: create/update -> confirm -> test_connector BY EXACT ID reaches
  real Snowflake auth, never "not found"/"record unreadable"/wrong-auth-method
  (key-pair for a PAT/password record).
- Warehouse DATAFLOW_WH / database DATAFLOW_QA / schema PUBLIC / role
  ACCOUNTADMIN settable end-to-end.
"""

from __future__ import annotations

import contextlib
import hashlib

import pytest

import src.ai.copilot.lifecycle_tools as lt

_SF = {
    "id": "sf-1", "name": "SF Trial", "type": "snowflake",
    "host": "MYOFKOK-UA81716", "port": 443, "database": "DATAFLOW_QA",
    "schema": "PUBLIC", "username": "VENKAT", "role": "both",
    "warehouse": "", "auth_role": "", "auth_mode": "pat",
    "password": "old-token", "private_key": "stale-pem",
}


@pytest.fixture
def staged(monkeypatch):
    acks: list[dict] = []

    class Ledger:
        def put(self, *, kind, payload, preview):
            acks.append({"kind": kind, "payload": payload, "preview": preview})
            return f"ack-{len(acks)}"

    import src.ai.copilot.ack_ledger as ledger_mod

    monkeypatch.setattr(ledger_mod, "get_ack_ledger", lambda: Ledger())
    monkeypatch.setattr(
        lt,
        "_connector_dict",
        lambda cid="", name="", **_kw: dict(_SF) if (name or cid) in {"SF Trial", "sf-1"} else None,
    )
    return acks


# --- 1. update path accepts warehouse + account role distinctly -------------

def test_update_stages_warehouse_and_account_role(staged):
    tr = lt.update_connector(
        connector_id="sf-1",
        warehouse="DATAFLOW_WH",
        auth_role="ACCOUNTADMIN",
    )
    assert tr.success, tr.error
    changes = staged[0]["payload"]["changes"]
    assert changes["warehouse"] == "DATAFLOW_WH"
    assert changes["auth_role"] == "ACCOUNTADMIN"
    assert "role" not in changes  # topology role untouched
    # Preview shows non-secret fields; secrets are masked.
    assert staged[0]["preview"]["changes"]["warehouse"] == "DATAFLOW_WH"


def test_update_stages_auth_mode_and_masks_private_key(staged):
    tr = lt.update_connector(
        connector_id="sf-1", auth_mode="key_pair", private_key="PEM-BYTES"
    )
    assert tr.success, tr.error
    changes = staged[0]["payload"]["changes"]
    assert changes["auth_mode"] == "key_pair"
    assert changes["private_key"] == "PEM-BYTES"
    assert "PEM-BYTES" not in repr(staged[0]["preview"])


def test_update_passphrase_rides_in_password(staged):
    tr = lt.update_connector(
        connector_id="sf-1", auth_mode="key_pair", private_key_passphrase="pp"
    )
    assert tr.success, tr.error
    assert staged[0]["payload"]["changes"]["password"] == "pp"


# --- 2. secret length/hash check fails loudly on truncation -----------------

def test_truncated_secret_fails_at_stage_time(staged):
    tr = lt.update_connector(
        connector_id="sf-1",
        password="short",               # truncated PAT: ~10 bytes of a 215-byte token
        secret_length=215,
    )
    assert tr.success is False
    assert "215" in tr.error and "truncated" in tr.error.lower()
    assert "short" not in tr.error  # secret never logged
    assert staged == []             # nothing was staged


def test_hash_mismatch_fails_at_stage_time(staged):
    tr = lt.update_connector(
        connector_id="sf-1",
        password="real-token",
        secret_sha256="0" * 64,
    )
    assert tr.success is False
    assert "sha-256" in tr.error.lower() or "hash" in tr.error.lower()
    assert staged == []


def test_matching_integrity_passes(staged):
    secret = "a-215-byte-token"
    tr = lt.update_connector(
        connector_id="sf-1",
        password=secret,
        secret_length=len(secret.encode()),
        secret_sha256=hashlib.sha256(secret.encode()).hexdigest(),
    )
    assert tr.success, tr.error


# --- 3. driver selection: PAT/password never routes to key-pair -------------

def test_pat_mode_sends_token_authenticator_not_key_pair():
    from connectors.snowflake_conn import snowflake_connect_kwargs

    kw = snowflake_connect_kwargs(
        account="MYOFKOK-UA81716",
        username="VENKAT",
        password="the-pat-token",
        private_key="stale-pem-that-must-be-ignored",
        warehouse="DATAFLOW_WH",
        database="DATAFLOW_QA",
        schema="PUBLIC",
        role="ACCOUNTADMIN",
        auth_mode="pat",
    )
    assert kw["authenticator"] == "PROGRAMMATIC_ACCESS_TOKEN"
    assert kw["token"] == "the-pat-token"
    assert "private_key" not in kw
    assert kw["warehouse"] == "DATAFLOW_WH"
    assert kw["database"] == "DATAFLOW_QA"
    assert kw["schema"] == "PUBLIC"
    assert kw["role"] == "ACCOUNTADMIN"


def test_user_pass_mode_ignores_stray_private_key():
    from connectors.snowflake_conn import snowflake_connect_kwargs

    kw = snowflake_connect_kwargs(
        account="acct",
        username="u",
        password="p",
        private_key="stale-pem",
        auth_mode="user_pass",
    )
    assert kw["password"] == "p"
    assert "private_key" not in kw


def test_empty_mode_keeps_key_pair_inference(monkeypatch):
    import connectors.snowflake_conn as sc

    monkeypatch.setattr(sc, "load_snowflake_private_key", lambda pem, passphrase="": b"der")
    kw = sc.snowflake_connect_kwargs(
        account="acct", username="u", private_key="pem", auth_mode=""
    )
    # empty mode + populated key -> legacy key-pair inference
    assert "private_key" in kw


def test_session_kwargs_strip_private_key_for_password_modes():
    from services.connector_auth import snowflake_session_kwargs

    for mode in ("pat", "user_pass", "connection_string"):
        out = snowflake_session_kwargs(
            {"auth_mode": mode, "private_key": "leftover", "role": "both"}
        )
        assert out["private_key"] == "", mode
        assert out["auth_mode"] == mode
    out = snowflake_session_kwargs({"private_key": "pem", "auth_role": "ACCOUNTADMIN"})
    assert out["private_key"] == "pem"
    assert out["role"] == "ACCOUNTADMIN"



def _no_egress(monkeypatch):
    """Hermetic probe runs: no DNS / metadata-endpoint lookups."""
    monkeypatch.setattr(
        "services.egress_guard.metadata_endpoint_refusal",
        lambda *values, allowlist=None, workspace_id="", db_type="": "",
    )
    monkeypatch.setattr(
        "services.egress_guard.pinned_egress",
        lambda workspace_id="": contextlib.nullcontext(),
    )


def test_probe_honors_declared_auth_mode(monkeypatch):
    """A PAT record with a stray private_key field must NOT open key-pair auth —
    the probe passes auth_mode through to test_snowflake."""
    _no_egress(monkeypatch)
    seen: dict = {}

    def fake_test_snowflake(**kw):
        seen.update(kw)

        class R:
            ok = True
            tables = ["T"]
            error = ""
            message = "ok"
            user = "u"
            database = "d"
            objects: list = []

        return R()

    monkeypatch.setattr("connectors.snowflake.test_snowflake", fake_test_snowflake)
    from src.transfer.connector_registry import run_probe

    ok, _msg = run_probe(
        "snowflake",
        {
            "type": "snowflake",
            "host": "acct",
            "username": "u",
            "password": "pat-token",
            "private_key": "stale-pem",
            "auth_mode": "pat",
            "warehouse": "DATAFLOW_WH",
            "auth_role": "ACCOUNTADMIN",
        },
    )
    assert ok
    assert seen.get("auth_mode") == "pat"


def _pat_jwt_probe(monkeypatch, auth_mode, *, raises=False):
    """Drive run_probe (the entry MCP test_connector uses) with a raw
    'JWT token is invalid' driver error and return the operator message."""
    _no_egress(monkeypatch)

    def fake_test_snowflake(**kw):
        if raises:
            raise RuntimeError("JWT token is invalid. [390144]")

        class R:
            ok = False
            tables: list = []
            error = "JWT token is invalid. [390144]"
            message = ""
            user = "u"
            database = "d"
            objects: list = []

        return R()

    monkeypatch.setattr("connectors.snowflake.test_snowflake", fake_test_snowflake)
    from src.transfer.connector_registry import run_probe

    return run_probe(
        "snowflake",
        {
            "type": "snowflake",
            "host": "acct",
            "username": "u",
            "password": "stored-secret",
            "auth_mode": auth_mode,
            "warehouse": "DATAFLOW_WH",
        },
    )


def test_probe_pat_record_jwt_refusal_names_pat(monkeypatch):
    """auth_mode=pat + 'JWT token is invalid' -> PAT copy, never ALTER USER —
    via the ConnectResult error path and the raise path alike."""
    ok, msg = _pat_jwt_probe(monkeypatch, "pat")
    assert not ok
    assert "programmatic access token" in msg
    assert "ALTER USER" not in msg

    ok, msg = _pat_jwt_probe(monkeypatch, "pat", raises=True)
    assert not ok
    assert "programmatic access token" in msg
    assert "ALTER USER" not in msg


def test_probe_user_pass_jwt_refusal_names_password(monkeypatch):
    ok, msg = _pat_jwt_probe(monkeypatch, "user_pass")
    assert not ok
    assert "password" in msg.lower()
    assert "ALTER USER" not in msg


def test_probe_key_pair_jwt_refusal_keeps_hint(monkeypatch):
    ok, msg = _pat_jwt_probe(monkeypatch, "key_pair")
    assert not ok
    assert "ALTER USER" in msg


# --- 4. create path accepts the same fields ----------------------------------

def test_create_schema_accepts_snowflake_fields():
    import src.ai.copilot.tools as t

    props = {
        p for tool in t.TOOL_DEFINITIONS if tool["name"] == "create_connector"
        for p in tool["input_schema"]["properties"]
    }
    for want in (
        "warehouse", "auth_role", "auth_mode",
        "private_key", "secret_sha256", "secret_length",
    ):
        assert want in props, want


def test_update_schema_accepts_snowflake_fields():
    from src.ai.copilot.lifecycle_tools import LIFECYCLE_TOOL_DEFINITIONS

    props = {
        p for tool in LIFECYCLE_TOOL_DEFINITIONS if tool["name"] == "update_connector"
        for p in tool["input_schema"]["properties"]
    }
    for want in (
        "warehouse", "auth_role", "auth_mode",
        "private_key", "secret_sha256", "secret_length",
    ):
        assert want in props, want


def test_multi_secret_call_requires_secret_field(staged):
    """Two secret fields + a declared hash without secret_field = refused."""
    tr = lt.update_connector(
        connector_id="sf-1",
        password="tok",
        private_key="pem",
        secret_sha256="0" * 64,
    )
    assert tr.success is False
    assert "secret_field" in tr.error
    assert staged == []


def test_secret_field_selects_the_checked_secret(staged):
    import hashlib as h

    tok = "tok-bytes"
    tr = lt.update_connector(
        connector_id="sf-1",
        password=tok,
        private_key="pem",
        secret_field="password",
        secret_sha256=h.sha256(tok.encode()).hexdigest(),
    )
    assert tr.success, tr.error


def test_empty_mode_non_pem_key_routes_to_password():
    """A masked/garbage private_key must not silently route to key-pair when a
    password is present and no auth_mode is declared (route by secret type)."""
    from connectors.snowflake_conn import snowflake_connect_kwargs

    kw = snowflake_connect_kwargs(
        account="acct", username="u", password="pat-or-pw",
        private_key="****", auth_mode="",
    )
    assert kw["password"] == "pat-or-pw"
    assert "private_key" not in kw


def test_empty_mode_non_pem_key_no_password_still_reports_key_error():
    from connectors.snowflake_conn import snowflake_connect_kwargs

    with pytest.raises((ValueError, RuntimeError)):
        snowflake_connect_kwargs(
            account="acct", username="u", private_key="not-a-pem", auth_mode=""
        )


def test_router_test_endpoint_jwt_refusal_names_pat(monkeypatch):
    """The modal Test button (unsaved config) must also name the PAT — the
    outer except in connectors_router was classifying without auth_mode."""
    import asyncio
    import importlib

    router_mod = importlib.import_module("src.routers.connectors_router")

    def fake_run_probe(driver, cfg):
        raise RuntimeError("JWT token is invalid. [390144]")

    monkeypatch.setattr(router_mod, "run_probe", fake_run_probe)

    class _Op:
        def update(self, **kw):
            pass

        def errored(self, exc):
            pass

    async def _go():
        req = router_mod.TestConnectionRequest(
            type="snowflake",
            host="acct",
            username="u",
            password="tok",
            auth_mode="pat",
        )
        return await router_mod._test_connection_impl(req, _Op())

    out = asyncio.run(_go())
    assert out["success"] is False
    assert "programmatic access token" in out["message"]
    assert "ALTER USER" not in out["message"]
