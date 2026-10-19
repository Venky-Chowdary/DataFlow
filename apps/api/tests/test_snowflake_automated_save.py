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


def test_probe_honors_declared_auth_mode(monkeypatch):
    """A PAT record with a stray private_key field must NOT open key-pair auth —
    the probe passes auth_mode through to test_snowflake."""
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
