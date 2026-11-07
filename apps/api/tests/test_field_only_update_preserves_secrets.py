"""Field-only connector updates must never decrypt-and-rewrite stored secrets.

Prod guard: secret_vault refuses plaintext passthrough, so an update that
re-materialized the record (decrypt → merge → re-encrypt) failed the whole
write when the stored doc held a legacy plaintext secret. The update path now
merges on the raw document: ciphertext is preserved byte-for-byte, plaintext
is re-encrypted by _encrypt_connector_doc, and the explicit clear sentinel
removes a stored secret.
"""

from __future__ import annotations

import pytest

import services.connector_store as cs
import services.secret_vault as sv


class _FakeColl:
    """Minimal stand-in for the connectors collection: find_one + replace_one."""

    def __init__(self, docs: dict):
        self.docs = docs

    def find_one(self, query):
        return self.docs.get(query.get("_id") or query.get("$or", [{}])[0].get("_id"))

    def replace_one(self, query, doc):
        self.docs[query.get("_id")] = doc


@pytest.fixture()
def mongo_store(monkeypatch):
    monkeypatch.setattr(sv, "_is_production", lambda: True)
    monkeypatch.setenv("DATAFLOW_SECRETS_KEY", "test-secrets-key-0123456789abcdef")
    docs: dict = {}

    def fake_coll():
        return _FakeColl(docs)

    monkeypatch.setattr(cs, "_use_mongo", lambda: True)
    monkeypatch.setattr(cs, "_mongo_collection", fake_coll)
    monkeypatch.setattr(
        cs,
        "_connector_id_query",
        lambda cid, ws=None: {"_id": cid},
    )
    return docs


def _seed(docs: dict, **over):
    doc = {
        "_id": "c1",
        "name": "C",
        "type": "snowflake",
        "role": "source",
        "host": "MYOFKOK-UA81716",
        "database": "QA",
        "username": "u",
        "password": sv.encrypt_secret("pat-token-215", label="connector-password"),
        "private_key": "",
        "schema": "PUBLIC",
        "auth_mode": "",
        "workspace_id": "",
    }
    doc.update(over)
    docs["c1"] = doc
    return doc


def test_field_only_update_preserves_ciphertext_byte_for_byte(mongo_store):
    doc = _seed(mongo_store)
    stored_pw = doc["password"]
    out = cs.update_connector("c1", {"auth_mode": "pat", "warehouse": "WH"})
    assert out is not None
    assert mongo_store["c1"]["password"] == stored_pw
    assert mongo_store["c1"]["auth_mode"] == "pat"
    assert out.auth_mode == "pat"


def test_field_only_update_heals_plaintext_secret(mongo_store):
    pem = "-----BEGIN PRIVATE KEY-----\nABC\n-----END PRIVATE KEY-----\n"
    _seed(mongo_store, private_key=pem)
    out = cs.update_connector("c1", {"auth_mode": "pat"})
    assert out is not None
    stored_key = mongo_store["c1"]["private_key"]
    assert stored_key.startswith("enc:v1:")
    assert sv.decrypt_secret(stored_key) == pem


def test_update_on_plaintext_secret_record_no_longer_refuses(mongo_store):
    """The pre-fix path failed with 'Production refuses plaintext secret
    passthrough' because _get_mongo decrypt-raised before the merge."""
    _seed(mongo_store, private_key="-----BEGIN PRIVATE KEY-----\nX\n-----END PRIVATE KEY-----\n")
    out = cs.update_connector("c1", {"auth_mode": "pat", "auth_role": "ACCOUNTADMIN"})
    assert out is not None
    assert mongo_store["c1"]["auth_mode"] == "pat"
    assert mongo_store["c1"]["auth_role"] == "ACCOUNTADMIN"


def test_private_key_clear_sentinel_removes_stored_key(mongo_store):
    pem = "-----BEGIN PRIVATE KEY-----\nABC\n-----END PRIVATE KEY-----\n"
    _seed(mongo_store, private_key=sv.encrypt_secret(pem, label="connector-private_key"))
    out = cs.update_connector("c1", {"private_key": cs.SECRET_CLEAR_SENTINEL})
    assert out is not None
    assert mongo_store["c1"]["private_key"] == ""
    assert out.private_key == ""


def test_masked_placeholder_keeps_stored_secret(mongo_store):
    doc = _seed(mongo_store)
    out = cs.update_connector("c1", {"password": "****", "auth_mode": "user_pass"})
    assert out is not None
    assert mongo_store["c1"]["password"] == doc["password"]


def test_auth_mode_pat_routes_password_ignoring_stale_pem():
    """test_snowflake kwargs: auth_mode=pat suppresses any stored key."""
    from connectors.snowflake import test_snowflake
    from unittest.mock import patch

    captured: dict = {}

    def fake_get_connection(**kwargs):
        captured.update(kwargs)

        class _Cur:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def execute(self, *a, **k):
                return None

            def fetchall(self):
                return []

        class _Conn:
            def cursor(self):
                return _Cur()

            def close(self):
                return None

        return _Conn()

    with patch("connectors.snowflake.get_connection", fake_get_connection):
        res = test_snowflake(
            host="MYOFKOK-UA81716",
            port=0,
            database="DATAFLOW_QA",
            username="u",
            password="pat-token",
            schema="PUBLIC",
            connection_string="",
            ssl=True,
            private_key="-----BEGIN PRIVATE KEY-----\nX\n-----END PRIVATE KEY-----\n",
            auth_mode="pat",
        )
    assert captured.get("auth_mode") == "pat"
    assert captured.get("private_key") in ("", None)
    assert captured.get("password") == "pat-token"
    assert res.ok is True


def test_lifecycle_stages_private_key_clear_in_preview(monkeypatch):
    """update_connector(private_key='') stages the clear sentinel, shown as
    '(cleared)' in the preview, so confirm removes the stored key."""
    import src.ai.copilot.lifecycle_tools as lt
    import src.ai.copilot.ack_ledger as ledger_mod

    staged: dict = {}

    class _Ledger:
        def put(self, *, kind, payload, preview):
            staged.update(payload)
            staged["preview"] = preview
            return "ack-x"

    monkeypatch.setattr(ledger_mod, "get_ack_ledger", lambda: _Ledger())
    pem = "-----BEGIN PRIVATE KEY-----\nABC\n-----END PRIVATE KEY-----\n"
    monkeypatch.setattr(
        lt,
        "_connector_dict",
        lambda *a, **k: {
            "id": "c1",
            "name": "C",
            "type": "snowflake",
            "host": "h",
            "auth_mode": "",
            "private_key": pem,
            "extra": {},
        },
    )
    out = lt.update_connector(connector_id="c1", private_key="")
    assert out.success, out.error
    assert staged["changes"].get("private_key") == cs.SECRET_CLEAR_SENTINEL
    assert staged["preview"]["changes"]["private_key"] == "(cleared)"


def test_lifecycle_omitted_private_key_stages_no_change(monkeypatch):
    """private_key not passed (None) must stage nothing — not a clear."""
    import src.ai.copilot.lifecycle_tools as lt
    import src.ai.copilot.ack_ledger as ledger_mod

    staged: dict = {}

    class _Ledger:
        def put(self, *, kind, payload, preview):
            staged.update(payload)
            return "ack-y"

    monkeypatch.setattr(ledger_mod, "get_ack_ledger", lambda: _Ledger())
    pem = "-----BEGIN PRIVATE KEY-----\nABC\n-----END PRIVATE KEY-----\n"
    monkeypatch.setattr(
        lt,
        "_connector_dict",
        lambda *a, **k: {
            "id": "c1",
            "name": "C",
            "type": "snowflake",
            "host": "h",
            "auth_mode": "",
            "private_key": pem,
            "extra": {},
        },
    )
    out = lt.update_connector(connector_id="c1", auth_mode="pat")
    assert out.success, out.error
    assert "private_key" not in staged["changes"]


def test_invalid_token_under_pat_names_the_token_not_key_pair():
    """A PAT is a JWT — an invalid-token refusal under auth_mode=pat must say
    the token was refused, never send operators to ALTER USER."""
    from connectors.snowflake_conn import classify_snowflake_connect_error

    msg = classify_snowflake_connect_error(
        "JWT token is invalid. [390144]", auth_mode="pat"
    )
    assert "programmatic access token" in msg.lower()
    assert "RSA_PUBLIC_KEY" not in msg


def test_invalid_token_under_key_pair_keeps_the_hint():
    from connectors.snowflake_conn import classify_snowflake_connect_error

    msg = classify_snowflake_connect_error(
        "JWT token is invalid. [390144]", auth_mode="key_pair"
    )
    assert "RSA_PUBLIC_KEY" in msg
    # empty mode keeps the legacy copy
    assert "RSA_PUBLIC_KEY" in classify_snowflake_connect_error(
        "JWT token is invalid. [390144]"
    )


def test_test_snowflake_pat_invalid_token_reports_pat_failure():
    """End-to-end: mode=pat + driver raising an invalid-token error surfaces
    the PAT copy, proving the routing used the token (not a stored key)."""
    from connectors.snowflake import test_snowflake
    from unittest.mock import patch

    def boom(**kwargs):
        assert kwargs.get("auth_mode") == "pat"
        assert kwargs.get("password") == "stored-token"
        assert kwargs.get("private_key") in ("", None)
        raise RuntimeError("JWT token is invalid. [390144]")

    with patch("connectors.snowflake.get_connection", boom):
        res = test_snowflake(
            host="MYOFKOK-UA81716",
            port=0,
            database="DATAFLOW_QA",
            username="u",
            password="stored-token",
            schema="PUBLIC",
            connection_string="",
            ssl=True,
            private_key="-----BEGIN PRIVATE KEY-----\nX\n-----END PRIVATE KEY-----\n",
            auth_mode="pat",
        )
    assert res.ok is False
    assert "programmatic access token" in res.error.lower()
    assert "RSA_PUBLIC_KEY" not in res.error


def test_lifecycle_clear_on_empty_field_shows_already_empty(monkeypatch):
    """private_key='' on a record with no stored key still stages the request —
    the preview must show it was honored, not silently drop it."""
    import src.ai.copilot.lifecycle_tools as lt
    import src.ai.copilot.ack_ledger as ledger_mod

    staged: dict = {}

    class _Ledger:
        def put(self, *, kind, payload, preview):
            staged.update(payload)
            staged["preview"] = preview
            return "ack-z"

    monkeypatch.setattr(ledger_mod, "get_ack_ledger", lambda: _Ledger())
    monkeypatch.setattr(
        lt,
        "_connector_dict",
        lambda *a, **k: {
            "id": "c1",
            "name": "C",
            "type": "snowflake",
            "host": "h",
            "auth_mode": "",
            "private_key": "",
            "extra": {},
        },
    )
    out = lt.update_connector(connector_id="c1", private_key="")
    assert out.success, out.error
    assert staged["changes"].get("private_key") == cs.SECRET_CLEAR_SENTINEL
    assert staged["preview"]["changes"]["private_key"] == "(already empty)"
