"""CI-1 isolation regression — the cached vault must follow the configured
backend, not the first-built one.

Pre-fix: a test that set ``DATAFLOW_SECRETS_BACKEND=aws_secretsmanager`` and
reset ``_vault_instance`` cached an ``AwsSecretsManagerVault``; the
monkeypatch reverted the env var but the instance stayed cached, so the next
``encrypt_secret`` in the suite stamped ``sm:`` references where ``enc:`` was
configured (``enterprise_p0_p2`` failed with an ``sm:`` prefix under
pytest-randomly ordering).
"""

from __future__ import annotations

from services import secret_vault


def test_vault_rebuilds_when_cached_backend_no_longer_matches(monkeypatch):
    monkeypatch.delenv("DATAFLOW_SECRETS_BACKEND", raising=False)
    # Simulate the leak: an AWS vault cached while env now wants fernet.
    secret_vault._vault_instance = secret_vault.AwsSecretsManagerVault()
    try:
        vault = secret_vault._get_vault()
        assert isinstance(vault, secret_vault.FernetVault)
        ref = secret_vault.encrypt_secret("ci-order-leak", label="t")
        assert not ref.startswith("sm:"), ref
    finally:
        secret_vault._vault_instance = None


def test_vault_keeps_matching_cached_instance(monkeypatch):
    monkeypatch.delenv("DATAFLOW_SECRETS_BACKEND", raising=False)
    secret_vault._vault_instance = None
    first = secret_vault._get_vault()
    try:
        assert secret_vault._get_vault() is first
    finally:
        secret_vault._vault_instance = None
