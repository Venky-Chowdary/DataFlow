"""Ops review round 3, Low C: production compose gets transactions; a BYOK save never raises a raw ValueError.

* ``docker-compose.prod.yml`` ran MongoDB standalone (no ``--replSet``), so the
  control-plane stores had no multi-document transactions: a BYOK rotation
  that failed part-way left the tenant with no active key. It now runs a
  one-node replica set, like ``docker-compose.yml``, and every service's
  connection string names it.
* ``encrypt_secret`` checked for an active BYOK key and ``tenant_encrypt``
  then looked it up again. A rotation between the two made the second lookup
  find none and raise ``ValueError("No BYOK key configured ...")`` (28 of 752
  saves in the review's probe) instead of the typed ``SecretVaultError``
  refusal. The key the check found is now the key the secret is wrapped with.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from services import byok_key_manager, secret_vault, tenant_store
from tests._control_plane_fakes import install_shared_store

ROOT = Path(__file__).resolve().parents[3]
TID = "tenant-c"


def _services(name: str) -> dict:
    return yaml.safe_load((ROOT / name).read_text(encoding="utf-8"))["services"]


def test_production_compose_runs_mongodb_as_a_one_node_replica_set():
    prod, dev = _services("docker-compose.prod.yml"), _services("docker-compose.yml")
    mongo = prod["mongodb"]
    launch = " ".join(mongo.get("entrypoint") or []) + " " + " ".join(mongo.get("command") or [])
    dev_launch = " ".join(dev["mongodb"].get("command") or [])
    assert "--replSet rs0" in dev_launch and "--replSet rs0" in launch, launch
    # Authentication is on in production: a replica set with auth needs an internal key file.
    assert "--keyFile" in launch
    check = " ".join(mongo["healthcheck"]["test"])
    assert "rs.initiate" in check and "mongodb:27017" in check, "the node initiates its own replica set"
    assert "isWritablePrimary" in check, "healthy only once it is a primary, so dependants get transactions"
    uris = {name: (svc.get("environment") or {}).get("MONGODB_URI") for name, svc in prod.items()}
    uris = {name: uri for name, uri in uris.items() if uri}
    assert {"api", "worker", "cdc-worker", "scheduler"} <= set(uris)
    assert all("replicaSet=rs0" in uri for uri in uris.values()), uris
    assert all("directConnection" not in uri for uri in uris.values()), "directConnection reads as standalone"
    for name in uris:
        assert (prod[name].get("depends_on") or {}).get("mongodb", {}).get("condition") == "service_healthy", name


def test_the_deploy_docs_describe_the_compose_replica_set():
    readme = (ROOT / "deploy/enterprise/helm/dataflow/README.md").read_text(encoding="utf-8")
    assert "docker-compose.prod.yml" in readme and "replicaSet=rs0" in readme


@pytest.fixture
def byok_tenant(tmp_path, monkeypatch):
    install_shared_store(monkeypatch)
    monkeypatch.setattr(tenant_store, "STORE_PATH", tmp_path / "tenants.json")
    monkeypatch.setattr(byok_key_manager, "STORE_PATH", tmp_path / "byok_keys.json")
    return byok_key_manager.create_key(TID, "k0")


def test_a_rotation_between_the_check_and_the_encryption_never_raises_a_raw_value_error(byok_tenant, monkeypatch):
    """The second lookup sees the gap a rotation on another replica leaves: no active key."""
    real = byok_key_manager.get_active_key_for_tenant
    calls = {"n": 0}

    def racing(tenant_id):
        calls["n"] += 1
        return real(tenant_id) if calls["n"] == 1 else None

    monkeypatch.setattr(byok_key_manager, "get_active_key_for_tenant", racing)
    try:
        stored = secret_vault.encrypt_secret("pw", tenant_id=TID)
    except secret_vault.SecretVaultError:
        return  # the typed refusal is acceptable
    assert stored.startswith(f"byok:{byok_tenant.id}:"), "wrapped with the key the check found"
    monkeypatch.setattr(byok_key_manager, "get_active_key_for_tenant", real)
    assert secret_vault.decrypt_secret(stored, tenant_id=TID) == "pw"


def test_a_secret_saved_while_the_key_is_rotated_still_decrypts(byok_tenant):
    stored = secret_vault.encrypt_secret("pw", tenant_id=TID)
    byok_key_manager.rotate_key(TID)
    assert secret_vault.decrypt_secret(stored, tenant_id=TID) == "pw"
    assert secret_vault.encrypt_secret("pw2", tenant_id=TID) != stored
