"""Security follow-up 6: ``type`` in a connector's ``extra`` cannot change which safety rule applies.

``adapters.resolve_connector_config`` did ``cfg.update(endpoint.extra)`` after
setting ``cfg["type"]`` from the endpoint, so an inline PostgreSQL endpoint with
``extra={"type": "s3"}`` resolved to ``cfg["type"] == "s3"``. Every check keyed
on ``cfg["type"]`` then applied another engine's rule: the pre-dial egress
check skipped the AWS region label in ``host`` (an S3 client only signs with
it) while the PostgreSQL driver dialled that name (review round 2, L-E3, on the
transfer path). The endpoint's own type now wins, and the attempt is recorded
in the audit log.
"""

from __future__ import annotations

import socket

import pytest

from src.transfer.adapters import resolve_connector_config
from src.transfer.models import EndpointConfig


def _endpoint(**extra) -> EndpointConfig:
    return EndpointConfig(
        kind="database", format="postgresql", host="us-east-1", port=0, database="d",
        username="u", password="p", table="t", extra=dict(extra),
    )


@pytest.fixture
def events(monkeypatch):
    import services.audit_log as audit_log

    seen: list[dict] = []
    monkeypatch.setattr(audit_log, "append_audit_event", lambda **kw: seen.append(kw))
    try:
        from services.endpoint_extra_guard import reset_dedupe
    except ImportError:  # before the fix: nothing to reset
        reset_dedupe = None
    if reset_dedupe is not None:
        reset_dedupe()
    return seen


@pytest.mark.parametrize("key", ["type", "db_type"])
def test_the_endpoints_own_type_wins(events, key):
    cfg = resolve_connector_config(_endpoint(**{key: "s3"}, sslrootcert="/certs/ca.pem"))
    assert cfg["type"] == "postgresql"
    assert cfg.get("db_type") in (None, "", "postgresql")
    assert cfg["sslrootcert"] == "/certs/ca.pem", "other extra settings still reach the driver"


def test_the_attempt_is_recorded_in_the_audit_log_once(events):
    for _ in range(3):
        resolve_connector_config(_endpoint(type="s3"))
    recorded = [e for e in events if e.get("action") == "connector.extra_type_ignored"]
    assert len(recorded) == 1, events
    details = recorded[0]["details"]
    assert details["endpoint_type"] == "postgresql"
    assert details["extra_type"] == "s3"


def test_a_matching_or_empty_extra_type_is_not_recorded(events):
    resolve_connector_config(_endpoint(type="postgresql"))
    resolve_connector_config(_endpoint(type=""))
    assert not [e for e in events if e.get("action") == "connector.extra_type_ignored"]


def test_the_pre_dial_check_applies_the_endpoints_rule(events, monkeypatch):
    """The reviewer's case: ``us-east-1`` resolves to loopback; no allowlist.

    With ``cfg["type"] == "s3"`` the check skipped the region label (an S3
    client only signs with it) and the port became S3's 443; PostgreSQL then
    dialled ``us-east-1``.
    """
    from services import egress_guard as eg

    monkeypatch.delenv("DATAFLOW_EGRESS_ALLOWLIST", raising=False)
    monkeypatch.delenv("DATAWRAP_EGRESS_ALLOWLIST", raising=False)
    # Enforce the newer pre-dial sites (audit-only by default) so the rule applied is visible.
    monkeypatch.delenv("DATAWRAP_EGRESS_ENFORCE_NEW", raising=False)
    monkeypatch.setenv("DATAFLOW_EGRESS_ENFORCE_NEW", "1")
    real = socket.getaddrinfo

    def resolve(host, port, *a, **k):
        name = host.decode() if isinstance(host, bytes) else str(host)
        if name.lower().rstrip(".") == "us-east-1":
            return real("127.0.0.1", port, *a, **k)
        return real(host, port, *a, **k)

    monkeypatch.setattr(socket, "getaddrinfo", resolve)
    cfg = resolve_connector_config(_endpoint(type="s3"))
    assert int(cfg["port"]) == 5432, "the port is PostgreSQL's, not S3's"
    with pytest.raises(eg.EgressRefused, match="us-east-1"):
        eg.assert_cfg_dial_allowed(dict(cfg), point="transfer")
