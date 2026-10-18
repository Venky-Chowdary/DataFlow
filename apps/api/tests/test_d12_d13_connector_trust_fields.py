"""D-12/D-13: SQL Server self-signed TLS + SFTP host-key pin must be reachable
from the saved form and the in-form Test — not hidden in ``extra``.

Verified bugs:
- D-12: saved SQL Server connector could not pass ``trust_server_certificate``
  (DTO dropped it, form never offered it) while the engine requires
  ``extra.trust_server_certificate=True`` for self-signed servers.
- D-13: the SFTP form offered no host-key field even though the probe's own
  failure text tells the operator to save ``host_key=<fingerprint>``.
"""

from services.connector_store import connector_extra_from_payload
from src.routers.connectors_router import TestConnectionRequest
from src.routers.saved_connectors_router import ConnectorSaveDTO


def test_save_dto_accepts_trust_server_certificate():
    dto = ConnectorSaveDTO(
        name="mssql",
        type="sqlserver",
        host="db.example.com",
        port=1433,
        database="app",
        username="sa",
        password="x",
        trust_server_certificate=True,
    )
    assert dto.trust_server_certificate is True


def test_trust_server_certificate_lifted_into_extra():
    extra = connector_extra_from_payload({"trust_server_certificate": True})
    assert extra["trust_server_certificate"] is True


def test_trust_server_certificate_explicit_false_clears():
    # False must persist (operator un-checked the box); only None/"" stay put.
    extra = connector_extra_from_payload({"trust_server_certificate": False})
    assert extra["trust_server_certificate"] is False


def test_sftp_host_key_lifted_into_extra():
    extra = connector_extra_from_payload({"host_key": "SHA256:abc123"})
    assert extra["host_key"] == "SHA256:abc123"


def test_test_connection_request_accepts_trust_fields():
    req = TestConnectionRequest(
        type="sftp",
        database="/uploads",
        host="sftp.example.com",
        host_key="SHA256:abc123",
        host_key_policy="",
        known_hosts="",
        trust_server_certificate=None,
    )
    assert req.host_key == "SHA256:abc123"
    req2 = TestConnectionRequest(
        type="sqlserver",
        database="app",
        host="db.example.com",
        trust_server_certificate=True,
    )
    assert req2.trust_server_certificate is True
