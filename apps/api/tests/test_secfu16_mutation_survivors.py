"""Follow-up review 1, L2: tests for the three checks no test held (mutants m11, m15, m17).

* m11: ``egress_url_allowed`` also judges the percent-decoded host, which is the
  one urllib3 dials. The reviewer thought it redundant (``_normalize_host``
  stops at ``%``), but a prefix before the ``%`` can be a public address while
  the decoded host is the metadata service: ``285203916%36`` is judged as
  ``285203916`` (16.255.221.204) and dialled as ``2852039166``
  (169.254.169.254).
* m15: ``follow_url_allowed`` never follows a server-supplied next/redirect URL
  with an encoded host; without that check ``http://%31%36%39.254.169.254/``
  read as an empty host, was only audited, and was followed.
* m17: with no workspace bound under multi-tenant auth, an upload another
  workspace owns opens only for a platform administrator; an unscoped upload
  opens for anyone.
"""

from __future__ import annotations

import socket
import uuid

import pytest

from services import egress_guard as eg

_REAL_GETADDRINFO = socket.getaddrinfo


def _numeric_only(host, *_a, **_k):
    """No DNS: numeric spellings resolve as the C library reads them."""
    return _REAL_GETADDRINFO(host, None, flags=socket.AI_NUMERICHOST)


@pytest.fixture
def no_dns(monkeypatch):
    for prefix in ("DATAFLOW", "DATAWRAP"):
        monkeypatch.delenv(f"{prefix}_EGRESS_ALLOWLIST", raising=False)
        monkeypatch.delenv(f"{prefix}_EGRESS_ENFORCE_NEW", raising=False)
    monkeypatch.setattr(socket, "getaddrinfo", _numeric_only)


def test_premise_urllib3_dials_the_decoded_host():
    from urllib3.util import parse_url

    assert parse_url("http://285203916%36/latest/meta-data/").host == "2852039166"
    assert _numeric_only("2852039166")[0][4][0] == "169.254.169.254"
    assert _numeric_only("285203916")[0][4][0] == "16.255.221.204"


def test_m11_the_decoded_host_is_the_one_judged(no_dns):
    assert eg.egress_url_allowed("http://16.255.221.204/", allowlist=()) is True  # the prefix alone
    assert eg.egress_url_allowed("http://285203916%36/latest/meta-data/", allowlist=()) is False


@pytest.mark.parametrize(
    "url",
    ["http://%31%36%39.254.169.254/latest/meta-data/", "http://285203916%36/latest/meta-data/"],
)
def test_m15_a_followed_url_with_an_encoded_host_is_refused(no_dns, monkeypatch, url):
    import services.audit_log as audit_log

    monkeypatch.setattr(audit_log, "append_audit_event", lambda **_kw: None)
    assert eg.follow_url_allowed(url, "https://api.example.com/v1/items") is False


@pytest.fixture
def multi_tenant_uploads(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_UPLOAD_DIR", str(tmp_path / "uploads"))
    (tmp_path / "uploads").mkdir()
    from services import file_parser

    def register(owner: str):
        file_id = uuid.uuid4().hex[:16]
        path = tmp_path / "uploads" / f"{file_id}_rows.sqlite"
        path.write_bytes(b"")
        monkeypatch.setitem(
            file_parser._file_registry, file_id,
            {"file_id": file_id, "path": str(path), "workspace_id": owner},
        )
        return path.resolve()

    return register


def test_m17_an_unbound_caller_cannot_open_another_workspaces_upload(multi_tenant_uploads):
    from services.connector_access import ConnectorCaller, bind_connector_caller, current_caller
    from services.local_file_policy import connector_may_open_upload_path

    assert current_caller() is None
    owned = multi_tenant_uploads("ws-b")
    unscoped = multi_tenant_uploads("")
    assert connector_may_open_upload_path(owned) is False
    assert connector_may_open_upload_path(unscoped) is True

    with bind_connector_caller(ConnectorCaller(actor="admin@example.com", platform_admin=True)):
        assert connector_may_open_upload_path(owned) is True
