"""Follow-up review 2, BQ: a tenant's BigQuery / GCS connector never uses the API host's Google identity.

``connectors.bigquery_conn.get_client`` built ``bigquery.Client`` with no
credentials when a non-emulator connector had no ``service_account``,
``connection_string`` or ``credentials_path``: google-cloud-core then calls
``google.auth.default()``, the API host's Application Default Credentials
(environment key file, gcloud user, GCE / GKE metadata identity), so the
tenant's queries ran as the platform in a project the tenant named. And a
credential reference that was not JSON was read with
``service_account.Credentials.from_service_account_file`` from any path on the
API host (``bigquery_conn`` and ``connectors.gcs_common._credentials``; GCS also
reads the ``password`` field there), so a tenant could use any service-account
key the host holds.

Now, as for Iceberg's GCS / Azure FileIO (follow-up review 1, commit 21):
without ``DATAFLOW_ALLOW_PLATFORM_GCP_AZURE_CREDENTIALS`` a BigQuery client
for a non-emulator host needs the connector's own key (JSON), and a key file is
read only from inside the areas a connector may open (an upload the workspace
owns, its own export folder, or a directory the operator lists in
``DATAFLOW_SOURCE_PATH_ROOTS``). Refused at save and where each client is
built, naming the setting and never the path. Emulators (anonymous
credentials) are unchanged, and so is GCS without a credential (anonymous).
"""

from __future__ import annotations

import json

import pytest

FLAG = "DATAFLOW_ALLOW_PLATFORM_GCP_AZURE_CREDENTIALS"
_SENTINEL = "SENTINEL-HOST-KEY-PATH"
_INFO = {"type": "service_account", "project_id": "tenant-proj", "client_email": "x@tenant-proj.iam.gserviceaccount.com"}


@pytest.fixture(params=["off", "on"])
def mode(request, monkeypatch):
    for prefix in ("DATAFLOW", "DATAWRAP"):
        monkeypatch.delenv(f"{prefix}_ALLOW_PLATFORM_GCP_AZURE_CREDENTIALS", raising=False)
        monkeypatch.delenv(f"{prefix}_SOURCE_PATH_ROOTS", raising=False)
    if request.param == "on":
        monkeypatch.setenv(FLAG, "1")
    monkeypatch.setattr(
        "socket.socket.connect",
        lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("a test dialled")),
    )
    return request.param


@pytest.fixture
def google(monkeypatch):
    """Records every way a Google client picks up a credential; none touches the host's own."""
    import google.auth
    from google.auth.credentials import AnonymousCredentials
    from google.oauth2 import service_account

    calls: list[tuple[str, str]] = []

    def default(*_a, **_k):
        calls.append(("adc", ""))
        return AnonymousCredentials(), "platform-project"

    def from_file(path, *_a, **_k):
        calls.append(("file", str(path)))
        return AnonymousCredentials()

    def from_info(info, *_a, **_k):
        calls.append(("info", str(info.get("project_id"))))
        return AnonymousCredentials()

    monkeypatch.setattr(google.auth, "default", default)
    monkeypatch.setattr(service_account.Credentials, "from_service_account_file", staticmethod(from_file))
    monkeypatch.setattr(service_account.Credentials, "from_service_account_info", staticmethod(from_info))
    return calls


@pytest.fixture
def host_key(tmp_path):
    """A key file on the API host, outside every area a connector may open."""
    path = tmp_path / _SENTINEL / "platform-key.json"
    path.parent.mkdir()
    path.write_text(json.dumps(_INFO))
    return str(path)


def _bq(**kwargs):
    from connectors.bigquery_conn import get_client

    return get_client(project_id="tenant-proj", **kwargs)


def _clean(message: str) -> None:
    assert _SENTINEL not in message and "platform-key" not in message, message


# --- BigQuery with no credential of its own -----------------------------------


@pytest.mark.parametrize("host", ["", "tenant-proj", "bigquery.googleapis.com"])
def test_bigquery_without_a_credential_never_uses_the_hosts_adc(mode, google, host):
    if mode == "off":
        with pytest.raises(ValueError) as caught:
            _bq(host=host)
        assert FLAG in str(caught.value)
        assert google == []
    else:
        _bq(host=host)
        assert google == [("adc", "")]


@pytest.mark.parametrize("value", [0, False, [], {}, None, "", "   "])
@pytest.mark.parametrize("field", ["service_account", "connection_string", "credentials_path"])
def test_a_save_with_no_usable_credential_is_refused(mode, field, value):
    from services.egress_guard import connector_save_refusal

    data = {"name": "bq", "type": "bigquery", "host": "tenant-proj", "database": "tenant-proj", field: value}
    refusal = connector_save_refusal(data)
    if mode == "off":
        assert FLAG in refusal
    else:
        assert refusal == ""


# --- a key file on the API host ------------------------------------------------


@pytest.mark.parametrize("field", ["credentials_path", "connection_string", "service_account"])
def test_bigquery_never_reads_a_key_file_outside_the_connector_areas(mode, google, host_key, field):
    if mode == "off":
        with pytest.raises(ValueError) as caught:
            _bq(host="tenant-proj", **{field: host_key})
        _clean(str(caught.value))
        assert google == []
    else:
        _bq(host="tenant-proj", **{field: host_key})
        assert google == [("file", host_key)]


@pytest.mark.parametrize("field", ["service_account", "connection_string", "password"])
def test_gcs_never_reads_a_key_file_outside_the_connector_areas(mode, google, host_key, field):
    from connectors.gcs_common import gcs_client

    cfg = {"type": "gcs", "host": "tenant-proj", field: host_key}
    if mode == "off":
        with pytest.raises(ValueError) as caught:
            gcs_client(cfg)
        _clean(str(caught.value))
        assert google == []
    else:
        gcs_client(cfg)
        assert google == [("file", host_key)]


@pytest.mark.parametrize(
    "data",
    [
        {"type": "bigquery", "host": "tenant-proj", "credentials_path": "K"},
        {"type": "bigquery", "host": "tenant-proj", "connection_string": "K"},
        {"type": "bigquery", "host": "tenant-proj", "service_account": "K"},
        {"type": "gcs", "host": "tenant-proj", "password": "K"},
        {"type": "gcs", "host": "tenant-proj", "service_account": "K"},
        {"type": "gcs", "host": "tenant-proj", "connection_string": "K"},
    ],
    ids=["bq-credentials-path", "bq-connection-string", "bq-service-account", "gcs-password", "gcs-service-account", "gcs-connection-string"],
)
def test_save_refuses_a_key_file_outside_the_connector_areas(mode, host_key, data):
    from services.egress_guard import connector_save_refusal

    form = {"name": "g", **{k: (host_key if v == "K" else v) for k, v in data.items()}}
    refusal = connector_save_refusal(form)
    if mode == "off":
        assert refusal, form
        _clean(refusal)
    else:
        assert refusal == ""


def test_a_key_file_in_an_operator_listed_folder_is_read(mode, google, host_key, monkeypatch):
    from pathlib import Path

    monkeypatch.setenv("DATAFLOW_SOURCE_PATH_ROOTS", str(Path(host_key).parent))
    _bq(host="tenant-proj", credentials_path=host_key)
    assert google == [("file", host_key)]


def test_a_key_file_that_climbs_out_of_a_listed_folder_is_refused(google, host_key, tmp_path, monkeypatch):
    for prefix in ("DATAFLOW", "DATAWRAP"):
        monkeypatch.delenv(f"{prefix}_ALLOW_PLATFORM_GCP_AZURE_CREDENTIALS", raising=False)
    listed = tmp_path / "listed"
    listed.mkdir()
    monkeypatch.setenv("DATAFLOW_SOURCE_PATH_ROOTS", str(listed))
    sneaky = str(listed / ".." / _SENTINEL / "platform-key.json")
    with pytest.raises(ValueError):
        _bq(host="tenant-proj", credentials_path=sneaky)
    assert google == []


# --- what is unchanged ----------------------------------------------------------


def test_the_connectors_own_json_key_is_used(mode, google):
    _bq(host="tenant-proj", service_account=json.dumps(_INFO))
    assert google == [("info", "tenant-proj")]


@pytest.mark.parametrize("host", ["localhost", "127.0.0.1", "host.docker.internal"])
def test_an_emulator_uses_anonymous_credentials(mode, google, host):
    client = _bq(host=host, port=9050)
    assert google == []
    assert type(client._credentials).__name__ == "AnonymousCredentials"


def test_an_emulator_url_uses_anonymous_credentials(mode, google):
    _bq(connection_string="http://bq-emulator.example:9050")
    assert google == []


def test_gcs_without_a_credential_stays_anonymous(mode, google):
    from connectors.gcs_common import gcs_client

    client = gcs_client({"type": "gcs", "host": "tenant-proj"})
    assert google == []
    assert type(client._credentials).__name__ == "AnonymousCredentials"


def test_save_accepts_a_json_key_and_an_emulator(mode):
    from services.egress_guard import connector_save_refusal

    assert connector_save_refusal(
        {"name": "bq", "type": "bigquery", "host": "tenant-proj", "service_account": json.dumps(_INFO)}
    ) == ""
    assert connector_save_refusal({"name": "gcs", "type": "gcs", "host": "tenant-proj"}) == ""
