"""U1 round 7: TLS file keys are judged by where the path points, not by name.

PR #433 refused every ``sslrootcert`` / ``sslcert`` / ``sslkey`` / ``sslcrl``
key outright. libpq reads those files from the API host's disk, so a host path
must be refused (R11), but the product's own ways of supplying CA material
are an uploaded file, inline PEM, an operator-listed folder and libpq's
``sslrootcert=system`` keyword (``connectors.tls_file_options``). Those must be
accepted at save and test. ``service`` / ``servicefile`` / ``passfile`` /
``DSN`` / ``FILEDSN`` stay refused whatever their value: a service file or file
DSN can name further files and drivers.
"""

from __future__ import annotations

import pytest

PEM = "-----BEGIN CERTIFICATE-----\nMIIBszCCAVmgAwIBAgIU\n-----END CERTIFICATE-----"


@pytest.mark.parametrize("value", ["system", PEM])
@pytest.mark.parametrize("key", ["sslrootcert", "sslcert", "sslkey"])
def test_keyword_and_inline_pem_are_accepted(key, value):
    from services.connector_store import server_side_dsn_refusal

    assert server_side_dsn_refusal({"extra": {key: value}}) == ""


def test_a_file_in_an_operator_listed_folder_is_accepted(tmp_path, monkeypatch):
    from services.connector_store import server_side_dsn_refusal

    monkeypatch.setenv("DATAFLOW_SOURCE_PATH_ROOTS", str(tmp_path))
    ca = str(tmp_path / "ca.pem")
    assert server_side_dsn_refusal({"extra": {"sslrootcert": ca}}) == ""
    assert server_side_dsn_refusal(f"host=db sslrootcert={ca}") == ""


@pytest.mark.parametrize("raw", [
    {"extra": {"sslrootcert": "/etc/ssl/private/platform-ca.pem"}},
    {"extra": {"sslkey": "/root/.postgresql/postgresql.key"}},
    {"extra": {"sslcert": "/var/lib/datawrap/client.crt"}},
    "host=db sslkey=/root/.postgresql/postgresql.key",
    "postgresql://u:p@db/app?sslcert=/var/lib/datawrap/client.crt",
])
def test_a_host_path_is_refused(raw, monkeypatch, tmp_path):
    from services.connector_store import server_side_dsn_refusal

    monkeypatch.setenv("DATAFLOW_SOURCE_PATH_ROOTS", str(tmp_path))
    refused = server_side_dsn_refusal(raw)
    assert "outside the folders connectors may open" in refused
    assert "/root" not in refused and "/etc" not in refused and "/var" not in refused


@pytest.mark.parametrize("raw", [
    {"extra": {"service": "prod"}},
    {"extra": {"passfile": "PLACEHOLDER"}},
    {"extra": {"servicefile": "PLACEHOLDER"}},
    {"extra": {"dsn": "Prod"}},
    {"extra": {"filedsn": "PLACEHOLDER"}},
    "host=db service=prod",
    "postgresql://u:p@db/app?service=prod",
    "Driver={/tmp/x.so};Server=db",
])
def test_service_passfile_and_dsn_stay_refused_even_inside_an_area(raw, tmp_path, monkeypatch):
    from services.connector_store import server_side_dsn_refusal

    monkeypatch.setenv("DATAFLOW_SOURCE_PATH_ROOTS", str(tmp_path))
    inside = str(tmp_path / "f")

    def _fill(value):
        if isinstance(value, dict):
            return {k: _fill(v) for k, v in value.items()}
        return inside if value == "PLACEHOLDER" else value

    assert server_side_dsn_refusal(_fill(raw)) != ""


def test_the_saved_probe_judges_the_saved_fields_not_the_derived_dial_dsn(tmp_path, monkeypatch):
    """The dial DSN percent-encodes a form CA path (``?ssl_ca=%2F...``); the
    saved-connector probe refused an allowed CA because of it (R14 CI failure)."""
    from services.connector_store import dial_connection_string, server_side_dsn_refusal

    monkeypatch.setenv("DATAFLOW_SOURCE_PATH_ROOTS", str(tmp_path))
    ca = str(tmp_path / "ca.pem")
    derived = dial_connection_string("mysql", "require", "", {"ssl_ca": ca})
    assert "%2F" in derived  # the encoded form the probe must not judge
    assert server_side_dsn_refusal("", "db", {"ssl_ca": ca}) == ""
