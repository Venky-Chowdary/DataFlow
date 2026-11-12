"""Follow-up review 3 (L2): an ODBC key's ``,password:`` suffix cannot climb back into a listed folder.

ODBC Driver 18 reads ``ClientKey=file:<path>[,password:<pwd>]`` (and the same
shape for ``ClientCertificate``): the driver opens the text before the first
``,``. The check judged the whole value, so
``file:/etc/ssl/private/c.key,password:/../../../../<listed folder>/x``
collapsed (``..`` across the suffix) into the listed folder and passed while the
driver opened ``/etc/ssl/private/c.key``. Now a value is also judged up to its
first ``,``.
"""

from __future__ import annotations

import pytest

_KEY = "/etc/ssl/private/c.key"


@pytest.fixture
def listed(tmp_path, monkeypatch):
    for prefix in ("DATAFLOW", "DATAWRAP"):
        monkeypatch.delenv(f"{prefix}_SOURCE_PATH_ROOTS", raising=False)
    folder = tmp_path / "tls"
    folder.mkdir()
    monkeypatch.setenv("DATAFLOW_SOURCE_PATH_ROOTS", str(folder))
    return folder


def _climb(listed) -> str:
    prefix = f"{_KEY},password:"
    depth = len([part for part in prefix.split("/") if part])
    return f"file:{prefix}" + "/.." * depth + f"{listed}/x"


@pytest.mark.parametrize("option", ["ClientKey", "ClientCertificate", "ServerCertificate"])
def test_an_odbc_file_value_is_judged_up_to_its_first_comma(listed, option):
    from connectors import tls_file_options as t

    odbc = f"Driver={{ODBC Driver 18 for SQL Server}};Server=db.example.com;{option}={_climb(listed)}"
    refusal = t.text_refusal(odbc, "in this connector's connection settings")
    assert option in refusal
    assert "/etc/" not in refusal and str(listed) not in refusal


def test_save_refuses_the_climbing_key(listed):
    from services.egress_guard import connector_save_refusal

    odbc = (f"Driver={{ODBC Driver 18 for SQL Server}};Server=db.example.com;"
            f"ClientCertificate=file:{listed}/c.pem;ClientKey={_climb(listed)}")
    assert "ClientKey" in connector_save_refusal({"type": "sqlserver", "name": "x", "connection_string": odbc})


def test_a_listed_key_with_a_password_suffix_is_still_accepted(listed):
    from connectors import tls_file_options as t

    odbc = (f"Driver={{ODBC Driver 18 for SQL Server}};Server=db.example.com;"
            f"ClientCertificate=file:{listed}/c.pem;ClientKey=file:{listed}/c.key,password:secret")
    assert t.text_refusal(odbc, "w") == ""
