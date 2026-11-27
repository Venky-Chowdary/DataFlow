"""D375: a transfer into an SFTP sub-folder through a SAVED connector honours its pin.

QA: "Target DDL: Could not read qav3/q3_csv.csv ... SFTP host key ... is not
trusted" while the connector's test passed and a top-level dest_table worked.
The saved connector keeps the pin under ``extra`` (the store lifts
``host_key`` there). This drives the user's path end to end: a connector
saved through the store, a destination that names it by id with the nested
table ``qav3/q3_csv.csv``, and ``UniversalTransferEngine.execute_tracked`` with
preflight on (the Target DDL read-back runs) against a real in-process SFTP
server (paramiko, host key generated per run).

AC-1: with the right pin the run succeeds and the missing folder is created.
AC-2: with a wrong pin the run fails on the host key and writes nothing.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import paramiko
import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

#: The QA path, one folder per test (the SFTP server fixture is shared).
NESTED = "qav3_{}/q3_csv.csv"
_CSV = b"id,name\n1,alpha\n2,beta\n3,gamma\n"


@pytest.fixture
def stores(tmp_path, monkeypatch):
    from tests import r4_route_harness as h

    h.isolate(tmp_path, monkeypatch)


@pytest.fixture
def server(local_sftp):
    if local_sftp is None:
        pytest.skip("local SFTP server unavailable")
    return local_sftp


def _wrong_key() -> str:
    key = paramiko.RSAKey.generate(2048)
    return f"{key.get_name()} {key.get_base64()}"


def _saved_sftp(server, pin: str) -> str:
    from services.connector_store import create_connector, get_connector

    conn = create_connector({
        "name": f"d375-{uuid.uuid4().hex[:6]}", "type": "sftp", "role": "destination",
        "host": server.host, "port": server.port, "username": server.username,
        "password": server.password, "database": "/", "host_key": pin, "workspace_id": "",
    })
    saved = get_connector(conn.id)
    assert saved.extra.get("host_key") == pin and not getattr(saved, "host_key", "")
    return conn.id


def _run(connector_id: str, nested: str):
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        destination=EndpointConfig(kind="database", format="sftp", connector_id=connector_id,
                                   database="/", table=nested),
        source_content=_CSV,
        source_filename="q3.csv",
        sync_mode="full_refresh_overwrite",
        skip_preflight=False,
        validation_mode="strict",
        mappings=[{"source": c, "target": c, "confidence": 0.99} for c in ("id", "name")],
    )
    return UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])


@pytest.mark.parametrize("name", ["q3_csv.csv", "q3_tsv.tsv", "q3_xlsx.xlsx"])
def test_right_pin_runs_into_a_new_sub_folder(stores, server, name):
    nested = f"qav3_{uuid.uuid4().hex[:6]}/{name}"
    assert not Path(server.local_path("/" + nested)).parent.exists()
    result = _run(_saved_sftp(server, server.host_key), nested)
    assert result.success is True, result.error
    assert result.records_transferred == 3
    landed = Path(server.local_path("/" + nested))
    assert landed.is_file() and not Path(server.local_path("/" + name)).exists()
    if name.endswith(".csv"):
        lines = landed.read_text(encoding="utf-8").splitlines()
        assert lines[0].strip() == "id,name" and len(lines) == 4, lines


def test_wrong_pin_fails_on_the_host_key_and_writes_nothing(stores, server):
    nested = NESTED.format(uuid.uuid4().hex[:6])
    result = _run(_saved_sftp(server, _wrong_key()), nested)
    assert result.success is False
    text = str(result.error or "").lower()
    assert "host key" in text or "trust" in text, result.error
    assert not Path(server.local_path("/" + nested)).exists()
