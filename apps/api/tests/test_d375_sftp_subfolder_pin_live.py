"""D375: SFTP sub-folder destinations use the pinned host key on every path — live.

QA: "Target DDL: Could not read qav3/q3_csv.csv ... SFTP host key for
<host>:44093 is not trusted" while test_connector passed and a top-level
dest_table worked: the pin saved under ``extra`` did not reach the nested
path's readers. 8d2b01127 makes ``host_key_settings``/``parse_sftp_config``
read ``extra``; its test mocked ``connect_sftp``. The review gap: no proof
that a WRONG pin still fails.

Against a real in-process SFTP server (paramiko; its host key generated per
run), with the pin only under ``extra`` as a saved connector stores it, for
a nested path ``qav3/q3_csv.csv``: the probe, the writer (which creates the
missing folder) and the destination read-back (the Target DDL / count path)
all succeed with the right key, and each refuses a wrong key.

Two paths still dropped a pin saved only under ``extra`` (found by this
test): ``test_sftp`` (the introspect "can we reach it" probe) did not hand
``extra`` to ``parse_sftp_config``, and the destination privilege probe
passed only the top-level ``host_key``. Both answered "host key ... is not
trusted" for a pinned connector, the message QA recorded.
"""

from __future__ import annotations

import paramiko
import pytest

NESTED = "qav3/q3_csv.csv"


@pytest.fixture
def server(local_sftp):
    if local_sftp is None:
        pytest.skip("local SFTP server unavailable")
    return local_sftp


def _wrong_key() -> str:
    key = paramiko.RSAKey.generate(2048)
    return f"{key.get_name()} {key.get_base64()}"


def _cfg(server, pin: str) -> dict:
    return {
        "host": server.host, "port": server.port, "username": server.username,
        "password": server.password, "database": "/", "table": NESTED,
        "extra": {"host_key": pin},
    }


def _write(server, pin: str):
    from connectors.sftp_writer import write_mapped_rows

    cfg = _cfg(server, pin)
    return write_mapped_rows(
        host=cfg["host"], port=cfg["port"], username=cfg["username"], password=cfg["password"],
        database="/", table_name=NESTED, headers=["id", "name"], data_rows=[["1", "a"], ["2", "b"]],
        mappings=[{"source": "id", "target": "id"}, {"source": "name", "target": "name"}],
        column_types={"id": "integer", "name": "text"}, extra=cfg["extra"],
    )


def test_the_right_pin_under_extra_reaches_every_nested_path(server):
    from connectors.sftp_common import test_sftp
    from services.object_streaming import open_sftp_binary

    cfg = _cfg(server, server.host_key)
    ok, message = test_sftp(host=cfg["host"], port=cfg["port"], username=cfg["username"],
                            password=cfg["password"], database="/", table=NESTED,
                            extra=cfg["extra"], require_object=False)
    assert ok, message
    result = _write(server, server.host_key)
    assert result.ok, result.error
    with open(server.local_path("/" + NESTED), encoding="utf-8") as fh:
        assert fh.read().splitlines()[0].strip() == "id,name"
    opened = open_sftp_binary(cfg)
    assert opened not in (None, False), "the read-back (Target DDL / count) must open the nested file"
    stream, closer = opened
    try:
        assert b"id,name" in stream.read()
    finally:
        closer()


def test_the_privilege_probe_honours_the_pin_under_extra(server):
    from services.destination_privilege_probe import probe_destination_privileges

    def probe(pin: str):
        return probe_destination_privileges(
            "sftp", host=server.host, port=server.port, username=server.username,
            password=server.password, schema="qav3", table="q3_csv.csv",
            extra={"host_key": pin}, table_exists=False,
        )

    good = probe(server.host_key)
    assert "not trusted" not in good.detail, good.detail
    assert good.can_write is not False, good.detail
    bad = probe(_wrong_key())
    assert bad.can_write is not True, bad.detail


def test_a_wrong_pin_fails_on_every_path(server):
    from connectors.sftp_common import test_sftp
    from services.object_streaming import open_sftp_binary

    wrong = _wrong_key()
    cfg = _cfg(server, wrong)
    ok, message = test_sftp(host=cfg["host"], port=cfg["port"], username=cfg["username"],
                            password=cfg["password"], database="/", table=NESTED,
                            extra=cfg["extra"], require_object=False)
    assert not ok and "trust" in message.lower(), message
    try:
        result = _write(server, wrong)
    except Exception as exc:  # noqa: BLE001 - a raised refusal is also a refusal
        assert "trust" in str(exc).lower() or "host key" in str(exc).lower(), exc
    else:
        assert not result.ok, "a wrong pinned key must not write"
    # Unknowable (None), never "missing" (False) and never an open handle.
    assert open_sftp_binary(cfg) is None
