"""SFTP transfers against a real SFTP server — not a patched ``connect_sftp``.

Every other SFTP test in this repository mocks the connection and asserts the
mock was called, which proves the call site and nothing about whether a row
survives. SFTP was also the only named connector with ``preflight: False``, so
mocks were the entire evidence base behind a driver the catalog offered.

These run paramiko's server half in-process (see ``sftp_test_server``) with the
generated host key pinned, so the transfers exercise real host-key verification,
the real privilege probe, the real uniqueness scan and a real Gate-8 read-back.
"""

from __future__ import annotations

import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

_CSV = b"id,amount,ordered_at\n1,10.50,2024-01-05\n2,20.25,2024-02-11\n"


def _sftp_endpoint(server, remote_path: str) -> EndpointConfig:
    cfg = server.endpoint_config(remote_path)
    return EndpointConfig(
        kind="database",
        format="sftp",
        host=cfg["host"],
        port=cfg["port"],
        username=cfg["username"],
        password=cfg["password"],
        database=cfg["database"],
        table=cfg["table"],
        extra={"host_key": cfg["host_key"]},
    )


def _pg_endpoint(table: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database",
        format="postgresql",
        host="localhost",
        port=5432,
        database="dataflow",
        username="dataflow",
        password="dataflow",
        schema="public",
        table=table,
    )


def _mappings(*columns: str) -> list[dict]:
    return [{"source": c, "target": c, "confidence": 0.99} for c in columns]


def _run(source: EndpointConfig, destination: EndpointConfig, **kwargs):
    request = TransferRequest(
        source=source,
        destination=destination,
        sync_mode="full_refresh_overwrite",
        # Preflight ON: a write-only pass would not prove SFTP is transfer-live.
        skip_preflight=False,
        validation_mode="strict",
        **kwargs,
    )
    return UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])


def _pg_conn():
    psycopg2 = pytest.importorskip("psycopg2")
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="dataflow",
            user="dataflow",
            password="dataflow",
        )
    except psycopg2.OperationalError as exc:
        pytest.skip(f"PostgreSQL unavailable: {exc}")
    conn.autocommit = True
    return conn


def test_file_to_sftp_writes_the_rows(local_sftp):
    if local_sftp is None:
        pytest.skip("local SFTP server unavailable")
    result = _run(
        EndpointConfig(kind="file", format="csv"),
        _sftp_endpoint(local_sftp, "/out.csv"),
        source_content=_CSV,
        source_filename="data.csv",
        mappings=_mappings("id", "amount", "ordered_at"),
    )
    assert result.success is True, result.error
    assert result.records_transferred == 2
    landed = Path(local_sftp.local_path("/out.csv")).read_text()
    assert "10.50" in landed and "20.25" in landed


def test_file_to_sftp_survives_a_server_without_posix_rename(tmp_path):
    """Managed file-transfer appliances do not implement the OpenSSH extension.

    The writer used to gate the atomic rename on ``hasattr`` of the paramiko
    *client*, which is always true, so those servers answered "Operation
    unsupported" and the write failed after the bytes had already landed.
    """
    from tests.sftp_test_server import start_sftp_server

    root = tmp_path / "srv"
    root.mkdir()
    server, runner = start_sftp_server(str(root), posix_rename=False)
    try:
        result = _run(
            EndpointConfig(kind="file", format="csv"),
            _sftp_endpoint(server, "/out.csv"),
            source_content=_CSV,
            source_filename="data.csv",
            mappings=_mappings("id", "amount", "ordered_at"),
        )
        assert result.success is True, result.error
        assert result.records_transferred == 2
        assert "10.50" in (root / "out.csv").read_text()
        # The staged temp file must not survive the fallback path.
        assert [p.name for p in root.iterdir()] == ["out.csv"]
    finally:
        runner.stop()


def test_sftp_to_postgresql_lands_typed_columns(local_sftp):
    """The payload types must survive the transport.

    An object store handed the engine bare strings until it was fixed to infer
    types from the rows it had already parsed; SFTP carries the same payloads
    over a different transport and would otherwise land three text columns.
    """
    if local_sftp is None:
        pytest.skip("local SFTP server unavailable")
    Path(local_sftp.local_path("/orders.csv")).write_bytes(_CSV)
    table = f"sftp_typed_{uuid.uuid4().hex[:8]}"
    conn = _pg_conn()
    try:
        result = _run(
            _sftp_endpoint(local_sftp, "/orders.csv"),
            _pg_endpoint(table),
            mappings=_mappings("id", "amount", "ordered_at"),
        )
        assert result.success is True, result.error
        assert result.records_transferred == 2
        with conn.cursor() as cur:
            cur.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_name = %s ORDER BY ordinal_position",
                (table,),
            )
            types = dict(cur.fetchall())
            assert types["id"] == "bigint", types
            assert types["amount"] == "numeric", types
            assert types["ordered_at"] == "date", types
            cur.execute(f'SELECT id, amount, ordered_at FROM "{table}" ORDER BY id')
            rows = cur.fetchall()
        assert [r[0] for r in rows] == [1, 2]
        assert str(rows[0][1]).startswith("10.5")
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        conn.close()


def test_sftp_privilege_probe_measures_and_cleans_up(local_sftp):
    """G2 must measure write access, and must not litter the landing directory."""
    if local_sftp is None:
        pytest.skip("local SFTP server unavailable")
    from services.destination_privilege_probe import probe_destination_privileges

    before = sorted(p.name for p in Path(local_sftp.root).iterdir())
    cfg = local_sftp.endpoint_config("/probe_target.csv")
    probe = probe_destination_privileges(
        "sftp",
        host=cfg["host"],
        port=cfg["port"],
        database=cfg["database"],
        table=cfg["table"],
        username=cfg["username"],
        password=cfg["password"],
        host_key=cfg["host_key"],
        table_exists=False,
    )
    assert probe.status == "ok", probe.detail
    assert probe.can_write is True
    assert probe.can_create_table is True
    assert sorted(p.name for p in Path(local_sftp.root).iterdir()) == before


def test_sftp_privilege_probe_refuses_an_unreachable_directory(local_sftp):
    if local_sftp is None:
        pytest.skip("local SFTP server unavailable")
    from services.destination_privilege_probe import probe_destination_privileges

    cfg = local_sftp.endpoint_config("/no/such/dir/out.csv")
    probe = probe_destination_privileges(
        "sftp",
        host=cfg["host"],
        port=cfg["port"],
        database=cfg["database"],
        table=cfg["table"],
        username=cfg["username"],
        password=cfg["password"],
        host_key=cfg["host_key"],
        table_exists=False,
    )
    assert probe.status == "denied"
    assert probe.can_write is False


def test_sftp_uniqueness_scan_reads_the_whole_payload(local_sftp):
    """Duplicates beyond the Validate sample must still be found.

    The probe was ``skipped_unsupported`` for every object-like source, so a
    uniqueness-required sync could only ever fail closed. A payload the platform
    can read end to end is the one case where population proof is always
    available.
    """
    if local_sftp is None:
        pytest.skip("local SFTP server unavailable")
    from services.source_duplicate_probe import probe_source_duplicate_keys_result

    rows = "".join(f"{i},{i}.50\n" for i in range(1, 300))
    Path(local_sftp.local_path("/dupes.csv")).write_text(
        "id,amount\n" + rows + "42,999.00\n"
    )
    cfg = local_sftp.endpoint_config("/dupes.csv")
    probe = probe_source_duplicate_keys_result(
        source_config={
            "type": "sftp",
            "host": cfg["host"],
            "port": cfg["port"],
            "username": cfg["username"],
            "password": cfg["password"],
            "host_key": cfg["host_key"],
            "database": cfg["database"],
        },
        source_table=cfg["table"],
        primary_key="id",
    )
    assert probe.status == "ran", probe.message
    assert [f["value"] for f in probe.findings] == ["42"]
    assert probe.findings[0]["count"] == 2


def test_sftp_uniqueness_scan_passes_a_clean_payload(local_sftp):
    if local_sftp is None:
        pytest.skip("local SFTP server unavailable")
    from services.source_duplicate_probe import probe_source_duplicate_keys_result

    Path(local_sftp.local_path("/clean.csv")).write_bytes(_CSV)
    cfg = local_sftp.endpoint_config("/clean.csv")
    probe = probe_source_duplicate_keys_result(
        source_config={
            "type": "sftp",
            "host": cfg["host"],
            "port": cfg["port"],
            "username": cfg["username"],
            "password": cfg["password"],
            "host_key": cfg["host_key"],
            "database": cfg["database"],
        },
        source_table=cfg["table"],
        primary_key="id",
    )
    assert probe.status == "ran", probe.message
    assert probe.findings == []


def test_sftp_introspect_reports_objects_and_types(local_sftp):
    if local_sftp is None:
        pytest.skip("local SFTP server unavailable")
    from src.transfer.endpoint_intelligence import introspect_endpoint

    Path(local_sftp.local_path("/introspect.csv")).write_bytes(_CSV)
    info = introspect_endpoint(_sftp_endpoint(local_sftp, "/introspect.csv"))
    assert info["connected"] is True
    assert info["table_exists"] is True
    assert info["columns"] == ["id", "amount", "ordered_at"]
    assert info["schema"]["id"] == "INTEGER"
    assert info["schema"]["ordered_at"] == "DATE"
    assert any(o["name"] == "introspect.csv" for o in info["objects"])


def test_sftp_introspect_reports_a_missing_file_as_absent(local_sftp):
    if local_sftp is None:
        pytest.skip("local SFTP server unavailable")
    from src.transfer.endpoint_intelligence import introspect_endpoint

    info = introspect_endpoint(_sftp_endpoint(local_sftp, "/definitely-not-here.csv"))
    assert info["connected"] is True
    assert info["table_exists"] is False


def test_sftp_refuses_an_unpinned_host_key(local_sftp):
    """The transport must fail closed rather than trust on first use."""
    if local_sftp is None:
        pytest.skip("local SFTP server unavailable")
    from connectors.sftp_common import connect_sftp, parse_sftp_config

    cfg = parse_sftp_config(
        host=local_sftp.host,
        port=local_sftp.port,
        username=local_sftp.username,
        password=local_sftp.password,
        database="/",
        # A pinned fingerprint that is not this server's.
        host_key="SHA256:" + "A" * 43,
        known_hosts="/nonexistent",
    )
    with pytest.raises(RuntimeError, match="host key"):
        connect_sftp(cfg)


def test_sftp_is_declared_transfer_live():
    """The capability declaration must match what the tests above prove."""
    from src.transfer.connector_capabilities import (
        get_capabilities,
        transfer_ready,
        transfer_live_driver_types,
    )

    caps = get_capabilities("sftp", "sftp")
    assert caps["introspect"] is True
    assert caps["preflight"] is True
    assert transfer_ready(caps) is True
    assert "sftp" in transfer_live_driver_types()
