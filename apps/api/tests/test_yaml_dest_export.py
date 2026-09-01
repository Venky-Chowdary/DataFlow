"""YAML dest export is the inverse of tabular YAML ingest.

#136 made yaml a transfer-live *source*. Dest export still refused and
``write_destination_file`` would have landed JSON under a ``.yaml`` name
(D11). This file proves the export is a YAML sequence of flat mappings,
YAML 1.1 cannot coerce ``yes``/``NO``, empty population is still YAML,
and dest COUNT is ``iter_yaml_dicts`` on disk — never the writer's ack.
"""

from __future__ import annotations

import os
import sqlite3
import sys
import tempfile
import uuid
from pathlib import Path

import pytest
import yaml

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.dest_precount import count_artifact_rows  # noqa: E402
from services.yaml_tabular import (  # noqa: E402
    YAMLTabularError,
    count_yaml_records,
    dump_yaml_records,
    iter_yaml_dicts,
)
from src.transfer.adapters import write_destination_file  # noqa: E402
from src.transfer.connector_capabilities import dest_ready, get_capabilities  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402
from src.transfer.registry import PRODUCTION_SKU, validate_transfer  # noqa: E402
from tests.helpers.live_env import pg_creds, pg_up  # noqa: E402

RECORDS = [
    {"id": "1", "amount": "1000.00", "flag": "yes"},
    {"id": "2", "amount": "2000.50", "flag": "no"},
]
COLUMNS = ["id", "amount", "flag"]


def test_yaml_is_a_live_file_export() -> None:
    caps = get_capabilities("yaml")
    assert caps.get("file_export") is True
    assert dest_ready(caps) is True
    ok, msg = validate_transfer("database", "sqlite", "file_export", "yaml")
    assert ok, msg
    ok, msg = validate_transfer("file", "csv", "file_export", "yaml")
    assert ok, msg
    assert ("database", "sqlite", "file_export", "yaml") in PRODUCTION_SKU
    assert ("file", "yaml", "file_export", "yaml") in PRODUCTION_SKU


def test_dump_round_trips_yes_as_text() -> None:
    body = dump_yaml_records(RECORDS, COLUMNS)
    assert body.startswith(b"- ")
    assert b'"yes"' in body
    rows = list(iter_yaml_dicts(body))
    assert rows == RECORDS
    assert count_yaml_records(body) == 2
    loaded = yaml.safe_load(body)
    assert loaded[0]["flag"] == "yes"
    assert loaded[1]["flag"] == "no"
    assert loaded[0]["amount"] == "1000.00"
    assert isinstance(loaded[0]["flag"], str)
    assert isinstance(loaded[0]["amount"], str)


def test_dump_quotes_yaml_1_1_bools_and_leading_zeros() -> None:
    body = dump_yaml_records(
        [{"code": "NO", "zip": "007", "on": "off"}],
        ["code", "zip", "on"],
    )
    loaded = yaml.safe_load(body)
    assert loaded == [{"code": "NO", "zip": "007", "on": "off"}]
    assert list(iter_yaml_dicts(body)) == [{"code": "NO", "zip": "007", "on": "off"}]


def test_empty_population_is_yaml_sequence_not_json() -> None:
    body = dump_yaml_records([], COLUMNS)
    assert body == b"[]\n"
    assert count_yaml_records(body) == 0
    assert yaml.safe_load(body) == []


def test_dump_refuses_nested_cells() -> None:
    with pytest.raises(YAMLTabularError, match="nested"):
        dump_yaml_records([{"id": "1", "extra": {"inner": "2"}}], ["id", "extra"])


def test_write_destination_file_yaml_is_not_json() -> None:
    content, filename, summary = write_destination_file(
        EndpointConfig(kind="file_export", format="yaml", table="t"),
        RECORDS,
        COLUMNS,
        source_format="postgresql",
        column_types={"id": "TEXT", "amount": "TEXT", "flag": "TEXT"},
    )
    assert filename.endswith(".yaml")
    assert summary["mime"] == "application/yaml"
    assert summary["rows"] == 2
    assert content.startswith(b"- ")
    assert b'"yes"' in content
    assert list(iter_yaml_dicts(content)) == RECORDS


def test_write_destination_file_empty_yaml() -> None:
    content, filename, summary = write_destination_file(
        EndpointConfig(kind="file_export", format="yaml"),
        [],
        COLUMNS,
        source_format="sqlite",
    )
    assert filename.endswith(".yaml")
    assert summary["rows"] == 0
    assert count_yaml_records(content) == 0


def test_count_artifact_rows_yaml(tmp_path: Path) -> None:
    path = tmp_path / "export.yaml"
    path.write_bytes(dump_yaml_records(RECORDS, COLUMNS))
    assert count_artifact_rows(path, fmt="yaml") == 2
    assert count_artifact_rows(path) == 2
    empty = tmp_path / "empty.yaml"
    empty.write_bytes(dump_yaml_records([], COLUMNS))
    assert count_artifact_rows(empty, fmt="yaml") == 0
    yml = tmp_path / "export.yml"
    yml.write_bytes(dump_yaml_records(RECORDS, COLUMNS))
    assert count_artifact_rows(yml) == 2
    nested = tmp_path / "nested.yaml"
    nested.write_text("- id: '1'\n  extra:\n    inner: 2\n", encoding="utf-8")
    assert count_artifact_rows(nested, fmt="yaml") is None


def test_csv_to_yaml_export_artifact_count() -> None:
    csv_content = b"id,flag\n1,yes\n2,no\n"
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        destination=EndpointConfig(kind="file_export", format="yaml"),
        source_filename="flags.csv",
        source_content=csv_content,
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
    )
    result = UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])
    assert result.success is True, result.error
    assert result.records_transferred == 2
    assert result.destination_summary.get("filename", "").endswith(".yaml")
    recon = result.reconciliation or {}
    assert recon.get("artifact_row_count") == 2
    assert recon.get("dest_count_source") == "artifact_readback"
    path = result.destination_summary["path"]
    rows = list(iter_yaml_dicts(Path(path)))
    flags = [r["flag"] for r in rows]
    assert flags == ["yes", "no"]
    loaded = yaml.safe_load(Path(path).read_bytes())
    assert loaded[0]["flag"] == "yes"


def test_sqlite_to_yaml_to_sqlite_roundtrip() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        source_db = os.path.join(tmp, "source.db")
        target_db = os.path.join(tmp, "target.db")
        conn = sqlite3.connect(source_db)
        try:
            conn.execute("CREATE TABLE ledger (id TEXT, amount TEXT, flag TEXT)")
            conn.execute("INSERT INTO ledger VALUES ('1', '1000.00', 'yes')")
            conn.execute("INSERT INTO ledger VALUES ('2', '2000.50', 'no')")
            conn.commit()
        finally:
            conn.close()

        engine = UniversalTransferEngine()
        export_result = engine.execute_tracked(
            TransferRequest(
                source=EndpointConfig(
                    kind="database",
                    format="sqlite",
                    connection_string=source_db,
                    database=source_db,
                    table="ledger",
                ),
                destination=EndpointConfig(kind="file_export", format="yaml"),
                skip_preflight=True,
            ),
            uuid.uuid4().hex[:24],
        )
        assert export_result.success is True, export_result.error
        export_path = export_result.destination_summary["path"]
        body = Path(export_path).read_bytes()
        assert count_yaml_records(body) == 2
        assert count_artifact_rows(export_path, fmt="yaml") == 2
        assert export_result.reconciliation.get("artifact_row_count") == 2
        flags = [r["flag"] for r in iter_yaml_dicts(body)]
        assert flags == ["yes", "no"]
        amounts = [r["amount"] for r in iter_yaml_dicts(body)]
        assert amounts == ["1000.00", "2000.50"]

        import_result = engine.execute_tracked(
            TransferRequest(
                source=EndpointConfig(kind="file", format="yaml"),
                source_filename="ledger.yaml",
                source_content=body,
                destination=EndpointConfig(
                    kind="database",
                    format="sqlite",
                    connection_string=target_db,
                    database=target_db,
                    table="ledger",
                ),
                sync_mode="full_refresh_overwrite",
                skip_preflight=True,
                mappings=[
                    {"source": "id", "target": "id"},
                    {"source": "amount", "target": "amount"},
                    {"source": "flag", "target": "flag"},
                ],
            ),
            uuid.uuid4().hex[:24],
        )
        assert import_result.success is True, import_result.error
        back = sqlite3.connect(target_db)
        try:
            n = back.execute("SELECT COUNT(*) FROM ledger").fetchone()[0]
            rows = list(
                back.execute("SELECT id, amount, flag FROM ledger ORDER BY id")
            )
        finally:
            back.close()
        assert n == 2
        assert [str(r[2]) for r in rows] == ["yes", "no"]
        assert [str(r[1]) for r in rows] == ["1000.00", "2000.50"]


@pytest.mark.skipif(not pg_up(), reason="Postgres not authenticated")
def test_postgres_to_yaml_dest_count() -> None:
    creds = pg_creds()
    table = f"yaml_dest_{uuid.uuid4().hex[:10]}"
    import psycopg2

    conn = psycopg2.connect(
        host=creds["host"],
        port=int(creds["port"]),
        dbname=creds["database"],
        user=creds["username"],
        password=creds["password"],
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                f'CREATE TABLE "{table}" (id TEXT, amount TEXT, flag TEXT)'
            )
            cur.execute(
                f"INSERT INTO \"{table}\" VALUES ('1', '1000.00', 'yes'), "
                f"('2', '2000.50', 'no')"
            )
        conn.commit()
        result = UniversalTransferEngine().execute_tracked(
            TransferRequest(
                source=EndpointConfig(
                    kind="database",
                    format="postgresql",
                    host=str(creds["host"]),
                    port=int(creds["port"]),
                    database=str(creds["database"]),
                    username=str(creds["username"]),
                    password=str(creds["password"]),
                    schema="public",
                    table=table,
                ),
                destination=EndpointConfig(kind="file_export", format="yaml"),
                skip_preflight=True,
                mappings=[
                    {"source": "id", "target": "id", "confidence": 0.99},
                    {"source": "amount", "target": "amount", "confidence": 0.99},
                    {"source": "flag", "target": "flag", "confidence": 0.99},
                ],
            ),
            uuid.uuid4().hex[:24],
        )
        assert result.success is True, result.error
        path = result.destination_summary["path"]
        assert count_artifact_rows(path, fmt="yaml") == 2
        flags = [r["flag"] for r in iter_yaml_dicts(Path(path))]
        assert flags == ["yes", "no"]
        assert yaml.safe_load(Path(path).read_bytes())[0]["flag"] == "yes"
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        conn.commit()
        conn.close()
