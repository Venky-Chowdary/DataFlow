"""Iceberg → Oracle snapshot Parquet + executemany — dest COUNT(*)."""

from __future__ import annotations

import os
import socket
import sys
import uuid
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.copy_fast_path import FastPathUnavailable  # noqa: E402
from services.copy_iceberg_oracle import (  # noqa: E402
    copy_iceberg_to_oracle,
    iceberg_oracle_copy_enabled,
)
from services.copy_iceberg_pg import iceberg_type_is_copy_safe  # noqa: E402
from services.copy_oracle_iceberg import copy_oracle_to_iceberg  # noqa: E402
from services.copy_oracle_pg import oracle_type_is_copy_safe  # noqa: E402
from services.dest_precount import destination_row_count  # noqa: E402

REST_URI = os.environ.get("DATAFLOW_ICEBERG_REST_URI", "http://127.0.0.1:8181").rstrip("/")
REST_WAREHOUSE = os.environ.get(
    "DATAFLOW_ICEBERG_REST_WAREHOUSE", "file:///tmp/iceberg-rest-wh"
)


def _rest_reachable() -> bool:
    try:
        host = REST_URI.split("://", 1)[-1].split("/", 1)[0]
        hostname, _, port_s = host.partition(":")
        port = int(port_s or "8181")
        with socket.create_connection((hostname, port), timeout=1.5):
            pass
        with urlopen(f"{REST_URI}/v1/config", timeout=2) as resp:
            return int(getattr(resp, "status", 0) or 0) == 200
    except (OSError, URLError, ValueError):
        return False


requires_rest = pytest.mark.skipif(
    not _rest_reachable(),
    reason=f"Iceberg REST catalog not reachable at {REST_URI}",
)


def _oracle_password() -> str:
    env = (
        os.environ.get("DATAFLOW_ORACLE_PASSWORD")
        or os.environ.get("ORA_PASSWORD")
        or ""
    ).strip()
    if env:
        return env
    path = Path("/tmp/df-desktop-lab/oracle_password")
    if path.is_file():
        return path.read_text().strip()
    return "dataflow"


def _ora_or_skip():
    try:
        with socket.create_connection(("127.0.0.1", 1521), timeout=1):
            pass
    except OSError:
        pytest.skip("Oracle 1521 not reachable")


def _ora_cfg() -> dict:
    return {
        "host": "127.0.0.1",
        "port": 1521,
        "database": "XEPDB1",
        "service_name": "XEPDB1",
        "username": "dataflow",
        "password": _oracle_password(),
        "schema": "DATAFLOW",
    }


def _iceberg_cfg(table: str) -> dict:
    return {
        "type": "iceberg",
        "connection_string": REST_URI,
        "warehouse": REST_WAREHOUSE,
        "table": table,
        "schema": "default",
        "extra": {"catalog_type": "rest", "warehouse": REST_WAREHOUSE},
    }


def _mysql_cfg() -> dict:
    return {
        "host": "127.0.0.1",
        "port": 3306,
        "database": "dataflow",
        "username": "dataflow",
        "password": "dataflow",
    }


def _ora_connect():
    _ora_or_skip()
    oracledb = pytest.importorskip("oracledb")
    try:
        return oracledb.connect(
            user="dataflow",
            password=_oracle_password(),
            dsn="127.0.0.1:1521/XEPDB1",
        )
    except Exception as exc:
        pytest.skip(f"Oracle auth failed: {exc}")


def _mysql_connect():
    try:
        with socket.create_connection(("127.0.0.1", 3306), timeout=1):
            pass
    except OSError:
        pytest.skip("MySQL 3306 not reachable")
    pymysql = pytest.importorskip("pymysql")
    try:
        return pymysql.connect(
            host="127.0.0.1",
            port=3306,
            user="dataflow",
            password="dataflow",
            database="dataflow",
            autocommit=True,
        )
    except Exception as exc:
        pytest.skip(f"MySQL auth failed: {exc}")


def _drop_ora(cur, table: str) -> None:
    cur.execute(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE "
        f"{table} PURGE'; EXCEPTION WHEN OTHERS THEN "
        "IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


def _seed_ora(cur, table: str, rows: int) -> None:
    _drop_ora(cur, table)
    cur.execute(
        f"CREATE TABLE {table} (ID NUMBER NOT NULL PRIMARY KEY, LABEL VARCHAR2(32))"
    )
    cur.execute(
        f"INSERT INTO {table} (ID, LABEL) "
        f"SELECT LEVEL, 'r' || LEVEL FROM dual CONNECT BY LEVEL <= {int(rows)}"
    )


def _drop_iceberg(table: str) -> None:
    from connectors.iceberg_catalog import load_catalog, parse_iceberg_catalog_config
    from pyiceberg.exceptions import NoSuchTableError

    cfg = _iceberg_cfg(table)
    try:
        parsed = parse_iceberg_catalog_config(cfg)
        catalog = load_catalog(cfg)
        catalog.drop_table(parsed["namespace"] + (parsed["table_name"],))
    except NoSuchTableError:
        return


def _iceberg_count(table: str) -> int:
    cfg = _iceberg_cfg(table)
    n = destination_row_count("iceberg", cfg, schema="default", table_name=table)
    assert n is not None
    return int(n)


def _seed_iceberg_from_ora(ora, src: str, ice: str, rows: int) -> None:
    cur = ora.cursor()
    _seed_ora(cur, src, rows)
    ora.commit()
    _drop_iceberg(ice)
    result = copy_oracle_to_iceberg(
        source_cfg=_ora_cfg(),
        source_table=src,
        dest_cfg=_iceberg_cfg(ice),
        dest_table=ice,
        pairs=[("id", "id"), ("label", "label")],
        iceberg_ddls=["long", "string"],
        replace_destination=True,
    )
    assert result.target_rows == rows
    assert _iceberg_count(ice) == rows


def test_iceberg_oracle_copy_safe_types():
    assert iceberg_type_is_copy_safe("string") is True
    assert iceberg_type_is_copy_safe("long") is True
    assert iceberg_type_is_copy_safe("VARCHAR(32)") is True
    assert iceberg_type_is_copy_safe("VARCHAR2(32)") is False
    assert oracle_type_is_copy_safe("VARCHAR2(32)") is True
    assert iceberg_type_is_copy_safe("binary") is False
    assert iceberg_type_is_copy_safe("uuid") is False
    assert iceberg_type_is_copy_safe("timestamptz") is False


def test_iceberg_oracle_copy_kill_switch(monkeypatch):
    monkeypatch.setenv("DATAFLOW_ICEBERG_ORACLE_COPY", "0")
    assert iceberg_oracle_copy_enabled() is False
    with pytest.raises(FastPathUnavailable, match="disabled"):
        copy_iceberg_to_oracle(
            source_cfg=_iceberg_cfg("missing"),
            source_table="missing",
            dest_cfg=_ora_cfg(),
            dest_table="NOPE",
            pairs=[("id", "id")],
            oracle_ddls=["NUMBER"],
            replace_destination=True,
        )


@requires_rest
def test_live_iceberg_oracle_dest_count(monkeypatch):
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    monkeypatch.delenv("DATAFLOW_ICEBERG_ORACLE_COPY", raising=False)
    ora = _ora_connect()
    tag = uuid.uuid4().hex[:8].upper()
    src = f"ICE_ORA_SRC_{tag}"
    ice = f"ice_ora_mid_{tag.lower()}"
    dest = f"ICE_ORA_DST_{tag}"
    try:
        _seed_iceberg_from_ora(ora, src, ice, 800)
        _drop_ora(ora.cursor(), dest)
        ora.commit()
        result = copy_iceberg_to_oracle(
            source_cfg=_iceberg_cfg(ice),
            source_schema="default",
            source_table=ice,
            dest_cfg=_ora_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            oracle_ddls=["NUMBER", "VARCHAR2(32)"],
            replace_destination=True,
        )
        assert result.source_rows == 800
        assert result.target_rows == 800
        assert result.source_snapshot.get("copy_split") == "serial"
        assert result.source_snapshot.get("iceberg_read") == "snapshot_parquet"
        cur = ora.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {dest}")
        assert int(cur.fetchone()[0]) == 800
    finally:
        _drop_ora(ora.cursor(), src)
        _drop_ora(ora.cursor(), dest)
        ora.commit()
        _drop_iceberg(ice)
        ora.close()


@requires_rest
def test_live_iceberg_oracle_empty_string_as_null():
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    from services.copy_mysql_iceberg import copy_mysql_to_iceberg

    mysql = _mysql_connect()
    ora = _ora_connect()
    tag = uuid.uuid4().hex[:8].upper()
    mysql_src = f"ice_ora_null_{tag.lower()}"
    ice = f"ice_ora_null_mid_{tag.lower()}"
    dest = f"ICE_ORA_NULL_DST_{tag}"
    try:
        with mysql.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{mysql_src}`")
            cur.execute(
                f"CREATE TABLE `{mysql_src}` ("
                "id BIGINT NOT NULL PRIMARY KEY, label VARCHAR(32) NULL)"
            )
            cur.execute(
                f"INSERT INTO `{mysql_src}` (id, label) VALUES "
                "(1, NULL), (2, ''), (3, 'x')"
            )
        _drop_iceberg(ice)
        copy_mysql_to_iceberg(
            source_cfg=_mysql_cfg(),
            source_table=mysql_src,
            dest_cfg=_iceberg_cfg(ice),
            dest_table=ice,
            pairs=[("id", "id"), ("label", "label")],
            iceberg_ddls=["long", "string"],
            replace_destination=True,
        )
        _drop_ora(ora.cursor(), dest)
        ora.commit()
        result = copy_iceberg_to_oracle(
            source_cfg=_iceberg_cfg(ice),
            source_schema="default",
            source_table=ice,
            dest_cfg=_ora_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            oracle_ddls=["NUMBER", "VARCHAR2(32)"],
            replace_destination=True,
        )
        assert result.source_rows == 3
        assert int(result.source_snapshot.get("empty_string_as_null_cells") or 0) >= 1
        cur = ora.cursor()
        cur.execute(f"SELECT ID, LABEL FROM {dest} ORDER BY ID")
        rows = list(cur.fetchall())
        assert int(rows[0][0]) == 1
        assert rows[0][1] is None
        assert int(rows[1][0]) == 2
        assert rows[1][1] is None
        assert int(rows[2][0]) == 3
        assert str(rows[2][1]) == "x"
    finally:
        with mysql.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{mysql_src}`")
        _drop_ora(ora.cursor(), dest)
        ora.commit()
        _drop_iceberg(ice)
        mysql.close()
        ora.close()


@requires_rest
def test_live_iceberg_oracle_skip_when_dest_count_matches():
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    ora = _ora_connect()
    tag = uuid.uuid4().hex[:8].upper()
    src = f"ICE_ORA_SKIP_{tag}"
    ice = f"ice_ora_skip_mid_{tag.lower()}"
    dest = f"ICE_ORA_SKIP_DST_{tag}"
    try:
        _seed_iceberg_from_ora(ora, src, ice, 800)
        _drop_ora(ora.cursor(), dest)
        ora.commit()
        first = copy_iceberg_to_oracle(
            source_cfg=_iceberg_cfg(ice),
            source_schema="default",
            source_table=ice,
            dest_cfg=_ora_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            oracle_ddls=["NUMBER", "VARCHAR2(32)"],
            replace_destination=False,
        )
        assert first.target_rows == 800
        second = copy_iceberg_to_oracle(
            source_cfg=_iceberg_cfg(ice),
            source_schema="default",
            source_table=ice,
            dest_cfg=_ora_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            oracle_ddls=["NUMBER", "VARCHAR2(32)"],
            replace_destination=False,
        )
        assert second.source_snapshot.get("copy_split") == "skip"
        assert second.source_snapshot.get("partitions_skipped") == 1
        cur = ora.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {dest}")
        assert int(cur.fetchone()[0]) == 800
    finally:
        _drop_ora(ora.cursor(), src)
        _drop_ora(ora.cursor(), dest)
        ora.commit()
        _drop_iceberg(ice)
        ora.close()


@requires_rest
def test_live_iceberg_oracle_occupied_mismatch_declines():
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    ora = _ora_connect()
    tag = uuid.uuid4().hex[:8].upper()
    src = f"ICE_ORA_OCC_{tag}"
    ice = f"ice_ora_occ_mid_{tag.lower()}"
    dest = f"ICE_ORA_OCC_DST_{tag}"
    try:
        _seed_iceberg_from_ora(ora, src, ice, 800)
        cur = ora.cursor()
        _drop_ora(cur, dest)
        cur.execute(
            f"CREATE TABLE {dest} (ID NUMBER NOT NULL PRIMARY KEY, LABEL VARCHAR2(32))"
        )
        cur.execute(
            f"INSERT INTO {dest} (ID, LABEL) "
            "SELECT 1, 'ghost' FROM dual UNION ALL "
            "SELECT 2, 'ghost' FROM dual"
        )
        ora.commit()
        with pytest.raises(FastPathUnavailable, match="occupied Oracle dest"):
            copy_iceberg_to_oracle(
                source_cfg=_iceberg_cfg(ice),
                source_schema="default",
                source_table=ice,
                dest_cfg=_ora_cfg(),
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                oracle_ddls=["NUMBER", "VARCHAR2(32)"],
                replace_destination=False,
            )
        cur.execute(f"SELECT COUNT(*) FROM {dest}")
        assert int(cur.fetchone()[0]) == 2
    finally:
        _drop_ora(ora.cursor(), src)
        _drop_ora(ora.cursor(), dest)
        ora.commit()
        _drop_iceberg(ice)
        ora.close()


@requires_rest
def test_live_iceberg_oracle_overwrite_replaces_dest():
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    ora = _ora_connect()
    tag = uuid.uuid4().hex[:8].upper()
    src = f"ICE_ORA_OW_{tag}"
    ice = f"ice_ora_ow_mid_{tag.lower()}"
    dest = f"ICE_ORA_OW_DST_{tag}"
    try:
        _seed_iceberg_from_ora(ora, src, ice, 800)
        cur = ora.cursor()
        _drop_ora(cur, dest)
        cur.execute(
            f"CREATE TABLE {dest} (ID NUMBER NOT NULL PRIMARY KEY, LABEL VARCHAR2(32))"
        )
        cur.execute(f"INSERT INTO {dest} (ID, LABEL) SELECT 1, 'ghost' FROM dual")
        ora.commit()
        result = copy_iceberg_to_oracle(
            source_cfg=_iceberg_cfg(ice),
            source_schema="default",
            source_table=ice,
            dest_cfg=_ora_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            oracle_ddls=["NUMBER", "VARCHAR2(32)"],
            replace_destination=True,
        )
        assert result.target_rows == 800
        cur.execute(f"SELECT COUNT(*) FROM {dest}")
        assert int(cur.fetchone()[0]) == 800
    finally:
        _drop_ora(ora.cursor(), src)
        _drop_ora(ora.cursor(), dest)
        ora.commit()
        _drop_iceberg(ice)
        ora.close()


@requires_rest
def test_live_iceberg_oracle_source_count_is_not_scan(monkeypatch):
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    from pyiceberg.table import DataScan

    ora = _ora_connect()
    tag = uuid.uuid4().hex[:8].upper()
    src = f"ICE_ORA_SCAN_{tag}"
    ice = f"ice_ora_scan_mid_{tag.lower()}"
    dest = f"ICE_ORA_SCAN_DST_{tag}"
    try:
        _seed_iceberg_from_ora(ora, src, ice, 80)
        _drop_ora(ora.cursor(), dest)
        ora.commit()

        def _no_count(self):
            raise AssertionError("Iceberg→Oracle source COUNT must not scan().count()")

        def _no_arrow(self):
            raise AssertionError("Iceberg→Oracle COPY must not scan().to_arrow()")

        monkeypatch.setattr(DataScan, "count", _no_count)
        monkeypatch.setattr(DataScan, "to_arrow", _no_arrow)
        result = copy_iceberg_to_oracle(
            source_cfg=_iceberg_cfg(ice),
            source_schema="default",
            source_table=ice,
            dest_cfg=_ora_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            oracle_ddls=["NUMBER", "VARCHAR2(32)"],
            replace_destination=True,
        )
        assert result.target_rows == 80
        cur = ora.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {dest}")
        assert int(cur.fetchone()[0]) == 80
    finally:
        _drop_ora(ora.cursor(), src)
        _drop_ora(ora.cursor(), dest)
        ora.commit()
        _drop_iceberg(ice)
        ora.close()


@requires_rest
def test_live_iceberg_oracle_stream_load_method(monkeypatch):
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    monkeypatch.delenv("DATAFLOW_ICEBERG_ORACLE_COPY", raising=False)
    ora = _ora_connect()
    tag = uuid.uuid4().hex[:8].upper()
    src = f"ICE_ORA_STREAM_{tag}"
    ice = f"ice_ora_stream_mid_{tag.lower()}"
    dest = f"ICE_ORA_STREAM_DST_{tag}"
    try:
        _seed_iceberg_from_ora(ora, src, ice, 800)
        _drop_ora(ora.cursor(), dest)
        ora.commit()
        from services.million_row_proof import ensure_memory_job_store_if_mongo_down
        from services.mongodb_service import get_mongodb_service
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        ensure_memory_job_store_if_mongo_down()
        job_id = f"ice-ora-copy-{tag.lower()}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        source = EndpointConfig.from_dict(
            "database",
            {
                "format": "iceberg",
                "connection_string": REST_URI,
                "warehouse": REST_WAREHOUSE,
                "table": ice,
                "schema": "default",
                "extra": {"catalog_type": "rest", "warehouse": REST_WAREHOUSE},
            },
        )
        destination = EndpointConfig.from_dict(
            "database", {**_ora_cfg(), "format": "oracle", "table": dest}
        )
        mappings = [
            {"source": "id", "target": "id", "type": "NUMBER", "transform": "none"},
            {
                "source": "label",
                "target": "label",
                "type": "VARCHAR2(32)",
                "transform": "none",
            },
        ]
        transferred, ddl_log, summary, _cols = stream_database_transfer(
            source,
            destination,
            mappings,
            {"id": "NUMBER", "label": "VARCHAR2(32)"},
            sync_mode="full_refresh_overwrite",
            job_id=job_id,
        )
        assert transferred == 800
        assert summary.get("load_method") == "iceberg_parquet_executemany_oracle"
        assert summary.get("source_row_count") == 800
        assert int(summary.get("rejected_rows") or 0) == 0
        assert any("Iceberg" in line for line in ddl_log)
        cur = ora.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {dest}")
        assert int(cur.fetchone()[0]) == 800
    finally:
        _drop_ora(ora.cursor(), src)
        _drop_ora(ora.cursor(), dest)
        ora.commit()
        _drop_iceberg(ice)
        ora.close()
