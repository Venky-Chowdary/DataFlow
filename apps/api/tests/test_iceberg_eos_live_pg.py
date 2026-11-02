"""Live PostgreSQL CDC to REST Iceberg exactly-once proof.

This keeps the real-service proof in pytest so it is reproducible from the
repository. The REST destination fixture and PostgreSQL readiness helper are
shared with the existing live tests rather than recreated here.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import uuid
from pathlib import Path
from urllib.parse import urlparse
from unittest.mock import patch

import pytest

try:
    import test_cdc_exactly_once_postgres_restart_live as _pg_tests
except ImportError:
    _pg_tests = None

try:
    import test_iceberg_eos as _eos_tests
except ImportError:
    _eos_tests = None

try:
    from connectors import iceberg_eos
    from connectors.cdc_eos_sql import (
        apply_change_batch_exactly_once,
        eos_stream_key,
    )
    from connectors.postgresql_change_stream import PostgreSqlChangeStreamCdc
    from connectors.postgresql_conn import get_connection
    from services.cdc_engine import ChangeBatch
    from services.cdc_exactly_once import EosCrash, ExactlyOnceRouteError
except ImportError:
    iceberg_eos = None
    apply_change_batch_exactly_once = None
    eos_stream_key = None
    PostgreSqlChangeStreamCdc = None
    get_connection = None
    ChangeBatch = None
    EosCrash = None
    ExactlyOnceRouteError = None


_API_ROOT = Path(__file__).resolve().parents[1]
_PG_CONFIG = getattr(_pg_tests, "CFG", None)
_logical_decoding_ready = getattr(
    _pg_tests, "_logical_decoding_ready", lambda: False
)
_MAPPINGS = [
    {"source": "id", "target": "id", "transform": "direct"},
    {"source": "v", "target": "v", "transform": "direct"},
]
_COLUMN_TYPES = {"id": "string", "v": "string", "_df_lsn": "string"}


def _batch(
    lsn: str,
    *,
    inserts: list[dict[str, str]] | None = None,
    resume_token: dict[str, str] | None = None,
) -> ChangeBatch:
    token = dict(resume_token or {"lsn": lsn})
    token.setdefault("lsn", lsn)
    return ChangeBatch(
        inserts=list(inserts or []),
        updates=[],
        deletes=[],
        resume_token=token,
    )


def _apply(
    endpoint: dict[str, object],
    change: ChangeBatch,
    stream_key: str,
    *,
    crash_after: str | None = None,
    writer_fence: int = 1,
) -> tuple[list[dict[str, object]], str, dict[str, object], int]:
    assert apply_change_batch_exactly_once is not None
    return apply_change_batch_exactly_once(
        dest_type="iceberg",
        dest_cfg=endpoint,
        dest_table=str(endpoint["table"]),
        change=change,
        mappings=_MAPPINGS,
        column_types=_COLUMN_TYPES,
        headers=["id", "v"],
        pk_target_cols=["id"],
        cursor_key=stream_key,
        writer_fence=writer_fence,
        crash_after=crash_after,
    )


def _load_table(endpoint: dict[str, object]):
    assert _eos_tests is not None
    return _eos_tests._load_table(endpoint)


def _source_rows(source_table: str) -> list[tuple[str, str]]:
    assert get_connection is not None
    assert _PG_CONFIG is not None
    with get_connection(**_PG_CONFIG) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT id, v FROM {source_table}")
        return [(str(row[0]), row[1]) for row in cur.fetchall()]


def _lsn_from_token(token: object) -> str:
    if isinstance(token, dict):
        return str(token["lsn"])
    if isinstance(token, str):
        if token.startswith("{"):
            decoded = json.loads(token)
            if isinstance(decoded, dict):
                return str(decoded["lsn"])
        for part in token.split("|"):
            key, separator, value = part.partition("=")
            if separator and key == "lsn":
                return value
    raise AssertionError(f"CDC token has no LSN: {token!r}")


@pytest.fixture(scope="module")
def live_eos_state():
    if not _logical_decoding_ready():
        pytest.skip(
            "PostgreSQL with wal_level=logical not reachable on localhost:5432"
        )
    if _eos_tests is None or _eos_tests._rest_reachable() is False:
        rest_uri = getattr(_eos_tests, "REST_URI", "unknown")
        s3_endpoint = getattr(_eos_tests, "S3_ENDPOINT", "unknown")
        pytest.skip(
            f"Iceberg REST or S3 endpoint is not reachable: "
            f"{rest_uri}, {s3_endpoint}"
        )
    if (
        iceberg_eos is None
        or apply_change_batch_exactly_once is None
        or PostgreSqlChangeStreamCdc is None
        or get_connection is None
        or ChangeBatch is None
        or EosCrash is None
        or ExactlyOnceRouteError is None
        or _PG_CONFIG is None
    ):
        pytest.fail("Iceberg live EOS implementation is not importable")

    source_table = f"m6_src_{uuid.uuid4().hex[:10]}"
    stream_key = f"m6-pg-iceberg-{uuid.uuid4().hex}"
    slot_name = ""
    patcher = pytest.MonkeyPatch()
    fixture_generator = None
    endpoint = None
    try:
        assert _eos_tests is not None
        fixture_generator = _eos_tests.rest_dest.__wrapped__(patcher)
        endpoint = next(fixture_generator)
        catalog_table = _load_table(endpoint)
        with get_connection(**_PG_CONFIG) as conn, conn.cursor() as cur:
            cur.execute(
                f"CREATE TABLE {source_table} "
                "(id INT PRIMARY KEY, v TEXT)"
            )
            cur.execute(
                f"INSERT INTO {source_table} (id, v) VALUES "
                "(1, 'one'), (2, 'two')"
            )
            conn.commit()

        cdc = PostgreSqlChangeStreamCdc(
            _PG_CONFIG,
            table=source_table,
            primary_key="id",
            cursor_key=stream_key,
            output_plugin="test_decoding",
            batch_size=1000,
        )
        slot_name = cdc.slot_name
        initial = next(batch for batch in cdc.snapshot() if batch.inserts)
        try:
            _apply(
                endpoint,
                initial,
                stream_key,
                crash_after="after_watermark_before_commit",
            )
        except EosCrash as exc:
            assert "after_watermark_before_commit" in str(exc)
        else:
            raise AssertionError("pre-commit crash hook did not fire")
        assert catalog_table.scan().to_arrow().num_rows == 0
        _apply(endpoint, initial, stream_key)
        cdc.ack(initial.resume_token)

        with get_connection(**_PG_CONFIG) as conn, conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO {source_table} (id, v) VALUES (3, 'three')"
            )
            conn.commit()
        streamed = next(batch for batch in cdc.poll() if batch.inserts)
        assert iceberg_eos is not None
        import connectors.cdc_eos_sql as eos_sql

        with patch.object(
            eos_sql,
            "log_apply_outcome",
            side_effect=EosCrash("after_commit_before_ack"),
        ):
            with pytest.raises(EosCrash, match="after_commit_before_ack"):
                _apply(endpoint, streamed, stream_key)

        snapshots_after_crash = len(_load_table(endpoint).snapshots())
        replay = _apply(endpoint, streamed, stream_key)
        assert replay[2]["eos_status"] in {
            "already_committed",
            "stream_wins_skip",
        }
        assert len(_load_table(endpoint).snapshots()) == snapshots_after_crash
        cdc.ack(streamed.resume_token)

        assert eos_stream_key is not None
        stream_id = eos_stream_key(
            dest_type="iceberg",
            dest_database=str(endpoint.get("schema", "default")),
            dest_object=str(endpoint["table"]),
            cursor_key=stream_key,
            stream_name="",
        )
        child = r"""
import json
import os
from connectors import iceberg_eos
from connectors.iceberg_catalog import parse_iceberg_catalog_config
from pyiceberg.catalog.rest import RestCatalog

config = json.loads(os.environ["M6_ENDPOINT"])
parsed = parse_iceberg_catalog_config(config)
properties = dict(parsed["properties"])
properties.update(
    {
        "s3.endpoint": os.environ["M6_S3_ENDPOINT"],
        "s3.access-key-id": os.environ["M6_MINIO_USER"],
        "s3.secret-access-key": os.environ["M6_MINIO_PASSWORD"],
        "s3.path-style-access": "true",
        "s3.region": "us-east-1",
    }
)
iceberg_eos.load_catalog = lambda _endpoint: RestCatalog(
    parsed["catalog_name"], **properties
)
opened = iceberg_eos.open_iceberg_eos_session(
    dest_type="iceberg",
    dest_cfg=config,
    stream_key=os.environ["M6_STREAM_KEY"],
    incoming_fence=2,
    job_resume=None,
)
print(
    json.dumps(
        {
            "dest_lsn": opened.dest_lsn,
            "resume": opened.resume,
            "fence_epoch": opened.fence_epoch,
        }
    )
)
"""
        child_env = os.environ.copy()
        child_env.update(
            {
                "PYTHONPATH": (
                    f"{_API_ROOT}:{_API_ROOT / 'packages/preflight/src'}"
                ),
                "M6_ENDPOINT": json.dumps(endpoint),
                "M6_S3_ENDPOINT": _eos_tests.S3_ENDPOINT,
                "M6_MINIO_USER": os.environ.get("MINIO_ROOT_USER", "admin"),
                "M6_MINIO_PASSWORD": os.environ.get(
                    "MINIO_ROOT_PASSWORD", "password"
                ),
                "M6_STREAM_KEY": stream_id,
            }
        )
        child_result = subprocess.run(
            [sys.executable, "-c", child],
            cwd=_API_ROOT,
            env=child_env,
            capture_output=True,
            text=True,
            check=True,
        )
        restarted = json.loads(
            child_result.stdout.strip().splitlines()[-1]
        )
        streamed_lsn = _lsn_from_token(streamed.resume_token)
        assert restarted["dest_lsn"] == streamed_lsn
        assert restarted["resume"] is not None
        with pytest.raises(ExactlyOnceRouteError, match="fence"):
            _apply(endpoint, streamed, stream_key, writer_fence=1)

        table = _load_table(endpoint)
        iceberg_rows = sorted(
            (row["id"], row["v"]) for row in table.scan().to_arrow().to_pylist()
        )
        source_rows = _source_rows(source_table)
        assert iceberg_rows == source_rows
        assert len(iceberg_rows) == len({row[0] for row in iceberg_rows})

        prefix = (
            "dataflow.eos."
            + hashlib.sha256(stream_id.encode("utf-8")).hexdigest()[:16]
            + "."
        )
        summaries = [
            snapshot.summary.model_dump() for snapshot in table.snapshots()
        ]
        commit_ids_by_lsn: dict[str, set[str]] = {}
        for summary in summaries:
            if summary.get("dataflow.operation") != "eos_apply":
                continue
            lsn = summary[prefix + "committed_lsn"]
            commit_ids_by_lsn.setdefault(lsn, set()).add(
                summary["dataflow.commit-id"]
            )
        initial_lsn = _lsn_from_token(initial.resume_token)
        assert set(commit_ids_by_lsn) == {initial_lsn, streamed_lsn}
        assert all(
            len(commit_ids) == 1 for commit_ids in commit_ids_by_lsn.values()
        ), f"more than one commit id for an LSN: {commit_ids_by_lsn}"

        yield {
            "source_rows": source_rows,
            "iceberg_rows": iceberg_rows,
            "metadata_location": table.metadata_location,
            "s3_endpoint": _eos_tests.S3_ENDPOINT,
            "endpoint": endpoint,
        }
    finally:
        if slot_name:
            with get_connection(**_PG_CONFIG) as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_drop_replication_slot(%s) WHERE EXISTS "
                    "(SELECT 1 FROM pg_replication_slots "
                    "WHERE slot_name=%s)",
                    (slot_name, slot_name),
                )
                conn.commit()
        with get_connection(**_PG_CONFIG) as conn, conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {source_table}")
            conn.commit()
        if fixture_generator is not None:
            try:
                next(fixture_generator)
            except StopIteration:
                pass
        patcher.undo()


def test_iceberg_eos_module_is_available() -> None:
    """Keep the parent fail-before import-tolerant."""
    assert iceberg_eos is not None, "Iceberg EOS adapter is not available"


def test_live_pg_cdc_to_iceberg_eos_survives_restarts(live_eos_state) -> None:
    assert live_eos_state["source_rows"] == live_eos_state["iceberg_rows"]


def test_live_pg_cdc_to_iceberg_eos_duckdb_readback(live_eos_state) -> None:
    try:
        import duckdb
    except ImportError as exc:
        pytest.skip(f"DuckDB is unavailable: {exc}")

    duck = duckdb.connect()
    try:
        try:
            duck.execute("LOAD iceberg")
        except Exception as exc:  # noqa: BLE001 - extension availability check
            pytest.skip(f"DuckDB Iceberg extension unavailable: {exc}")
        parsed = urlparse(live_eos_state["s3_endpoint"])
        endpoint = parsed.hostname or "127.0.0.1"
        if parsed.port:
            endpoint = f"{endpoint}:{parsed.port}"
        duck.execute(
            """
            CREATE SECRET (
                TYPE S3,
                KEY_ID ?,
                SECRET ?,
                REGION 'us-east-1',
                ENDPOINT ?,
                URL_STYLE 'path',
                USE_SSL false
            )
            """,
            [
                os.environ.get("MINIO_ROOT_USER", "admin"),
                os.environ.get("MINIO_ROOT_PASSWORD", "password"),
                endpoint,
            ],
        )
        rows = sorted(
            duck.execute(
                "SELECT id, v FROM iceberg_scan(?)",
                [live_eos_state["metadata_location"]],
            ).fetchall()
        )
        assert rows == live_eos_state["source_rows"]
    finally:
        duck.close()


def test_live_pg_cdc_auto_routes_through_transfer_eos(live_eos_state, monkeypatch):
    from connectors import cdc_eos_sql
    from src.transfer.cdc_transfer import run_cdc_database_transfer
    from src.transfer.models import EndpointConfig

    assert _PG_CONFIG is not None
    assert get_connection is not None
    assert EosCrash is not None
    source_table = f"m6_auto_src_{uuid.uuid4().hex[:10]}"
    job_id = f"m6-auto-{uuid.uuid4().hex[:8]}"
    slot_name = ""
    endpoint = live_eos_state["endpoint"]
    destination = EndpointConfig.from_dict(
        "database", {**endpoint, "format": "iceberg"}
    )
    source = EndpointConfig.from_dict(
        "database",
        {
            **_PG_CONFIG,
            "format": "postgresql",
            "schema": "public",
            "table": source_table,
        },
    )
    contract = [
        {
            "name": source_table,
            "selected": True,
            "primary_key": "id",
            "cursor_field": "id",
            "cursor_semantics": "cdc_position",
            "snapshot_mode": "initial",
        }
    ]

    def run():
        return run_cdc_database_transfer(
            source,
            destination,
            _MAPPINGS,
            {"id": "string", "v": "string"},
            sync_mode="cdc",
            stream_contracts=contract,
            job_id=job_id,
            limit=2,
            delivery_guarantee="auto",
            delivery_pinned=False,
        )

    def source_rows() -> list[tuple[str, str]]:
        return _source_rows(source_table)

    def drop_slot() -> None:
        if not slot_name:
            return
        with get_connection(**_PG_CONFIG) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT pg_drop_replication_slot(%s) WHERE EXISTS "
                "(SELECT 1 FROM pg_replication_slots WHERE slot_name=%s)",
                (slot_name, slot_name),
            )
            conn.commit()

    try:
        with get_connection(**_PG_CONFIG) as conn, conn.cursor() as cur:
            cur.execute(
                f"CREATE TABLE {source_table} (id INT PRIMARY KEY, v TEXT)"
            )
            cur.execute(
                f"INSERT INTO {source_table} (id, v) VALUES (201, 'two-oh-one')"
            )
            conn.commit()

        _rows, _ddl, summary, _headers = run()
        assert summary["cdc_delivery"] == "exactly_once"
        assert summary["exactly_once_active"] is True
        slot_name = str(summary.get("cdc_slot_name") or "")
        cursor_key = str(summary["cursor_key"])
        assert cursor_key

        with get_connection(**_PG_CONFIG) as conn, conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO {source_table} (id, v) VALUES (202, 'two-oh-two')"
            )
            conn.commit()
        _rows, _ddl, summary, _headers = run()
        assert summary["cdc_delivery"] == "exactly_once"

        real_apply = cdc_eos_sql.apply_change_batch_exactly_once

        def crash_before_commit(*args, **kwargs):
            return real_apply(
                *args,
                **kwargs,
                crash_after="after_watermark_before_commit",
            )

        with get_connection(**_PG_CONFIG) as conn, conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO {source_table} (id, v) VALUES (203, 'two-oh-three')"
            )
            conn.commit()
        monkeypatch.setattr(
            cdc_eos_sql, "apply_change_batch_exactly_once", crash_before_commit
        )
        with pytest.raises(EosCrash, match="after_watermark_before_commit"):
            run()
        monkeypatch.setattr(
            cdc_eos_sql, "apply_change_batch_exactly_once", real_apply
        )
        after_precommit = _load_table(endpoint)
        assert len(after_precommit.scan().to_arrow().to_pylist()) == 5

        _rows, _ddl, summary, _headers = run()
        assert summary["cdc_delivery"] == "exactly_once"

        snapshots_before_postcommit = len(_load_table(endpoint).snapshots())
        with get_connection(**_PG_CONFIG) as conn, conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO {source_table} (id, v) VALUES (204, 'two-oh-four')"
            )
            conn.commit()

        real_log_apply = cdc_eos_sql.log_apply_outcome

        def crash_after_commit(**_kwargs):
            raise EosCrash("after_commit_before_ack")

        monkeypatch.setattr(cdc_eos_sql, "log_apply_outcome", crash_after_commit)
        with pytest.raises(EosCrash, match="after_commit_before_ack"):
            run()
        monkeypatch.setattr(cdc_eos_sql, "log_apply_outcome", real_log_apply)
        snapshots_after_commit = len(_load_table(endpoint).snapshots())
        assert snapshots_after_commit == snapshots_before_postcommit + 1

        _rows, _ddl, summary, _headers = run()
        assert summary["cdc_delivery"] == "exactly_once"
        assert len(_load_table(endpoint).snapshots()) == snapshots_after_commit

        cursor_key = str(summary["cursor_key"])
        stream_id = eos_stream_key(
            dest_type="iceberg",
            dest_database=str(endpoint.get("schema", "default")),
            dest_object=str(endpoint["table"]),
            cursor_key=cursor_key,
            stream_name="",
        )
        assert stream_id == cursor_key
        child = r"""
import json
import os
from connectors import iceberg_eos
from connectors.iceberg_catalog import parse_iceberg_catalog_config
from pyiceberg.catalog.rest import RestCatalog

config = json.loads(os.environ["M6_ENDPOINT"])
parsed = parse_iceberg_catalog_config(config)
properties = dict(parsed["properties"])
properties.update(
    {
        "s3.endpoint": os.environ["M6_S3_ENDPOINT"],
        "s3.access-key-id": os.environ["M6_MINIO_USER"],
        "s3.secret-access-key": os.environ["M6_MINIO_PASSWORD"],
        "s3.path-style-access": "true",
        "s3.region": "us-east-1",
    }
)
iceberg_eos.load_catalog = lambda _endpoint: RestCatalog(
    parsed["catalog_name"], **properties
)
opened = iceberg_eos.open_iceberg_eos_session(
    dest_type="iceberg",
    dest_cfg=config,
    stream_key=os.environ["M6_STREAM_KEY"],
    incoming_fence=4,
    job_resume=None,
)
print(
    json.dumps(
        {
            "dest_lsn": opened.dest_lsn,
            "resume": opened.resume,
            "fence_epoch": opened.fence_epoch,
        }
    )
)
"""
        child_env = os.environ.copy()
        child_env.update(
            {
                "PYTHONPATH": (
                    f"{_API_ROOT}:{_API_ROOT / 'packages/preflight/src'}"
                ),
                "M6_ENDPOINT": json.dumps(endpoint),
                "M6_S3_ENDPOINT": _eos_tests.S3_ENDPOINT,
                "M6_MINIO_USER": os.environ.get("MINIO_ROOT_USER", "admin"),
                "M6_MINIO_PASSWORD": os.environ.get(
                    "MINIO_ROOT_PASSWORD", "password"
                ),
                "M6_STREAM_KEY": stream_id,
            }
        )
        reopened = subprocess.run(
            [sys.executable, "-c", child],
            cwd=_API_ROOT,
            env=child_env,
            capture_output=True,
            text=True,
            check=True,
        )
        resume_state = json.loads(reopened.stdout.strip().splitlines()[-1])
        assert resume_state["dest_lsn"]
        assert resume_state["resume"] is not None
        with pytest.raises(ExactlyOnceRouteError, match="fence"):
            _apply(
                endpoint,
                _batch(
                    str(resume_state["dest_lsn"]),
                    inserts=[{"id": "999", "v": "stale"}],
                ),
                stream_id,
                writer_fence=1,
            )

        table = _load_table(endpoint)
        prefix = (
            "dataflow.eos."
            + hashlib.sha256(stream_id.encode("utf-8")).hexdigest()[:16]
            + "."
        )
        properties = table.properties
        assert properties[prefix + "committed_lsn"]
        assert int(properties[prefix + "fence_epoch"]) >= 4
        summaries = [snapshot.summary.model_dump() for snapshot in table.snapshots()]
        commit_ids_by_lsn: dict[str, set[str]] = {}
        for snapshot_summary in summaries:
            if snapshot_summary.get("dataflow.operation") != "eos_apply":
                continue
            if prefix + "committed_lsn" not in snapshot_summary:
                continue
            if int(snapshot_summary.get("added-records") or 0) < 1:
                continue
            lsn = snapshot_summary[prefix + "committed_lsn"]
            commit_ids_by_lsn.setdefault(lsn, set()).add(
                snapshot_summary["dataflow.commit-id"]
            )
        assert commit_ids_by_lsn
        assert all(
            len(commit_ids) == 1 for commit_ids in commit_ids_by_lsn.values()
        ), (
            "more than one data-bearing commit id for an LSN: "
            f"{commit_ids_by_lsn}"
        )

        expected_rows = sorted(live_eos_state["source_rows"] + source_rows())
        iceberg_rows = sorted(
            (str(row["id"]), row["v"])
            for row in table.scan().to_arrow().to_pylist()
        )
        assert iceberg_rows == expected_rows

        try:
            import duckdb
        except ImportError as exc:
            pytest.skip(f"DuckDB is unavailable: {exc}")
        duck = duckdb.connect()
        try:
            try:
                duck.execute("LOAD iceberg")
            except Exception as exc:  # noqa: BLE001 - extension availability check
                pytest.skip(f"DuckDB Iceberg extension unavailable: {exc}")
            parsed_endpoint = urlparse(live_eos_state["s3_endpoint"])
            s3_endpoint = parsed_endpoint.hostname or "127.0.0.1"
            if parsed_endpoint.port:
                s3_endpoint = f"{s3_endpoint}:{parsed_endpoint.port}"
            duck.execute(
                """
                CREATE SECRET (
                    TYPE S3,
                    KEY_ID ?,
                    SECRET ?,
                    REGION 'us-east-1',
                    ENDPOINT ?,
                    URL_STYLE 'path',
                    USE_SSL false
                )
                """,
                [
                    os.environ.get("MINIO_ROOT_USER", "admin"),
                    os.environ.get("MINIO_ROOT_PASSWORD", "password"),
                    s3_endpoint,
                ],
            )
            duck_rows = sorted(
                (str(row[0]), row[1])
                for row in duck.execute(
                    "SELECT id, v FROM iceberg_scan(?)",
                    [table.metadata_location],
                ).fetchall()
            )
            assert duck_rows == expected_rows
        finally:
            duck.close()
    finally:
        monkeypatch.undo()
        drop_slot()
        with get_connection(**_PG_CONFIG) as conn, conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {source_table}")
            conn.commit()
