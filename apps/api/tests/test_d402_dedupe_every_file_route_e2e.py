"""D402 — a declared dedupe_key lands one row per key on every file/object route.

QA (rounds 51-75, build 70cb): an S3 CSV with id 3 twice, overwrite +
dedupe_key=id, landed 31 rows (id 3 twice) under a green "Row fidelity
verified". The production test covered SFTP only; each source route owned its
own copy of the collapse, and nothing checked the destination afterwards.

Table-driven over every file and object source the engine reads (upload,
local path, SFTP, S3 and its S3-compatible catalog aliases, GCS, Azure Blob),
through ``UniversalTransferEngine.execute_tracked`` into live PostgreSQL, with
the request ``start_transfer`` builds (``in_file_dedupe=keep_last`` plus
``dedupe_key`` on both route extras):

* the exact rows land: 30 keys, id 3 holds the LAST copy in file order;
* when a route skips the collapse (the S3 escape, forced here), the job is
  not green: Gate-8 counts duplicated keys on the destination and fails it.

HTTP: the product has no HTTP/URL file source to test (rest_api reads JSON
records, and the plan refuses dedupe_key there).
"""

from __future__ import annotations

import csv
import io
import os
import tempfile
import uuid
from pathlib import Path
from typing import Any, Callable

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402
from tests.conftest import pg_test_cfg  # noqa: E402

_BUCKET = "dataflow-matrix"  # tests.conftest LOCAL_OBJECT_STORE_BUCKET
_DEDUPE = {"in_file_dedupe": "keep_last", "dedupe_key": "id"}


def _seed() -> list[dict[str, Any]]:
    """The canonical seed's hard cells, plus a later copy of id 3."""
    rows: list[dict[str, Any]] = []
    for i in range(1, 31):
        note: str | None = f"note {i}"
        if i == 2:
            note = ""
        elif i == 3:
            note = None
        elif i == 5:
            note = "none"
        elif i == 6:
            note = "NULL"
        elif i == 7:
            note = 'q"uo\\te, x\ty'
        elif i == 8:
            note = "line1\nline2"
        elif i == 11:
            note = "汉字ß"
        rows.append({"id": i, "name": f"NAME_{i}", "note": note, "amount": f"{i}.50"})
    rows.append({"id": 3, "name": "NAME_3 LATEST", "note": "dup", "amount": "3.75"})
    return rows


def _csv_bytes() -> bytes:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=["id", "name", "note", "amount"])
    writer.writeheader()
    for row in _seed():
        writer.writerow({k: ("" if v is None else v) for k, v in row.items()})
    return buf.getvalue().encode("utf-8")


def _expected() -> list[tuple]:
    last: dict[int, dict] = {}
    for row in _seed():
        last[int(row["id"])] = row
    return [(i, last[i]["name"]) for i in sorted(last)]


# ---------------------------------------------------------------------------
# In-process object stores. S3 and its aliases run against moto's real S3
# API (conftest ``local_object_store``). GCS and Azure Blob have no emulator
# here, so their SDK clients are faked at the client factory: the connector's
# own download, version-token and spill code still runs.
# ---------------------------------------------------------------------------


class _GcsBlob:
    def __init__(self, store: dict[str, bytes], name: str):
        self._store, self.name = store, name
        self.generation, self.metageneration, self.etag = 1, 1, "e1"

    def download_to_filename(self, path: str, **_kw: Any) -> None:
        if self.name not in self._store:
            raise FileNotFoundError(self.name)
        Path(path).write_bytes(self._store[self.name])


class _GcsBucket:
    def __init__(self, store: dict[str, bytes]):
        self._store = store

    def blob(self, name: str) -> _GcsBlob:
        return _GcsBlob(self._store, name)

    def get_blob(self, name: str) -> _GcsBlob | None:
        return _GcsBlob(self._store, name) if name in self._store else None


class _GcsClient:
    def __init__(self, store: dict[str, bytes]):
        self._store = store

    def bucket(self, _name: str) -> _GcsBucket:
        return _GcsBucket(self._store)

    def list_blobs(self, _bucket: str, prefix: str = "", **_kw: Any) -> list[_GcsBlob]:
        return [_GcsBlob(self._store, k) for k in sorted(self._store) if k.startswith(prefix)]


class _AdlsDownload:
    def __init__(self, body: bytes):
        self._body = body

    def chunks(self):
        yield self._body

    def readall(self) -> bytes:
        return self._body


class _AdlsBlobClient:
    def __init__(self, store: dict[str, bytes], name: str):
        self._store, self.name = store, name

    def download_blob(self, **_kw: Any) -> _AdlsDownload:
        if self.name not in self._store:
            raise FileNotFoundError(self.name)
        return _AdlsDownload(self._store[self.name])

    def get_blob_properties(self) -> Any:
        return type("Props", (), {"etag": '"e1"', "version_id": "", "size": len(self._store[self.name])})()


class _AdlsContainer:
    def __init__(self, store: dict[str, bytes]):
        self._store = store

    def list_blobs(self, name_starts_with: str = "", **_kw: Any) -> list[Any]:
        return [type("B", (), {"name": k})() for k in sorted(self._store) if k.startswith(name_starts_with)]

    def get_blob_client(self, name: str) -> _AdlsBlobClient:
        return _AdlsBlobClient(self._store, name)


class _AdlsService:
    def __init__(self, store: dict[str, bytes]):
        self._store = store

    def get_blob_client(self, _container: str, name: str) -> _AdlsBlobClient:
        return _AdlsBlobClient(self._store, name)

    def get_container_client(self, _container: str) -> _AdlsContainer:
        return _AdlsContainer(self._store)


# ---------------------------------------------------------------------------
# Routes: each returns the request's source side (and any file bytes).
# ---------------------------------------------------------------------------


def _upload(ctx: dict) -> dict:
    return {"source": EndpointConfig(kind="file", format="csv", extra=dict(_DEDUPE)),
            "source_filename": "dups.csv", "source_content": _csv_bytes()}


def _local_path(ctx: dict) -> dict:
    path = Path(tempfile.mkdtemp(prefix="d402_", dir=tempfile.gettempdir())) / "dups.csv"
    path.write_bytes(_csv_bytes())
    return {"source": EndpointConfig(kind="file", format="csv", extra=dict(_DEDUPE)),
            "source_filename": "dups.csv", "source_path": str(path)}


def _sftp(ctx: dict) -> dict:
    server = ctx["sftp"]
    if server is None:
        pytest.fail("In-process SFTP (paramiko) is required for the SFTP route")
    remote = f"/d402/{uuid.uuid4().hex[:6]}/dups.csv"
    local = Path(server.local_path(remote))
    local.parent.mkdir(parents=True, exist_ok=True)
    local.write_bytes(_csv_bytes())
    cfg = server.endpoint_config(remote)
    return {"source": EndpointConfig.from_dict("database", {
        "kind": "database", "format": "sftp", **cfg, "ssl": False, "extra": dict(_DEDUPE)})}


def _s3_like(fmt: str) -> Callable[[dict], dict]:
    def build(ctx: dict) -> dict:
        endpoint = ctx["s3"]
        if not endpoint:
            pytest.fail("moto is required for the S3 routes")
        import boto3

        key = f"d402/{uuid.uuid4().hex[:6]}/dups.csv"
        boto3.client(
            "s3", endpoint_url=endpoint, aws_access_key_id="test",
            aws_secret_access_key="test", region_name="us-east-1",
        ).put_object(Bucket=_BUCKET, Key=key, Body=_csv_bytes())
        return {"source": EndpointConfig.from_dict("database", {
            "kind": "database", "format": fmt, "host": "us-east-1",
            "port": int(endpoint.rsplit(":", 1)[1]), "database": _BUCKET, "table": key,
            "username": "test", "password": "test", "ssl": False,
            "extra": {**_DEDUPE, "endpoint_url": endpoint, "path_style": True}})}

    return build


def _gcs(ctx: dict) -> dict:
    store = {"d402/dups.csv": _csv_bytes()}
    ctx["monkeypatch"].setattr("connectors.gcs_common.gcs_client", lambda _cfg: _GcsClient(store))
    return {"source": EndpointConfig.from_dict("database", {
        "kind": "database", "format": "gcs", "database": "d402-bucket", "table": "d402/dups.csv",
        "extra": dict(_DEDUPE)})}


def _adls(ctx: dict) -> dict:
    store = {"d402/dups.csv": _csv_bytes()}
    ctx["monkeypatch"].setattr(
        "connectors.adls_common.blob_service_client", lambda _cfg: _AdlsService(store)
    )
    return {"source": EndpointConfig.from_dict("database", {
        "kind": "database", "format": "adls", "database": "d402-container",
        "table": "d402/dups.csv", "username": "acct", "extra": dict(_DEDUPE)})}


ROUTES: dict[str, Callable[[dict], dict]] = {
    "upload": _upload,
    "local_path": _local_path,
    "sftp": _sftp,
    "s3": _s3_like("s3"),
    "minio": _s3_like("minio"),
    "wasabi": _s3_like("wasabi"),
    "cloudflare_r2": _s3_like("cloudflare_r2"),
    "backblaze_b2": _s3_like("backblaze_b2"),
    "digitalocean_spaces": _s3_like("digitalocean_spaces"),
    "gcs": _gcs,
    "adls": _adls,
}


@pytest.fixture()
def route_ctx(monkeypatch, local_object_store, local_sftp):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    return {"monkeypatch": monkeypatch, "s3": local_object_store, "sftp": local_sftp, "jobs": fake}


def _pg():
    import psycopg2

    cfg = pg_test_cfg()
    try:
        conn = psycopg2.connect(
            host=cfg["host"], port=cfg["port"], dbname=cfg["database"],
            user=cfg["username"], password=cfg["password"],
        )
    except Exception as exc:  # noqa: BLE001
        pytest.fail(f"PostgreSQL is required for the D402 route matrix: {exc}")
    conn.autocommit = True
    return conn


def _run(route: str, ctx: dict, sync_mode: str) -> tuple[Any, dict, list[tuple], str]:
    built = ROUTES[route](ctx)
    cfg = pg_test_cfg()
    table = f"d402_{route}_{uuid.uuid4().hex[:6]}"
    request = TransferRequest(
        source=built.pop("source"),
        destination=EndpointConfig.from_dict("database", {
            "kind": "database", "format": "postgresql", "host": cfg["host"], "port": cfg["port"],
            "database": cfg["database"], "username": cfg["username"], "password": cfg["password"],
            "schema": "public", "table": table, "ssl": False, "extra": dict(_DEDUPE)}),
        sync_mode=sync_mode,
        stream_contracts=(
            [{"name": "dups", "sync_mode": sync_mode, "primary_key": "id", "selected": True}]
            if sync_mode == "upsert" else []
        ),
        schema_policy="propagate_columns",
        validation_mode="strict",
        **built,
    )
    job_id = uuid.uuid4().hex[:24]
    result = UniversalTransferEngine().execute_tracked(request, job_id)
    job = dict(ctx["jobs"].get_job(job_id) or {})
    conn = _pg()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass(%s)", (f'public."{table}"',))
            rows: list[tuple] = []
            if cur.fetchone()[0] is not None:
                cur.execute(f'SELECT id, name FROM public."{table}" ORDER BY id, name')
                rows = [(int(r[0]), str(r[1])) for r in cur.fetchall()]
            cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
    finally:
        conn.close()
    return result, job, rows, table


@pytest.mark.parametrize("sync_mode", ["full_refresh_overwrite", "upsert"])
@pytest.mark.parametrize("route", list(ROUTES))
def test_dedupe_key_lands_one_row_per_key(route, sync_mode, route_ctx) -> None:
    result, job, rows, _ = _run(route, route_ctx, sync_mode)
    assert result.success is True, (route, result.error, job.get("message"))
    assert job.get("status") == "completed", (route, job.get("status"), job.get("error"))
    assert rows == _expected(), (route, len(rows), [r for r in rows if r[0] == 3])
    message = str(job.get("message") or result.error or "")
    assert "unverified" not in message.lower() and "failed" not in message.lower(), message


@pytest.mark.parametrize("route", list(ROUTES))
def test_duplicates_that_escape_the_collapse_are_never_green(route, route_ctx) -> None:
    """The QA escape, forced on every route: the read side skips keep_last."""
    import services.duplicate_policy as dp

    mp = route_ctx["monkeypatch"]
    mp.setattr(dp, "keep_last_reads_whole", lambda _request: False)
    mp.setattr(dp, "iter_keep_last", lambda batches, _keys: batches)
    result, job, rows, _ = _run(route, route_ctx, "full_refresh_overwrite")
    assert sorted(r[0] for r in rows).count(3) == 2, (route, "the escape was not reproduced", rows)
    assert result.success is False, (route, job.get("message"))
    assert job.get("status") != "completed", (route, job.get("status"), job.get("message"))
    text = f"{result.error or ''} {job.get('error') or ''} {job.get('message') or ''}"
    assert "dedupe" in text.lower() and "more than once" in text.lower(), text
