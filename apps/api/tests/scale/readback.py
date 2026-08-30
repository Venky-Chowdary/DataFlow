"""Independent verification for the scale matrices.

Nothing here imports the product's writers. A destination is proven by opening
a *new* driver connection (or a *new* object-store client), counting rows,
reading the mapped projection back and hashing it with the fixture's own
canonicalizer. The engine's own acknowledgement, ``records_transferred`` and
``reconciliation`` block are recorded as claims and compared against these
numbers — they are never the proof.
"""

from __future__ import annotations

import os
import socket
from dataclasses import dataclass, field
from typing import Any, Iterator

from tests.scale import dirty_fixture as fixture

PG = {
    "host": os.getenv("SCALE_PG_HOST", "localhost"),
    "port": int(os.getenv("SCALE_PG_PORT", "5432")),
    "database": os.getenv("SCALE_PG_DB", "dataflow"),
    "user": os.getenv("SCALE_PG_USER", "dataflow"),
    "password": os.getenv("SCALE_PG_PASSWORD", "dataflow"),
}
MYSQL = {
    "host": os.getenv("SCALE_MYSQL_HOST", "localhost"),
    "port": int(os.getenv("SCALE_MYSQL_PORT", "3306")),
    "database": os.getenv("SCALE_MYSQL_DB", "dataflow"),
    "user": os.getenv("SCALE_MYSQL_USER", "dataflow"),
    "password": os.getenv("SCALE_MYSQL_PASSWORD", "dataflow"),
}
MINIO = {
    "endpoint_url": os.getenv("SCALE_MINIO_ENDPOINT", "http://localhost:9000"),
    "access_key": os.getenv("SCALE_MINIO_ACCESS_KEY", "dataflow"),
    "secret_key": os.getenv("SCALE_MINIO_SECRET_KEY", "dataflowsecret"),
    "region": os.getenv("SCALE_MINIO_REGION", "us-east-1"),
    "bucket": os.getenv("SCALE_MINIO_BUCKET", "scale-matrix"),
}
GCS = {
    "endpoint_url": os.getenv("SCALE_GCS_ENDPOINT", "http://localhost:4443"),
    "bucket": os.getenv("SCALE_GCS_BUCKET", "scale-matrix"),
    "project": os.getenv("SCALE_GCS_PROJECT", "dataflow-local"),
}
AZURITE = {
    "connection_string": os.getenv(
        "SCALE_AZURITE_CONNECTION_STRING",
        "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
        "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq"
        "/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/devstoreaccount1;",
    ),
    "container": os.getenv("SCALE_AZURITE_CONTAINER", "scale-matrix"),
}


def port_open(host: str, port: int, timeout: float = 2.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


@dataclass
class Readback:
    """What an independent reader actually found at the destination."""

    row_count: int = 0
    checksum: str = ""
    checksum_rows: int = 0
    schema: dict[str, str] = field(default_factory=dict)
    null_tokens: dict[str, int] = field(default_factory=dict)
    detail: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "row_count": self.row_count,
            "checksum": self.checksum,
            "checksum_rows": self.checksum_rows,
            "schema": self.schema,
            "null_tokens": self.null_tokens,
            "detail": self.detail,
        }


def _null_token_tally(records: list[dict[str, Any]]) -> dict[str, int]:
    """How the destination spells the fixture's null tokens, as landed."""
    tally: dict[str, int] = {}
    for rec in records:
        raw = rec.get("null_token")
        if raw is None:
            key = "<NULL>"
        elif str(raw) in {"NULL", "\\N"}:
            key = str(raw)
        elif str(raw) == "":
            key = "<EMPTY>"
        else:
            continue
        tally[key] = tally.get(key, 0) + 1
    return tally


# --------------------------------------------------------------------------- #
# PostgreSQL
# --------------------------------------------------------------------------- #


def pg_connect():
    import psycopg2

    return psycopg2.connect(
        host=PG["host"],
        port=PG["port"],
        dbname=PG["database"],
        user=PG["user"],
        password=PG["password"],
    )


def pg_reachable() -> bool:
    return port_open(str(PG["host"]), int(PG["port"]))


def _dlq_name(table: str) -> str:
    from services.dest_quarantine import dlq_table_name

    return dlq_table_name(table)


def pg_drop(table: str, schema: str = "public") -> None:
    """Drop the primary table and its DLQ, so held rows are this run's evidence."""
    with pg_connect() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{schema}"."{table}"')
            cur.execute(f'DROP TABLE IF EXISTS "{schema}"."{_dlq_name(table)}"')


def pg_table_count(table: str, schema: str = "public") -> int:
    """``COUNT(*)`` through a fresh connection, 0 when the table does not exist."""
    with pg_connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass(%s)", (f'{schema}."{table}"',))
            if cur.fetchone()[0] is None:
                return 0
            cur.execute(f'SELECT COUNT(*) FROM {schema}."{table}"')
            return int(cur.fetchone()[0])


def pg_seed(table: str, rows: int, schema: str = "public") -> None:
    """Seed the dirty population natively, for the database→file direction."""
    import psycopg2.extras

    ddl_cols = ", ".join(
        f'"{col}" {fixture.DEST_TYPES[col]}' for col in fixture.COLUMNS
    )
    with pg_connect() as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{schema}"."{table}"')
            cur.execute(f'CREATE TABLE "{schema}"."{table}" ({ddl_cols})')
            placeholders = ", ".join(["%s"] * len(fixture.COLUMNS))
            sql = (
                f'INSERT INTO "{schema}"."{table}" '
                f'({", ".join(chr(34) + c + chr(34) for c in fixture.COLUMNS)}) '
                f"VALUES ({placeholders})"
            )
            batch: list[tuple] = []
            for rec in fixture.iter_rows_text(rows):
                batch.append(_typed_tuple(rec))
                if len(batch) >= 5000:
                    psycopg2.extras.execute_batch(cur, sql, batch, page_size=1000)
                    batch = []
            if batch:
                psycopg2.extras.execute_batch(cur, sql, batch, page_size=1000)
        conn.commit()


def _typed_tuple(rec: dict[str, str]) -> tuple:
    """Fixture row in native types, for a native seed of a typed table.

    ``qty`` is INTEGER in the seed table, so the non-numeric quarantine cell
    cannot be seeded — those rows carry NULL there and the ``database→file``
    direction proves the *other* dirt (unicode, decimals, long strings, dates,
    embedded delimiters) at full width.
    """
    from datetime import datetime
    from decimal import Decimal

    qty_raw = rec["qty"]
    return (
        int(rec["id"]),
        rec["acct_code"],
        Decimal(rec["amount"]).quantize(Decimal("0.0001")),
        None if qty_raw == fixture.QUARANTINE_VALUE else int(qty_raw),
        rec["note"],
        rec["unicode_name"],
        datetime.fromisoformat(rec["created_at"]),
        rec["dob_mixed"],
        rec["flag"] == "true",
        rec["null_empty"],
        rec["null_token"],
    )


def pg_schema(table: str, schema: str = "public") -> dict[str, str]:
    with pg_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name, data_type, character_maximum_length,
                       numeric_precision, numeric_scale
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
                """,
                (schema, table),
            )
            out: dict[str, str] = {}
            for name, dtype, char_len, prec, scale in cur.fetchall():
                if char_len:
                    out[name] = f"{dtype}({char_len})"
                elif dtype in {"numeric", "decimal"} and prec:
                    out[name] = f"{dtype}({prec},{scale})"
                else:
                    out[name] = dtype
            return out


def pg_readback(table: str, schema: str = "public", *, columns: list[str] | None = None) -> Readback:
    cols = columns or list(fixture.COLUMNS)
    quoted = ", ".join(f'"{c}"' for c in cols)
    with pg_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
            count = int(cur.fetchone()[0])
        with conn.cursor(name="scale_readback") as cur:
            cur.itersize = 10000
            cur.execute(f'SELECT {quoted} FROM "{schema}"."{table}"')
            records = [dict(zip(cols, row)) for row in cur]
    checksum, hashed = fixture.checksum_rows(records)
    return Readback(
        row_count=count,
        checksum=checksum,
        checksum_rows=hashed,
        schema=pg_schema(table, schema),
        null_tokens=_null_token_tally(records),
        detail=f"psycopg2 → {schema}.{table}",
    )


# --------------------------------------------------------------------------- #
# MySQL
# --------------------------------------------------------------------------- #


def mysql_connect():
    import pymysql

    return pymysql.connect(
        host=MYSQL["host"],
        port=int(MYSQL["port"]),
        user=MYSQL["user"],
        password=MYSQL["password"],
        database=MYSQL["database"],
        charset="utf8mb4",
        autocommit=True,
    )


def mysql_reachable() -> bool:
    return port_open(str(MYSQL["host"]), int(MYSQL["port"]))


def mysql_drop(table: str) -> None:
    """Drop the primary table and its DLQ (see :func:`pg_drop`)."""
    with mysql_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")
            cur.execute(f"DROP TABLE IF EXISTS `{_dlq_name(table)}`")


def mysql_table_count(table: str) -> int:
    with mysql_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema=%s AND table_name=%s",
                (MYSQL["database"], table),
            )
            if not int(cur.fetchone()[0]):
                return 0
            cur.execute(f"SELECT COUNT(*) FROM `{table}`")
            return int(cur.fetchone()[0])


def mysql_schema(table: str) -> dict[str, str]:
    with mysql_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name, column_type
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
                """,
                (MYSQL["database"], table),
            )
            return {str(name): str(ctype) for name, ctype in cur.fetchall()}


def mysql_readback(table: str, *, columns: list[str] | None = None) -> Readback:
    import pymysql.cursors

    cols = columns or list(fixture.COLUMNS)
    quoted = ", ".join(f"`{c}`" for c in cols)
    with mysql_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM `{table}`")
            count = int(cur.fetchone()[0])
    # A second, unbuffered connection streams the projection so a 100K read
    # cannot be served from the counting connection's buffers.
    stream = pymysql.connect(
        host=MYSQL["host"],
        port=int(MYSQL["port"]),
        user=MYSQL["user"],
        password=MYSQL["password"],
        database=MYSQL["database"],
        charset="utf8mb4",
        cursorclass=pymysql.cursors.SSCursor,
    )
    try:
        with stream.cursor() as cur:
            cur.execute(f"SELECT {quoted} FROM `{table}`")
            records = [dict(zip(cols, row)) for row in cur]
    finally:
        stream.close()
    checksum, hashed = fixture.checksum_rows(records)
    return Readback(
        row_count=count,
        checksum=checksum,
        checksum_rows=hashed,
        schema=mysql_schema(table),
        null_tokens=_null_token_tally(records),
        detail=f"pymysql → {MYSQL['database']}.{table}",
    )


# --------------------------------------------------------------------------- #
# File destinations
# --------------------------------------------------------------------------- #


def file_readback(path: str, export_format: str) -> Readback:
    """Parse an exported file with a third-party reader, then hash it."""
    from pathlib import Path

    records = fixture.read_export(Path(path), export_format)
    checksum, hashed = fixture.checksum_rows(records)
    return Readback(
        row_count=len(records),
        checksum=checksum,
        checksum_rows=hashed,
        schema=_observed_shape(records),
        null_tokens=_null_token_tally(records),
        detail=f"{export_format} reader → {path} ({os.path.getsize(path)} bytes)",
    )


def _observed_shape(records: list[dict[str, Any]]) -> dict[str, str]:
    """Honest schema *as read back*: column → python types actually seen."""
    seen: dict[str, set[str]] = {}
    for rec in records[:5000]:
        for key, value in rec.items():
            seen.setdefault(str(key), set()).add(type(value).__name__)
    return {key: "|".join(sorted(kinds)) for key, kinds in seen.items()}


# --------------------------------------------------------------------------- #
# Object stores — independent clients, listing every part object.
# --------------------------------------------------------------------------- #


def s3_client():
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=MINIO["endpoint_url"],
        aws_access_key_id=MINIO["access_key"],
        aws_secret_access_key=MINIO["secret_key"],
        region_name=MINIO["region"],
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 3}),
    )


def minio_reachable() -> bool:
    try:
        s3_client().list_buckets()
        return True
    except Exception:  # noqa: BLE001
        return False


def minio_ensure_bucket(bucket: str | None = None) -> str:
    name = bucket or str(MINIO["bucket"])
    client = s3_client()
    try:
        client.head_bucket(Bucket=name)
    except Exception:  # noqa: BLE001
        client.create_bucket(Bucket=name)
    return name


def minio_put(bucket: str, key: str, path: str) -> None:
    s3_client().upload_file(path, bucket, key)


def minio_delete_prefix(bucket: str, prefix: str) -> None:
    client = s3_client()
    token: str | None = None
    while True:
        kwargs: dict[str, Any] = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        page = client.list_objects_v2(**kwargs)
        keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if keys:
            client.delete_objects(Bucket=bucket, Delete={"Objects": keys})
        if not page.get("IsTruncated"):
            return
        token = page.get("NextContinuationToken")


def _minio_iter_payloads(bucket: str, base_key: str) -> Iterator[tuple[str, bytes]]:
    """Every object that belongs to ``base_key`` — the single object or parts.

    Chunked writers emit ``{stem}/part-NNNNN{ext}`` and may drop the single
    object, so a reader that only opens ``base_key`` would under-count. This
    lists both and yields whatever exists, in key order.
    """
    client = s3_client()
    stem = base_key.rsplit(".", 1)[0] if "." in base_key.rsplit("/", 1)[-1] else base_key
    found = False
    token: str | None = None
    while True:
        kwargs: dict[str, Any] = {"Bucket": bucket, "Prefix": f"{stem}/"}
        if token:
            kwargs["ContinuationToken"] = token
        page = client.list_objects_v2(**kwargs)
        for obj in sorted(page.get("Contents", []), key=lambda o: o["Key"]):
            found = True
            yield obj["Key"], client.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read()
        if not page.get("IsTruncated"):
            break
        token = page.get("NextContinuationToken")
    if not found:
        yield base_key, client.get_object(Bucket=bucket, Key=base_key)["Body"].read()


def objstore_readback(
    payloads: list[tuple[str, bytes]],
    export_format: str,
    *,
    suffix: str = "",
    store: str = "",
) -> Readback:
    records: list[dict[str, Any]] = []
    total_bytes = 0
    for _key, payload in payloads:
        total_bytes += len(payload)
        records.extend(fixture.read_bytes_as(export_format, payload, suffix=suffix))
    checksum, hashed = fixture.checksum_rows(records)
    keys = ", ".join(key for key, _ in payloads[:4])
    return Readback(
        row_count=len(records),
        checksum=checksum,
        checksum_rows=hashed,
        schema=_observed_shape(records),
        null_tokens=_null_token_tally(records),
        detail=(
            f"{store or 'object store'} client → {len(payloads)} object(s) "
            f"({total_bytes} bytes): {keys}"
        ),
    )


def minio_readback(bucket: str, base_key: str, export_format: str, *, suffix: str = "") -> Readback:
    payloads = list(_minio_iter_payloads(bucket, base_key))
    return objstore_readback(payloads, export_format, suffix=suffix, store="MinIO (boto3)")


def gcs_client():
    from google.auth.credentials import AnonymousCredentials
    from google.cloud import storage

    return storage.Client(
        project=str(GCS["project"]),
        credentials=AnonymousCredentials(),
        client_options={"api_endpoint": str(GCS["endpoint_url"])},
    )


def gcs_reachable() -> bool:
    try:
        list(gcs_client().list_buckets(max_results=1))
        return True
    except Exception:  # noqa: BLE001
        return False


def gcs_ensure_bucket(bucket: str | None = None) -> str:
    name = bucket or str(GCS["bucket"])
    client = gcs_client()
    try:
        client.get_bucket(name)
    except Exception:  # noqa: BLE001
        client.create_bucket(name)
    return name


def gcs_put(bucket: str, key: str, path: str) -> None:
    gcs_client().bucket(bucket).blob(key).upload_from_filename(path)


def gcs_delete_prefix(bucket: str, prefix: str) -> None:
    client = gcs_client()
    for blob in client.list_blobs(bucket, prefix=prefix):
        blob.delete()


def gcs_readback(bucket: str, base_key: str, export_format: str, *, suffix: str = "") -> Readback:
    client = gcs_client()
    stem = base_key.rsplit(".", 1)[0] if "." in base_key.rsplit("/", 1)[-1] else base_key
    blobs = sorted(client.list_blobs(bucket, prefix=f"{stem}/"), key=lambda b: b.name)
    payloads = [(b.name, b.download_as_bytes()) for b in blobs]
    if not payloads:
        blob = client.bucket(bucket).blob(base_key)
        payloads = [(base_key, blob.download_as_bytes())]
    return objstore_readback(
        payloads, export_format, suffix=suffix, store="fake-gcs (google-cloud-storage)"
    )


def azurite_service():
    from azure.storage.blob import BlobServiceClient

    return BlobServiceClient.from_connection_string(str(AZURITE["connection_string"]))


def azurite_reachable() -> bool:
    try:
        list(azurite_service().list_containers(results_per_page=1).by_page())
        return True
    except Exception:  # noqa: BLE001
        return False


def azurite_ensure_container(container: str | None = None) -> str:
    name = container or str(AZURITE["container"])
    service = azurite_service()
    try:
        service.create_container(name)
    except Exception as exc:  # noqa: BLE001 — already-exists is the normal case
        print(f"azurite container {name}: {type(exc).__name__}: {exc}")
    return name


def azurite_put(container: str, key: str, path: str) -> None:
    client = azurite_service().get_blob_client(container, key)
    with open(path, "rb") as fh:
        client.upload_blob(fh, overwrite=True)


def azurite_delete_prefix(container: str, prefix: str) -> None:
    service = azurite_service().get_container_client(container)
    for blob in service.list_blobs(name_starts_with=prefix):
        service.delete_blob(blob.name)


def azurite_readback(container: str, base_key: str, export_format: str, *, suffix: str = "") -> Readback:
    service = azurite_service().get_container_client(container)
    stem = base_key.rsplit(".", 1)[0] if "." in base_key.rsplit("/", 1)[-1] else base_key
    names = sorted(b.name for b in service.list_blobs(name_starts_with=f"{stem}/"))
    payloads = [(name, service.download_blob(name).readall()) for name in names]
    if not payloads:
        payloads = [(base_key, service.download_blob(base_key).readall())]
    return objstore_readback(
        payloads, export_format, suffix=suffix, store="Azurite (azure-storage-blob)"
    )
