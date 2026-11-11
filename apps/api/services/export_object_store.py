"""Transfer Studio file exports in a private bucket, downloaded through signed URLs.

Why: on the hosted cloud the API, the worker and the scheduler are separate
services with separate disks (``services.local_file_policy``, D276). A
"File export" is a queued job that may run on the worker, so a file written to
``export_paths.exports_dir()`` there is not on the API's disk that
``GET /transfer/download`` serves, and a redeploy forgets it. With an export
bucket configured the job puts the file in object storage instead, and any API
replica hands a signed-in member of the job's workspace a short-lived presigned
GET URL (``GET /api/v1/jobs/{job_id}/export/download``).

When the bucket is used: local-disk destinations are off for this process
(``local_file_policy.local_file_destinations_allowed()`` is false: a deployed
Railway service, or ``DATAWRAP_LOCAL_FILE_DESTINATIONS=off``) and
``DATAWRAP_EXPORT_BUCKET`` is set. Self-hosted installs keep the exports folder
exactly as before; set the flag off plus a bucket to use the bucket there too.

Settings (``getenv_brand``: ``DATAWRAP_*``, or the legacy ``DATAFLOW_*``).
Credentials are never in code; they come from the environment or the
platform's default credential chain:

``DATAWRAP_EXPORT_BUCKET``
    The private bucket. Unset: no object-store exports (and on the hosted cloud
    file exports are refused with the "not available on cloud" message).
``DATAWRAP_EXPORT_PREFIX``
    Key prefix, default ``exports``.
``DATAWRAP_EXPORT_S3_ENDPOINT``
    S3-compatible endpoint URL (Cloudflare R2: the account's R2 S3 API
    endpoint). Unset: the staging store's ``DATAWRAP_S3_ENDPOINT`` /
    ``AWS_ENDPOINT_URL``, else AWS S3. A value without a scheme is https
    unless ``DATAWRAP_S3_SECURE=0``.
``DATAWRAP_EXPORT_S3_PATH_STYLE``
    ``0`` for virtual-hosted addressing; default follows ``DATAWRAP_S3_PATH_STYLE`` (path style).
``DATAWRAP_EXPORT_S3_REGION``
    Default ``AWS_DEFAULT_REGION`` / ``AWS_REGION`` / ``us-east-1`` (R2: ``auto``).
``DATAWRAP_EXPORT_S3_ACCESS_KEY`` / ``DATAWRAP_EXPORT_S3_SECRET_KEY``
    Optional, for a key scoped to this bucket. Else the staging store's
    ``DATAWRAP_S3_ACCESS_KEY`` / ``DATAWRAP_S3_SECRET_KEY``, ``AWS_ACCESS_KEY_ID`` /
    ``AWS_SECRET_ACCESS_KEY``, else boto3's default chain (an instance role).
``DATAWRAP_EXPORT_RETENTION_HOURS``
    How long an export is kept: default 48, clamped to 24..72.
``DATAWRAP_EXPORT_URL_TTL_SECONDS``
    Presigned URL lifetime: default 900 (15 minutes), clamped to 60..900.
``DATAWRAP_EXPORT_PART_SIZE_MB``
    Multipart part size: default 16, clamped to 5..512 (S3's minimum part is 5 MiB).

Key layout: ``<prefix>/<workspace_id>/<job_id>/<file name>`` (``_unscoped``
for a job with no workspace). Every segment is reduced to ``[A-Za-z0-9._-]``.

Retention: :func:`sweep_expired_exports` runs on the scheduler beat
(``schedule_runner._run_maintenance``), at most every 15 minutes per
deployment (``services.maintenance_claim``). It deletes objects under the
prefix whose creation time is older than the retention and stamps the job's
``export_expired_at``; the download route answers 410 "export expired" from
that stamp, from the ``expires_at`` the job recorded, or when the object is gone.

Recommended bucket configuration (defense in depth; the sweeper is primary):

* Private: block all public access; no public bucket URL / ``r2.dev`` domain;
  no bucket policy granting anonymous reads. Downloads are presigned only.
* Lifecycle rule on the export prefix, so an object outlives the app's sweeper
  by at most a day even if the scheduler is down. AWS S3 (``put-bucket-lifecycle-configuration``)::

      {"Rules": [{"ID": "datawrap-export-expiry", "Status": "Enabled",
                  "Filter": {"Prefix": "exports/"},
                  "Expiration": {"Days": 3},
                  "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 1}}]}

  Cloudflare R2 (bucket settings, Object lifecycle rules): prefix ``exports/``,
  "Delete uploaded objects after 3 days", "Abort incomplete multipart uploads
  after 1 day". Use the retention ceiling (72 h = 3 days) so the rule never
  removes an export the app still promises.
* Credentials for this bucket only: GetObject, PutObject, DeleteObject,
  ListBucket and the multipart actions (Create/UploadPart/Complete/Abort).
"""

from __future__ import annotations

import hashlib
import io
import logging
import mimetypes
import os
import re
import shutil
import tempfile
import time
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, BinaryIO

from services.brand_env import getenv_brand

logger = logging.getLogger(__name__)

STORAGE_OBJECT = "object_store"
DEFAULT_PREFIX = "exports"
RETENTION_HOURS_DEFAULT, RETENTION_HOURS_MIN, RETENTION_HOURS_MAX = 48, 24, 72
URL_TTL_DEFAULT, URL_TTL_MIN, URL_TTL_MAX = 900, 60, 900
PART_MB_DEFAULT, PART_MB_MIN, PART_MB_MAX = 16, 5, 512
MAX_PARTS = 10_000
SWEEP_CLAIM = "export_object_sweep"
SWEEP_INTERVAL = timedelta(minutes=15)
SWEEP_ACTOR = "system:export-retention"
AUDIT_ISSUED = "job.export.download_url.issued"
AUDIT_EXPIRED = "job.export.expired"
#: A staged copy older than this belongs to no live job on this host.
_STAGING_STALE_SECONDS = 6 * 3600
_SAFE = re.compile(r"[^A-Za-z0-9._-]+")


# --------------------------------------------------------------------- config


def _int_setting(name: str, default: int, low: int, high: int) -> int:
    raw = str(getenv_brand(name, "") or "").strip()
    try:
        value = int(float(raw)) if raw else default
    except ValueError:
        logger.warning("%s=%r is not a number; using %s", name, raw, default)
        value = default
    return max(low, min(high, value))


def export_bucket() -> str:
    return str(getenv_brand("EXPORT_BUCKET", "") or "").strip()


def export_bucket_configured() -> bool:
    return bool(export_bucket())


def export_prefix() -> str:
    raw = str(getenv_brand("EXPORT_PREFIX", "") or "").strip().strip("/")
    parts = [_SAFE.sub("_", p) for p in raw.split("/") if p not in ("", ".", "..")]
    return "/".join(parts) or DEFAULT_PREFIX


def export_retention_hours() -> int:
    return _int_setting("EXPORT_RETENTION_HOURS", RETENTION_HOURS_DEFAULT, RETENTION_HOURS_MIN, RETENTION_HOURS_MAX)


def export_url_ttl_seconds() -> int:
    return _int_setting("EXPORT_URL_TTL_SECONDS", URL_TTL_DEFAULT, URL_TTL_MIN, URL_TTL_MAX)


def export_part_size() -> int:
    return _int_setting("EXPORT_PART_SIZE_MB", PART_MB_DEFAULT, PART_MB_MIN, PART_MB_MAX) * 1024 * 1024


def exports_use_object_store() -> bool:
    """Does a file export in this process go to the export bucket? Env reads only."""
    from services.local_file_policy import local_file_destinations_allowed

    return export_bucket_configured() and not local_file_destinations_allowed()


def export_client() -> Any:
    """A boto3 S3 client for the export bucket (signature v4, for AWS and R2).

    Same endpoint/credential conventions as the staging store
    (``object_store._get_boto3_client``), with ``EXPORT_S3_*`` taking precedence.
    """
    import boto3
    from botocore.client import Config

    endpoint = (
        getenv_brand("EXPORT_S3_ENDPOINT")
        or getenv_brand("S3_ENDPOINT")
        or os.getenv("AWS_ENDPOINT_URL")
        or ""
    ).strip()
    if endpoint and "://" not in endpoint:
        insecure = str(getenv_brand("S3_SECURE", "1") or "").lower() in ("0", "false", "no")
        endpoint = f"{'http' if insecure else 'https'}://{endpoint}"
    region = (
        getenv_brand("EXPORT_S3_REGION")
        or os.getenv("AWS_DEFAULT_REGION")
        or os.getenv("AWS_REGION")
        or "us-east-1"
    )
    path_style = str(getenv_brand("EXPORT_S3_PATH_STYLE", getenv_brand("S3_PATH_STYLE", "1"))) != "0"
    kwargs: dict[str, Any] = {
        "region_name": region,
        "config": Config(
            signature_version="s3v4",
            s3={"addressing_style": "path" if path_style else "auto"},
            retries={"max_attempts": 5, "mode": "standard"},
        ),
    }
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    access = (
        getenv_brand("EXPORT_S3_ACCESS_KEY") or getenv_brand("S3_ACCESS_KEY") or os.getenv("AWS_ACCESS_KEY_ID")
    )
    secret = (
        getenv_brand("EXPORT_S3_SECRET_KEY") or getenv_brand("S3_SECRET_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY")
    )
    if access and secret:
        kwargs["aws_access_key_id"] = access
        kwargs["aws_secret_access_key"] = secret
    return boto3.client("s3", **kwargs)


# ------------------------------------------------------------------ key layout


def _segment(value: Any, fallback: str) -> str:
    text = _SAFE.sub("_", str(value or "").strip()).strip("._")
    return text[:128] or fallback


def export_object_key(workspace_id: str, job_id: str, filename: str) -> str:
    return "/".join(
        (export_prefix(), _segment(workspace_id, "_unscoped"), _segment(job_id, "job"), filename)
    )


def export_filename(destination: Any, export_name: str, job_id: str) -> str:
    """A safe file name for the object: the operator's ``output_path`` name, else ``export_<job>.<ext>``."""
    from services.export_paths import EXPORT_EXTENSIONS

    fmt = str(getattr(destination, "format", "") or "")
    ext = (os.path.splitext(export_name)[1].lstrip(".") or fmt or "json").lower()
    configured = str(getattr(destination, "output_path", "") or "").strip()
    base = configured.replace("\\", "/").rsplit("/", 1)[-1] if configured else ""
    stem, dot_ext = os.path.splitext(base)
    if not stem:
        return f"export_{_segment(job_id, 'job')}.{ext}"
    chosen = dot_ext.lstrip(".").lower() or ext
    if chosen not in EXPORT_EXTENSIONS:
        chosen = ext
    return f"{_segment(stem, 'export')[:120]}.{chosen}"


# ---------------------------------------------------------------- job binding

_bound_job: ContextVar[tuple[str, str] | None] = ContextVar("export_job_scope", default=None)


def _staging_root() -> Path:
    return Path(tempfile.gettempdir()) / "datawrap-export-staging"


def _staging_dir(job_id: str) -> Path:
    return _staging_root() / _segment(job_id, "job")


def discard_staged_export(job_id: str) -> None:
    """Remove this job's verified local copy (it exists only for Gate-8 read-back)."""
    shutil.rmtree(_staging_dir(job_id), ignore_errors=True)


def _reap_stale_staging(now: float | None = None) -> None:
    """Copies a crashed process left behind on this host (cheap: one directory scan)."""
    root = _staging_root()
    cutoff = (now or time.time()) - _STAGING_STALE_SECONDS
    try:
        entries = list(os.scandir(root))
    except OSError:
        return
    for entry in entries:
        try:
            if entry.stat().st_mtime < cutoff:
                shutil.rmtree(entry.path, ignore_errors=True)
        except OSError:
            continue


@contextmanager
def export_job_scope(job_id: str, workspace_id: str) -> Iterator[None]:
    """Held for a whole tracked job (``src.transfer.job_scope``): names the
    workspace the export key is scoped to, and discards the job's staged copy
    on every outcome once reconciliation has read it."""
    token = _bound_job.set((str(job_id), str(workspace_id or "")))
    try:
        yield
    finally:
        _bound_job.reset(token)
        discard_staged_export(job_id)


def _job_workspace(job_id: str) -> str:
    bound = _bound_job.get()
    if bound is not None and bound[0] == str(job_id) and bound[1]:
        return bound[1]
    try:
        from services.mongodb_service import get_mongodb_service

        job = get_mongodb_service().get_job(job_id) or {}
    except Exception:  # noqa: BLE001 - the key only needs a scope segment
        logger.debug("export workspace lookup failed for %s", job_id, exc_info=True)
        job = {}
    return str(job.get("workspace_id") or "")


# --------------------------------------------------------------------- upload


@dataclass(frozen=True)
class StoredExport:
    key: str
    size_bytes: int
    sha256: str
    parts: int


def upload_export_stream(
    stream: BinaryIO,
    key: str,
    *,
    content_type: str = "application/octet-stream",
    part_size: int | None = None,
    tee: BinaryIO | None = None,
    client: Any = None,
) -> StoredExport:
    """Upload ``stream`` to the export bucket in one pass, hashing as it goes.

    A body of one part is a single PUT; anything larger is a multipart upload
    of ``part_size`` parts read one at a time (memory holds one part), aborted
    on any failure so no partial object is left. ``tee`` receives the same
    bytes (the staged copy reconciliation reads back).
    """
    client = client or export_client()
    bucket = export_bucket()
    size = part_size or export_part_size()
    digest = hashlib.sha256()
    total = 0

    def _take(chunk: bytes) -> None:
        nonlocal total
        digest.update(chunk)
        total += len(chunk)
        if tee is not None:
            tee.write(chunk)

    first = stream.read(size)
    following = stream.read(size) if len(first) == size else b""
    if not following:
        _take(first)
        client.put_object(Bucket=bucket, Key=key, Body=first, ContentType=content_type)
        return StoredExport(key=key, size_bytes=total, sha256=digest.hexdigest(), parts=1)

    upload_id = client.create_multipart_upload(Bucket=bucket, Key=key, ContentType=content_type)["UploadId"]
    parts: list[dict[str, Any]] = []
    try:
        chunk, number = first, 1
        while chunk:
            if number > MAX_PARTS:
                raise ValueError(
                    f"Export is larger than {MAX_PARTS} parts of {size} bytes; "
                    "raise DATAWRAP_EXPORT_PART_SIZE_MB"
                )
            _take(chunk)
            resp = client.upload_part(
                Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=number, Body=chunk
            )
            parts.append({"ETag": resp["ETag"], "PartNumber": number})
            chunk, following = (following, b"") if following else (stream.read(size), b"")
            number += 1
        client.complete_multipart_upload(
            Bucket=bucket, Key=key, UploadId=upload_id, MultipartUpload={"Parts": parts}
        )
    except BaseException:
        try:
            client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
        except Exception:  # noqa: BLE001 - the lifecycle rule aborts it later
            logger.warning("Could not abort the multipart upload of %s", key, exc_info=True)
        raise
    return StoredExport(key=key, size_bytes=total, sha256=digest.hexdigest(), parts=len(parts))


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def store_job_export(
    destination: Any,
    export_bytes: bytes,
    *,
    job_id: str,
    export_name: str,
) -> dict[str, Any]:
    """Put a job's export in the bucket and describe it for the destination summary.

    The summary's ``path`` is a job-scoped local copy written from the very
    chunks that were hashed and uploaded, so Gate-8's artifact count and cell
    read-back check the bytes behind ``sha256``; :func:`export_job_scope`
    deletes it when the job ends. Raises ``ValueError`` when the upload fails
    (the job fails; nothing is served from a local disk).
    """
    _reap_stale_staging()
    workspace_id = _job_workspace(job_id)
    filename = export_filename(destination, export_name, job_id)
    key = export_object_key(workspace_id, job_id, filename)
    content_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"
    staged = _staging_dir(job_id) / filename
    staged.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(staged, "wb") as tee:
            stored = upload_export_stream(
                io.BytesIO(export_bytes), key, content_type=content_type, tee=tee
            )
    except Exception as exc:
        discard_staged_export(job_id)
        logger.warning("Export upload to the export bucket failed for job %s", job_id, exc_info=True)
        raise ValueError(
            f"The export could not be stored in the export bucket ({type(exc).__name__}); "
            "nothing was published. Check the export bucket settings and retry."
        ) from exc
    uploaded = datetime.now(timezone.utc)
    hours = export_retention_hours()
    return {
        "storage": STORAGE_OBJECT,
        "filename": filename,
        "path": str(staged),
        "object_key": stored.key,
        "size_bytes": stored.size_bytes,
        "sha256": stored.sha256,
        "upload_parts": stored.parts,
        "content_type": content_type,
        "uploaded_at": _iso(uploaded),
        "expires_at": _iso(uploaded + timedelta(hours=hours)),
        "retention_hours": hours,
        "download_url": f"/api/v1/jobs/{job_id}/export/download",
    }


def export_object_proof(dest_summary: Any, recon: Any) -> dict[str, Any] | None:
    """The stored object's identity for Gate-8: key, size, rows, SHA-256.

    ``rows`` is the independent artifact count read back from the same bytes
    when it was measured, else the writer's count.
    """
    ds = dest_summary if isinstance(dest_summary, dict) else {}
    if ds.get("storage") != STORAGE_OBJECT or not ds.get("object_key"):
        return None
    counted = (recon or {}).get("artifact_row_count")
    writer_rows = int(ds.get("rows") or 0)
    return {
        "storage": STORAGE_OBJECT,
        "object_key": ds["object_key"],
        "size_bytes": int(ds.get("size_bytes") or 0),
        "sha256": str(ds.get("sha256") or ""),
        "rows": counted if isinstance(counted, int) and counted >= 0 else writer_rows,
        "writer_rows": writer_rows,
        "expires_at": ds.get("expires_at"),
    }


# ------------------------------------------------------------------- download


def _parse_ts(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def stored_export(job: dict[str, Any]) -> dict[str, Any] | None:
    """The job's bucket export record, or ``None`` when it has none."""
    ds = job.get("destination_summary") if isinstance(job, dict) else None
    if not isinstance(ds, dict) or ds.get("storage") != STORAGE_OBJECT or not ds.get("object_key"):
        return None
    return ds


def export_expired(job: dict[str, Any], ds: dict[str, Any], now: datetime | None = None) -> bool:
    if job.get("export_expired_at"):
        return True
    expires = _parse_ts(ds.get("expires_at"))
    return expires is not None and expires <= (now or datetime.now(timezone.utc))


def export_object_exists(key: str, *, client: Any = None) -> bool:
    from botocore.exceptions import ClientError

    try:
        (client or export_client()).head_object(Bucket=export_bucket(), Key=key)
        return True
    except ClientError as exc:
        code = str((exc.response or {}).get("Error", {}).get("Code") or "")
        if code in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def presign_export(key: str, filename: str, *, client: Any = None) -> tuple[str, datetime, int]:
    """A presigned GET for ``key`` that downloads as ``filename``: (url, expires_at, ttl)."""
    ttl = export_url_ttl_seconds()
    issued = datetime.now(timezone.utc)
    safe_name = _segment(filename, "export")
    url = (client or export_client()).generate_presigned_url(
        "get_object",
        Params={
            "Bucket": export_bucket(),
            "Key": key,
            "ResponseContentDisposition": f'attachment; filename="{safe_name}"',
        },
        ExpiresIn=ttl,
    )
    return url, issued + timedelta(seconds=ttl), ttl


def mark_export_expired(job_id: str, when: datetime | None = None) -> None:
    from services.mongodb_service import get_mongodb_service

    try:
        get_mongodb_service().update_job_fields(
            job_id, {"export_expired_at": _iso(when or datetime.now(timezone.utc))}, touch_updated_at=False
        )
    except Exception:  # noqa: BLE001 - expires_at on the job still answers 410
        logger.warning("Could not mark the export of job %s expired", job_id, exc_info=True)


# -------------------------------------------------------------------- sweeper


def _key_scope(key: str) -> tuple[str, str]:
    """(workspace_id, job_id) from ``<prefix>/<workspace>/<job>/<file>``."""
    rest = key[len(export_prefix()) + 1:].split("/")
    if len(rest) < 3:
        return "", ""
    workspace = "" if rest[0] == "_unscoped" else rest[0]
    return workspace, rest[1]


def sweep_expired_exports(
    *, now: datetime | None = None, force: bool = False, client: Any = None
) -> dict[str, Any] | None:
    """Delete export objects older than the retention and mark their jobs expired.

    Runs on the scheduler beat; claimed once per ``SWEEP_INTERVAL`` per
    deployment (a file-backed deployment throttles per process). ``None`` when
    no bucket is configured or the period is not due. Never raises per object.
    """
    if not export_bucket_configured():
        return None
    from services.maintenance_claim import claim_local, claim_run, complete_run

    current = now or datetime.now(timezone.utc)
    if not force:
        claimed = claim_run(SWEEP_CLAIM, SWEEP_INTERVAL, now=current)
        if claimed is None:
            claimed = claim_local(SWEEP_CLAIM, SWEEP_INTERVAL, now=current)
        if not claimed:
            return None
    client = client or export_client()
    bucket = export_bucket()
    cutoff = current - timedelta(hours=export_retention_hours())
    expired: list[str] = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{export_prefix()}/"):
        for item in page.get("Contents") or []:
            created = _parse_ts(item.get("LastModified"))
            if created is not None and created <= cutoff:
                expired.append(str(item["Key"]))
    deleted: list[str] = []
    for start in range(0, len(expired), 1000):
        batch = expired[start:start + 1000]
        resp = client.delete_objects(
            Bucket=bucket, Delete={"Objects": [{"Key": k} for k in batch], "Quiet": True}
        )
        failed = {str(e.get("Key")) for e in resp.get("Errors") or []}
        for err in resp.get("Errors") or []:
            logger.warning("Expired export %s could not be deleted: %s", err.get("Key"), err.get("Code"))
        deleted.extend(k for k in batch if k not in failed)
    _record_expired(deleted, current)
    complete_run(SWEEP_CLAIM)
    return {"ran_at": _iso(current), "cutoff": _iso(cutoff), "deleted": len(deleted)}


def _record_expired(keys: list[str], when: datetime) -> None:
    from services.audit_log import append_audit_event

    for key in keys:
        workspace_id, job_id = _key_scope(key)
        if job_id:
            mark_export_expired(job_id, when)
        try:
            append_audit_event(
                action=AUDIT_EXPIRED,
                resource=f"job:{job_id}" if job_id else "export",
                actor=SWEEP_ACTOR,
                outcome="succeeded",
                workspace_id=workspace_id or None,
                details={"job_id": job_id, "object_key": key, "retention_hours": export_retention_hours()},
            )
        except Exception:  # noqa: BLE001 - the deletion stands; the log keeps it
            logger.warning("Audit of expired export %s failed", key, exc_info=True)
