"""Amazon S3 connector — bucket probe via boto3, credential validation fallback."""

from __future__ import annotations

from typing import Any

from connectors.aws_common import boto3_client
from connectors.base import ConnectResult


def s3_object_exists(cfg: dict[str, Any], bucket: str, key: str) -> bool | None:
    """True when the key is in the bucket, False on 404, None when unproven.

    A failed ``ListObjects`` is not proof the key is absent. Create-new
    requires this head to say the key is not there.
    """
    blob = (key or "").strip()
    bucket_name = (bucket or "").strip()
    if not blob or not bucket_name:
        return None
    try:
        from botocore.exceptions import ClientError
    except ImportError:
        return None
    try:
        client = boto3_client("s3", cfg)
        client.head_object(Bucket=bucket_name, Key=blob)
        return True
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code in {"404", "NoSuchKey", "NotFound"}:
            return False
        return None
    except Exception:
        return None


def test_s3(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    schema: str,
    connection_string: str,
    ssl: bool,
    warehouse: str = "",
) -> ConnectResult:
    del schema, warehouse

    bucket = (database or "").strip()
    access_key = (username or "").strip()
    secret_key = (password or "").strip()

    if not bucket:
        return ConnectResult(ok=False, tables=[], error="Bucket name is required (Database field).")
    if not access_key or not secret_key:
        return ConnectResult(
            ok=False,
            tables=[],
            error="AWS Access Key ID (username) and Secret Access Key (password) are required.",
        )

    cfg = {
        "host": host,
        "port": port,
        "database": database,
        "username": username,
        "password": password,
        "connection_string": connection_string,
        "ssl": ssl,
    }

    try:
        from botocore.exceptions import BotoCoreError, ClientError
    except ImportError:
        from connectors.driver_guard import require_driver
        return ConnectResult(
            ok=False,
            tables=[],
            error=require_driver("boto3"),
            driver="none",
        )

    try:
        client = boto3_client("s3", cfg)
        client.head_bucket(Bucket=bucket)
        keys: list[str] = []
        list_error = ""
        try:
            from connectors.s3_reader import list_objects

            keys = list_objects(cfg, bucket)
        except Exception as exc:
            keys = []
            list_error = str(exc)
        if list_error:
            # head_bucket succeeded. Substituting the bucket name for a
            # failed list made MinIO look like one object and hid the error.
            return ConnectResult(
                ok=True,
                tables=[],
                message=(
                    f"S3 bucket `{bucket}` reachable, but listing objects failed: "
                    f"{list_error}"
                ),
                driver="boto3",
            )
        return ConnectResult(
            ok=True,
            tables=keys,
            message=f"S3 bucket `{bucket}` reachable — {len(keys)} object(s) listed.",
            driver="boto3",
        )
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchBucket"):
            return ConnectResult(ok=False, tables=[], error=f"Bucket `{bucket}` not found.")
        if code in ("403", "AccessDenied"):
            return ConnectResult(ok=False, tables=[], error=f"Access denied to bucket `{bucket}` — check IAM policy.")
        return ConnectResult(ok=False, tables=[], error=str(exc))
    except BotoCoreError as exc:
        return ConnectResult(ok=False, tables=[], error=str(exc))
    except Exception as exc:
        return ConnectResult(ok=False, tables=[], error=str(exc))
