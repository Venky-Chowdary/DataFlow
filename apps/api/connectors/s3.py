"""Amazon S3 connector — bucket probe via boto3, credential validation fallback."""

from __future__ import annotations

from connectors.base import ConnectResult


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
    del port, schema, ssl, warehouse

    region = (host or "").strip() or "us-east-1"
    bucket = (database or connection_string or "").strip()
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

    try:
        import boto3
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
        client = boto3.client(
            "s3",
            region_name=region,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )
        client.head_bucket(Bucket=bucket)
        keys: list[str] = []
        try:
            from connectors.s3_reader import list_objects

            keys = list_objects({"host": region, "username": access_key, "password": secret_key}, bucket)
        except Exception:
            keys = []
        objects = keys or [bucket]
        return ConnectResult(
            ok=True,
            tables=objects,
            message=f"S3 bucket `{bucket}` reachable — {len(keys) or 1} object(s) listed.",
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
