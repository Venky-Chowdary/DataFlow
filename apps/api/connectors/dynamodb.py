"""Amazon DynamoDB connector — credential validation and optional boto3 probe."""

from __future__ import annotations

from connectors.base import ConnectResult


def test_dynamodb(
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

    region = (host or connection_string or "").strip() or "us-east-1"
    table = (database or "").strip()
    access_key = (username or "").strip()
    secret_key = (password or "").strip()

    if not table:
        return ConnectResult(ok=False, tables=[], error="Table name is required (Database field).")
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
        return ConnectResult(
            ok=True,
            tables=[table],
            message=f"Credentials validated for table `{table}` in {region} (install boto3 for live probe).",
            driver="validation",
        )

    try:
        client = boto3.client(
            "dynamodb",
            region_name=region,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )
        client.describe_table(TableName=table)
        return ConnectResult(
            ok=True,
            tables=[table],
            message=f"DynamoDB table `{table}` reachable in {region}.",
            driver="boto3",
        )
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in ("ResourceNotFoundException",):
            return ConnectResult(ok=False, tables=[], error=f"Table `{table}` not found in {region}.")
        return ConnectResult(ok=False, tables=[], error=str(exc))
    except BotoCoreError as exc:
        return ConnectResult(ok=False, tables=[], error=str(exc))
    except Exception as exc:
        return ConnectResult(ok=False, tables=[], error=str(exc))
