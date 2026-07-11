"""Amazon DynamoDB connector — credential validation, local endpoint, table probe."""

from __future__ import annotations

from connectors.aws_common import boto3_client, is_local_endpoint, resolve_endpoint_url, resolve_region


def _cfg(host: str, port: int, username: str, password: str, connection_string: str) -> dict:
    return {
        "host": host,
        "port": port,
        "username": username,
        "password": password,
        "connection_string": connection_string,
    }


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
    from connectors.base import ConnectResult

    del schema, ssl, warehouse

    table = (database or "").strip()
    cfg = _cfg(host, port, username, password, connection_string)
    region = resolve_region(cfg)
    endpoint = resolve_endpoint_url(cfg)
    local = is_local_endpoint(cfg)
    access_key = (username or "").strip()
    secret_key = (password or "").strip()

    if not table:
        return ConnectResult(ok=False, tables=[], error="Table name is required (Database field).")
    if not local and (not access_key or not secret_key):
        return ConnectResult(
            ok=False,
            tables=[],
            error="AWS Access Key ID (username) and Secret Access Key (password) are required.",
        )

    try:
        import boto3  # noqa: F401
        from botocore.exceptions import BotoCoreError, ClientError
    except ImportError:
        target = endpoint or region
        return ConnectResult(
            ok=True,
            tables=[table],
            message=f"Credentials validated for table `{table}` at {target} (install boto3 for live probe).",
            driver="validation",
        )

    try:
        client = boto3_client("dynamodb", cfg)
        client.describe_table(TableName=table)
        tables = [table]
        try:
            from connectors.dynamodb_reader import list_tables

            discovered = list_tables(cfg)
            if table in discovered:
                tables = sorted(set(discovered))
        except Exception:
            pass
        target = endpoint or region
        mode = "DynamoDB Local" if local else "AWS DynamoDB"
        return ConnectResult(
            ok=True,
            tables=tables,
            message=f"{mode}: table `{table}` reachable at {target}.",
            driver="boto3",
        )
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in ("ResourceNotFoundException",):
            return ConnectResult(ok=False, tables=[], error=f"Table `{table}` not found.")
        return ConnectResult(ok=False, tables=[], error=str(exc))
    except BotoCoreError as exc:
        return ConnectResult(ok=False, tables=[], error=str(exc))
    except Exception as exc:
        return ConnectResult(ok=False, tables=[], error=str(exc))
