"""BigQuery connection helper — project + optional service account JSON path.

Supports local emulators (e.g. goccy/bigquery-emulator) by detecting
localhost/api_endpoint URLs and using anonymous credentials.
"""

from __future__ import annotations

from typing import Any


def _is_local_endpoint(host: str, connection_string: str) -> tuple[bool, str]:
    """Return (is_local, endpoint_url) for BigQuery-compatible emulators."""
    raw = (connection_string or "").strip()
    if raw.startswith("http://") or raw.startswith("https://"):
        return True, raw.rstrip("/")
    raw_host = (host or "").strip()
    if raw_host in ("localhost", "127.0.0.1", "host.docker.internal"):
        return True, ""
    return False, ""


# The emulator reports every SQL failure as HTTP 500 ``internalError`` (hosted
# BigQuery reserves that reason for transient backend faults), and the client's
# default job retry replays such jobs for 10 minutes — one refused statement
# reads as a hang. Bound the replay so the refusal surfaces in seconds.
_EMULATOR_RETRY_DEADLINE_S = 20.0
_emulator_client_cls: type | None = None


def _emulator_client_class(bigquery: Any) -> type:
    global _emulator_client_cls
    if _emulator_client_cls is not None:
        return _emulator_client_cls
    from google.cloud.bigquery.retry import DEFAULT_JOB_RETRY, DEFAULT_RETRY

    bounded_retry = DEFAULT_RETRY.with_deadline(_EMULATOR_RETRY_DEADLINE_S)
    bounded_job_retry = DEFAULT_JOB_RETRY.with_deadline(_EMULATOR_RETRY_DEADLINE_S)

    class _EmulatorClient(bigquery.Client):  # type: ignore[misc, name-defined]
        def query(self, query: str, *args: Any, **kwargs: Any) -> Any:
            kwargs.setdefault("retry", bounded_retry)
            kwargs.setdefault("job_retry", bounded_job_retry)
            return super().query(query, *args, **kwargs)

        def query_and_wait(self, query: str, *args: Any, **kwargs: Any) -> Any:
            kwargs.setdefault("retry", bounded_retry)
            kwargs.setdefault("job_retry", bounded_job_retry)
            return super().query_and_wait(query, *args, **kwargs)

    _emulator_client_cls = _EmulatorClient
    return _EmulatorClient


def get_client(
    *,
    project_id: str,
    credentials_path: str = "",
    service_account: str = "",
    location: str = "",
    host: str = "",
    port: int = 0,
    connection_string: str = "",
) -> Any:
    from google.api_core.client_options import ClientOptions
    from google.cloud import bigquery

    creds_ref = (service_account or connection_string or credentials_path or "").strip()
    is_local, endpoint_url = _is_local_endpoint(host, creds_ref)
    # If no explicit emulator URL/credentials but the host itself is local, treat
    # it as an emulator so tests and local stacks don't require ADC.
    if not is_local and host in ("localhost", "127.0.0.1", "host.docker.internal"):
        is_local = True

    if is_local:
        from google.auth.credentials import AnonymousCredentials

        creds = AnonymousCredentials()
        client_options = None
        if endpoint_url:
            client_options = ClientOptions(api_endpoint=endpoint_url)
        elif port:
            client_options = ClientOptions(api_endpoint=f"http://{host}:{port}")
        elif host in ("localhost", "127.0.0.1"):
            client_options = ClientOptions(api_endpoint="http://127.0.0.1:9050")
        return _emulator_client_class(bigquery)(
            project=project_id,
            credentials=creds,
            location=location or None,
            client_options=client_options,
        )

    if creds_ref:
        from google.oauth2 import service_account

        if creds_ref.startswith("{"):
            import json

            info = json.loads(creds_ref)
            creds = service_account.Credentials.from_service_account_info(info)
        else:
            creds = service_account.Credentials.from_service_account_file(creds_ref)
        return bigquery.Client(project=project_id, credentials=creds, location=location or None)
    return bigquery.Client(project=project_id, location=location or None)
