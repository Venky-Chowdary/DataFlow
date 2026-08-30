"""Fail-fast Google client retry for localhost emulators.

fake-gcs-server and goccy/bigquery-emulator answer missing bucket/table as
404 or InternalServerError 500. The google client default retry sleeps until
the operator/pytest timeout — that is the create-new hang, not a missing
algorithm. Production Google APIs keep default retry.
"""

from __future__ import annotations

from typing import Any

EMULATOR_DEADLINE_S = 8.0


def looks_like_google_emulator(
    *,
    endpoint: str = "",
    host: str = "",
    port: int = 0,
) -> bool:
    text = f"{endpoint} {host} {port}".lower()
    return any(
        token in text
        for token in (
            "localhost",
            "127.0.0.1",
            "0.0.0.0",
            "fake-gcs",
            ":4443",
            ":9050",
        )
    )


def google_emulator_retry(deadline: float = EMULATOR_DEADLINE_S) -> Any:
    from google.api_core import retry as retries

    return retries.Retry(predicate=lambda _exc: False, deadline=float(deadline))


def google_emulator_timeout(deadline: float = EMULATOR_DEADLINE_S) -> float:
    return float(deadline)
