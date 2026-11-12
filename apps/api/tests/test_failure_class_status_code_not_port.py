"""A port, host or id that contains "403" or "429" is not an HTTP status.

CI, api-tests shard 5/6 (PR 405):
``test_r5_schedule_run_lifecycle_e2e::test_source_auth_failure_is_a_failed_run_recorded_before_work_and_surfaced``
failed with ``KeyError: 'retry_scheduled'``. The test points the source at a
closed ephemeral port and expects one retry. The scheduler gave up instead
(``schedule.run.give_up`` classification ``deterministic``) and parked the
schedule on RUN_REFUSED, because the credential-refusal pattern matched a bare
``403`` anywhere in the message — here inside the port number of
``connection to server at "127.0.0.1", port 40312 failed: Connection refused``.
About one ephemeral port in 180 contains ``403``. The same bare ``429`` made
any unrecognised failure on such a port "transient".
"""

from __future__ import annotations

import pytest

from services.failure_retry_policy import (
    DETERMINISTIC,
    TRANSIENT,
    UNKNOWN,
    classify_failure,
    classify_job_failure,
)

REFUSED = (
    'connection to server at "127.0.0.1", port {port} failed: Connection refused\n'
    "\tIs the server running on that host and accepting TCP/IP connections?"
)


@pytest.mark.parametrize("port", [40312, 34031, 51403, 60403, 4030, 42913, 54290])
def test_a_refused_connection_is_transient_whatever_the_port(port):
    got = classify_job_failure({
        "status": "failed", "phase": "failed", "error": REFUSED.format(port=port),
        "error_code": "transfer_failed", "records_processed": 0,
    })
    assert got.kind == TRANSIENT, (port, got)
    assert got.retryable


@pytest.mark.parametrize("message", [
    'could not translate host name "db-403.internal" to address: try again',
    "connect to 10.0.0.7:40300 timed out",
    "Snowflake warehouse wh_4031 is being resumed",
])
def test_digits_inside_a_host_or_name_are_not_a_status(message):
    assert classify_failure(error=message).kind == TRANSIENT, message


def test_an_unrecognised_failure_on_a_429_port_is_not_rate_limiting():
    got = classify_failure(
        error='connection to server at "127.0.0.1", port 42913 failed: '
        "expected authentication request from server, but received x"
    )
    assert got.kind == UNKNOWN, got


@pytest.mark.parametrize("message", [
    "403 Client Error: Forbidden for url: https://api.example.com/v1/rows",
    "HTTP 403 returned by the destination",
    "the API answered with status code 403.",
    "Error 403: Forbidden",
    "upload refused (403)",
    "status=403 body={}",
])
def test_a_real_403_is_still_a_credential_refusal(message):
    got = classify_failure(error=message)
    assert got.kind == DETERMINISTIC, (message, got)
    assert "Credentials" in got.corrective_action


@pytest.mark.parametrize("message", [
    "429 Too Many Requests",
    "HTTP 429 from the warehouse API",
    "status code 429; retry after 2s",
])
def test_a_real_429_is_still_rate_limiting(message):
    assert classify_failure(error=message).kind == TRANSIENT, message


def test_the_scheduler_retries_a_refused_connection_on_a_403_port():
    """What the r5 e2e asserts: one retry owed, not a parked RUN_REFUSED."""
    from services.schedule_runner import _retry_decision

    decision = _retry_decision(
        "failed", 0, 1,
        sync_mode="full_refresh_overwrite",
        rows_committed=0,
        rows_committed_known=True,
        job_doc={
            "status": "failed", "phase": "failed", "error": REFUSED.format(port=40312),
            "error_code": "transfer_failed", "records_processed": 0,
        },
    )
    assert decision["retry"] is True, decision
    assert decision["failure_class"]["kind"] == TRANSIENT
