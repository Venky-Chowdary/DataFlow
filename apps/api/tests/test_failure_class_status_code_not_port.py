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


# Review L2: compact status shapes the bare code matched and the port-safe
# pattern must keep matching (Google/Elasticsearch compact JSON, ``status:403``,
# ``HTTP/403``, ``HTTP-429``, ``403-forbidden``, ``E403``).
@pytest.mark.parametrize("message", [
    '{"error":{"code":403,"message":"The caller does not have permission",'
    '"status":"PERMISSION_DENIED"}}',
    'HTTP status:403 body={"message":"forbidden"}',
    "status:403",
    "returned HTTP/403",
    "error 403-forbidden from gateway",
    "E403 forbidden",
])
def test_a_compact_403_is_still_a_credential_refusal(message):
    got = classify_failure(error=message)
    assert got.kind == DETERMINISTIC, (message, got)
    assert "Credentials" in got.corrective_action


@pytest.mark.parametrize("message", [
    '{"error":{"root_cause":[],"type":"es_rejected_execution_exception"},"status":429}',
    "got status:429 from upstream",
    "status:429",
    "received HTTP-429 from api",
])
def test_a_compact_429_is_still_rate_limiting(message):
    assert classify_failure(error=message).kind == TRANSIENT, message


# A port that IS the code, or starts with it after "host:", is still a port:
# each message says nothing else, so it stays unclassified.
@pytest.mark.parametrize("message", [
    'connection to server at "10.0.0.5", port 403 failed: '
    "expected authentication request from server, but received x",
    'connection to server at "10.0.0.5", port 429 failed: '
    "expected authentication request from server, but received x",
    "upstream host:4290 answered with an unexpected payload",
    "upstream host:4031 answered with an unexpected payload",
])
def test_a_port_is_not_a_status_even_when_it_is_the_code(message):
    got = classify_failure(error=message)
    assert got.kind == UNKNOWN, (message, got)


# Review round 2, L-F: a position, a row count, a labelled port or an id is not
# a status either. Each message says nothing else, so it stays unclassified.
@pytest.mark.parametrize("code", ["403", "429"])
@pytest.mark.parametrize("template", [
    "SQL compilation error: syntax error line 1 at position {code} unexpected 'x'.",
    "inserted {code} rows before: something odd",
    "port: {code} failed: something odd",
    "port={code} something odd",
    "row id={code} something odd",
])
def test_a_position_count_port_or_id_is_not_a_status(template, code):
    message = template.format(code=code)
    got = classify_failure(error=message)
    assert got.kind == UNKNOWN, (message, got)


# The digits that follow the code keep it a number (mutants the review's pass
# found untested): a decimal, a hyphenated serial, a path segment.
@pytest.mark.parametrize("message", [
    "value 403.5 something odd",
    "batch 403-1 something odd",
    "GET /v1/403 something odd",
    "value 429.5 something odd",
    "batch 429-1 something odd",
    "GET /v1/429 something odd",
])
def test_digits_after_the_code_or_a_path_before_it_are_not_a_status(message):
    got = classify_failure(error=message)
    assert got.kind == UNKNOWN, (message, got)


# Review round 3, Low 4: a 403 or 429 is a status only in a status context:
# after ``http``, ``status``, ``code``, ``error`` (``client error``,
# ``HTTPError:``, ``ApiError(``) or ``exception(``, at the very start of the
# message, or in a recognised shape: ``(403)``, ``[403]``, ``403 Forbidden``,
# ``429 Too Many Requests``, ``403 Client Error``, google-api-core's
# ``403 POST https://...`` and urllib3's ``too many 429 error responses``.
# Every row of the review's retry table (round-1 and round-2 rows plus real
# SDK and driver strings). D: a credential refusal, T: transient, "?": the
# number is not read as a status (the verdict is what the message says
# without it).
STATUS_TABLE = [
    ("An error occurred (403) when calling the HeadObject operation: Forbidden", "D"),
    ("403 Client Error: Forbidden for url: https://api.example.com/v1/items", "D"),
    ("Client error '403 Forbidden' for url 'https://api.example.com/x'", "D"),
    ("HTTP Error 403: Forbidden", "D"),
    ("google.api_core.exceptions.Forbidden: 403 GET https://bigquery.googleapis.com/bigquery/v2/"
     "projects/p/datasets/d: Access Denied: Dataset p:d", "D"),
    ("403 POST https://bigquery.googleapis.com/upload/bigquery/v2/projects/p/jobs: User does not "
     "have bigquery.jobs.create permission", "D"),
    ("000403: 403: HTTP 403: Forbidden", "D"),
    ("SalesforceGeneralError: Error Code 403. Response content: "
     "[{'errorCode': 'API_DISABLED_FOR_ORG'}]", "D"),
    ("Elasticsearch AuthorizationException(403, 'security_exception', "
     "'action [indices:data/write/bulk] is unauthorized')", "D"),
    ('{"error":{"code":403,"message":"The caller does not have permission",'
     '"status":"PERMISSION_DENIED"}}', "D"),
    ('HTTP status:403 body={"message":"forbidden"}', "D"),
    ("status=403 reason=Forbidden", "D"),
    ("statusCode: 403", "D"),
    ("Response [403]", "D"),
    ("error 403-forbidden from gateway", "D"),
    ("HTTP/1.1 403 Forbidden", "D"),
    ("returned HTTP/403", "D"),
    ("E403 forbidden", "D"),
    ("429 Client Error: Too Many Requests for url: https://api.hubapi.com/crm/v3", "T"),
    ("Client error '429 Too Many Requests' for url 'https://api.example.com'", "T"),
    ("An error occurred (429) when calling the PutObject operation: Too Many Requests", "T"),
    ("google.api_core.exceptions.TooManyRequests: 429 POST https://bigquery.googleapis.com/x", "T"),
    ("ApiError(429, 'es_rejected_execution_exception', "
     "'rejected execution of coordinating operation')", "T"),
    ('{"error":{"root_cause":[],"type":"es_rejected_execution_exception"},"status":429}', "T"),
    ("SalesforceGeneralError: Error Code 429. Response content: REQUEST_LIMIT_EXCEEDED", "T"),
    ("HTTP 429", "T"),
    ("got status:429 from upstream", "T"),
    ("received HTTP-429 from api", "T"),
    ("aiohttp.ClientResponseError: 429, message='Too Many Requests', url=URL('https://x')", "T"),
    ("Max retries exceeded with url: /x (Caused by ResponseError('too many 429 error "
     "responses'))", "T"),
    ('connection to server at "127.0.0.1", port 40312 failed: Connection refused', "T"),
    ("could not connect to server: port 54290 ", "T"),
    ("connection to 10.4.0.3:4031 timed out", "T"),
    ("failed at row 4030: value too long", "?"),
    ("transfer of job 403e2f-ab failed: something odd", "?"),
    ("duration 0.403s then: something odd", "?"),
    ("inserted 403 rows before: something odd", "?"),
    ("port 403 failed: something odd", "?"),
    ("host db-403.internal: something odd", "?"),
    ("v4.0.3 driver: something odd", "?"),
    ("error 4.03: something odd", "?"),
    ("at 2026-04-03T04:29:00 something odd", "?"),
    ("SQL compilation error: syntax error line 1 at position 403 unexpected 'x'.", "?"),
    ("SQL compilation error: syntax error line 1 at position 429 unexpected 'x'.", "?"),
    ("syntax error at 12:429 near 'x'", "?"),
    ("parse error at line 3:403 something odd", "?"),
    ("connect to 10.0.0.7:403 failed: something odd", "?"),
    ("port: 403 failed: something odd", "?"),
    ("port=429 something odd", "?"),
    ("host db-e403.internal: something odd", "?"),
    ("chunk e403 checksum mismatch", "?"),
    ("SPACE403 is full", "?"),
    ("CODE403 something odd", "?"),
    ("row id=403 has bad value", "?"),
    ('column "c403" something odd', "?"),
    ("value 403.5 out of range", "?"),
    ("value 4.29e403 out of range", "?"),
    ("sha e429a1 something odd", "?"),
    ("timestamp 12:04:29 something odd", "?"),
    ("HTTP/2 403", "D"),
    ("http-403", "D"),
    ("Http/1.1 429 Too Many Requests", "T"),
    ('{"status":"429"}', "T"),
    ("'status': 403", "D"),
    ("[403] Forbidden", "D"),
    ("(403)", "D"),
    ("code=E429 rate", "T"),
    ("403:Forbidden", "D"),
    ("status 403.", "D"),
    ("status 429-", "T"),
    ("403-1 something odd", "?"),
    # Round 3: real SDK and driver strings.
    ("botocore.exceptions.ClientError: An error occurred (403) when calling the HeadObject "
     "operation: Forbidden", "D"),
    ("google.api_core.exceptions.Forbidden: 403 POST https://bigquery.googleapis.com/bigquery/v2/"
     "projects/p/jobs?prettyPrint=false: Access Denied: Project p", "D"),
    ("google.api_core.exceptions.TooManyRequests: 429 Exceeded rate limits: too many table "
     "update operations for this table.", "T"),
    ("requests.exceptions.HTTPError: 403 Client Error: Forbidden for url: "
     "https://api.hubapi.com/crm/v3/objects/contacts", "D"),
    ("requests.exceptions.HTTPError: 429 Client Error: Too Many Requests for url: "
     "https://api.github.com/repos/x/y/issues", "T"),
    ('HTTP 429: {"status":"error","message":"You have reached your secondly limit.",'
     '"errorType":"RATE_LIMIT"}', "T"),
    ("azure.core.exceptions.HttpResponseError: (AuthorizationPermissionMismatch) not authorized."
     "\nRequestId:abc\nTime:2024-01-01T00:00:00.4290000Z\nStatus: 403", "D"),
    ("250001 (08001): Failed to connect to DB: xy.snowflakecomputing.com:443. "
     "HTTP 403: Forbidden", "D"),
    ("stripe.error.RateLimitError: Request req_429abc: Too many requests hit the API too "
     "quickly.", "T"),
    ("aiohttp.client_exceptions.ClientResponseError: 429, message='Too Many Requests', "
     "url=URL('https://api.x.com/v2/q')", "T"),
    ("httpx.HTTPStatusError: Client error '403 Forbidden' for url "
     "'https://api.notion.com/v1/databases/x/query'", "D"),
    ("salesforce: Malformed request https://x.my.salesforce.com/services/data/v52.0/query/ "
     "Response content: [{'errorCode': 'REQUEST_LIMIT_EXCEEDED'}] (status 403)", "D"),
    ('psycopg2.errors.UniqueViolation: duplicate key value violates unique constraint '
     '"orders_pkey"\nDETAIL:  Key (id)=(429) already exists.', "?"),
    ("pymysql.err.IntegrityError: (1062, \"Duplicate entry '403' for key 'orders.PRIMARY'\")", "?"),
    ("psycopg2.errors.StringDataRightTruncation: value too long for type character varying(429)",
     "?"),
    ("snowflake.connector.errors.ProgrammingError: 100038 (22018): Numeric value '403' is not "
     "recognized", "?"),
    ('psycopg2.errors.InvalidTextRepresentation: invalid input syntax for type integer: "abc"\n'
     "LINE 1: ...VALUES (429, 'abc')", "?"),
    ("google.api_core.exceptions.BadRequest: 400 Error while reading data, error message: CSV "
     "table encountered too many errors, giving up. Rows: 403; errors: 1.", "?"),
    ("100080 (22000): Number of columns in file (12) does not match that of the corresponding "
     "table (11)  File 'x.csv.gz', line 429, character 1", "?"),
    ("ERROR: invalid input syntax for type integer at character 429", "?"),
    ("Row 403 has 7 columns, expected 6", "?"),
    ("pymongo.errors.BulkWriteError: batch op errors occurred, full error: {'writeErrors': "
     "[{'index': 429, 'code': 11000, 'errmsg': 'E11000 duplicate key'}]}", "?"),
    ("position: 429 unexpected token", "?"),
    ("Error 429 row limit exceeded for this API key", "T"),
    ("status 403 rows endpoint forbidden", "D"),
]


def _without_the_code(message: str) -> str:
    return message.replace("403", "xyz").replace("429", "xyz")


@pytest.mark.parametrize(("message", "expected"), STATUS_TABLE,
                         ids=[f"{i:02d}" for i in range(len(STATUS_TABLE))])
def test_the_retry_table(message, expected):
    got = classify_failure(error=message, phase="load")
    if expected == "D":
        assert got.kind == DETERMINISTIC, (message, got)
        assert "Credentials" in got.corrective_action, (message, got)
    elif expected == "T":
        assert got.kind == TRANSIENT, (message, got)
    else:
        unread = classify_failure(error=_without_the_code(message), phase="load")
        assert got == unread, (message, got)


# What the previous rounds pinned, now said by the context rule rather than by
# a lookbehind each: a position, a row count (one row or many), a labelled port
# and an id are not a status; a rate limit that mentions a row still is.
@pytest.mark.parametrize("message", [
    "SQL compilation error: syntax error line 1 at position 403 unexpected 'x'.",
    "SQL compilation error: syntax error line 1 at position 429 unexpected 'x'.",
    "inserted 403 rows before: something odd",
    "port: 403 failed: something odd",
    "port=429 something odd",
    "row id=403 has bad value",
    "parquet: read 403 row groups before: something odd",
    "parquet: read 429 row groups before: something odd",
])
def test_a_number_outside_a_status_context_is_not_a_status(message):
    got = classify_failure(error=message)
    assert got.kind == UNKNOWN, (message, got)


def test_too_many_requests_is_rate_limiting_without_a_status():
    got = classify_failure(error="Request req_abc: Too many requests hit the API too quickly.")
    assert got.kind == TRANSIENT, got
