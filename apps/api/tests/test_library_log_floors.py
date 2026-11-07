"""Third-party loggers that print credentials, auth headers, signed URLs or bound
row values at DEBUG/INFO never reach Datawrap's handlers, even when the operator
runs the process at DEBUG."""
from __future__ import annotations

import logging

import pytest

from services import logging_config

SENTINEL = "s3ntinel-Pa55w0rd"

# Each name is a logger the library really writes to (child loggers included).
FLOORED = [
    "kafka.net.connection",          # PLAIN auth_bytes = user\0user\0password
    "kafka.protocol.parser",
    "botocore.endpoint",             # full request incl. Authorization header
    "botocore.auth",                 # signature / string-to-sign
    "boto3.resources.action",
    "urllib3.connectionpool",        # method + URL incl. presigned query string
    "httpx",                         # INFO "HTTP Request: <full URL>"
    "httpcore.http11",
    "snowflake.connector.connection",  # session parameters / tokens
    "google.auth._helpers",          # request/response logs of token exchange
    "azure.core.pipeline.policies.http_logging_policy",
    "paramiko.transport",            # packet hexdumps when enabled
    "pymongo.command",               # command documents carry row values
    "sqlalchemy.engine.Engine",      # SQL plus bound params (row values)
]


@pytest.fixture
def debug_logging(monkeypatch):
    root = logging.getLogger()
    saved_handlers, saved_level = list(root.handlers), root.level
    names = {n for n in FLOORED} | {n.split(".")[0] for n in FLOORED} | {
        "sqlalchemy.engine", "snowflake.connector", "google.auth", "asyncio"
    }
    saved_levels = {n: logging.getLogger(n).level for n in names}
    monkeypatch.setenv("DATAFLOW_LOG_LEVEL", "DEBUG")
    monkeypatch.delenv("DATAFLOW_LOG_LEVELS", raising=False)
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    seen: list[logging.LogRecord] = []

    def install():
        logging_config.configure_logging(force=True)
        for h in logging.getLogger().handlers:
            h.emit = seen.append  # capture what passed the handler's filters
        return seen

    yield install
    for h in list(root.handlers):
        root.removeHandler(h)
    for h in saved_handlers:
        root.addHandler(h)
    root.setLevel(saved_level)
    for n, lvl in saved_levels.items():
        logging.getLogger(n).setLevel(lvl)
    logging_config.reset_for_tests()


def _leaked(seen):
    return [r.name for r in seen if SENTINEL in r.getMessage()]


def test_debug_root_still_logs_our_own_debug(debug_logging):
    seen = debug_logging()
    logging.getLogger("services.something").debug("ours %s", "x")
    assert [r for r in seen if r.name == "services.something"], "DEBUG not active"


@pytest.mark.parametrize("name", FLOORED)
def test_floored_library_debug_and_info_never_reach_handlers(debug_logging, name):
    seen = debug_logging()
    log = logging.getLogger(name)
    log.debug("auth_bytes=%s", SENTINEL)
    log.info("HTTP Request: POST https://h/x?sig=%s", SENTINEL)
    assert _leaked(seen) == []


def test_floor_holds_when_library_lowers_its_own_level(debug_logging):
    # boto3.set_stream_logger / botocore.session.set_debug_logger do exactly this.
    seen = debug_logging()
    for top in ("botocore", "boto3", "kafka", "sqlalchemy.engine"):
        logging.getLogger(top).setLevel(logging.DEBUG)
    for name in FLOORED:
        logging.getLogger(name).debug("password=%s", SENTINEL)
    assert _leaked(seen) == []


def test_override_cannot_lower_sensitive_floor_but_still_works_elsewhere(
    debug_logging, monkeypatch
):
    monkeypatch.setenv(
        "DATAFLOW_LOG_LEVELS", "sqlalchemy.engine=DEBUG,kafka=INFO,asyncio=DEBUG"
    )
    seen = debug_logging()
    logging.getLogger("sqlalchemy.engine.Engine").info("params %s", SENTINEL)
    logging.getLogger("kafka.net.connection").info("auth %s", SENTINEL)
    logging.getLogger("asyncio").debug("loop detail")
    assert _leaked(seen) == []
    assert logging.getLogger("sqlalchemy.engine").level >= logging.WARNING
    assert any(r.name == "asyncio" for r in seen)


def test_floored_library_warning_still_reaches_handlers(debug_logging):
    seen = debug_logging()
    logging.getLogger("kafka.net.connection").warning("broker unreachable")
    logging.getLogger("sqlalchemy.engine.Engine").error("pool exhausted")
    assert {r.name for r in seen} >= {"kafka.net.connection", "sqlalchemy.engine.Engine"}
