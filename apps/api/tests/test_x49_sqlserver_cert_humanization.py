"""X-49: raw ODBC SSL self-signed text is humanized everywhere it can surface.

The failure arrives as ``(pyodbc.OperationalError) ('08001', '...SSL
Provider: ...certificate verify failed:self-signed certificate...')`` plus
SQLAlchemy's "Background on this error" link — on the probe/connection-test
path via ``humanize_connection_error`` and inside a job via
``humanize_transfer_failure`` (where it can be wrapped in e.g.
FullRefreshDropFailed). Both must return the SQL Server cert copy naming
the 'Trust server certificate' connector option — never raw driver text.
"""

from services.error_handling import humanize_transfer_failure
from src.transfer.connector_registry import humanize_connection_error

_RAW_ODBC = (
    "(pyodbc.OperationalError) ('08001', '[08001] [Microsoft][ODBC Driver 18 "
    "for SQL Server]SSL Provider: [error:0A000086:SSL routines::certificate "
    "verify failed:self-signed certificate] (-1) (SQLDriverConnect)') "
    "(Background on this error at: https://sqlalche.me/e/20/e3q8)"
)

_MSSQL_TOKENS = ["mssql", "sqlserver", "azure_sql"]


def test_probe_path_humanizes_mssql_self_signed():
    for driver in _MSSQL_TOKENS:
        msg = humanize_connection_error(driver, _RAW_ODBC)
        assert "Trust server certificate" in msg, driver
        assert "self-signed" in msg, driver
        assert "08001" not in msg and "sqlalche.me" not in msg, msg


def test_probe_path_generic_driver_keeps_generic_cert_copy():
    msg = humanize_connection_error("postgres", "certificate verify failed")
    assert "TLS certificate could not be verified" in msg
    assert "Trust server certificate" not in msg


def test_job_path_humanizes_wrapped_cert_failure():
    wrapped = (
        "full_refresh could not clear destination table 'x_opts': " + _RAW_ODBC
    )
    h = humanize_transfer_failure(wrapped)
    assert h["code"] == "tls_certificate_untrusted"
    assert "Trust server certificate" in h["message"]
    assert "08001" not in h["message"] and "sqlalche.me" not in h["message"]
    assert "trust_server_certificate" in h["fix"]


def test_job_path_non_cert_failure_unaffected():
    h = humanize_transfer_failure("relation 'foo' does not exist")
    assert h.get("code") != "tls_certificate_untrusted"
