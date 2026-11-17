"""Round-5 review: connection-string values, DNS, and a long Oracle key bind."""

from __future__ import annotations


def test_odbc_connect_with_a_driver_path_is_refused_at_save():
    from services.connector_store import server_side_dsn_refusal

    refused = server_side_dsn_refusal(
        {"extra": {"odbc_connect": "Driver={/tmp/x.so};Server=db;PWD={secret}"}}
    )
    assert "driver" in refused


def test_a_quoted_password_may_contain_service_equals():
    from services.connector_store import server_side_dsn_refusal

    assert server_side_dsn_refusal("password='service=prod' host=db") == ""
    assert server_side_dsn_refusal("PWD={service=prod};Server=db") == ""
    assert server_side_dsn_refusal(
        {"extra": {"odbc_connect": "Driver={ODBC Driver 18 for SQL Server};PWD={service=prod}"}}
    ) == ""
    refused = server_side_dsn_refusal("host=db service=prod")
    assert "service" in refused


def test_a_dead_dns_name_is_not_a_connect_timeout():
    from services.error_taxonomy import classify

    exc = OSError("nodename nor servname provided, or not known: no-such-host.example")
    classified = classify(exc)
    assert classified is not None
    assert classified.code == "network.dns_not_found"

    wrapped = Exception(
        "(20009, 'DB-Lib error message 20009, severity 9:\\n"
        "Unable to connect: Adaptive Server is unavailable or does not exist "
        "(no-such-host.example)\\nNet-Lib error during getaddrinfo (name or service not known)\\n')"
    )
    classified = classify(wrapped)
    assert classified is not None
    assert classified.code == "network.dns_not_found"


def test_a_40k_oracle_key_is_bound_in_varchar2_pieces():
    from services.reconciliation_oracle import _ORACLE_BIND_CHARS, _oracle_clob_equals

    key = "é" * 40_000
    clause, bound = _oracle_clob_equals('"NOTES"', 0, key)
    assert "TO_CLOB(" in clause
    assert bound
    assert max(len(value) for value in bound.values()) <= _ORACLE_BIND_CHARS
    assert "".join(bound[name] for name in bound) == key
    try:
        raise RuntimeError("ORA-01460: unimplemented or unreasonable conversion requested")
    except RuntimeError as exc:
        assert "01460" in str(exc)
