"""Regression tests for the Nov 6 and Nov 9 production QA defects.

Each case is the shared algorithm, not the one route in the note.
"""

from services.dial_target import displayed_dial, is_loopback_host, reconcile_dial_host
from services.job_status import cancel_blocks_status_write, cancel_status_write_filter


def test_atlas_uri_replaces_placeholder_localhost():
    cfg = {
        "host": "localhost",
        "connection_string": "mongodb+srv://user:p@ss@cluster0.ab.mongodb.net/app?retryWrites=true",
    }
    dial = reconcile_dial_host(cfg)
    assert dial == "cluster0.ab.mongodb.net"
    assert cfg["host"] == "cluster0.ab.mongodb.net"
    shown, loop = displayed_dial("localhost", cfg["connection_string"])
    assert shown == "cluster0.ab.mongodb.net"
    assert loop is False


def test_loopback_only_connector_stays_loopback():
    cfg = {"host": "localhost", "connection_string": "mongodb://localhost:27017/app"}
    assert reconcile_dial_host(cfg) == "localhost"
    assert is_loopback_host(cfg["host"])
    shown, loop = displayed_dial("localhost", "")
    assert shown == "localhost"
    assert loop is True


def test_blank_host_is_not_invented_as_localhost():
    cfg = {"host": "", "connection_string": ""}
    assert reconcile_dial_host(cfg) == ""
    assert cfg["host"] == ""


def test_cancel_guard_blocks_completed_and_allows_cancelled():
    guard = cancel_status_write_filter("completed")
    assert guard == {
        "cancel_requested": {"$ne": True},
        "status": {"$ne": "cancelled"},
    }
    assert cancel_blocks_status_write("running") is True
    assert cancel_status_write_filter("cancelled") is None
    assert cancel_status_write_filter("failed") is None


def test_mysql_decimal_18_6_is_a_physical_stamp_not_the_floor():
    from services.decision_kernel.type_invent import (
        _is_explicit_physical_stamp,
        materialize_dest_ddl,
    )

    assert _is_explicit_physical_stamp("DECIMAL(18,6)", "mysql") is True
    assert materialize_dest_ddl("mysql", "DECIMAL(18,6)", source_type="DECIMAL(18,6)") == "DECIMAL(18,6)"
    assert _is_explicit_physical_stamp("DECIMAL", "mysql") is False
    # BigQuery has no DECIMAL type name. (18,6) must rematerialize, not pass through.
    assert _is_explicit_physical_stamp("DECIMAL(18,6)", "bigquery") is False
    assert materialize_dest_ddl("bigquery", "DECIMAL(18,6)") == "BIGNUMERIC(18,6)"
