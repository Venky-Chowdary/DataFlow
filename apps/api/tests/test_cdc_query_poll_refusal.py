"""CDC without a readable change log must not run as a primary-key poll.

DEF-B2-006: MariaDB and SQL Server CDC passed Validate with no binlog and no
capture instance. The run fell back to polling the primary key, which carries
the snapshot and new inserts only, so updates and deletes were lost while the
job reported completed.
"""

from __future__ import annotations

import pytest

from services import cdc_log_capture_probe as probe_mod
from services.cdc_capability import (
    CAUSE_PRIVILEGE,
    CAUSE_SERVER_NOT_CONFIGURED,
    classify_log_capture_failure,
    query_cdc_change_refusal,
)
from services.cdc_log_capture_probe import LogCaptureProbe, build_log_capture_gate


def _gate(probe: LogCaptureProbe, **kwargs):
    return build_log_capture_gate(
        probe,
        cursor_field=kwargs.get("cursor_field", ""),
        cursor_semantics=kwargs.get("cursor_semantics", ""),
        primary_key_columns=kwargs.get("keys", ["id"]),
        pass_status="pass",
        block_status="block",
    )


@pytest.mark.parametrize(
    ("cursor", "semantics", "refused"),
    [
        ("id", "", True),
        ("id", "cdc_position", True),
        ("id", "monotonic_sequence", True),
        ("created_at", "", True),
        ("updated_at", "modification_timestamp", False),
        ("id", "insert_only", False),
    ],
)
def test_a_poll_is_allowed_only_when_it_still_carries_updates(
    cursor: str, semantics: str, refused: bool
) -> None:
    text = query_cdc_change_refusal(
        dialect="mariadb",
        cursor_field=cursor,
        cursor_semantics=semantics,
        primary_key_columns=["id"],
        downgrade_cause=CAUSE_SERVER_NOT_CONFIGURED,
    )
    assert bool(text) is refused
    if refused:
        assert "updates and deletes made after the snapshot would be lost" in text


def test_validate_blocks_mariadb_cdc_without_a_binlog() -> None:
    gate = _gate(
        LogCaptureProbe("mariadb", False, "binlog", CAUSE_SERVER_NOT_CONFIGURED, "log_bin=OFF")
    )
    assert gate["status"] == "block"
    assert "primary key" in gate["message"]


def test_validate_blocks_a_grant_failure_instead_of_degrading() -> None:
    gate = _gate(LogCaptureProbe("mysql", False, "binlog", CAUSE_PRIVILEGE, "Access denied"))
    assert gate["status"] == "block"
    assert "refusing to fall back to query CDC" in gate["message"]


def test_validate_blocks_a_declared_timestamp_poll_that_drops_deletes() -> None:
    gate = _gate(
        LogCaptureProbe("sqlserver", False, "", CAUSE_SERVER_NOT_CONFIGURED, ""),
        cursor_field="updated_at",
        cursor_semantics="modification_timestamp",
    )
    assert gate["status"] == "block"
    assert "does not emit a change log" in gate["message"]
    assert "CDC cannot start" in gate["message"]
    assert gate["details"]["cdc_delete_capture"] is False


def test_unknown_source_blocks_and_postgres_stays_on_the_slot_probe() -> None:
    sqlite = probe_mod.probe_log_capture(
        "sqlite", {"database": ":memory:"}, table="orders", primary_key="id"
    )
    assert sqlite.available is False
    gate = _gate(
        sqlite,
        cursor_field="updated_at",
        cursor_semantics="modification_timestamp",
    )
    assert gate is not None
    assert gate["status"] == "block"
    assert "does not emit a change log" in gate["message"]
    postgres = probe_mod.probe_log_capture(
        "postgresql", {"host": "h"}, table="orders", primary_key="id"
    )
    assert postgres.available is None
    assert postgres.dialect == "postgresql"
    assert _gate(postgres) is None
    assert probe_mod.probe_log_capture("", {}, table="orders", primary_key="id").available is None


def test_readable_log_passes_and_an_undecided_probe_adds_no_gate() -> None:
    assert _gate(LogCaptureProbe("sqlserver", True, "sqlserver_native"))["status"] == "pass"
    assert _gate(LogCaptureProbe("sqlserver", None)) is None


def test_sqlserver_without_cdc_or_tracking_is_server_not_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(probe_mod, "_try_reader", lambda cls, cfg, **kw: (False, None))
    probe = probe_mod.probe_log_capture(
        "mssql", {"host": "h"}, table="orders", schema="dbo", primary_key="id"
    )
    assert probe.available is False
    assert probe.cause == CAUSE_SERVER_NOT_CONFIGURED
    assert _gate(probe)["status"] == "block"


def test_unreachable_sqlserver_probe_does_not_block(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        probe_mod, "_try_reader", lambda cls, cfg, **kw: (None, RuntimeError("timeout"))
    )
    probe = probe_mod.probe_log_capture(
        "sqlserver", {"host": "h"}, table="orders", primary_key="id"
    )
    assert probe.available is None


def test_runtime_fallback_messages_classify_as_server_not_configured() -> None:
    sqlserver = classify_log_capture_failure(
        "sqlserver",
        "SQL Server change data capture is not enabled for dbo.orders and Change Tracking is off",
    )
    oracle = classify_log_capture_failure(
        "oracle",
        "Oracle LogMiner and flashback are not available: supplemental logging "
        "is not enabled for APP.ORDERS",
    )
    assert sqlserver.cause == CAUSE_SERVER_NOT_CONFIGURED
    assert oracle.cause == CAUSE_SERVER_NOT_CONFIGURED


def test_preflight_policy_gates_include_g9c_for_mariadb(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from services import preflight_service

    monkeypatch.setattr(
        probe_mod,
        "probe_log_capture",
        lambda *a, **kw: LogCaptureProbe(
            "mariadb", False, "binlog", CAUSE_SERVER_NOT_CONFIGURED, "log_bin=OFF"
        ),
    )
    monkeypatch.setattr(
        "services.cdc_slot_resume.probe_postgres_slot_for_preflight",
        lambda *a, **kw: None,
    )
    gates = preflight_service.run_transfer_policy_gates(
        sync_mode="cdc",
        stream_contracts=[{"name": "orders", "primary_key": "id", "cursor_semantics": "cdc_position"}],
        source_type="mysql",
        source_kind="database",
        dest_type="postgresql",
        source_table="orders",
        source_config={"type": "mariadb", "host": "h", "database": "shop"},
        source_columns=["id", "qty"],
    )
    g9c = [g for g in gates if g["id"] == "g9c_cdc_log_capture"]
    assert len(g9c) == 1 and g9c[0]["status"] == "block"
