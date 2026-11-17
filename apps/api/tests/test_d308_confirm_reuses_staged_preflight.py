"""D308 — Confirm reuses the staged preflight unless an input changed.

The second "Validating mapping and schema" pass was a full catalog walk.
Same inputs return the staged result and do not dial. A different sync mode
does not reuse it.
"""

from __future__ import annotations

from src.transfer.models import EndpointConfig, TransferRequest


def _request(mode: str = "full_refresh_overwrite") -> TransferRequest:
    return TransferRequest(
        source=EndpointConfig(
            kind="database", format="mysql", connector_id="src-1", table="orders",
        ),
        destination=EndpointConfig(
            kind="database", format="postgresql", connector_id="dst-1", table="orders",
        ),
        mappings=[{"source": "id", "target": "id"}, {"source": "qty", "target": "qty"}],
        sync_mode=mode,
        schema_policy="manual_review",
        validation_mode="strict",
        source_filter={"column": "qty", "operator": "lt", "value": 10},
        preflight_run_id="pf_d308",
    )


def test_unchanged_inputs_reuse_the_staged_result_and_do_not_dial(monkeypatch):
    import connectors.mysql_conn as mysql_conn
    from services.preflight_service import run_file_preflight
    from services.staged_preflight import (
        bind_staged_preflight,
        remember_staged_preflight,
        request_preflight_fingerprint,
    )

    def _dial(*_a, **_k):
        raise AssertionError("Confirm dialed MySQL for an unchanged preflight")

    monkeypatch.setattr(mysql_conn, "dial_pymysql", _dial)
    request = _request()
    fingerprint = request_preflight_fingerprint(request)
    remember_staged_preflight(
        "pf_d308",
        fingerprint,
        {"passed": True, "gates": [{"id": "g1", "status": "pass"}], "blockers": []},
    )
    with bind_staged_preflight("pf_d308", fingerprint):
        out = run_file_preflight(
            columns=["id", "qty"],
            column_types={"id": "int", "qty": "int"},
            row_count=30,
            mappings=request.mappings,
            source_kind="database",
            source_format="mysql",
            source_connector_id="src-1",
            source_table="orders",
            source_config={"host": "db.example", "port": 3306, "type": "mysql"},
            destination_db_type="postgresql",
            destination_table="orders",
            sync_mode=request.sync_mode,
            validation_mode="strict",
        )
    assert out["reused_staged_preflight"] is True
    assert out["passed"] is True
    assert out["gates"][0]["id"] == "g1"


def test_a_changed_sync_mode_does_not_reuse_the_staged_result():
    from services.preflight_service import run_file_preflight
    from services.staged_preflight import (
        bind_staged_preflight,
        remember_staged_preflight,
        request_preflight_fingerprint,
    )

    staged = _request()
    remember_staged_preflight(
        "pf_d308",
        request_preflight_fingerprint(staged),
        {"passed": True, "gates": [{"id": "staged"}], "blockers": [], "reused_marker": True},
    )
    changed = _request("incremental_append")
    with bind_staged_preflight("pf_d308", request_preflight_fingerprint(changed)):
        out = run_file_preflight(
            columns=["id"],
            column_types={"id": "int"},
            row_count=1,
            mappings=[{"source": "id", "target": "id"}],
            source_kind="file",
            source_format="csv",
            sync_mode="incremental_append",
            destination_db_type="postgresql",
            destination_connected=False,
        )
    assert out.get("reused_staged_preflight") is not True
    assert out.get("reused_marker") is not True


def test_destination_extra_ack_and_locale_are_part_of_the_fingerprint():
    from services.staged_preflight import request_preflight_fingerprint

    base = _request()
    same = request_preflight_fingerprint(base)
    changed_extra = _request()
    changed_extra.destination.extra = {"foreign_keys_mode": "defer"}
    changed_ack = _request()
    changed_ack.compliance_acknowledged = True
    changed_locale = _request()
    changed_locale.date_locale = "DMY"
    changed_policy = _request()
    changed_policy.duplicate_policy = "append"
    changed_backfill = _request()
    changed_backfill.backfill_new_fields = True
    changed_reversible = _request()
    changed_reversible.require_reversibility = True
    assert request_preflight_fingerprint(changed_extra) != same
    assert request_preflight_fingerprint(changed_ack) != same
    assert request_preflight_fingerprint(changed_locale) != same
    assert request_preflight_fingerprint(changed_policy) != same
    assert request_preflight_fingerprint(changed_backfill) != same
    assert request_preflight_fingerprint(changed_reversible) != same
    assert "plan_version" not in same  # the hash, not the payload


def test_a_staged_result_older_than_ten_minutes_is_not_reused(monkeypatch):
    import services.staged_preflight as staged
    from services.staged_preflight import (
        bind_staged_preflight,
        remember_staged_preflight,
        request_preflight_fingerprint,
        staged_preflight_for_bound,
    )

    clock = {"t": 1_000_000.0}
    monkeypatch.setattr(staged.time, "time", lambda: clock["t"])
    request = _request()
    fingerprint = request_preflight_fingerprint(request)
    remember_staged_preflight("pf_old", fingerprint, {"passed": True, "gates": []})
    clock["t"] += 601
    with bind_staged_preflight("pf_old", fingerprint):
        assert staged_preflight_for_bound() is None


def test_a_changed_destination_schema_version_is_not_reused():
    from services.staged_preflight import (
        bind_staged_preflight,
        remember_staged_preflight,
        request_preflight_fingerprint,
        staged_preflight_for_bound,
    )

    request = _request()
    fingerprint = request_preflight_fingerprint(request)
    remember_staged_preflight(
        "pf_schema", fingerprint, {"passed": True, "gates": []}, schema_version="v1"
    )
    with bind_staged_preflight("pf_schema", fingerprint, "v1"):
        assert staged_preflight_for_bound()["reused_staged_preflight"] is True
    with bind_staged_preflight("pf_schema", fingerprint, "v2"):
        assert staged_preflight_for_bound() is None
    with bind_staged_preflight("pf_schema", fingerprint, "unread"):
        assert staged_preflight_for_bound() is None
