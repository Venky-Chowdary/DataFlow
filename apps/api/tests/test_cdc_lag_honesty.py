"""CDC lag honesty — heartbeat must not greenwash Freshness SLO."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from services.cdc_lag_honesty import (
    BYTE_CRITICAL,
    BYTE_WARN,
    LAG_BASIS_COMMIT_TS,
    LAG_BASIS_UNKNOWN,
    LAG_BASIS_WAL_BYTES,
    observe_cdc_lag,
)


def test_heartbeat_alone_does_not_zero_lag():
    now = datetime.now(timezone.utc)
    obs = observe_cdc_lag(
        last_event_commit_at=None,
        last_heartbeat_at=now,
        replication_lag_bytes=BYTE_CRITICAL + 1,
        now=now,
    )
    assert obs["cdc_lag_seconds"] is None
    assert obs["cdc_lag_basis"] == LAG_BASIS_WAL_BYTES
    assert obs["freshness_severity"] == "critical"
    assert obs["cdc_heartbeat_age_sec"] is not None
    assert obs["cdc_heartbeat_age_sec"] < 1.0


def test_wal_catch_up_is_zero_seconds_even_if_last_event_old():
    now = datetime.now(timezone.utc)
    old = now - timedelta(hours=2)
    obs = observe_cdc_lag(
        last_event_commit_at=old,
        last_heartbeat_at=now,
        replication_lag_bytes=4096,  # well inside catch-up band
        now=now,
    )
    assert obs["cdc_lag_seconds"] == 0.0
    assert obs["cdc_lag_basis"] == LAG_BASIS_WAL_BYTES
    assert obs["freshness_severity"] == "ok"
    assert obs["caught_up"] is True


def test_commit_timestamp_lag_when_behind_bytes():
    now = datetime.now(timezone.utc)
    commit = now - timedelta(seconds=120)
    obs = observe_cdc_lag(
        last_event_commit_at=commit,
        last_heartbeat_at=now,
        replication_lag_bytes=BYTE_WARN + 1,
        now=now,
        max_lag_warn_seconds=60.0,
    )
    assert obs["cdc_lag_basis"] == LAG_BASIS_COMMIT_TS
    assert obs["cdc_lag_seconds"] is not None
    assert 110 <= obs["cdc_lag_seconds"] <= 130
    assert obs["freshness_severity"] in {"warn", "critical"}


def test_unknown_without_anchors():
    obs = observe_cdc_lag(
        last_event_commit_at=None,
        last_heartbeat_at=datetime.now(timezone.utc),
        replication_lag_bytes=None,
    )
    assert obs["cdc_lag_seconds"] is None
    assert obs["cdc_lag_basis"] == LAG_BASIS_UNKNOWN
    assert obs["freshness_severity"] == "unknown"


def test_pg_reader_lag_ignores_heartbeat(monkeypatch):
    from connectors.postgresql_change_stream import PostgreSqlChangeStreamCdc

    cdc = object.__new__(PostgreSqlChangeStreamCdc)
    cdc._last_event_at = None
    cdc._last_event_commit_at = None
    cdc._last_heartbeat_at = datetime.now(timezone.utc)
    cdc._lag_observation = None
    monkeypatch.setattr(cdc, "replication_lag_bytes", lambda: BYTE_CRITICAL + 10)
    assert cdc.replication_lag_seconds() is None
    assert cdc._lag_observation["freshness_severity"] == "critical"


def test_mysql_commit_ts_drives_lag(monkeypatch):
    from connectors.mysql_change_stream import MySqlChangeStreamCdc

    cdc = object.__new__(MySqlChangeStreamCdc)
    now = datetime.now(timezone.utc)
    cdc._last_event_commit_at = now - timedelta(seconds=45)
    cdc._last_event_at = cdc._last_event_commit_at
    cdc._last_heartbeat_at = now
    cdc._lag_observation = None
    monkeypatch.setattr(cdc, "replication_lag_bytes", lambda: None)
    lag = cdc.replication_lag_seconds()
    assert lag is not None
    assert 40 <= lag <= 50


def test_freshness_summary_byte_lag_warns_without_seconds(monkeypatch):
    from services import ops_metrics

    ops_metrics._labeled_gauges["dataflow_pipeline_lag_seconds"] = {}
    ops_metrics._labeled_gauges["dataflow_pipeline_lag_bytes"] = {}
    ops_metrics._pipeline_heartbeat.clear()

    ops_metrics.record_cdc_poll(
        lag_seconds=None,
        lag_bytes=BYTE_WARN + 1000,
        lag_basis="wal_bytes",
        job_id="j1",
        stream="orders",
    )
    summary = ops_metrics.freshness_summary(max_lag_warn_seconds=60.0)
    assert summary["slo_status"] in {"warn", "critical"}
    assert summary["pipelines"]
    assert summary["pipelines"][0]["lag_seconds"] is None
    assert summary["pipelines"][0]["lag_bytes"] is not None
    assert any("WAL" in (a.get("title") or "") or "binlog" in (a.get("title") or "").lower()
               or a.get("severity") in {"warn", "critical"}
               for a in summary["alerts"])


def test_cdc_lag_fields_promotes_basis():
    from src.transfer.cdc_transfer import _cdc_lag_fields
    from src.transfer.engine import _CDC_JOB_FIELDS

    class _Fake:
        _last_event_commit_at = None
        _last_heartbeat_at = datetime.now(timezone.utc)

        def replication_lag_bytes(self):
            return 2048

        def replication_lag_seconds(self):
            from services.cdc_lag_honesty import observe_cdc_lag

            return observe_cdc_lag(
                replication_lag_bytes=2048,
                last_heartbeat_at=self._last_heartbeat_at,
            ).get("cdc_lag_seconds")

    fields = _cdc_lag_fields(_Fake())
    assert fields["cdc_lag_seconds"] == 0.0
    assert fields["cdc_lag_basis"] == LAG_BASIS_WAL_BYTES
    assert "cdc_lag_basis" in _CDC_JOB_FIELDS
    assert "cdc_heartbeat_age_sec" in _CDC_JOB_FIELDS


def test_job_trust_does_not_score_100_on_missing_lag():
    from services.job_trust import compute_job_trust

    trust = compute_job_trust(
        {
            "status": "running",
            "records_processed": 100,
            "rejected_rows": 0,
            # heartbeat-looking fields without proven lag
            "cdc_heartbeat_at": datetime.now(timezone.utc).isoformat(),
        }
    )
    fresh = next(f for f in trust["factors"] if f["id"] == "freshness")
    assert fresh.get("present") is False
    assert fresh.get("score") is None
