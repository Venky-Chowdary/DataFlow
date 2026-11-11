"""D148 — a millisecond narrowing passes unsigned only when every value fits.

BSON ``date`` and the Elasticsearch ``date`` field keep milliseconds. A
``DATETIME(6)`` / ``TIMESTAMPTZ(6)`` source was judged on its declaration plus a
preview sample: a column of whole milliseconds with no measured cell was a
"fidelity collapse" behind a signed contract, while a sub-millisecond value past
the sample passed Validate and only met the writer, which quarantined the row
(or, on Elasticsearch, indexed it truncated without a word). Owner decision:

* every value is checked — a SQL source answers at preflight with one aggregate
  over the read scope (source filter, incremental cursor), every source again at
  write time, batch by batch;
* all whole milliseconds: the run passes unsigned and the proof says what was
  checked; any finer value: the acknowledgement (a signed contract) is required,
  the message names the columns and the count, and nothing is truncated
  silently; with it, the proof counts the values truncated.

Live: MySQL / PostgreSQL / SQL Server sources, MongoDB destination (preflight
ON, strict). Elasticsearch through its writer with a fake client.
"""

from __future__ import annotations

import socket
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from tests import r13_mapping_harness as h
from tests.r13_mapping_harness import isolated  # noqa: F401 - fixture

pytestmark = pytest.mark.timeout(300)


def _up(port: int) -> bool:
    try:
        socket.create_connection(("127.0.0.1", port), timeout=1).close()
        return True
    except OSError:
        return False


def _mongo():
    pymongo = pytest.importorskip("pymongo")
    if not _up(27017):
        pytest.skip("MongoDB not reachable")
    return pymongo.MongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=3000)


def _dest(collection: str):
    from src.transfer.models import EndpointConfig

    return EndpointConfig(
        kind="database", format="mongodb", host="127.0.0.1", port=27017,
        database="dataflow_test", collection=collection,
    )


def _docs(client, collection: str) -> list[dict]:
    return list(client["dataflow_test"][collection].find({}, {"_id": 0}).sort("id", 1))


def _contract(column: str = "ts") -> dict:
    from services.migration_risk_contract import create_migration_risk_contract

    return create_migration_risk_contract(
        column=column,
        source_type="TIMESTAMPTZ(6)",
        destination_type="date",
        approved_by="risk-approver@example.com",
        reason="Accept millisecond truncation",
        execution_policy="CAST_AND_CONTINUE",
        expected_precision_loss=True,
    ).to_dict()


def _signed_mappings(extra: tuple[str, ...] = ()) -> list[dict]:
    return [
        {"source": "id", "target": "id", "confidence": 1.0},
        *({"source": c, "target": c, "confidence": 1.0} for c in extra),
        {"source": "ts", "target": "ts", "confidence": 1.0,
         "risk_acknowledged": True, "risk_contract": _contract()},
    ]


_COLDEF = {"mysql": "DATETIME(6)", "postgresql": "timestamptz(6)"}
# A zoneless MySQL DATETIME needs its zone declared (D136); PG timestamptz has one.
_ZONE = {"mysql": {"source_timezone": "UTC"}, "postgresql": {}}


def _whole_ms(i: int) -> str:
    return f"2024-01-02 03:04:{i % 60:02d}.{i % 1000:03d}000"


def _table(kind: str, rows: list[tuple], *, extra_cols: str = "") -> str:
    table = "d148_" + h.token()
    h.run_sql(
        kind,
        f"CREATE TABLE {h.quote(kind, table)} "
        f"(id INT PRIMARY KEY, ts {_COLDEF[kind]} NULL{extra_cols})",
    )
    marks = ", ".join(["%s"] * len(rows[0]))
    h.run_sql(kind, f"INSERT INTO {h.quote(kind, table)} VALUES ({marks})", params=rows)  # nosec B608
    return table


def _run(fake, kind: str, table: str, **kw):
    return h.execute(
        fake, h.endpoint(kind, table), _dest(table),
        sync_mode=kw.pop("sync_mode", "full_refresh_overwrite"), **{**_ZONE[kind], **kw},
    )


def _gate(result) -> dict:
    gates = [
        g for g in (result.validation_plan or {}).get("gates") or []
        if g.get("id") == "g3_millisecond_fit"
    ]
    return gates[0] if gates else {}


# ── preflight aggregate + proof ────────────────────────────────────────────


@pytest.mark.parametrize("kind", ["mysql", "postgresql"])
def test_whole_millisecond_column_passes_unsigned_and_the_proof_records_the_check(
    isolated, kind  # noqa: F811
):
    rows = [(i, _whole_ms(i)) for i in range(1, 1201)] + [(1201, None)]
    table = _table(kind, rows)
    client = _mongo()
    try:
        result = _run(isolated, kind, table)
        docs = _docs(client, table)
    finally:
        h.drop(kind, table)
        client["dataflow_test"][table].drop()
        client.close()
    assert result.success, result.error
    assert len(docs) == 1201
    # BSON dates read back naive in UTC.
    assert docs[0]["ts"].replace(tzinfo=timezone.utc) == datetime(
        2024, 1, 2, 3, 4, 1, 1000, tzinfo=timezone.utc
    )
    proof = result.reconciliation.get("millisecond_fit") or {}
    assert proof.get("method") == "source aggregate", result.reconciliation
    assert proof.get("checked_values") == 1200
    assert proof.get("truncated_values") == {}
    assert (
        "checked 1200 values in columns ts, all fit milliseconds (method: source aggregate)"
        in result.reconciliation["message"]
    )
    # Nothing was dropped, so the column is no longer called a declared narrowing.
    assert not result.reconciliation.get("carrier_rounded_columns")
    assert _gate(result).get("status") == "pass"


@pytest.mark.parametrize("kind", ["mysql", "postgresql"])
def test_one_sub_millisecond_value_past_the_sample_is_refused_at_preflight(
    isolated, kind  # noqa: F811
):
    rows = [(i, _whole_ms(i)) for i in range(1, 3001)]
    rows.append((3001, "2024-01-02 03:04:05.123456"))
    table = _table(kind, rows)
    client = _mongo()
    try:
        result = _run(isolated, kind, table)
        docs = _docs(client, table)
    finally:
        h.drop(kind, table)
        client["dataflow_test"][table].drop()
        client.close()
    assert not result.success
    assert "ts (1 value)" in result.error, result.error
    assert "checked 3001 values (source aggregate)" in result.error
    gate = _gate(result)
    assert gate.get("status") == "block"
    assert gate["details"]["sub_millisecond_values"] == {"ts": 1}
    assert docs == []


def test_acknowledged_narrowing_loads_and_the_proof_counts_the_truncation(isolated):  # noqa: F811
    rows = [(1, "2024-01-02 03:04:05.123000"), (2, "2024-01-02 03:04:05.123456"), (3, None)]
    table = _table("mysql", rows)
    client = _mongo()
    try:
        result = _run(
            isolated, "mysql", table,
            mappings=_signed_mappings(), triggered_by="risk-approver@example.com",
        )
        docs = _docs(client, table)
    finally:
        h.drop("mysql", table)
        client["dataflow_test"][table].drop()
        client.close()
    assert result.success, result.error
    assert [d.get("ts") and d["ts"].replace(tzinfo=timezone.utc) for d in docs] == [
        datetime(2024, 1, 2, 3, 4, 5, 123000, tzinfo=timezone.utc),
        datetime(2024, 1, 2, 3, 4, 5, 123000, tzinfo=timezone.utc),
        None,
    ]
    proof = result.reconciliation.get("millisecond_fit") or {}
    assert proof.get("truncated_values") == {"ts": 1}, result.reconciliation
    assert "ts: 1 value truncated to milliseconds under the accepted risk" in (
        result.reconciliation["message"]
    )
    # The truncated column stays named as compared at the carrier's granularity.
    assert [c["column"] for c in result.reconciliation.get("carrier_rounded_columns") or []] == [
        "ts"
    ]


def test_the_aggregate_counts_only_the_rows_the_source_filter_reads(isolated):  # noqa: F811
    rows = [
        (1, "2024-01-02 03:04:05.001000", "EU"),
        (2, "2024-01-02 03:04:05.002000", "EU"),
        (3, "2024-01-02 03:04:05.003000", "EU"),
        (4, "2024-01-02 03:04:05.123456", "US"),
    ]
    table = _table("mysql", rows, extra_cols=", region VARCHAR(8)")
    client = _mongo()
    try:
        unfiltered = _run(isolated, "mysql", table)
        client["dataflow_test"][table].drop()
        filtered = _run(
            isolated, "mysql", table,
            source_filter={"column": "region", "op": "eq", "value": "EU"},
        )
        docs = _docs(client, table)
    finally:
        h.drop("mysql", table)
        client["dataflow_test"][table].drop()
        client.close()
    assert not unfiltered.success
    assert "ts (1 value)" in unfiltered.error
    assert filtered.success, filtered.error
    assert [d["id"] for d in docs] == [1, 2, 3]
    proof = filtered.reconciliation.get("millisecond_fit") or {}
    assert proof.get("method") == "source aggregate"
    assert proof.get("checked_values") == 3


def test_incremental_second_run_bringing_a_sub_millisecond_value_is_refused(
    isolated,  # noqa: F811
):
    contracts = [{"name": "t", "cursor_field": "id", "primary_key": ["id"],
                  "cursor_semantics": "monotonic_sequence",
                  "sync_mode": "incremental_append", "selected": True}]
    table = _table("mysql", [(1, "2024-01-02 03:04:05.001000"), (2, "2024-01-02 03:04:06")])
    client = _mongo()

    def run():
        return _run(
            isolated, "mysql", table, sync_mode="incremental_append",
            stream_contracts=contracts,
        )

    try:
        first = run()
        h.run_sql("mysql", f"INSERT INTO `{table}` VALUES (3, '2024-01-02 03:04:07.123456')")  # nosec B608
        second = run()
        docs = _docs(client, table)
    finally:
        h.drop("mysql", table)
        client["dataflow_test"][table].drop()
        client.close()
    assert first.success, first.error
    assert (first.reconciliation.get("millisecond_fit") or {}).get("checked_values") == 2
    assert not second.success
    assert "ts (1 value)" in second.error, second.error
    assert [d["id"] for d in docs] == [1, 2]


def test_the_aggregate_counts_only_the_incremental_range():
    """The run reads past its watermark; a value behind it is not counted."""
    from services.millisecond_fit import source_sub_millisecond_counts
    from services.sync_cursor import IncrementalReadScope

    table = _table("mysql", [
        (1, "2024-01-02 03:04:05.123456"),
        (2, "2024-01-02 03:04:06.002000"),
        (3, "2024-01-02 03:04:07.003000"),
    ])
    try:
        whole = source_sub_millisecond_counts(h.endpoint("mysql", table), ["ts"])
        scoped = source_sub_millisecond_counts(
            h.endpoint("mysql", table), ["ts"],
            read_scope=IncrementalReadScope(cursor_column="id", watermark="2"),
        )
    finally:
        h.drop("mysql", table)
    assert (whole.checked, whole.sub_millisecond) == ({"ts": 3}, {"ts": 1})
    # ``>=`` keeps the tie row at the watermark: a superset never under-counts.
    assert (scoped.checked, scoped.sub_millisecond) == ({"ts": 2}, {"ts": 0})


# ── write-time guard ───────────────────────────────────────────────────────


def test_file_source_value_past_the_sample_is_refused_by_the_write_guard(isolated):  # noqa: F811
    """No pushdown for a file: preflight passes provisionally, the writer refuses."""
    from src.transfer.models import EndpointConfig

    bad_id = 1700
    lines = ["id,ts"]
    for i in range(1, 2001):
        frac = ".123456" if i == bad_id else f".{i % 1000:03d}"
        lines.append(f"{i},2024-01-02T03:04:{i % 60:02d}{frac}Z")
    collection = "d148_csv_" + h.token()
    client = _mongo()
    try:
        result = h.execute(
            isolated,
            EndpointConfig(kind="file", format="csv"),
            _dest(collection),
            source_filename="d148.csv",
            source_content=("\n".join(lines) + "\n").encode(),
            column_types={"id": "INTEGER", "ts": "TIMESTAMPTZ(6)"},
            sync_mode="full_refresh_overwrite",
        )
        docs = _docs(client, collection)
    finally:
        client["dataflow_test"][collection].drop()
        client.close()
    assert not result.success
    assert "Nothing from this batch was written" in result.error, result.error
    assert "ts (1 value)" in result.error
    assert not [d for d in docs if d.get("id") == bad_id]


def test_sqlserver_aggregate_counts_100ns_ticks():
    """DATETIME2(7) keeps 100 ns ticks; a tick below the millisecond counts."""
    from services.millisecond_fit import source_sub_millisecond_counts

    table = "d148_" + h.token()
    h.run_sql(
        "sqlserver",
        f"CREATE TABLE [{table}] (id INT PRIMARY KEY, ts DATETIME2(7) NULL)",
        f"INSERT INTO [{table}] VALUES (1, '2024-01-02 03:04:05.1230000'), "
        "(2, '2024-01-02 03:04:05.1230001'), (3, NULL)",
    )
    try:
        counts = source_sub_millisecond_counts(
            h.endpoint("sqlserver", table, schema="dbo"), ["ts"]
        )
    finally:
        h.drop("sqlserver", table)
    assert counts is not None
    assert counts.checked == {"ts": 2}
    assert counts.sub_millisecond == {"ts": 1}


# ── Elasticsearch (fake client) ────────────────────────────────────────────


def _es_write(rows, *, mappings=None, index_exists=False, live_mapping=None):
    from connectors.elasticsearch_writer import write_mapped_rows

    client = MagicMock()
    client.indices.exists.return_value = index_exists
    if live_mapping is not None:
        client.indices.get_mapping.return_value = {
            "idx": {"mappings": {"properties": live_mapping}}
        }
    captured: dict = {}

    def _bulk(_client, actions, **_kw):
        captured["actions"] = list(actions)
        return len(captured["actions"]), []

    with patch("connectors.elasticsearch_writer._client", return_value=client), patch(
        "elasticsearch.helpers.bulk", side_effect=_bulk
    ):
        result = write_mapped_rows(
            host="localhost", port=9200, database="", username="", password="",
            schema="", connection_string="", ssl=False, table_name="idx",
            headers=["id", "ts"], data_rows=[list(r) for r in rows],
            mappings=mappings or [
                {"source": "id", "target": "id", "source_type": "INTEGER"},
                {"source": "ts", "target": "ts", "source_type": "TIMESTAMPTZ(6)"},
            ],
            column_types={"id": "INTEGER", "ts": "TIMESTAMPTZ(6)"},
            error_policy="quarantine",
        )
    return result, captured, client


def test_es_date_refuses_the_batch_before_the_bulk():
    result, captured, _client = _es_write(
        [(1, "2024-01-02T03:04:05.123+00:00"), (2, "2024-01-02T03:04:05.123456+00:00")]
    )
    assert result.ok is False
    assert result.rows_written == 0
    assert "Elasticsearch date keeps milliseconds" in result.error
    assert "ts (1 value)" in result.error
    assert "actions" not in captured


def test_es_date_whole_milliseconds_pass_unsigned():
    result, captured, client = _es_write([(1, "2024-01-02T03:04:05.123+00:00")])
    assert result.ok is True, result.error
    assert len(captured["actions"]) == 1
    body = client.indices.create.call_args.kwargs["body"]
    assert body["mappings"]["properties"]["ts"]["type"] == "date"


def test_es_date_truncates_under_the_contract_and_counts_it():
    mappings = [
        {"source": "id", "target": "id", "source_type": "INTEGER"},
        {"source": "ts", "target": "ts", "source_type": "TIMESTAMPTZ(6)",
         "risk_acknowledged": True, "risk_contract": _contract()},
    ]
    result, captured, _client = _es_write(
        [(1, "2024-01-02T03:04:05.123456+00:00")], mappings=mappings
    )
    assert result.ok is True, result.error
    stored = captured["actions"][0]["_source"]["ts"]
    assert stored == datetime(2024, 1, 2, 3, 4, 5, 123000, tzinfo=timezone.utc)
    assert result.meta["ms_truncated_values"] == {"ts": 1}
    assert any("ts: 1 value with sub-millisecond digits truncated" in w for w in result.warnings)


def test_es_existing_date_nanos_field_does_not_narrow():
    result, captured, _client = _es_write(
        [(1, "2024-01-02T03:04:05.123456+00:00")],
        index_exists=True,
        live_mapping={"id": {"type": "long"}, "ts": {"type": "date_nanos"}},
    )
    assert result.ok is True, result.error
    assert len(captured["actions"]) == 1


# ── the preflight aggregate is bounded (never stalls the plan) ─────────────


@pytest.mark.parametrize("kind", ["postgresql", "mysql"])
def test_a_slow_aggregate_is_cancelled_by_the_database(kind, monkeypatch):
    """A full-column scan of a large source must not stall preflight
    (D321/D281). Past the bound the database cancels it; the gate's exception
    path then passes provisionally and the write guard checks every value."""
    import time

    from services import millisecond_fit

    monkeypatch.setattr(millisecond_fit, "aggregate_timeout_seconds", lambda: 0.3)
    table = _table(kind, [(i, _whole_ms(i)) for i in range(1, 6)])
    view = "d148v_" + h.token()
    sleep = "pg_sleep(0.5)" if kind == "postgresql" else "SLEEP(0.5)"
    h.run_sql(
        kind,
        f"CREATE VIEW {h.quote(kind, view)} AS SELECT id, ts FROM {h.quote(kind, table)} "  # nosec B608
        f"WHERE {sleep} IS NOT NULL" if kind == "postgresql"
        else f"CREATE VIEW {h.quote(kind, view)} AS SELECT id, ts FROM {h.quote(kind, table)} "  # nosec B608
        f"WHERE {sleep} = 0",
    )
    try:
        started = time.monotonic()
        with pytest.raises(Exception):
            millisecond_fit.source_sub_millisecond_counts(h.endpoint(kind, view), ["ts"])
        # Unbounded, five rows at 0.5 s each would take 2.5 s.
        assert time.monotonic() - started < 2.0
    finally:
        h.run_sql(kind, f"DROP VIEW IF EXISTS {h.quote(kind, view)}")
        h.drop(kind, table)


def test_the_mysql_session_bound_is_restored_on_the_pooled_connection(monkeypatch):
    from services import millisecond_fit
    from services.dest_precount import warehouse_sql_engine
    from src.transfer.adapters import resolve_connector_config

    monkeypatch.setattr(millisecond_fit, "aggregate_timeout_seconds", lambda: 0.25)
    cfg = resolve_connector_config(h.endpoint("mysql", "unused"))
    with warehouse_sql_engine("mysql", cfg) as engine, engine.connect() as conn:
        before = conn.exec_driver_sql("SELECT @@SESSION.max_execution_time").scalar()
        with millisecond_fit._bound_aggregate(conn, "mysql"):
            inside = conn.exec_driver_sql("SELECT @@SESSION.max_execution_time").scalar()
        after = conn.exec_driver_sql("SELECT @@SESSION.max_execution_time").scalar()
    assert inside == 250
    assert after == before
