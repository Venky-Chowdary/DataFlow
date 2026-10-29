"""Round-4 temporal family, end to end on real engines (D190, D65, D52, D148, M6, D59).

One canonical temporal model: era (BC), zone, precision, the DST gap / fold
and naive-vs-instant are decided by the same converter for Validate, the
writers and Gate-8. Every test drives QA's route - MCP ``start_transfer`` ->
``/api/v1/copilot/confirm`` -> job (or ``start_dataset_transfer`` for an
upload) - against PostgreSQL, MySQL, SQL Server, SQLite and MongoDB. No
Elasticsearch runs on this host, so the ES legs drive DataFlow's real
planner / writer / reader through an in-memory transport
(``_r4_temporal_harness.FakeElasticsearch``).

* D190 - ``source_timezone`` re-typed a text column of digits ('00123',
  '20240101', '1700000000') into TIMESTAMPTZ: ``parse_sql_datetime`` reads digit
  runs as epoch seconds, and the SQLite text-carrier branch of
  ``declare_zone_on_schema`` used it as wall-clock evidence on every engine.
* D65 - psycopg2 cannot decode a BC date, so the source *read* crashed with
  "year -44 is out of range" and Validate blamed the connection.
* D52 - ``to_timestamp`` kept its plain parse under ``source_timezone``: the
  zone never applied, all rows failed the instant carrier with a generic
  "1 type coercion issue(s)". Temporal refusals also classified UNKNOWN.
* D148 - the gate's own remedy (``assume_timezone:UTC`` as a column transform)
  was stamped after Map graded the zoneless pair lossy, so G4 still held it.
* M6 - an offset-bearing SQLite text timestamp passed Validate into a
  PostgreSQL TIMESTAMP and was refused at write: Validate and the writer used
  different rules.
* D59 - the float bind dropped the engine, so SQLite's 8-byte REAL was checked
  as a 4-byte float and 5e-324 / DBL_MAX were refused after a green preflight.
"""

from __future__ import annotations

import sqlite3
from datetime import date, datetime, timezone

import pytest

from tests._r4_temporal_harness import (  # noqa: F401 - fixtures
    confirm,
    connector,
    connector_name,
    engine_mappings,
    fake_es,
    mcp,
    mongo_db,
    mssql_exec,
    mysql_exec,
    pg_exec,
    run,
    stage,
    staged_summary,
    tag,
    transfer_client,
    wait_job,
)

UTC = timezone.utc
_QUARANTINE = {
    "approved_by": "qa@example.com",
    "reason": "hold out the cells the carrier cannot store",
    "execution_policy": "QUARANTINE_ROW",
}


def _dest(client, kind: str, index: str = "") -> str:
    if kind == "sqlite":
        return connector("sqlite", "destination", database=str(client.sqlite_root / "dest.db"))
    if kind == "elasticsearch":
        return connector("elasticsearch", "destination", database=index)
    return connector(kind, "destination")


def _rows(client, kind: str, table: str, cols: str, store=None) -> list[tuple]:
    if kind == "postgresql":
        return pg_exec(f'SELECT {cols} FROM public."{table}" ORDER BY 1')
    if kind == "mysql":
        return mysql_exec(f"SELECT {cols.replace(chr(34), '`')} FROM `{table}` ORDER BY 1")
    if kind == "sqlserver":
        return mssql_exec(f"SELECT {cols.replace(chr(34), '')} FROM dbo.[{table}] ORDER BY 1")
    if kind == "sqlite":
        with sqlite3.connect(client.sqlite_root / "dest.db") as conn:
            return conn.execute(f'SELECT {cols} FROM "{table}" ORDER BY 1').fetchall()
    if kind == "elasticsearch":
        names = [c.strip().strip('"') for c in cols.split(",")]
        docs = (store or {}).get(table, {}).get("docs", {})
        return sorted(tuple(d.get(n) for n in names) for d in docs.values())
    raise AssertionError(kind)


def _drop(kind: str, table: str) -> None:
    try:
        if kind == "postgresql":
            pg_exec(f'DROP TABLE IF EXISTS public."{table}"')
        elif kind == "mysql":
            mysql_exec(f"DROP TABLE IF EXISTS `{table}`")
        elif kind == "sqlserver":
            mssql_exec(f"DROP TABLE IF EXISTS dbo.[{table}]")
    except Exception:
        pass


def _headline(staged) -> str:
    return str(staged_summary(staged)["error"] or "")


def _sample_transform_classes(staged) -> str:
    import re

    found = re.search(r"classes: ([A-Z_, ]+)", _headline(staged))
    return found.group(1) if found else ""


# ===================================================================== D190

# Every non-empty ``code`` is a digit run, as in QA's column: one non-digit
# value would have hidden the defect (the old rule needed every sample to
# parse). Unicode rides in ``label``.
_DIGITS = [
    (1, "00123", "Zoë ✓", "2024-01-15 10:30:00"),
    (2, "20240101", "", "2024-11-03 01:30:00.123456"),
    (3, "1700000000", None, None),
    (4, "", " ", "2024-06-01 00:00:00"),
    (5, None, "x", "2024-06-02 00:00:00"),
    (6, "0042", "y", "2024-06-03 00:00:00"),
]


@pytest.mark.parametrize("dest", ["postgresql", "mysql", "sqlite", "elasticsearch"])
def test_d190_text_digits_stay_text_under_source_timezone(transfer_client, fake_es, dest):
    """source_timezone=UTC: the text column arrives byte for byte; only the
    real TIMESTAMP column takes the zone."""
    src_table, out = tag("d190s"), tag("d190d")
    pg_exec(f'CREATE TABLE public."{src_table}" (id int primary key, code text, label text, ts timestamp(6))')
    for row in _DIGITS:
        pg_exec(f'INSERT INTO public."{src_table}" VALUES (%s, %s, %s, %s)', row)
    try:
        staged = stage(
            source_connector_id=connector("postgresql", "source"), source_table=src_table,
            dest_connector_id=_dest(transfer_client, dest, out), dest_table=out,
            sync_mode="full_refresh_overwrite", source_timezone="UTC",
            # ES ``date`` keeps milliseconds; row 2 has real microseconds, so the
            # truncation is accepted explicitly there (D148 covers that gate).
            risk_acceptance=(
                {"approved_by": "qa@example.com", "reason": "ms instants",
                 "execution_policy": "CAST_AND_CONTINUE"}
                if dest == "elasticsearch" else None
            ),
        )
        by_source = {m["source"]: m for m in engine_mappings(staged)} if staged.success else {}
        assert staged.success, staged_summary(staged)
        assert not str(by_source["code"].get("transform") or "").startswith("assume_timezone"), by_source["code"]
        assert str(by_source["ts"].get("transform") or "") == "assume_timezone:UTC", by_source["ts"]
        job = confirm(transfer_client, staged)
        assert str(job.get("status")) == "completed", job.get("error")
        got = _rows(transfer_client, dest, out, '"id", "code", "label"', fake_es)
        assert got == [(r[0], r[1], r[2]) for r in _DIGITS]
        if dest == "postgresql":
            types = dict(pg_exec(
                "SELECT column_name, data_type FROM information_schema.columns WHERE table_name=%s",
                (out,),
            ))
            assert types["code"] == "text" and types["ts"] == "timestamp with time zone", types
            ts = dict(_rows(transfer_client, dest, out, '"id", "ts"'))
            assert ts[1] == datetime(2024, 1, 15, 10, 30, tzinfo=UTC)
            assert ts[2] == datetime(2024, 11, 3, 1, 30, 0, 123456, tzinfo=UTC)
            assert ts[3] is None
    finally:
        pg_exec(f'DROP TABLE IF EXISTS public."{src_table}"')
        _drop(dest, out)


def test_d190_kafka_int_key_and_digit_text_arrive_unchanged(transfer_client):
    """Kafka -> PG with source_timezone=UTC: the message key and '00123' are
    not re-typed (QA saw an INTEGER _kafka_key turn temporal)."""
    from tests._r4_temporal_harness import up

    if not up("127.0.0.1", 9092):
        pytest.skip("Kafka 127.0.0.1:9092 not reachable")
    pytest.importorskip("kafka")
    from connectors.kafka_writer import _producer
    from services.connector_store import create_connector
    from services.copy_kafka_common import kafka_admin

    topic = tag("d190k")
    kafka = {"host": "127.0.0.1", "port": 9092}
    messages = [
        (b"1", {"id": 1, "code": "00123"}),
        (b"2", {"id": 2, "code": "1700000000"}),
        (b"3", {"id": 3, "code": ""}),
    ]
    producer = _producer(kafka)
    try:
        for key, value in messages:
            producer.send(topic, key=key, value=value)
        producer.flush(10)
    finally:
        producer.close()
    source = str(create_connector({
        "name": tag("d190-kafka"), "type": "kafka", "role": "source", "host": "127.0.0.1",
        "port": 9092, "database": "", "ssl": False, "workspace_id": "",
        "extra": {"group_id": f"g_{topic}"},
    }).id)
    out = topic + "_out"
    try:
        run(transfer_client, source_connector_id=source, source_table=topic,
            dest_connector_id=connector("postgresql", "destination"), dest_table=out,
            sync_mode="full_refresh_overwrite", source_timezone="UTC")
        got = pg_exec(f'SELECT "_kafka_key", "id", "code" FROM public."{out}" ORDER BY "id"')
        assert [(str(k), int(i), c) for k, i, c in got] == [
            ("1", 1, "00123"), ("2", 2, "1700000000"), ("3", 3, ""),
        ]
        types = dict(pg_exec(
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_name=%s",
            (out,),
        ))
        assert "time" not in types["_kafka_key"] and types["code"] == "text", types
    finally:
        _drop("postgresql", out)
        admin = kafka_admin(kafka)
        try:
            admin.delete_topics([topic])
        except Exception:
            pass
        finally:
            admin.close()


def test_d190_csv_digit_text_stays_text_under_source_timezone(transfer_client):
    """CSV -> PG upload with source_timezone=UTC: '00123' stays text. (Guard: the
    upload route did not share the defect.)"""
    name = tag("d190csv")
    (transfer_client.uploads / f"{name}.csv").write_text(
        "id,ref,created_at\n1,00123,2024-01-15 10:30:00\n2,20240101,2024-11-03 01:30:00\n"
        "3,1700000000,\n4,0042,2024-06-01 00:00:00.5\n",
        encoding="utf-8",
    )
    dest = connector("postgresql", "destination")
    try:
        failed, staged = mcp(transfer_client, "start_dataset_transfer", {
            "dataset_name": name, "dest_connector_name": connector_name(dest),
            "dest_table": name, "sync_mode": "full_refresh_overwrite", "source_timezone": "UTC",
        })
        assert failed is False, staged
        failed, confirmed = mcp(transfer_client, "confirm_action", {"ack_id": staged["ack_id"], "reason": "r4"})
        assert failed is False, confirmed
        job = wait_job(confirmed["job_id"])
        assert str(job.get("status")) == "completed", job.get("error")
        assert [r[0] for r in pg_exec(f'SELECT "ref" FROM public."{name}" ORDER BY "id"')] == [
            "00123", "20240101", "1700000000", "0042",
        ]
    finally:
        _drop("postgresql", name)


# ====================================================================== D65

def _bc_source() -> str:
    table = tag("d65s")
    pg_exec(f'CREATE TABLE public."{table}" (id int primary key, d date, ts timestamp(6), note text)')
    pg_exec(
        f"""INSERT INTO public."{table}" VALUES
        (1, '2024-01-02', '2024-01-01 00:00:00.123456', 'a'),
        (2, '0044-03-15 BC', '0044-03-15 12:00:00 BC', ''),
        (3, NULL, NULL, NULL),
        (4, '1999-12-31', '2024-06-01 12:00:00.000001', 'Zoë')"""
    )
    return table


@pytest.mark.parametrize("dest", ["sqlite", "postgresql"])
def test_d65_bc_date_loads_where_the_carrier_has_a_bc_era(transfer_client, dest):
    """The read never crashes; PostgreSQL and SQLite's text carrier keep the BC value."""
    src, out = _bc_source(), tag("d65d")
    try:
        run(transfer_client, source_connector_id=connector("postgresql", "source"), source_table=src,
            dest_connector_id=_dest(transfer_client, dest), dest_table=out,
            sync_mode="full_refresh_overwrite")
        if dest == "sqlite":
            got = _rows(transfer_client, dest, out, '"id", "d", "ts", "note"')
            assert got == [
                (1, "2024-01-02", "2024-01-01T00:00:00.123456", "a"),
                (2, "0044-03-15 BC", "0044-03-15 12:00:00 BC", ""),
                (3, None, None, None),
                (4, "1999-12-31", "2024-06-01T12:00:00.000001", "Zoë"),
            ]
        else:
            got = pg_exec(f'SELECT "id", "d"::text, "ts"::text, "note" FROM public."{out}" ORDER BY 1')
            assert got == [
                (1, "2024-01-02", "2024-01-01 00:00:00.123456", "a"),
                (2, "0044-03-15 BC", "0044-03-15 12:00:00 BC", ""),
                (3, None, None, None),
                (4, "1999-12-31", "2024-06-01 12:00:00.000001", "Zoë"),
            ]
    finally:
        pg_exec(f'DROP TABLE IF EXISTS public."{src}"')
        _drop(dest, out)


@pytest.mark.parametrize("dest", ["mysql", "sqlserver"])
def test_d65_bc_date_is_refused_by_name_then_quarantined_on_an_ad_only_carrier(transfer_client, dest):
    src, out = _bc_source(), tag("d65d")
    source = connector("postgresql", "source")
    dest_id = _dest(transfer_client, dest)
    try:
        staged = stage(source_connector_id=source, source_table=src, dest_connector_id=dest_id,
                       dest_table=out, sync_mode="full_refresh_overwrite")
        text = _headline(staged)
        assert not staged.success
        assert "out of range" not in text and "source sample read failed" not in text, text
        assert "is a BC date" in text and "no BC era" in text, text
        assert "UNKNOWN" not in text, text
        # The guidance names the carrier that keeps it, not the type that refused.
        assert "create as DATE" not in text, text

        staged = stage(source_connector_id=source, source_table=src, dest_connector_id=dest_id,
                       dest_table=out, sync_mode="full_refresh_overwrite",
                       risk_acceptance=_QUARANTINE)
        job = confirm(transfer_client, staged)
        assert str(job.get("status")) == "completed_with_quarantine", job.get("error")
        got = _rows(transfer_client, dest, out, '"id", "d", "ts", "note"')
        assert [r[0] for r in got] == [1, 3, 4]
        by_id = {r[0]: r for r in got}
        assert by_id[1][1] == date(2024, 1, 2) and by_id[1][2] == datetime(2024, 1, 1, 0, 0, 0, 123456)
        assert by_id[3][1:] == (None, None, None)
        assert by_id[4][2] == datetime(2024, 6, 1, 12, 0, 0, 1) and by_id[4][3] == "Zoë"
    finally:
        pg_exec(f'DROP TABLE IF EXISTS public."{src}"')
        _drop(dest, out)


# ====================================================================== D52

@pytest.mark.parametrize("dest", ["postgresql", "mysql", "sqlserver", "sqlite"])
def test_d52_dst_gap_names_the_wall_clock_and_holds_out_only_that_row(transfer_client, dest):
    """Text wall clocks + to_timestamp + source_timezone=America/New_York."""
    src, out = tag("d52s"), tag("d52d")
    pg_exec(f'CREATE TABLE public."{src}" (id int primary key, ts text, note text)')
    pg_exec(
        f"""INSERT INTO public."{src}" VALUES
        (1, '2024-03-10 01:30', ''),
        (2, '2024-03-10 02:30', 'gap'),
        (3, '2024-11-03 01:30:00.123456', 'fold'),
        (4, NULL, NULL)"""
    )
    source = connector("postgresql", "source")
    dest_id = _dest(transfer_client, dest)
    args = dict(source_connector_id=source, source_table=src, dest_connector_id=dest_id,
                dest_table=out, sync_mode="full_refresh_overwrite",
                source_timezone="America/New_York", column_transforms={"ts": "to_timestamp"})
    try:
        staged = stage(**args)
        text = _headline(staged)
        assert not staged.success
        assert "type coercion issue(s) — ts: Wall clock '2024-03-10 02:30' does not exist in timezone" in text, text
        assert "1 value(s)" in text and "3 value(s)" not in text, text
        assert "INVALID_TIMESTAMP" in _sample_transform_classes(staged), text
        assert "UNKNOWN" not in text, text

        job = confirm(transfer_client, stage(**args, risk_acceptance=_QUARANTINE))
        assert str(job.get("status")) == "completed_with_quarantine", job.get("error")
        got = dict((r[0], r[1]) for r in _rows(transfer_client, dest, out, '"id", "ts"'))
        assert sorted(got) == [1, 3, 4]
        expected = {
            1: datetime(2024, 3, 10, 6, 30, tzinfo=UTC),
            # Fall-back fold: the later (standard-time) instant, as PostgreSQL.
            3: datetime(2024, 11, 3, 6, 30, 0, 123456, tzinfo=UTC),
        }
        for key, want in expected.items():
            have = got[key]
            if isinstance(have, str):
                have = datetime.fromisoformat(have)
            if have.tzinfo is None:  # MySQL DATETIME(6) holds the UTC clock
                have = have.replace(tzinfo=UTC)
            assert have.astimezone(UTC) == want, (key, got[key])
        assert got[4] is None
    finally:
        pg_exec(f'DROP TABLE IF EXISTS public."{src}"')
        _drop(dest, out)


# ===================================================================== D148

def _naive_source(client, kind: str, micro: bool) -> tuple[str, str]:
    us = ".123456" if micro else ""
    rows = [(1, f"2024-01-15 10:30:00{us}", "2024-01-15"), (2, "2024-11-03 01:30:00", None),
            (3, None, "1999-12-31")]
    table = tag("d148s")
    if kind == "postgresql":
        pg_exec(f'CREATE TABLE public."{table}" (id int primary key, ts timestamp(6), d date)')
        for row in rows:
            pg_exec(f'INSERT INTO public."{table}" VALUES (%s, %s, %s)', row)
        return connector("postgresql", "source"), table
    path = client.sqlite_root / "src.db"
    with sqlite3.connect(path) as conn:
        conn.execute(f'CREATE TABLE "{table}" (id INTEGER PRIMARY KEY, ts DATETIME, d DATE)')
        conn.executemany(f'INSERT INTO "{table}" VALUES (?, ?, ?)', rows)
    return connector("sqlite", "source", database=str(path)), table


def _instants(kind: str, table: str, store) -> dict:
    if kind == "mongodb":
        client, db = mongo_db()
        try:
            return {d["id"]: d.get("ts") for d in db[table].find({}, {"_id": 0})}
        finally:
            db[table].drop()
            client.close()
    docs = store.get(table, {}).get("docs", {})
    out = {}
    for doc in docs.values():
        value = doc.get("ts")
        out[doc["id"]] = datetime.fromisoformat(value) if value else None
    return out


@pytest.mark.parametrize("dest", ["mongodb", "elasticsearch"])
@pytest.mark.parametrize("src", ["postgresql", "sqlite"])
def test_d148_the_gates_own_assume_timezone_remedy_clears_it(transfer_client, fake_es, src, dest):
    source, table = _naive_source(transfer_client, src, micro=False)
    out = tag("d148d")
    try:
        staged = stage(source_connector_id=source, source_table=table,
                       dest_connector_id=_dest(transfer_client, dest, out), dest_table=out,
                       sync_mode="full_refresh_overwrite")
        assert not staged.success and "assume_timezone" in _headline(staged)
        run(transfer_client, source_connector_id=source, source_table=table,
            dest_connector_id=_dest(transfer_client, dest, out), dest_table=out,
            sync_mode="full_refresh_overwrite", column_transforms={"ts": "assume_timezone:UTC"})
        assert _instants(dest, out, fake_es) == {
            1: datetime(2024, 1, 15, 10, 30, tzinfo=UTC),
            2: datetime(2024, 11, 3, 1, 30, tzinfo=UTC),
            3: None,
        }
    finally:
        if src == "postgresql":
            pg_exec(f'DROP TABLE IF EXISTS public."{table}"')


@pytest.mark.parametrize("dest", ["mongodb", "elasticsearch"])
@pytest.mark.parametrize("src", ["postgresql", "sqlite"])
def test_d148_source_timezone_loads_the_correct_instant(transfer_client, fake_es, src, dest):
    source, table = _naive_source(transfer_client, src, micro=False)
    out = tag("d148d")
    try:
        run(transfer_client, source_connector_id=source, source_table=table,
            dest_connector_id=_dest(transfer_client, dest, out), dest_table=out,
            sync_mode="full_refresh_overwrite", source_timezone="America/New_York")
        assert _instants(dest, out, fake_es) == {
            1: datetime(2024, 1, 15, 15, 30, tzinfo=UTC),
            2: datetime(2024, 11, 3, 6, 30, tzinfo=UTC),
            3: None,
        }
    finally:
        if src == "postgresql":
            pg_exec(f'DROP TABLE IF EXISTS public."{table}"')


@pytest.mark.parametrize("dest", ["mongodb", "elasticsearch"])
def test_d148_real_microseconds_offer_an_explicit_truncation_acceptance(transfer_client, fake_es, dest):
    source, table = _naive_source(transfer_client, "postgresql", micro=True)
    out = tag("d148d")
    args = dict(source_connector_id=source, source_table=table,
                dest_connector_id=_dest(transfer_client, dest, out), dest_table=out,
                sync_mode="full_refresh_overwrite", column_transforms={"ts": "assume_timezone:UTC"})
    try:
        staged = stage(**args)
        text = _headline(staged)
        assert not staged.success and "3 fractional digits" in text and "CAST_AND_CONTINUE" in text, text
        if dest == "elasticsearch":
            assert "BSON" not in text, text
        run(transfer_client, **args, risk_acceptance={
            "approved_by": "qa@example.com", "reason": "millisecond instants are enough",
            "execution_policy": "CAST_AND_CONTINUE",
        })
        got = _instants(dest, out, fake_es)
        assert got[1].replace(microsecond=0) == datetime(2024, 1, 15, 10, 30, tzinfo=UTC)
        assert got[1].microsecond in {123000, 123456}
        assert got[3] is None
    finally:
        pg_exec(f'DROP TABLE IF EXISTS public."{table}"')


# ======================================================================= M6

def test_m6_sqlite_timestamp_round_trip_reads_and_gate8_agree(transfer_client):
    """PG -> SQLite -> PG: every temporal lands as ISO-8601 text in SQLite, Gate-8
    verifies both legs, and the read-back recovers the exact values."""
    src, mid, back = tag("m6s"), tag("m6m"), tag("m6b")
    pg_exec(f'CREATE TABLE public."{src}" (id int primary key, ts timestamp(6), tz timestamptz, d date, t time(6))')
    pg_exec(
        f"""INSERT INTO public."{src}" VALUES
        (1, '2025-01-01 10:00:00', '2025-01-01 10:00:00+05:30', '2025-01-01', '10:00:00'),
        (2, '2025-06-30 23:59:59.123456', '2025-06-30 23:59:59.5+00', '2025-06-30', '23:59:59.25'),
        (3, NULL, NULL, NULL, NULL)"""
    )
    pg_src = connector("postgresql", "source")
    sqlite_dst = _dest(transfer_client, "sqlite")
    sqlite_src = connector("sqlite", "source", database=str(transfer_client.sqlite_root / "dest.db"))
    pg_dst = connector("postgresql", "destination")
    try:
        job = run(transfer_client, source_connector_id=pg_src, source_table=src,
                  dest_connector_id=sqlite_dst, dest_table=mid, sync_mode="full_refresh_overwrite")
        assert (job.get("reconciliation") or {}).get("verification_verdict") == "verified"
        job = run(transfer_client, source_connector_id=sqlite_src, source_table=mid,
                  dest_connector_id=pg_dst, dest_table=back, sync_mode="full_refresh_overwrite")
        assert (job.get("reconciliation") or {}).get("verification_verdict") == "verified"
        with sqlite3.connect(transfer_client.sqlite_root / "dest.db") as conn:
            stored = conn.execute(
                f'SELECT typeof(ts), typeof(tz), typeof(d), typeof(t), ts, tz FROM "{mid}" WHERE id = 2'
            ).fetchone()
        assert stored == ("text", "text", "text", "text",
                          "2025-06-30T23:59:59.123456", "2025-06-30T23:59:59.500000+00:00")
        assert pg_exec(f'SELECT * FROM public."{back}" ORDER BY 1') == pg_exec(
            f'SELECT * FROM public."{src}" ORDER BY 1'
        )
    finally:
        for t in (src, back):
            pg_exec(f'DROP TABLE IF EXISTS public."{t}"')


def test_m6_offset_bearing_sqlite_text_is_refused_at_validate_like_the_writer(transfer_client):
    """Validate used to pass a mixed naive/offset SQLite column into a PG TIMESTAMP
    and the writer then refused 2 rows. Both now apply the one rule, and
    declaring the zone loads every row as the exact instant, verified."""
    path = transfer_client.sqlite_root / "src.db"
    values = ["2025-01-01 10:00:00+05:30", "2025-01-01T10:00:00Z", "2025-01-01 10:00",
              "2025-01-01", "2025-01-01 10:00:00.123456", None]
    with sqlite3.connect(path) as conn:
        conn.execute('CREATE TABLE ev (id INTEGER PRIMARY KEY, ts DATETIME)')
        conn.executemany("INSERT INTO ev VALUES (?, ?)", list(enumerate(values, 1)))
    source = connector("sqlite", "source", database=str(path))
    dest = connector("postgresql", "destination")
    out = tag("m6v")
    try:
        staged = stage(source_connector_id=source, source_table="ev", dest_connector_id=dest,
                       dest_table=out, sync_mode="full_refresh_overwrite")
        text = _headline(staged)
        assert not staged.success and "timezone-aware value into NTZ destination" in text, text
        job = run(transfer_client, source_connector_id=source, source_table="ev",
                  dest_connector_id=dest, dest_table=out, sync_mode="full_refresh_overwrite",
                  source_timezone="UTC")
        assert (job.get("reconciliation") or {}).get("verification_verdict") == "verified"
        assert [r[0] for r in pg_exec(f'SELECT ts FROM public."{out}" ORDER BY id')] == [
            datetime(2025, 1, 1, 4, 30, tzinfo=UTC), datetime(2025, 1, 1, 10, 0, tzinfo=UTC),
            datetime(2025, 1, 1, 10, 0, tzinfo=UTC), datetime(2025, 1, 1, 0, 0, tzinfo=UTC),
            datetime(2025, 1, 1, 10, 0, 0, 123456, tzinfo=UTC), None,
        ]
    finally:
        _drop("postgresql", out)


# ====================================================================== D59

_DOUBLES = [(1, 5e-324), (2, 1.7976931348623157e308), (3, 2.2250738585072014e-308),
            (4, None), (5, -1e-320)]


def _double_source() -> str:
    table = tag("d59s")
    pg_exec(f'CREATE TABLE public."{table}" (id int primary key, x double precision)')
    for row in _DOUBLES:
        pg_exec(f'INSERT INTO public."{table}" VALUES (%s, %s)', row)
    return table


def test_d59_subnormal_doubles_load_into_sqlite_real(transfer_client):
    src, out = _double_source(), tag("d59d")
    try:
        run(transfer_client, source_connector_id=connector("postgresql", "source"), source_table=src,
            dest_connector_id=_dest(transfer_client, "sqlite"), dest_table=out,
            sync_mode="full_refresh_overwrite")
        assert _rows(transfer_client, "sqlite", out, '"id", "x"') == _DOUBLES
    finally:
        pg_exec(f'DROP TABLE IF EXISTS public."{src}"')


def test_d59_a_refused_double_is_named_at_validate(transfer_client):
    """SQL Server FLOAT(53) has no subnormals: Validate says so, by value."""
    src, out = _double_source(), tag("d59d")
    try:
        staged = stage(source_connector_id=connector("postgresql", "source"), source_table=src,
                       dest_connector_id=_dest(transfer_client, "sqlserver"), dest_table=out,
                       sync_mode="full_refresh_overwrite")
        text = _headline(staged)
        assert not staged.success and "5e-324 is outside FLOAT on sqlserver" in text, text
    finally:
        pg_exec(f'DROP TABLE IF EXISTS public."{src}"')
        _drop("sqlserver", out)
