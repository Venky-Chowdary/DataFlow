"""MXD07 — a SQLite TIMESTAMPTZ cell keeps an explicit offset.

QA (PG -> SQLite): ``ts_tz: "2024-01-01 00:30:00"`` — no zone marker. 6d337140
stored bare UTC digits and leaned on the declared token, but no reader honours
that token: our own SQLite table then failed to load back into PostgreSQL
(``Column 'ts_tz' → TIMESTAMPTZ: 2 of 2 sampled value(s) cannot be cast``) —
a T2 case of rejecting a shape we created. The cell is now RFC 3339 UTC
(``+00:00``), the same ISO-8601 spelling ``cell_to_string`` gives naive cells.
"""

from __future__ import annotations

import sqlite3
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from connectors.sqlite_writer import _to_sqlite_value  # noqa: E402
from tests.test_mx2_16_overwrite_create_new_carries_keys import _engine, _pg, _pg_ep  # noqa: E402


@pytest.mark.parametrize("carrier", ["TIMESTAMPTZ", "TIMESTAMP_TZ", "TIMESTAMP_LTZ"])
def test_tz_carrier_writes_utc_with_explicit_offset(carrier):
    ist = timezone(timedelta(hours=5, minutes=30))
    out = _to_sqlite_value(datetime(2024, 1, 1, 6, 0, tzinfo=ist), carrier)
    assert out == "2024-01-01T00:30:00+00:00"
    assert datetime.fromisoformat(out) == datetime(2024, 1, 1, 0, 30, tzinfo=timezone.utc)


def test_tz_carrier_offset_text_wire_is_normalized_not_stripped():
    out = _to_sqlite_value("2024-01-01T05:00:00+05:30", "TIMESTAMPTZ")
    assert out == "2023-12-31T23:30:00+00:00"


def test_sqlite_date_functions_read_the_offset_cell():
    conn = sqlite3.connect(":memory:")
    cell = _to_sqlite_value(datetime(2024, 1, 1, 0, 30, tzinfo=timezone.utc), "TIMESTAMPTZ")
    assert conn.execute("SELECT datetime(?)", (cell,)).fetchone()[0] == "2024-01-01 00:30:00"


def test_ntz_carriers_still_refuse_an_offset():
    with pytest.raises(ValueError, match="refuses timezone-aware wire"):
        _to_sqlite_value("2024-01-01T00:30:00+00:00", "TIMESTAMP")


def test_live_pg_to_sqlite_and_back_keeps_instant_and_offset(monkeypatch, tmp_path):
    conn = _pg()
    cur = conn.cursor()
    tag = uuid.uuid4().hex[:8]
    src, back = f"qa_e2e_rt_tz_src_{tag}", f"qa_e2e_rt_tz_back_{tag}"
    cur.execute(f"CREATE TABLE {src} (id INT PRIMARY KEY, ts_tz TIMESTAMPTZ, ts_naive TIMESTAMP)")
    cur.execute(
        f"INSERT INTO {src} VALUES (1, '2024-01-01 06:00:00+05:30', '2024-06-01 00:36:00'),"
        "(2, '2024-01-02 00:00:00+00', '2024-06-02 00:00:00.123456')"
    )
    from src.transfer.models import EndpointConfig, TransferRequest

    db = tmp_path / "tz.sqlite"
    lite = EndpointConfig(kind="database", format="sqlite", database=str(db), table="tz_dst")
    cols = ["id", "ts_tz", "ts_naive"]

    def req(s, d):
        return TransferRequest(
            mappings=[{"source": c, "target": c, "confidence": 1.0} for c in cols],
            source=s, destination=d, sync_mode="full_refresh_overwrite", validation_mode="balanced",
        )

    run = _engine(monkeypatch)
    try:
        out = run(req(_pg_ep(src), lite))
        assert out.success, out.error
        with sqlite3.connect(db) as lc:
            rows = lc.execute("SELECT ts_tz, ts_naive FROM tz_dst ORDER BY id").fetchall()
        assert rows[0][0] == "2024-01-01T00:30:00+00:00"
        assert rows[1][0] == "2024-01-02T00:00:00+00:00"
        assert rows[0][1] == "2024-06-01T00:36:00"  # same ISO-8601 spelling, naive
        assert rows[1][1] == "2024-06-02T00:00:00.123456"

        again = run(req(lite, _pg_ep(back)))
        assert again.success, again.error
        cur.execute(
            f"SELECT s.id FROM {src} s JOIN {back} b USING (id) "
            "WHERE s.ts_tz = b.ts_tz AND s.ts_naive = b.ts_naive::timestamp"
        )
        assert sorted(r[0] for r in cur.fetchall()) == [1, 2]
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {src}")
        cur.execute(f"DROP TABLE IF EXISTS {back}")
        conn.close()

