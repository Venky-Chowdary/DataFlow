"""Zone-bearing incremental cursors with mixed UTC offsets are read exactly once.

Two defects found while fixing D207, both on QA's route (MCP ``start_transfer``
-> ``/api/v1/copilot/confirm`` -> job) and both present on production 557f556c:

* SQL Server ``DATETIMEOFFSET`` -> PG: run 1 wrote duplicate rows. Page maxima
  were ordered as *text*, so ``'2026-01-01 10:00 +05:30'`` (04:30Z) beat
  ``'2026-01-01 06:00 +00:00'`` (06:00Z); the next page seeked from an earlier
  instant and re-read rows.
* PG ``timestamptz`` -> PG: the job never got past Validate (CPU-bound).

Contract pinned here for SQL Server DATETIMEOFFSET, PG timestamptz and Oracle
TIMESTAMP WITH TIME ZONE sources: every comparison, ordering and maximum of the
cursor is by instant; the stored watermark is a canonical UTC instant; no page
is read twice or skipped. Source rows carry +05:30, -08:00 and +00:00 offsets,
interleaved so text order and instant order differ, with ties at one instant
written with different offsets. Two-row pages put page edges on those values.
Run 1, then edits and new rows, run 2, more edits, run 3 (incremental upsert:
an edit in append mode is refused as a key collision). Exactly once is pinned
on the job's row accounting — a re-read page edge adds to ``rows_read``, a
skipped one subtracts — and on the destination, compared with the source's
latest version of every row, cell by cell: the instant always, the offset
label where the destination column keeps it (text).
"""

from __future__ import annotations

import os
import re
import time
import uuid
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from tests import r4_route_harness as h

pytestmark = pytest.mark.timeout(900)

UTC = timezone.utc
VALIDATE_BUDGET_S = 45.0

# (id, note, cursor text with its own offset). Instant in the comment. The day
# is the US fall-back (2026-11-01 09:00Z): a PG session in America/Los_Angeles
# renders 08:15Z / 08:45Z as 01:15-07:00 / 01:45-07:00 and the 09:00Z tie group
# as 01:00-08:00, so text order and instant order differ there too.
_RUN1 = [
    (1, None, "2026-11-01 10:00:00 +05:30"),         # 04:30Z  (text sorts late)
    (2, "", "2026-11-01 06:00:00 +00:00"),           # 06:00Z
    (3, "  ", "2026-10-31 23:00:00 -08:00"),         # 07:00Z  (text sorts first)
    (40, "Łukasz 山田 🎉", "2026-11-01 14:30:00 +05:30"),  # 09:00Z  tie
    (20, "plain", "2026-11-01 09:00:00 +00:00"),     # 09:00Z  tie
    (30, "x", "2026-11-01 01:00:00 -08:00"),         # 09:00Z  tie
    (5, "pdt hour", "2026-11-01 08:15:00 +00:00"),   # 08:15Z  (LA: 01:15-07:00)
    (6, "Ω", "2026-11-01 14:15:00 +05:30"),          # 08:45Z  (LA: 01:45-07:00)
    (7, "y", "2026-11-01 10:00:00 +00:00"),          # 10:00Z
    (8, None, "2026-11-01 16:00:00 +05:30"),         # 10:30Z
    (9, "nine", "2026-11-01 02:45:00 -08:00"),       # 10:45Z  run-1 maximum
]
# Edits + new rows before run 2.
_RUN2 = [
    (2, "edited ✓", "2026-11-01 05:00:00 -08:00"),   # 13:00Z  edit, cursor moves
    (10, "", "2026-11-01 17:30:00 +05:30"),          # 12:00Z  tie
    (11, "  ", "2026-11-01 12:00:00 +00:00"),        # 12:00Z  tie
    (12, None, "2026-11-01 04:00:00 -08:00"),        # 12:00Z  tie
    (13, "Ünïcødé", "2026-11-01 11:00:00 +00:00"),   # 11:00Z
    # Wall clock earlier than the run-1 watermark text, instant later.
    (14, "wall earlier", "2026-11-01 03:15:00 -08:00"),  # 11:15Z
    # Same instant as the run-1 watermark, other offset, higher key.
    (15, "same instant", "2026-11-01 16:15:00 +05:30"),  # 10:45Z
]
# Edits + new rows before run 3.
_RUN3 = [
    (40, None, "2026-11-01 20:00:00 +05:30"),        # 14:30Z  edit
    (16, "sixteen", "2026-11-01 06:30:00 -08:00"),   # 14:30Z  tie
    (17, "  sp  ", "2026-11-01 14:00:00 +00:00"),    # 14:00Z
    (18, "", "2026-11-01 19:45:00 +05:30"),          # 14:15Z
]


def _parse(text: str) -> datetime:
    raw = text.strip()
    raw = re.sub(r"\s+([+-]\d{2}:?\d{2})$", r"\1", raw).replace(" ", "T", 1)
    return datetime.fromisoformat(raw.replace("Z", "+00:00"))


def _label(value: datetime) -> str:
    off = value.utcoffset() or timedelta(0)
    sign = "-" if off < timedelta(0) else "+"
    mins = int(abs(off).total_seconds() // 60)
    return f"{sign}{mins // 60:02d}:{mins % 60:02d}"


# --------------------------------------------------------------------------- #
# Engines
# --------------------------------------------------------------------------- #


def _oracle_cfg() -> dict[str, Any]:
    pw = os.environ.get("DATAFLOW_TEST_ORACLE_PASSWORD", "")
    if not pw or not h.port_up(1521):
        pytest.skip("Oracle not configured (DATAFLOW_TEST_ORACLE_PASSWORD, :1521)")
    pytest.importorskip("oracledb")
    svc = os.environ.get("DATAFLOW_TEST_ORACLE_SERVICE", "FREEPDB1")
    user = os.environ.get("DATAFLOW_TEST_ORACLE_USER", "system")
    return dict(
        type="oracle", host=os.environ.get("DATAFLOW_TEST_ORACLE_HOST", "127.0.0.1"),
        port=1521, database=svc, service_name=svc, username=user, password=pw,
        schema=os.environ.get("DATAFLOW_TEST_ORACLE_SCHEMA", user.upper()),
    )


def _ora(sql: str, params: Any = None) -> list[tuple]:
    import oracledb

    cfg = _oracle_cfg()
    conn = oracledb.connect(
        user=cfg["username"], password=cfg["password"],
        dsn=f"{cfg['host']}:{cfg['port']}/{cfg['database']}",
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params or [])
            return [tuple(r) for r in cur.fetchall()] if cur.description else []
    finally:
        conn.close()


class _Engine:
    kind = ""
    keeps_label = False
    session_tz = ""

    def config(self) -> dict[str, Any]:
        return h.dest_config(self.kind)

    def table(self, token: str) -> str:
        return f"r6tz_s_{token}"


class _PG(_Engine):
    kind = "postgresql"

    def create(self, t: str) -> None:
        h.execute(self.kind, f'CREATE TABLE "{t}" (id INT PRIMARY KEY, note TEXT NULL, '
                             "ts TIMESTAMPTZ NOT NULL)")

    def upsert(self, t: str, rows) -> None:
        conn = h.pg_conn()
        try:
            cur = conn.cursor()
            for rid, note, ts in rows:
                cur.execute(
                    f'INSERT INTO "{t}" (id, note, ts) VALUES (%s, %s, %s::timestamptz) '
                    "ON CONFLICT (id) DO UPDATE SET note = EXCLUDED.note, ts = EXCLUDED.ts",
                    (rid, note, ts),
                )
        finally:
            conn.close()

    def drop(self, t: str) -> None:
        h.drop_table(self.kind, t)


class _MSSQL(_Engine):
    kind = "sqlserver"
    keeps_label = True

    def create(self, t: str) -> None:
        h.execute(self.kind, f"CREATE TABLE [{t}] (id INT PRIMARY KEY, note NVARCHAR(40) NULL, "
                             "ts DATETIMEOFFSET(3) NOT NULL)")

    def upsert(self, t: str, rows) -> None:
        conn = h.mssql_conn()
        try:
            cur = conn.cursor()
            for rid, note, ts in rows:
                cur.execute(f"DELETE FROM [{t}] WHERE id = ?", rid)
                cur.execute(
                    f"INSERT INTO [{t}] (id, note, ts) VALUES (?, ?, "
                    "CAST(? AS DATETIMEOFFSET(3)))",
                    rid, note, ts,
                )
        finally:
            conn.close()

    def drop(self, t: str) -> None:
        h.drop_table(self.kind, t)


class _Oracle(_Engine):
    kind = "oracle"
    keeps_label = True

    def config(self) -> dict[str, Any]:
        return _oracle_cfg()

    def table(self, token: str) -> str:
        return f"R6TZ_S_{token.upper()}"

    def create(self, t: str) -> None:
        _ora(f'CREATE TABLE "{t}" (ID NUMBER(10) PRIMARY KEY, NOTE VARCHAR2(40) NULL, '
             "TS TIMESTAMP(3) WITH TIME ZONE NOT NULL)")

    def upsert(self, t: str, rows) -> None:
        for rid, note, ts in rows:
            _ora(f'DELETE FROM "{t}" WHERE ID = :1', [rid])
            _ora(
                f'INSERT INTO "{t}" (ID, NOTE, TS) VALUES (:1, :2, '
                "TO_TIMESTAMP_TZ(:3, 'YYYY-MM-DD HH24:MI:SS TZH:TZM'))",
                [rid, note, ts],
            )

    def drop(self, t: str) -> None:
        try:
            _ora(f'DROP TABLE "{t}" PURGE')
        except Exception:  # noqa: BLE001, S110 - best-effort cleanup
            pass


class _PGLosAngeles(_PG):
    """PG whose sessions render timestamptz in a DST zone (libpq ``PGTZ``)."""

    session_tz = "America/Los_Angeles"


_ENGINES = {"sqlserver_datetimeoffset": _MSSQL(), "pg_timestamptz": _PG(),
            "pg_timestamptz_la_session": _PGLosAngeles(), "oracle_tstz": _Oracle()}


# --------------------------------------------------------------------------- #
# Route
# --------------------------------------------------------------------------- #


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    import src.transfer.stream as stream_mod

    # Two-row pages: page edges fall on the mixed-offset values and the ties.
    monkeypatch.setattr(stream_mod, "CHUNK_SIZE", 2)
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def _run(client, args: dict, run: int) -> tuple[dict, float]:
    staged_at = time.monotonic()
    ok, staged = h.mcp_call(client, "start_transfer", args)
    staging_s = time.monotonic() - staged_at
    assert staging_s < VALIDATE_BUDGET_S, {"run": run, "staging_validate_s": staging_s}
    assert ok, {"run": run, "error": staged}
    assert not h.blockers_of(staged), h.blockers_of(staged)
    resp = client.post("/api/v1/copilot/confirm",
                       json={"ack_id": staged["ack_id"], "reason": "r6tz"})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body.get("ok") is True, body
    started = time.monotonic()
    job = h.wait_job(str(body.get("job_id") or ""), timeout_s=240)
    elapsed = time.monotonic() - started
    assert str(job.get("status")) == "completed", {
        "run": run, "status": job.get("status"), "phase": job.get("phase"),
        "error": str(job.get("error") or job.get("message") or "")[:600],
        "elapsed_s": round(elapsed, 1),
    }
    return job, elapsed


def _dest_rows(table: str) -> tuple[list[tuple], str]:
    cols = {
        r[0].lower(): str(r[1]).lower()
        for r in h.query(
            "postgresql",
            "SELECT column_name, data_type FROM information_schema.columns "  # nosec B608
            f"WHERE table_name = '{table}'",
        )
    }
    names = {
        r[0].lower(): r[0]
        for r in h.query(
            "postgresql",
            "SELECT column_name FROM information_schema.columns "  # nosec B608
            f"WHERE table_name = '{table}'",
        )
    }
    ts_col, id_col, note_col = names["ts"], names["id"], names["note"]
    rows = h.query(
        "postgresql",
        f'SELECT "{id_col}", "{note_col}", "{ts_col}" FROM "{table}"',  # nosec B608
    )
    return rows, cols["ts"]


def _cell(value: Any, keep_label: bool) -> tuple:
    if isinstance(value, str):
        value = _parse(value)
    assert isinstance(value, datetime) and value.tzinfo is not None, repr(value)
    instant = value.astimezone(UTC)
    return (instant, _label(value)) if keep_label else (instant,)


def _expected(versions: list[tuple], keep_label: bool, *, empty_is_null: bool) -> Counter:
    return Counter(
        (rid, None if (empty_is_null and note == "") else note, _cell(ts, keep_label))
        for rid, note, ts in versions
    )


def _actual(table: str, src_keeps_label: bool) -> tuple[Counter, bool]:
    rows, ts_type = _dest_rows(table)
    keep = src_keeps_label and ts_type in {"text", "character varying"}
    return Counter((int(r[0]), r[1], _cell(r[2], keep)) for r in rows), keep


def _assert_watermark(job: dict, source_now: list[tuple], run: int) -> None:
    """The watermark is the maximum instant, written canonically in UTC — on
    the job and in the cursor store the next run reads."""
    from services.sync_cursor import get_watermark, list_cursor_keys

    rep = job.get("incremental_watermark") or {}
    keys = list_cursor_keys()  # the store is isolated to this test: one route
    key = str(rep.get("cursor_key") or (keys[0] if len(keys) == 1 else ""))
    stored = str(get_watermark(key) or "") if key else ""
    # A build without the job's watermark report is judged on the store alone.
    after = str(rep.get("after") or "") if rep else stored
    want = max(_parse(ts).astimezone(UTC) for _, _, ts in source_now)
    for label, value in (("job", after), ("stored", stored)):
        assert value, {"run": run, label: value, "rep": rep}
        cursor_text = value.split("\x1f", 1)[0]
        assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?\+00:00", cursor_text), {
            "run": run, label: value}
        assert _parse(cursor_text) == want, {"run": run, label: value, "want": want.isoformat()}
    assert stored == after, {"run": run, "stored": stored, "job": after}


def _validate_seconds(job: dict) -> float | None:
    """Seconds the job spent in Validate (its ``preflight`` phase)."""
    for phase in job.get("phases") or []:
        if isinstance(phase, dict) and phase.get("name") == "preflight":
            ms = phase.get("elapsed_ms")
            return None if ms is None else float(ms) / 1000.0
    return None


@pytest.mark.parametrize("leg", list(_ENGINES), ids=list(_ENGINES))
def test_mixed_offset_cursor_reads_every_row_exactly_once(client, leg, monkeypatch):
    from services.connector_store import create_connector

    eng = _ENGINES[leg]
    if eng.session_tz:
        monkeypatch.setenv("PGTZ", eng.session_tz)
    token = uuid.uuid4().hex[:8]
    src_table = eng.table(token)
    dest_table = f"r6tz_d_{token}"
    src_cfg = eng.config()
    dst_cfg = h.dest_config("postgresql")
    try:
        eng.create(src_table)
        eng.upsert(src_table, _RUN1)
        src = create_connector({"name": f"r6tz-s-{token}", "role": "source",
                                "workspace_id": "", "ssl": False, **src_cfg})
        dst = create_connector({"name": f"r6tz-d-{token}", "role": "destination",
                                "workspace_id": "", "ssl": False, **dst_cfg})
        cursor = "TS" if eng.kind == "oracle" else "ts"
        args = {
            "source_connector_id": src.id, "source_table": src_table,
            "dest_connector_id": dst.id, "dest_table": dest_table,
            "sync_mode": "incremental_upsert", "validation_mode": "strict",
            "cursor_column": cursor, "cursor_semantics": "modification_timestamp",
            "primary_key": "ID" if eng.kind == "oracle" else "id",
        }
        latest: dict[int, tuple] = {}
        for run, edits in ((1, _RUN1), (2, _RUN2), (3, _RUN3)):
            if run > 1:
                eng.upsert(src_table, edits)
            latest.update({rid: (rid, note, ts) for rid, note, ts in edits})
            job, _elapsed = _run(client, args, run)
            validate_s = _validate_seconds(job)
            assert validate_s is not None, job.get("phases")
            assert validate_s < VALIDATE_BUDGET_S, {"run": run, "validate_s": validate_s}
            # Exactly once: every row past the watermark is read once — a
            # re-read page edge adds to rows_read, a skipped one subtracts.
            acct = job.get("row_accounting") or {}
            assert acct.get("rows_read") == len(edits), {"run": run, "accounting": acct}
            assert acct.get("rows_written") == len(edits), {"run": run, "accounting": acct}
            got, keep = _actual(dest_table, eng.keeps_label)
            want = _expected(
                list(latest.values()), keep, empty_is_null=eng.kind == "oracle"  # '' is NULL
            )
            assert got == want, {
                "run": run, "duplicates": {k: v for k, v in got.items() if v > 1},
                "missing": sorted(map(str, (want - got).elements())),
                "extra": sorted(map(str, (got - want).elements())),
            }
            _assert_watermark(job, list(latest.values()), run)
    finally:
        eng.drop(src_table)
        h.drop_table("postgresql", dest_table)
