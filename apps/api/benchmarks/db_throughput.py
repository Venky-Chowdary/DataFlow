"""Database-to-database throughput benchmark through the real transfer engine.

Usage (from ``apps/api``, with ``PYTHONPATH=.``)::

    python -m benchmarks.db_throughput --route pg-pg --rows 1000000 --out result.json
    python -m benchmarks.db_throughput --route pg-mysql --rows 200000 \
        --min-rows-per-second 4000 --out smoke.json

What one run does, in order:

1. Creates a source table and fills it server-side with ``--rows`` deterministic
   rows of mixed types: BIGINT key, INTEGER, NUMERIC(18,4), DOUBLE, TIMESTAMP,
   DATE, BOOLEAN, VARCHAR, TEXT (NULL, ``''``, tab/newline/backslash, plain),
   JSON and non-ASCII text. Every nullable column holds NULLs. Row ``g`` is a
   pure function of ``g``, so two runs at the same size move the same data.
2. Runs one transfer with ``UniversalTransferEngine().execute_tracked`` —
   the same entry point a job uses, including Gate-8 verification.
3. Re-reads both tables with streaming cursors, ordered by key, and compares
   every cell after a small, explicit cross-engine normalisation (bool ↔
   TINYINT, JSON text ↔ object). NULL and ``''`` stay distinct. This check is
   independent of the engine's own verification and its time is reported
   separately (``independent_check_seconds``).
4. Writes one JSON report: rows/s over the whole engine call, the engine's
   phase profile, verification time (Gate-8 reconciliation plus the in-stream
   checksum phase), peak RSS of this process, and the independent check.

Exit status is non-zero when the transfer fails, the independent check finds
a difference, or ``--min-rows-per-second`` is given and not met.

Connection settings come from the same variables the API test suite reads
(``PGHOST``/``PGPORT``/``PGUSER``/``PGPASSWORD``/``PGDATABASE`` and
``MYSQL_HOST``/``MYSQL_PORT``/``MYSQL_USER``/``MYSQL_PASSWORD``/``MYSQL_DATABASE``).
Tables are dropped at the end unless ``--keep`` is given.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import decimal
import hashlib
import json
import os
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Iterator

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

ENGINES = ("pg", "mysql")
COLUMNS = (
    "id", "qty", "amount", "ratio", "created_at", "event_day", "active",
    "label", "note", "payload", "city",
)
_CITIES = ("Tōkyō", "Δοκιμή", "🙂 emoji", "café", "Zürich")

_PG_DDL = """CREATE TABLE {t} (
 id BIGINT PRIMARY KEY, qty INTEGER, amount NUMERIC(18,4), ratio DOUBLE PRECISION,
 created_at TIMESTAMP, event_day DATE, active BOOLEAN, label VARCHAR(64), note TEXT,
 payload JSONB, city TEXT)"""

# Row g, written once for PostgreSQL and once for MySQL/MariaDB below. The two
# spellings must produce the same values: tests/test_db_throughput_harness.py
# checks both against the Python reference ``expected_row``.
_PG_FILL = """INSERT INTO {t}
SELECT g,
 CASE WHEN g %% 17 = 0 THEN NULL ELSE ((g * 7919) %% 100000)::int - 50000 END,
 CASE WHEN g %% 13 = 0 THEN NULL ELSE (((g * 104729) %% 100000000) / 10000.0)::numeric(18,4) END,
 CASE WHEN g %% 19 = 0 THEN NULL ELSE g / 8.0 END,
 CASE WHEN g %% 23 = 0 THEN NULL ELSE TIMESTAMP '2020-01-01' + ((g * 37) %% 157680000) * INTERVAL '1 second' END,
 CASE WHEN g %% 29 = 0 THEN NULL ELSE DATE '2000-01-01' + (g %% 9000)::int END,
 CASE WHEN g %% 11 = 0 THEN NULL ELSE g %% 2 = 0 END,
 'label-' || g,
 CASE WHEN g %% 10 = 0 THEN NULL WHEN g %% 10 = 1 THEN ''
      WHEN g %% 10 = 2 THEN E'tab\\tand\\nnewline\\\\back ' || g
      ELSE 'note ' || md5(g::text) END,
 CASE WHEN g %% 9 = 0 THEN NULL
      ELSE jsonb_build_object('k', g, 's', 'v' || (g %% 100), 'a', jsonb_build_array(g %% 3, 'x')) END,
 (ARRAY['Tōkyō','Δοκιμή','🙂 emoji','café','Zürich'])[1 + (g %% 5)::int]
FROM generate_series(%(lo)s::bigint, %(hi)s::bigint) AS g"""

_MY_DDL = """CREATE TABLE {t} (
 id BIGINT PRIMARY KEY, qty INT NULL, amount DECIMAL(18,4) NULL, ratio DOUBLE NULL,
 created_at DATETIME(6) NULL, event_day DATE NULL, active TINYINT(1) NULL,
 label VARCHAR(64) NOT NULL, note TEXT NULL, payload JSON NULL, city VARCHAR(32) NOT NULL
) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"""

_DIGITS = " UNION ALL ".join(f"SELECT {d} AS d" for d in range(10))
_MY_FILL = """INSERT INTO {t}
SELECT g,
 CASE WHEN g %% 17 = 0 THEN NULL ELSE ((g * 7919) %% 100000) - 50000 END,
 CASE WHEN g %% 13 = 0 THEN NULL ELSE CAST(((g * 104729) %% 100000000) / 10000.0 AS DECIMAL(18,4)) END,
 CASE WHEN g %% 19 = 0 THEN NULL ELSE g / 8.0 END,
 CASE WHEN g %% 23 = 0 THEN NULL ELSE TIMESTAMP('2020-01-01') + INTERVAL ((g * 37) %% 157680000) SECOND END,
 CASE WHEN g %% 29 = 0 THEN NULL ELSE DATE('2000-01-01') + INTERVAL (g %% 9000) DAY END,
 CASE WHEN g %% 11 = 0 THEN NULL ELSE g %% 2 = 0 END,
 CONCAT('label-', g),
 CASE WHEN g %% 10 = 0 THEN NULL WHEN g %% 10 = 1 THEN ''
      WHEN g %% 10 = 2 THEN CONCAT('tab\\tand\\nnewline\\\\back ', g)
      ELSE CONCAT('note ', MD5(CAST(g AS CHAR))) END,
 CASE WHEN g %% 9 = 0 THEN NULL
      ELSE JSON_OBJECT('k', g, 's', CONCAT('v', g %% 100), 'a', JSON_ARRAY(g %% 3, 'x')) END,
 ELT(1 + g %% 5, 'Tōkyō', 'Δοκιμή', '🙂 emoji', 'café', 'Zürich')
FROM (SELECT %(lo)s + a.d + 10*b.d + 100*c.d + 1000*d.d + 10000*e.d AS g
      FROM ({digits}) a, ({digits}) b, ({digits}) c, ({digits}) d, ({digits}) e) s
WHERE g <= %(hi)s"""


def expected_row(g: int) -> tuple:
    """The reference value of row ``g`` (what both fill statements produce)."""
    note: str | None
    if g % 10 == 0:
        note = None
    elif g % 10 == 1:
        note = ""
    elif g % 10 == 2:
        note = f"tab\tand\nnewline\\back {g}"
    else:
        note = "note " + hashlib.md5(str(g).encode()).hexdigest()  # noqa: S324 - test data
    return (
        g,
        None if g % 17 == 0 else (g * 7919) % 100000 - 50000,
        None if g % 13 == 0 else decimal.Decimal((g * 104729) % 100000000).scaleb(-4),
        None if g % 19 == 0 else g / 8.0,
        None if g % 23 == 0 else _dt.datetime(2020, 1, 1) + _dt.timedelta(seconds=(g * 37) % 157680000),  # noqa: DTZ001 - TIMESTAMP without zone
        None if g % 29 == 0 else _dt.date(2000, 1, 1) + _dt.timedelta(days=g % 9000),
        None if g % 11 == 0 else g % 2 == 0,
        f"label-{g}",
        note,
        None if g % 9 == 0 else {"k": g, "s": f"v{g % 100}", "a": [g % 3, "x"]},
        _CITIES[g % 5],
    )


# --------------------------------------------------------------------------
# connections


def pg_settings() -> dict[str, Any]:
    return {
        "host": os.environ.get("PGHOST", "127.0.0.1"),
        "port": int(os.environ.get("PGPORT", "5432")),
        "user": os.environ.get("PGUSER", "dataflow"),
        "password": os.environ.get("PGPASSWORD", ""),
        "dbname": os.environ.get("PGDATABASE", "dataflow"),
    }


def mysql_settings() -> dict[str, Any]:
    return {
        "host": os.environ.get("MYSQL_HOST", "127.0.0.1"),
        "port": int(os.environ.get("MYSQL_PORT", "3306")),
        "user": os.environ.get("MYSQL_USER", "dataflow"),
        "password": os.environ.get("MYSQL_PASSWORD", ""),
        "database": os.environ.get("MYSQL_DATABASE", "dataflow"),
    }


def _connect(engine: str):
    if engine == "pg":
        import psycopg2

        conn = psycopg2.connect(**pg_settings())
        conn.autocommit = True
        return conn
    import pymysql

    return pymysql.connect(**mysql_settings(), autocommit=True, charset="utf8mb4")


def _exec(engine: str, statements: list[tuple[str, dict | None]]) -> None:
    conn = _connect(engine)
    try:
        with conn.cursor() as cur:
            for sql, args in statements:
                cur.execute(sql, args)
    finally:
        conn.close()


def create_source(engine: str, table: str, rows: int, *, chunk: int = 100_000) -> float:
    """Create and fill the source table; returns seconds spent."""
    started = time.perf_counter()
    if engine == "pg":
        stmts: list[tuple[str, dict | None]] = [(_PG_DDL.format(t=table), None)]
        fill = _PG_FILL.format(t=table)
    else:
        stmts = [(_MY_DDL.format(t=table), None)]
        fill = _MY_FILL.format(t=table, digits=_DIGITS)
    for lo in range(1, rows + 1, chunk):
        stmts.append((fill, {"lo": lo, "hi": min(lo + chunk - 1, rows)}))
    if engine == "pg":
        stmts.append((f"ANALYZE {table}", None))
    _exec(engine, stmts)
    return time.perf_counter() - started


def drop_table(engine: str, table: str) -> None:
    _exec(engine, [(f"DROP TABLE IF EXISTS {table}", None)])


def _stream_rows(engine: str, table: str, batch: int = 20_000) -> Iterator[tuple]:
    cols = ", ".join(COLUMNS)
    conn = _connect(engine)
    try:
        if engine == "pg":
            conn.autocommit = False
            cur = conn.cursor(name=f"bench_{uuid.uuid4().hex[:8]}")
            cur.itersize = batch
            cur.execute(f"SELECT {cols} FROM {table} ORDER BY id")
            yield from cur
            cur.close()
            conn.rollback()
        else:
            import pymysql.cursors

            cur = conn.cursor(pymysql.cursors.SSCursor)
            cur.execute(f"SELECT {cols} FROM {table} ORDER BY id")
            while True:
                chunk = cur.fetchmany(batch)
                if not chunk:
                    break
                yield from chunk
            cur.close()
    finally:
        conn.close()


def _canonical(row: tuple) -> tuple:
    """Cross-engine spelling of one row; NULL stays NULL, '' stays ''."""
    out = list(row)
    active = out[6]
    if active is not None and not isinstance(active, bool):
        out[6] = bool(int(active))
    payload = out[9]
    if isinstance(payload, (str, bytes)):
        payload = json.loads(payload)
    if payload is not None:
        out[9] = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    amount = out[2]
    if amount is not None and not isinstance(amount, decimal.Decimal):
        amount = decimal.Decimal(str(amount))
    if amount is not None:
        out[2] = amount.quantize(decimal.Decimal("0.0001"))
    ratio = out[3]
    if ratio is not None:
        out[3] = float(ratio)
    for i in (4,):
        if isinstance(out[i], _dt.datetime) and out[i].tzinfo is not None:
            out[i] = out[i].replace(tzinfo=None)
    return tuple(out)


def independent_check(src_engine: str, src: str, dst_engine: str, dst: str, rows: int) -> dict[str, Any]:
    """Stream both tables in key order and compare every cell."""
    started = time.perf_counter()
    mismatches: list[dict[str, Any]] = []
    mismatch_count = 0
    compared = 0
    reference_mismatches = 0
    s_iter = _stream_rows(src_engine, src)
    d_iter = _stream_rows(dst_engine, dst)
    sentinel = object()
    while True:
        s = next(s_iter, sentinel)
        d = next(d_iter, sentinel)
        if s is sentinel and d is sentinel:
            break
        if s is sentinel or d is sentinel:
            mismatch_count += 1
            if len(mismatches) < 10:
                mismatches.append({"kind": "row_count", "source_ended": s is sentinel})
            break
        cs, cd = _canonical(s), _canonical(d)
        compared += 1
        if cs != cd:
            mismatch_count += 1
            if len(mismatches) < 10:
                cols = [COLUMNS[i] for i in range(len(COLUMNS)) if cs[i] != cd[i]]
                mismatches.append({"id": cs[0], "columns": cols,
                                   "source": [repr(cs[COLUMNS.index(c)]) for c in cols],
                                   "dest": [repr(cd[COLUMNS.index(c)]) for c in cols]})
        if cs != _canonical(expected_row(int(cs[0]))):
            reference_mismatches += 1
    return {
        "rows_compared": compared,
        "rows_expected": rows,
        "mismatched_rows": mismatch_count,
        "source_rows_not_matching_reference": reference_mismatches,
        "sample": mismatches,
        "passed": mismatch_count == 0 and compared == rows and reference_mismatches == 0,
        "seconds": round(time.perf_counter() - started, 3),
    }


# --------------------------------------------------------------------------
# the transfer


def _endpoint(engine: str, table: str, mysql_format: str):
    from src.transfer.models import EndpointConfig

    if engine == "pg":
        s = pg_settings()
        return EndpointConfig(kind="database", format="postgresql", host=s["host"], port=s["port"],
                              username=s["user"], password=s["password"], database=s["dbname"],
                              table=table)
    s = mysql_settings()
    return EndpointConfig(kind="database", format=mysql_format, host=s["host"], port=s["port"],
                          username=s["user"], password=s["password"], database=s["database"],
                          table=table)


class _Timer:
    """Wall time spent inside a wrapped callable (summed over calls)."""

    def __init__(self, fn):
        self._fn = fn
        self.seconds = 0.0
        self.calls = 0

    def __call__(self, *args, **kwargs):
        started = time.perf_counter()
        try:
            return self._fn(*args, **kwargs)
        finally:
            self.seconds += time.perf_counter() - started
            self.calls += 1


class _RssSampler(threading.Thread):
    """Samples RSS so the report can show the transfer's own peak."""

    def __init__(self) -> None:
        super().__init__(daemon=True)
        self.peak = 0
        self._stop_evt = threading.Event()

    def run(self) -> None:
        while not self._stop_evt.wait(0.2):
            self.peak = max(self.peak, _current_rss())

    def stop(self) -> None:
        self._stop_evt.set()
        self.join(timeout=1)
        self.peak = max(self.peak, _current_rss())


def _current_rss() -> int:
    try:
        with open("/proc/self/status", encoding="ascii") as fh:
            for line in fh:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) * 1024
    except (OSError, ValueError, IndexError):
        pass
    return 0


def run_transfer(args: argparse.Namespace, src_table: str, dst_table: str) -> dict[str, Any]:
    os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
    os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    os.environ.setdefault("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128")
    if not (os.environ.get("DATAWRAP_DATA_DIR") or os.environ.get("DATAFLOW_DATA_DIR")):
        # The run's local stores (sync cursors, audit chain, contracts) go to a
        # scratch directory, never into the source tree's apps/api/data.
        import tempfile

        os.environ["DATAWRAP_DATA_DIR"] = tempfile.mkdtemp(prefix="db-throughput-data-")
    import src.transfer.engine as engine_mod
    from services.mongodb_service import get_mongodb_service
    from services.process_peak_rss import own_peak_rss_bytes
    from src.transfer.models import TransferRequest

    src_engine, dst_engine = args.route.split("-")
    contracts = [{
        "name": src_table, "cursor_field": "id", "primary_key": ["id"],
        "sync_mode": args.mode, "selected": True,
    }]
    request = TransferRequest(
        source=_endpoint(src_engine, src_table, args.mysql_format),
        destination=_endpoint(dst_engine, dst_table, args.mysql_format),
        sync_mode=args.mode,
        stream_contracts=contracts,
        skip_preflight=True,
        validation_mode="strict",
        # ``--row-path``: a pass-through filter keeps the run on the streaming
        # row path even where an identity server-side COPY would take it.
        source_filter={"column": "id", "op": "gt", "value": 0} if args.row_path else {},
    )
    job_id = uuid.uuid4().hex[:24]
    get_mongodb_service().update_job_status(job_id, "pending", transfer_request={})
    recon_timer = _Timer(engine_mod.run_reconciliation)
    engine_mod.run_reconciliation = recon_timer
    rss_before = _current_rss()
    sampler = _RssSampler()
    sampler.start()
    profiler = None
    if args.profile:
        import cProfile

        profiler = cProfile.Profile()
        profiler.enable()
    started = time.perf_counter()
    try:
        result = engine_mod.UniversalTransferEngine().execute_tracked(request, job_id)
    finally:
        wall = time.perf_counter() - started
        if profiler is not None:
            profiler.disable()
            profiler.dump_stats(args.profile)
        sampler.stop()
        engine_mod.run_reconciliation = recon_timer._fn
    summary = result.destination_summary or {}
    profile = summary.get("phase_profile") or {}
    phases = {p.get("phase"): p for p in profile.get("phases") or [] if isinstance(p, dict)}
    checksum_s = float((phases.get("checksum") or {}).get("seconds") or 0.0)
    methods = summary.get("load_methods") or summary.get("load_method")
    return {
        "success": bool(result.success),
        "error": (result.error or "")[:1000],
        "records_transferred": int(result.records_transferred or 0),
        "wall_seconds": round(wall, 3),
        "rows_per_second": round(args.rows / wall, 1) if wall else 0.0,
        "verification_seconds": round(recon_timer.seconds + checksum_s, 3),
        "gate8_reconciliation_seconds": round(recon_timer.seconds, 3),
        "in_stream_checksum_seconds": round(checksum_s, 3),
        "peak_rss_mb": round(own_peak_rss_bytes() / 1048576, 1),
        "peak_rss_sampled_mb": round(sampler.peak / 1048576, 1),
        "rss_before_transfer_mb": round(rss_before / 1048576, 1),
        "load_methods": methods,
        "checksum_mode": summary.get("checksum_mode"),
        "verification_status": summary.get("verification_status") or summary.get("gate8_status"),
        "rejected_rows": summary.get("rejected_rows"),
        "phase_profile": profile,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--route", required=True,
                        choices=[f"{a}-{b}" for a in ENGINES for b in ENGINES])
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--mode", default="full_refresh_overwrite")
    parser.add_argument("--mysql-format", default="mysql", choices=["mysql", "mariadb"])
    parser.add_argument("--out", default="")
    parser.add_argument("--label", default="")
    parser.add_argument("--min-rows-per-second", type=float, default=0.0)
    parser.add_argument("--row-path", action="store_true",
                        help="force the streaming row path (pass-through source filter)")
    parser.add_argument("--skip-independent-check", action="store_true")
    parser.add_argument("--profile", default="", help="write a cProfile dump here")
    parser.add_argument("--keep", action="store_true", help="leave both tables in place")
    args = parser.parse_args(argv)
    if args.rows < 1:
        parser.error("--rows must be positive")

    src_engine, dst_engine = args.route.split("-")
    tag = uuid.uuid4().hex[:8]
    src_table, dst_table = f"bench_src_{tag}", f"bench_dst_{tag}"
    report: dict[str, Any] = {
        "benchmark": "db_throughput", "label": args.label, "route": args.route,
        "mode": args.mode, "rows": args.rows, "forced_row_path": bool(args.row_path),
        "timestamp": _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds"),
        "python": sys.version.split()[0], "cpu_count": os.cpu_count(),
    }
    try:
        report["generate_seconds"] = round(create_source(src_engine, src_table, args.rows), 3)
        report.update(run_transfer(args, src_table, dst_table))
        if report["success"] and not args.skip_independent_check:
            report["independent_check"] = independent_check(
                src_engine, src_table, dst_engine, dst_table, args.rows
            )
    finally:
        if not args.keep:
            for engine, table in ((src_engine, src_table), (dst_engine, dst_table)):
                try:
                    drop_table(engine, table)
                except Exception as exc:  # noqa: BLE001 - cleanup must not mask the result
                    report.setdefault("cleanup_errors", []).append(str(exc)[:200])
    failures = []
    if not report.get("success"):
        failures.append("transfer failed")
    check = report.get("independent_check")
    if check is not None and not check.get("passed"):
        failures.append("independent check found differences")
    if args.min_rows_per_second and report.get("rows_per_second", 0) < args.min_rows_per_second:
        failures.append(
            f"rows/s {report.get('rows_per_second')} is below the floor {args.min_rows_per_second}"
        )
    report["failures"] = failures
    text = json.dumps(report, indent=2, default=str)
    if args.out:
        Path(args.out).write_text(text + "\n", encoding="utf-8")
    print(text)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
