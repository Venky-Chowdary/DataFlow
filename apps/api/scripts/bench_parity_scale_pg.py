"""Parity at Scale benchmark: PostgreSQL→PostgreSQL, narrow table, injected faults.

Builds two copies of an 8-column table in-engine (``generate_series``), injects
known faults into the destination (changed cells, a missing row, an extra
row), runs the scan in mode R and mode H, checks every fault is found with the
exact key, prints one JSON document with wall time and the scan process's
peak RSS, and drops both tables (always).

Run from ``apps/api`` with the API's Python::

    DATAFLOW_PG_TEST_DSN=postgresql://user:pw@127.0.0.1:5432/db \\
        python scripts/bench_parity_scale_pg.py --rows 1000000

These are measurements of one machine, not product claims (design §5.4:
the docs quote only committed, measured CI figures).
"""

from __future__ import annotations

import argparse
import json
import os
import resource
import sys
import time
import uuid
from pathlib import Path
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

NARROW = ("id bigint PRIMARY KEY, i integer, amt numeric(18,4), name varchar(64), "
          "ts timestamp(6), ok boolean, d date, u uuid")
TYPES = {"id": "bigint", "i": "integer", "amt": "numeric(18,4)", "name": "varchar(64)",
         "ts": "timestamp(6)", "ok": "boolean", "d": "date", "u": "uuid"}


def _rss_mb() -> float:
    """Peak RSS of this process image, in MB.

    ``VmHWM`` belongs to the process's own address space, so it starts afresh
    at ``exec``. ``ru_maxrss`` does not: on Linux a child started by a large
    parent (pytest) inherits the parent's high-water mark through ``fork``.
    """
    try:
        for line in Path("/proc/self/status").read_text(encoding="ascii").splitlines():
            if line.startswith("VmHWM:"):
                return int(line.split()[1]) / 1024.0
    except OSError:
        pass
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0


def _connect(dsn: str):
    import psycopg2

    u = urlparse(dsn)
    return psycopg2.connect(host=u.hostname or "127.0.0.1", port=u.port or 5432,
                            dbname=(u.path or "/").lstrip("/"), user=u.username, password=u.password or "")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--modes", default="R,H")
    parser.add_argument("--segment-rows", type=int, default=50_000)
    parser.add_argument("--unlogged", action="store_true",
                        help="UNLOGGED tables (no WAL): for machines short on disk; reads are unchanged")
    args = parser.parse_args()
    os.environ.setdefault("DATAWRAP_PARITY_SCALE", "1")
    dsn = os.environ.get("DATAFLOW_PG_TEST_DSN") or "postgresql://dataflow:dataflow@127.0.0.1:5432/dataflow"

    from services.parity_scale.dialects.postgresql import PostgresSide
    from services.parity_scale.driver import ParityScan, ScanConfig
    from services.parity_scale.plan import build_plan
    from services.parity_scale.store import MemoryScanStore

    n = int(args.rows)
    src = f"psd1_bench_s_{uuid.uuid4().hex[:8]}"
    dst = f"psd1_bench_d_{uuid.uuid4().hex[:8]}"
    admin = _connect(dsn)
    admin.autocommit = True
    changed = sorted({7, n // 3, n // 2, n - 5, 1 + n // 7, 2 + n // 5, 3 + n // 11, 4 + n // 13, n // 17, n // 19})
    missing, extra = n // 4, n + 1000
    report: dict = {"rows": n, "unlogged": bool(args.unlogged), "table": "narrow (8 columns)", "faults": {
        "changed": changed, "missing": [missing], "extra": [extra]}, "runs": []}
    rss_after_imports = _rss_mb()
    try:
        with admin.cursor() as cur:
            t0 = time.monotonic()
            kind = "UNLOGGED TABLE" if args.unlogged else "TABLE"
            cur.execute(f'CREATE {kind} "{src}" ({NARROW})')
            cur.execute(
                f'INSERT INTO "{src}" SELECT g, (g * 7) % 100000, round((g * 1.37)::numeric, 4), '
                "'name ' || g, timestamp '2024-01-01' + g * interval '1.234567 second', g % 2 = 0, "
                f"date '2000-01-01' + g % 9000, md5(g::text)::uuid FROM generate_series(1, {n}) g"
            )
            cur.execute(f'CREATE {kind} "{dst}" ({NARROW})')
            cur.execute(f'INSERT INTO "{dst}" SELECT * FROM "{src}"')
            cur.execute(f'UPDATE "{dst}" SET i = i + 1 WHERE id = ANY(%s)', (changed,))
            cur.execute(f'DELETE FROM "{dst}" WHERE id = %s', (missing,))
            cur.execute(f'INSERT INTO "{dst}" SELECT {extra}, i, amt, name, ts, ok, d, u FROM "{dst}" WHERE id = 1')
            cur.execute(f'VACUUM ANALYZE "{src}"')
            cur.execute(f'VACUUM ANALYZE "{dst}"')
            report["load_seconds"] = round(time.monotonic() - t0, 2)
            cur.execute("SELECT pg_size_pretty(pg_total_relation_size(%s) + pg_total_relation_size(%s))", (src, dst))
            report["disk_both_tables"] = cur.fetchone()[0]
        plan = build_plan([{"source": c, "target": c} for c in TYPES], source_types=TYPES, dest_types=TYPES,
                          source_engine="postgresql", dest_engine="postgresql", key_columns=["id"])

        def open_side(role, plan_):
            return PostgresSide(role=role, plan=plan_, table=src if role == "source" else dst,
                                connection=_connect(dsn))

        for mode in [m.strip() for m in args.modes.split(",") if m.strip()]:
            cfg = ScanConfig(mode=mode, segment_rows=args.segment_rows, statement_timeout_s=600)
            started = time.monotonic()
            scan = ParityScan.start(plan, store=MemoryScanStore(), open_side=open_side, config=cfg)
            scan.run()
            seconds = time.monotonic() - started
            ev = scan.result()
            diff = ev["differences"]
            got = {
                "changed": sorted(int(k[0]["$i"]) for k in diff["changed_keys"]),
                "missing": sorted(int(k[0]["$i"]) for k in diff["missing_keys"]),
                "extra": sorted(int(k[0]["$i"]) for k in diff["extra_keys"]),
            }
            report["runs"].append({
                "mode": mode,
                "seconds": round(seconds, 2),
                "rows_per_second_per_side": int(n / seconds) if seconds else None,
                "statements": ev["statements"],
                "units": ev["units"],
                "outcome": ev["outcome"],
                "complete": ev["coverage"]["complete"],
                "faults_found_exactly": got == report["faults"],
                "found": got,
                "peak_rss_mb_process": round(_rss_mb(), 1),
            })
    finally:
        with admin.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{src}"')
            cur.execute(f'DROP TABLE IF EXISTS "{dst}"')
        admin.close()
    report["rss_mb_after_imports"] = round(rss_after_imports, 1)
    report["peak_rss_mb"] = round(_rss_mb(), 1)
    print(json.dumps(report, indent=1))
    ok = all(r["faults_found_exactly"] and r["complete"] for r in report["runs"])
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
