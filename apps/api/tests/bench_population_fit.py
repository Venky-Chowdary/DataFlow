"""Measure the pre-write scan cost on a 1M-row file (not a test — a measurement).

Run: python tests/bench_population_fit.py
"""

from __future__ import annotations

import csv
import io
import time

from services.population_fit_scan import scan_population_fit
from transfer.file_stream import iter_source_rows

COLUMNS = ["year", "month", "day", "dep_time", "arr_time", "carrier", "tailnum"]


def build(rows: int) -> bytes:
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=COLUMNS)
    w.writeheader()
    for i in range(rows):
        w.writerow(
            {
                "year": 2026,
                "month": (i % 12) + 1,
                "day": (i % 28) + 1,
                "dep_time": "12.34567890",
                "arr_time": "9999.99999999" if i == 431 else "12.34567890",
                "carrier": "AA",
                "tailnum": f"N{i:06d}",
            }
        )
    return buf.getvalue().encode("utf-8")


def main() -> None:
    rows = 1_000_000
    t0 = time.perf_counter()
    content = build(rows)
    t1 = time.perf_counter()
    print(f"build {rows} rows ({len(content)/1e6:.1f} MB): {t1 - t0:.1f}s")

    mappings = [
        {"source": c, "target": c, "target_type": "NUMBER(11,8)"}
        for c in ("dep_time", "arr_time")
    ]
    t2 = time.perf_counter()
    report = scan_population_fit(
        iter_source_rows(content, "flights-1m.csv"),
        mappings,
        source_types={c: "DECIMAL(12,9)" for c in COLUMNS},
        dest_types={c: "NUMBER(11,8)" for c in ("dep_time", "arr_time")},
        dest_db="snowflake",
        job_error_policy="fail",
        rows_total=rows,
        rows_are_population=True,
    )
    t3 = time.perf_counter()
    print(
        f"scan: {t3 - t2:.1f}s  evidence={report.evidence} "
        f"rows={report.rows_scanned} findings={report.unfit_rows} "
        f"({report.rows_scanned / max(t3 - t2, 1e-9):,.0f} rows/s)"
    )


if __name__ == "__main__":
    main()
