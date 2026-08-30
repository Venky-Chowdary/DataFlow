"""One command to re-run the SQL scale matrix and write the evidence.

    cd apps/api && DATAFLOW_SCALE_MATRIX=1 PYTHONPATH=. python -m tests.scale.run_matrix

Options: ``--rows`` (default 100000), ``--engines pg,mysql,...``, ``--out``
(JSON evidence), ``--markdown`` (result table for ``docs/SCALE_MATRIX_SQL.md``).
Engines that do not answer are reported as ``skip`` with the exact reason —
never as a pass, and never omitted.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Sequence

from tests.scale.fixture import ENGINES
from tests.scale.matrix import (
    BLOCKING_SHAPES,
    DATA_SHAPES,
    MODES,
    ROWS_DEFAULT,
    SLOW_ROWS_PER_SEC,
    Cell,
    run_matrix,
)


def markdown_table(report: dict[str, Any]) -> str:
    head = (
        "| source | destination | mode | shape | status | src rows | dest rows | "
        "expected | checksum | rejected | quarantined | coerced NULL | rows/sec | "
        "elapsed s | run id |\n"
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | "
        "--- | --- | --- |\n"
    )
    lines = []
    for cell in report["cells"]:
        chk = cell["checksum_match"]
        chk_text = "n/a" if chk is None else ("match" if chk else "MISMATCH")
        lines.append(
            "| {source} | {destination} | {mode} | {shape} | {status} | {source_rows} "
            "| {dest_rows} | {expected_rows} | {chk} | {rejected} | {quarantined} | "
            "{coerced_null} | {rps} | {elapsed} | `{run}` |".format(
                chk=chk_text,
                rps=f"{cell['rows_per_sec']:,.0f}" if cell["rows_per_sec"] else "-",
                elapsed=cell["elapsed_seconds"],
                run=(cell["run_ids"] or ["-"])[0],
                **{
                    k: cell[k]
                    for k in (
                        "source",
                        "destination",
                        "mode",
                        "shape",
                        "status",
                        "source_rows",
                        "dest_rows",
                        "expected_rows",
                        "rejected",
                        "quarantined",
                        "coerced_null",
                    )
                },
            )
        )
    return head + "\n".join(lines) + "\n"


def _progress(cell: Cell) -> None:
    print(
        f"[{time.strftime('%H:%M:%S')}] {cell.source}->{cell.destination} "
        f"{cell.mode}/{cell.shape}: {cell.status.upper()} "
        f"dest={cell.dest_rows}/{cell.expected_rows} "
        f"{cell.rows_per_sec:,.0f} rows/s"
        + (f" — {cell.reason}" if cell.status != "pass" else ""),
        flush=True,
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=ROWS_DEFAULT)
    parser.add_argument("--engines", default=",".join(ENGINES))
    parser.add_argument("--modes", default=",".join(MODES))
    parser.add_argument("--shapes", default=",".join(BLOCKING_SHAPES))
    parser.add_argument("--data-shapes", default=",".join(DATA_SHAPES))
    parser.add_argument("--prefix", default="scale")
    parser.add_argument("--out", default="")
    parser.add_argument("--markdown", default="")
    args = parser.parse_args(argv)

    if os.environ.get("DATAFLOW_SCALE_MATRIX") != "1":
        print("DATAFLOW_SCALE_MATRIX=1 is required (live engines are mutated).")
        return 2

    report = run_matrix(
        rows=args.rows,
        engines=[e for e in args.engines.split(",") if e],
        modes=[m for m in args.modes.split(",") if m],
        shapes=[s for s in args.shapes.split(",") if s],
        data_shapes=[s for s in args.data_shapes.split(",") if s],
        prefix=args.prefix,
        progress=_progress,
    )
    print(
        f"\npass={report['pass']} fail={report['fail']} skip={report['skip']} "
        f"slow(<{SLOW_ROWS_PER_SEC} rows/s)={len(report['slow_routes'])}"
    )
    for name, reason in report["skipped_engines"].items():
        print(f"skip {name}: {reason}")
    if args.out:
        Path(args.out).write_text(json.dumps(report, indent=2, default=str))
        print(f"evidence: {args.out}")
    if args.markdown:
        Path(args.markdown).write_text(markdown_table(report))
        print(f"table: {args.markdown}")
    return 1 if report["fail"] else 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
