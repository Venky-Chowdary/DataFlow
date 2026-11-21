"""CI throughput smoke: every database route at a fixed size, against floors.

Usage (from ``apps/api``, ``PYTHONPATH=.``)::

    python -m benchmarks.throughput_smoke --rows 200000 --out-dir /tmp/throughput

Runs ``benchmarks.db_throughput`` once per route in ``throughput_floors.json``
(each in its own process, so each peak RSS is its own), writes one JSON per
route plus ``summary.json``, and exits non-zero when any route fails, its
independent cell check finds a difference, or its rows/s is below the route's
floor. Floors are set well under the measured rate (see the file's ``about``)
so a slower runner does not flap the gate while a real regression still trips
it.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
FLOORS = _HERE / "throughput_floors.json"


def load_floors(path: Path = FLOORS) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--rows", type=int, default=200_000)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--routes", default="", help="comma list; default: every floored route")
    args = parser.parse_args(argv)
    floors = load_floors()
    routes = [r for r in (args.routes.split(",") if args.routes else floors["routes"]) if r]
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    summary: dict = {"rows": args.rows, "routes": {}, "failures": []}
    for name in routes:
        entry_cfg = floors["routes"].get(name, {})
        floor = float(entry_cfg.get("min_rows_per_second", 0))
        out = out_dir / f"{name}.json"
        cmd = [
            sys.executable, "-m", "benchmarks.db_throughput",
            "--route", str(entry_cfg.get("route") or name),
            "--rows", str(args.rows), "--out", str(out), "--label", f"ci-smoke-{name}",
            "--min-rows-per-second", str(floor),
        ]
        if entry_cfg.get("row_path"):
            cmd.append("--row-path")
        route = name
        proc = subprocess.run(cmd, cwd=str(_HERE.parent), capture_output=True, text=True)  # noqa: S603
        report = json.loads(out.read_text(encoding="utf-8")) if out.exists() else {}
        entry = {
            "exit": proc.returncode,
            "floor_rows_per_second": floor,
            "rows_per_second": report.get("rows_per_second"),
            "wall_seconds": report.get("wall_seconds"),
            "verification_seconds": report.get("verification_seconds"),
            "peak_rss_mb": report.get("peak_rss_mb"),
            "independent_check_passed": (report.get("independent_check") or {}).get("passed"),
            "failures": report.get("failures") or ([] if proc.returncode == 0 else ["no report"]),
        }
        summary["routes"][route] = entry
        if proc.returncode != 0:
            summary["failures"].append(f"{route}: {', '.join(entry['failures'])}")
            tail = (proc.stderr or "")[-3000:]
            print(f"--- {route} stderr tail ---\n{tail}", file=sys.stderr)
        print(json.dumps({route: entry}))
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    if summary["failures"]:
        print("throughput smoke failed: " + "; ".join(summary["failures"]), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
