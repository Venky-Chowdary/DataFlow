"""CI throughput smoke: every database route at a fixed size, against floors.

Usage (from ``apps/api``, ``PYTHONPATH=.``)::

    python -m benchmarks.throughput_smoke --rows 200000 --out-dir /tmp/throughput

Runs ``benchmarks.db_throughput`` once per route in ``throughput_floors.json``
(each in its own process, so each peak RSS is its own), writes one JSON per
route plus ``summary.json``, and exits non-zero when any route fails, its
independent cell check finds a difference, or its rows/s is below the route's
floor. Floors are set well under the measured rate (see the file's ``about``)
so a slower runner does not flap the gate while a real regression still trips
it, and scaled down by ``calibrate()`` (fastest of five samples before and
after each route) on a runner slower than the reference the floors were set
against (``reference_calibration_seconds``), by at most ``MIN_SCALE``.
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


def calibrate(rounds: int = 3) -> float:
    """Seconds for a fixed CPU-bound Python workload (best of ``rounds``).

    Hosted runners differ in speed by 2x from one job to the next. The
    workload is standard library only (regex, Decimal, json, str), so it
    measures the runner, not this code base.
    """
    import re
    import time
    from decimal import Decimal

    pattern = re.compile(r"-?[0-9]+(?:\.[0-9]+)?\Z")
    best = float("inf")
    for _ in range(rounds):
        started = time.perf_counter()
        acc = 0
        for i in range(60_000):
            text = f"{i * 7919 % 100000}.{i % 10000:04d}"
            if pattern.match(text):
                acc += int(Decimal(text).as_tuple().exponent)
            doc = json.dumps({"k": i, "s": text, "a": [i % 3, "x"]}, sort_keys=True)
            acc += len(json.loads(doc)["s"]) + len(text.upper().strip())
        best = min(best, time.perf_counter() - started)
    return best


#: A runner may scale a floor down by at most this much. Without a bound one
#: noisy calibration sample could lower a floor below the rate of the code
#: before the per-column fast paths, which is the regression the gate exists for.
MIN_SCALE = 0.75


def effective_floor(floor: float, reference: float, measured: float) -> float:
    """The floor scaled down for a runner slower than the reference, never up,
    and never below ``MIN_SCALE`` of itself."""
    if not reference or not measured or measured <= reference:
        return floor
    return floor * max(MIN_SCALE, reference / measured)


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
    reference = float(floors.get("reference_calibration_seconds") or 0)
    summary: dict = {"rows": args.rows, "routes": {}, "failures": [],
                     "reference_calibration_seconds": reference, "min_scale": MIN_SCALE}
    for name in routes:
        entry_cfg = floors["routes"].get(name, {})
        # Calibrate around the route: the fastest of several samples just before
        # and just after it is the runner's speed with the least neighbour noise.
        before = calibrate(rounds=5)
        out = out_dir / f"{name}.json"
        cmd = [
            sys.executable, "-m", "benchmarks.db_throughput",
            "--route", str(entry_cfg.get("route") or name),
            "--rows", str(args.rows), "--out", str(out), "--label", f"ci-smoke-{name}",
        ]
        if entry_cfg.get("row_path"):
            cmd.append("--row-path")
        proc = subprocess.run(cmd, cwd=str(_HERE.parent), capture_output=True, text=True)  # noqa: S603
        after = calibrate(rounds=5)
        measured = min(before, after)
        floor = round(effective_floor(
            float(entry_cfg.get("min_rows_per_second", 0)), reference, measured), 1)
        report = json.loads(out.read_text(encoding="utf-8")) if out.exists() else {}
        failures = list(report.get("failures") or ([] if proc.returncode == 0 else ["no report"]))
        rate = report.get("rows_per_second")
        if rate is not None and float(rate) < floor:
            failures.append(f"rows/s {rate} is below the floor {floor}")
        entry = {
            "exit": 1 if failures else 0,
            "calibration_seconds": round(measured, 4),
            "floor_rows_per_second": floor,
            "rows_per_second": rate,
            "wall_seconds": report.get("wall_seconds"),
            "verification_seconds": report.get("verification_seconds"),
            "peak_rss_mb": report.get("peak_rss_mb"),
            "independent_check_passed": (report.get("independent_check") or {}).get("passed"),
            "failures": failures,
        }
        summary["routes"][name] = entry
        if failures:
            summary["failures"].append(f"{name}: {', '.join(failures)}")
            if proc.returncode != 0:
                print(f"--- {name} stderr tail ---\n{(proc.stderr or '')[-3000:]}", file=sys.stderr)
        print(json.dumps({name: entry}))
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    if summary["failures"]:
        print("throughput smoke failed: " + "; ".join(summary["failures"]), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
