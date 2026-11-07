"""Audit Map's risk-contract verdict against the engine's loss verdict.

Map may be stricter than the engine — an extra Risk Contract is friction, not
loss. The reverse is a dead end: Map offers Approve, then Validate refuses the
route with nothing on the row to sign. This exits non-zero on any such pair.

    python scripts/carrier_parity_audit.py [--dest-db postgresql]
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "apps" / "api"))

from services.type_system import is_lossy_coercion

CARRIERS = (
    "TEXT",
    "VARCHAR(255)",
    "VARCHAR",
    "CHAR(10)",
    "CHAR(20)",
    "NCHAR(10)",
    "VARCHAR2(30)",
    "NVARCHAR(100)",
    "LONGTEXT",
    "INTEGER",
    "BIGINT",
    "SMALLINT",
    "DECIMAL(10,2)",
    "DECIMAL(38,15)",
    "DECIMAL(10,0)",
    "NUMERIC(38,9)",
    "DOUBLE PRECISION",
    "REAL",
    "FLOAT",
    "BOOLEAN",
    "DATE",
    "TIMESTAMP",
    "TIMESTAMPTZ",
    "TIME",
    "JSON",
    "JSONB",
    "VARIANT",
    "BYTEA",
    "BLOB",
)


def map_verdicts(pairs: list[tuple[str, str]]) -> dict[tuple[str, str], bool]:
    """Run the web-side probe once and read back one verdict per pair."""
    proc = subprocess.run(
        ["npx", "tsx", "scripts/carrier_parity_probe.ts"],
        cwd=REPO / "apps" / "web",
        input="\n".join(f"{s}|{t}" for s, t in pairs),
        capture_output=True,
        text=True,
        shell=sys.platform == "win32",
        check=False,
    )
    if proc.returncode != 0:
        sys.stderr.write(proc.stdout[-4000:] + proc.stderr[-4000:])
        raise SystemExit("carrier_parity_probe.ts failed")
    out: dict[tuple[str, str], bool] = {}
    for line in proc.stdout.splitlines():
        if line.count("|") == 2:
            src, tgt, verdict = line.split("|")
            out[(src, tgt)] = verdict.strip() == "true"
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dest-db", default="postgresql")
    args = ap.parse_args()

    pairs = [(s, t) for s in CARRIERS for t in CARRIERS if s != t]
    verdicts = map_verdicts(pairs)

    stricter: list[tuple[str, str]] = []
    dead_ends: list[tuple[str, str]] = []
    for pair in pairs:
        engine_lossy = bool(is_lossy_coercion(pair[0], pair[1], dest_db=args.dest_db))
        map_asks = verdicts.get(pair)
        if map_asks is None:
            continue
        if map_asks and not engine_lossy:
            stricter.append(pair)
        elif engine_lossy and not map_asks:
            dead_ends.append(pair)

    agree = len(pairs) - len(stricter) - len(dead_ends)
    print(f"dest_db={args.dest_db} pairs={len(pairs)} agree={agree}")
    print(f"\nMap asks for a contract, engine preserves ({len(stricter)}) — friction:")
    for src, tgt in stricter:
        print(f"  {src} -> {tgt}")
    print(f"\nEngine refuses, Map offers Approve ({len(dead_ends)}) — dead end:")
    for src, tgt in dead_ends:
        print(f"  {src} -> {tgt}")
    return 1 if dead_ends else 0


if __name__ == "__main__":
    raise SystemExit(main())
