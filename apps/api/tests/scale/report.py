"""Render the measured matrix JSONL into the markdown tables of the evidence doc.

The doc must never be hand-typed: every number in it comes from a run record
written by :mod:`tests.scale.file_matrix`. Regenerate with::

    python -m tests.scale.report            # reads exports/scale/file_matrix.jsonl
    python -m tests.scale.report --results /path/to.jsonl --out /path/to.md

Later records win, so a re-run of a fixed cell supersedes its earlier failure.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

_API_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_RESULTS = _API_ROOT / "exports" / "scale" / "file_matrix.jsonl"


def load(path: Path) -> list[dict[str, Any]]:
    """Latest record per cell name, in first-seen order."""
    order: list[str] = []
    latest: dict[str, dict[str, Any]] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        rec = json.loads(line)
        name = str(rec.get("name", ""))
        if name not in latest:
            order.append(name)
        latest[name] = rec
    return [latest[name] for name in order]


def _short(status: str) -> str:
    return status if len(status) <= 90 else status[:87] + "..."


def _skipped(rec: dict[str, Any]) -> bool:
    return str(rec.get("status", "")).startswith("skip")


def _rate(rec: dict[str, Any]) -> str:
    rate = rec.get("rows_per_second") or 0
    if _skipped(rec) or not rate:
        return "—"
    return f"{rate:,.0f}"


def result_table(records: list[dict[str, Any]]) -> str:
    head = (
        "| cell | route | store | source → dest | mode | status | src rows | "
        "dest rows (independent) | engine claim | rejected | quarantined | "
        "coerced null | skipped | checksum | secs | rows/s | run id |\n"
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | "
        "--- | --- | --- | --- | --- | --- |\n"
    )
    rows = []
    for r in records:
        if r.get("checksum_match"):
            checksum = "match"
        elif _skipped(r) or not r.get("checksum_expected"):
            checksum = "—"
        else:
            checksum = "MISMATCH"
        rows.append(
            "| {name} | {route} | {store} | {src} → {dst} | {mode} | {status} | "
            "{srows} | {drows} | {claim} | {rej} | {qua} | {nul} | {skp} | "
            "{chk} | {secs} | {rate} | `{run}` |".format(
                name=r.get("name", ""),
                route=r.get("route", ""),
                store=r.get("store", ""),
                src=r.get("source", ""),
                dst=r.get("destination", ""),
                mode=r.get("sync_mode", ""),
                status=_short(str(r.get("status", ""))),
                srows=f"{r.get('source_rows', 0):,}",
                drows=f"{r.get('dest_rows_independent', 0):,}",
                claim=f"{r.get('engine_rows_claimed', 0):,}",
                rej=r.get("rejected", 0),
                qua=r.get("quarantined", 0),
                nul=r.get("coerced_null", 0),
                skp=r.get("skipped", 0),
                chk=checksum,
                secs=r.get("elapsed_seconds", 0),
                rate=_rate(r),
                run=r.get("run_id", "") or "n/a",
            )
        )
    return head + "\n".join(rows) + "\n"


def detail_sections(records: list[dict[str, Any]]) -> str:
    out = []
    for r in records:
        lines = [f"### {r.get('name')} — {_short(str(r.get('status', '')))}", ""]
        if r.get("verification"):
            lines.append(f"- verification: {r['verification']}")
        if r.get("checksum_expected") or r.get("checksum_dest"):
            lines.append(
                f"- checksum expected `{r.get('checksum_expected', '')}` / "
                f"destination `{r.get('checksum_dest', '')}`"
            )
        if r.get("null_tokens"):
            lines.append(f"- null spelling tally: `{json.dumps(r['null_tokens'])}`")
        if r.get("schema"):
            lines.append(f"- schema: `{json.dumps(r['schema'])}`")
        if r.get("engine_reconciliation"):
            lines.append(
                f"- engine reconciliation: `{json.dumps(r['engine_reconciliation'])}`"
            )
        for note in r.get("notes", []):
            lines.append(f"- note: {note}")
        out.append("\n".join(lines))
    return "\n\n".join(out) + "\n"


def summary(records: list[dict[str, Any]]) -> tuple[int, int, int, int]:
    """``(pass, pass-by-refusal, fail, skip)``.

    A refused cell is counted apart from a transferred one: both are the
    product behaving, but only one moved rows, and reading them as one number
    is how a matrix claims a population it never wrote.
    """
    p = sum(1 for r in records if r.get("status") == "pass")
    refused = sum(1 for r in records if str(r.get("status", "")).startswith("pass (refused"))
    f = sum(1 for r in records if r.get("status") == "fail")
    s = sum(1 for r in records if str(r.get("status", "")).startswith("skip"))
    return p, refused, f, s


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--results", default=str(DEFAULT_RESULTS))
    ap.add_argument("--out", default="")
    args = ap.parse_args()
    records = load(Path(args.results))
    p, refused, f, s = summary(records)
    body = (
        f"<!-- generated by `python -m tests.scale.report` from {Path(args.results).name} -->\n\n"
        f"pass={p} pass-by-refusal={refused} fail={f} skip={s} cells={len(records)}\n\n"
        + result_table(records)
        + "\n## Per-cell detail\n\n"
        + detail_sections(records)
    )
    if args.out:
        Path(args.out).write_text(body, encoding="utf-8")
        print(
            f"wrote {args.out}: pass={p} pass-by-refusal={refused} "
            f"fail={f} skip={s}"
        )
    else:
        print(body)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
