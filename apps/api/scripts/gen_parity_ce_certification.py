"""Generate the Parity at Scale CE1 certification file from a CE-matrix record.

The ``parity-ce-matrix`` CI job runs ``tests/test_parity_ce_matrix.py`` with
``DATAFLOW_PARITY_CE_RECORD`` pointing at a JSON-lines record: one line per
passing case (an engine × kind rendering, a projection, a pair copied by the
real writer and found equal with every fault located). This script folds the
record into ``apps/api/data/parity_ce_certification.json`` and the
certification table in ``docs/reference/parity-canonical-encoding.md``::

    python scripts/gen_parity_ce_certification.py --record ce.jsonl \\
        --run-id 1234567890 --out data/parity_ce_certification.json \\
        --doc ../../docs/reference/parity-canonical-encoding.md

Only cases that passed are in the record, so the file can only claim what a
live engine executed (design §4.4). ``--check`` exits non-zero when the doc
table does not match the JSON (the docs test runs it).
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import date
from pathlib import Path
from typing import Any

API = Path(__file__).resolve().parents[1]
DEFAULT_OUT = API / "data" / "parity_ce_certification.json"
DEFAULT_DOC = API.parents[1] / "docs" / "reference" / "parity-canonical-encoding.md"
BEGIN = "<!-- BEGIN GENERATED: parity-ce-certification (apps/api/scripts/gen_parity_ce_certification.py) -->"
END = "<!-- END GENERATED: parity-ce-certification -->"
FAMILIES = ("postgresql", "mysql", "mariadb", "sqlserver")


def family(server: str) -> str:
    for name in FAMILIES:
        if str(server).startswith(name):
            return name
    return str(server)


def build(record_lines: list[dict[str, Any]], *, run_id: str, run_date: str) -> dict[str, Any]:
    engines: dict[str, dict[str, Any]] = {}
    pairs: dict[str, dict[str, Any]] = {}
    for line in record_lines:
        kind = line.get("kind")
        if kind in {"kind", "projection"}:
            server = line["server"]
            entry = engines.setdefault(server, {"family": family(server), "kinds": {}, "projections": []})
            if kind == "kind":
                entry["kinds"][line["case"]] = {"ce_kind": line["ce_kind"], "ddl": line["ddl"],
                                                "values": int(line.get("values") or 0)}
            else:
                entry["projections"] = sorted({*entry["projections"], line["case"]})
        elif kind == "pair":
            name = f"{line['source_server']}->{line['destination_server']}"
            pairs[name] = {
                "source": line["source_server"],
                "destination": line["destination_server"],
                "source_family": family(line["source_server"]),
                "destination_family": family(line["destination_server"]),
                "kinds": sorted(line["kinds"]),
                "rows": int(line.get("rows") or 0),
            }
    for name, pair in pairs.items():
        # A pair is certified for a kind only if both engines' renderings of it
        # were certified on their own as well.
        src = {v["ce_kind"] for v in engines.get(pair["source"], {}).get("kinds", {}).values()}
        dst = {v["ce_kind"] for v in engines.get(pair["destination"], {}).get("kinds", {}).values()}
        pair["kinds"] = sorted(set(pair["kinds"]) & src & dst)
    return {
        "schema_version": 1,
        "ce_version": "CE1",
        "generated_by": "parity-ce-matrix",
        "matrix_run": run_id,
        "date": run_date,
        "about": "Engine x kind renderings and engine pairs a live CE-matrix run certified. "
                 "Read by services.parity_scale.certification; a kind or pair not listed is uncertified.",
        "engines": dict(sorted(engines.items())),
        "pairs": dict(sorted(pairs.items())),
    }


def doc_table(data: dict[str, Any]) -> str:
    lines = [BEGIN, ""]
    if not data.get("pairs"):
        lines += ["No engine pair is certified yet.", "", END]
        return "\n".join(lines)
    lines += [
        f"Certified by CE-matrix run `{data.get('matrix_run') or 'local'}` on {data.get('date')} "
        f"(CE version {data.get('ce_version')}). A kind not listed for a pair is compared by "
        "streamed fingerprint.",
        "",
        "| Source | Destination | Kinds compared in-engine |",
        "|---|---|---|",
    ]
    for pair in data["pairs"].values():
        lines.append(f"| {pair['source']} | {pair['destination']} | {', '.join(pair['kinds'])} |")
    lines += ["", END]
    return "\n".join(lines)


def splice(doc: str, table: str) -> str:
    if BEGIN in doc and END in doc:
        return re.sub(re.escape(BEGIN) + r".*?" + re.escape(END), lambda _m: table, doc, flags=re.S)
    return doc.rstrip() + "\n\n## Certification\n\n" + table + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--record")
    parser.add_argument("--run-id", default="")
    parser.add_argument("--date", default=date.today().isoformat())
    parser.add_argument("--out", default=str(DEFAULT_OUT))
    parser.add_argument("--doc", default=str(DEFAULT_DOC))
    parser.add_argument("--check", action="store_true", help="fail when the doc table is out of date")
    parser.add_argument("--print", action="store_true", help="print the JSON (CI logs)")
    args = parser.parse_args(argv)
    out = Path(args.out)
    if args.record:
        lines = [json.loads(x) for x in Path(args.record).read_text(encoding="utf-8").splitlines() if x.strip()]
        data = build(lines, run_id=args.run_id, run_date=args.date)
        out.write_text(json.dumps(data, indent=1, sort_keys=False) + "\n", encoding="utf-8")
    else:
        data = json.loads(out.read_text(encoding="utf-8"))
    if args.print:
        print("PARITY_CE_CERTIFICATION_BEGIN")
        print(json.dumps(data, indent=1))
        print("PARITY_CE_CERTIFICATION_END")
    doc = Path(args.doc)
    if doc.exists():
        text = doc.read_text(encoding="utf-8")
        wanted = splice(text, doc_table(data))
        if args.check:
            if wanted != text:
                print(f"{doc} certification table is out of date; regenerate it", file=sys.stderr)
                return 1
        elif wanted != text:
            doc.write_text(wanted, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
