"""Run the ≥1000 Pilot answer-quality corpus and print a compact report."""
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

_API = Path(__file__).resolve().parents[1]
if str(_API) not in sys.path:
    sys.path.insert(0, str(_API))

os.environ["DATAFLOW_PILOT_ENGINE"] = "local"

from tests.test_pilot_answer_corpus_wave45 import run_answer_corpus  # noqa: E402


def main() -> int:
    t0 = time.perf_counter()
    report = run_answer_corpus()
    elapsed = round(time.perf_counter() - t0, 2)
    report["elapsed_s"] = elapsed
    out = _API / "tests" / "_wave45_answer_report.json"
    out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(
        f"total={report['total']} passed={report['passed']} "
        f"failed={report['failed']} pass_rate={report['pass_rate']} "
        f"elapsed_s={elapsed}"
    )
    print("fail_by_family:", report["fail_by_family"])
    print("top_reasons:", report["top_reasons"][:15])
    print("report:", out)
    for f in report["failures"][:20]:
        print(f"  FAIL {f['family']}: {f['prompt']!r} -> {f['reasons']}")
        print(f"       A: {f['answer'][:120]}")
    return 0 if report["pass_rate"] >= 0.92 else 1


if __name__ == "__main__":
    raise SystemExit(main())
