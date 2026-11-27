"""Write the schedule form's differential fixture from the scheduler's own next_run.

The web "Next 5 runs" preview (apps/web/src/lib/scheduleNextRuns.ts) must say
exactly what services/cron_schedule.next_run will do. This script builds a
seeded random corpus of cron expressions x time zones x start instants (DST
gaps and overlaps, Sunday as 0 and 7, Lord Howe's 30-minute shift, month-end
days) and records the scheduler's next five runs for each, or the refusal.

    python scripts/export_cron_next_runs_fixture.py            # write
    python scripts/export_cron_next_runs_fixture.py --check    # exit 1 if stale

tests/test_cron_preview_fixture.py runs --check, so a scheduler change that
the fixture does not reflect fails the build until it is regenerated.
"""

from __future__ import annotations

import json
import random
import sys
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve()
API = HERE.parents[1]
sys.path.insert(0, str(API))
FIXTURE = API.parents[1] / "apps" / "web" / "src" / "lib" / "__fixtures__" / "cron_next_runs.json"

from services.cron_schedule import CronError, next_run  # noqa: E402

ZONES = [
    "UTC", "America/New_York", "Europe/London", "Europe/Berlin", "Australia/Lord_Howe",
    "Australia/Sydney", "Asia/Kolkata", "Asia/Kathmandu", "America/Sao_Paulo", "Pacific/Chatham",
    "America/Santiago",
]
# Not in the corpus: Africa/Casablanca. Its future offsets differ between tz
# database releases (Python zoneinfo here, the browser's ICU there), so the
# two sides disagree on data, not on the rules this test checks.
# DST changes (and the days around them) plus ordinary dates.
ANCHORS = [
    "2026-11-01T04:50:00", "2026-11-01T05:10:00", "2026-03-08T06:30:00", "2026-03-29T00:30:00",
    "2026-10-25T00:10:00", "2026-04-05T14:45:00", "2026-10-03T15:20:00", "2026-04-04T14:50:00",
    "2026-01-31T22:59:00", "2026-02-28T23:30:00", "2027-02-27T12:00:00", "2026-12-31T23:58:00",
    "2026-06-15T09:07:00", "2026-09-06T03:00:00", "2026-04-05T03:00:00",
]
FIXED = [
    "0 0 * * 0,7", "0 0 * * 7", "0 0 * * 0", "*/15 * * * *", "30 2 * * *", "30 1 * * *", "0 2 * * *",
    "15 9 * * MON-FRI", "0 9 1 * MON", "0 0 31 * *", "0 0 29 2 *", "0 0 30 2 *", "*/7 * * * *",
    "0 12 * JAN-MAR SUN", "45 23 L * *", "0 0 * * 7-1", "5-10/2 */6 * * *", "0 0 1,15 * 1-5",
    "@daily", "0 0 * * * *", "61 * * * *", "0 0 * 13 *", "*/30 1-3 * * *", "0 */2 * * SAT,SUN",
]


def _rand_field(rng: random.Random, lo: int, hi: int, names: list[str] | None = None) -> str:
    kind = rng.random()
    if kind < 0.3:
        return "*"
    if kind < 0.45:
        return f"*/{rng.choice([2, 3, 5, 10, 15, 20, 30]) if hi >= 30 else rng.randint(1, max(1, hi // 2))}"
    if kind < 0.65:
        a = rng.randint(lo, hi)
        return str(a) if not names or rng.random() < 0.5 else names[a - lo]
    if kind < 0.85:
        a = rng.randint(lo, hi)
        b = rng.randint(a, hi)
        return f"{a}-{b}"
    picks = sorted({rng.randint(lo, hi) for _ in range(rng.randint(2, 3))})
    return ",".join(str(p) for p in picks)


def corpus(seed: int = 20261121, size: int = 260) -> list[tuple[str, str, str]]:
    rng = random.Random(seed)
    dow_names = ["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"]
    month_names = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
    cases: list[tuple[str, str, str]] = []
    for expr in FIXED:
        for zone in ("UTC", "America/New_York", "Australia/Lord_Howe", "Europe/London"):
            cases.append((expr, zone, rng.choice(ANCHORS)))
    for _ in range(size):
        minute = rng.choice(["0", "15", "30", "45", "*/15", "*/30", "5", _rand_field(rng, 0, 59)])
        hour = rng.choice(["*", "0", "1", "2", "3", "9", "23", _rand_field(rng, 0, 23)])
        dom = rng.choice(["*", "*", "*", "1", "15", "31", _rand_field(rng, 1, 31)])
        month = rng.choice(["*", "*", "*", _rand_field(rng, 1, 12, month_names)])
        dow = rng.choice(["*", "*", "0", "7", "1-5", "0,7", "SAT,SUN", _rand_field(rng, 0, 7, dow_names + ["SUN"])])
        expr = " ".join([minute, hour, dom, month, dow])
        cases.append((expr, rng.choice(ZONES), rng.choice(ANCHORS)))
    return cases


def expected(expr: str, zone: str, start: str) -> dict:
    after = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
    runs: list[str] = []
    try:
        cursor = after
        for _ in range(5):
            cursor = next_run(expr, cursor, zone)
            runs.append(cursor.strftime("%Y-%m-%dT%H:%M:%SZ"))
    except CronError as exc:
        return {"cron": expr, "zone": zone, "after": start + "Z", "error": type(exc).__name__, "runs": runs}
    return {"cron": expr, "zone": zone, "after": start + "Z", "runs": runs}


def build() -> list[dict]:
    return [expected(*case) for case in corpus()]


def main() -> int:
    rows = build()
    text = json.dumps({"generated_by": "apps/api/scripts/export_cron_next_runs_fixture.py", "cases": rows}, indent=0) + "\n"
    if "--check" in sys.argv:
        current = FIXTURE.read_text(encoding="utf-8") if FIXTURE.is_file() else ""
        if current != text:
            print(f"{FIXTURE} is stale: run {HERE.name}", file=sys.stderr)
            return 1
        return 0
    FIXTURE.write_text(text, encoding="utf-8")
    print(f"wrote {len(rows)} cases to {FIXTURE}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
