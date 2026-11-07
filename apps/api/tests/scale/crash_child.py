"""Child process that runs one CDC transfer so the parent can kill it.

Crash injection has to be a real process death: an in-process exception unwinds
the engine's ``finally`` blocks, which is exactly the cleanup a crash does not
get. ``SIGKILL`` on this child leaves the slot, the lease and the cursor in the
state a power loss would leave them, which is the state resume must cope with.

Not a test module and not run by pytest — it is spawned by ``crash_cells``.
"""

from __future__ import annotations

import json
import sys


def main(argv: list[str]) -> int:
    dialect, dest, snapshot_mode, job_id, tag = argv[0], argv[1], argv[2], argv[3], argv[4]
    from tests.scale.cdc_cells import CdcRoute

    route = CdcRoute("crash", dialect=dialect, dest=dest, tag=tag)
    assert route.job_slug == job_id, (route.job_slug, job_id)
    print("CHILD_READY", flush=True)
    result, elapsed, run_id = route.run(job_id, snapshot_mode=snapshot_mode)
    print(
        "CHILD_DONE "
        + json.dumps(
            {
                "success": bool(result.success),
                "records": int(result.records_transferred or 0),
                "elapsed": round(elapsed, 2),
                "run_id": run_id,
                "error": str(result.error or "")[:200],
            }
        ),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
