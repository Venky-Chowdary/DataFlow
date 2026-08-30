"""One command to re-run the Track D matrix.

    cd apps/api
    DATAFLOW_SCALE_MODES=1 python -m tests.scale.run_matrix            # everything
    DATAFLOW_SCALE_MODES=1 python -m tests.scale.run_matrix cdc crash  # selected suites

Env gate: without ``DATAFLOW_SCALE_MODES=1`` nothing runs and the process exits
0 with a printed skip, so CI without engines cannot report a green it never
measured. ``DATAFLOW_SCALE_ROWS`` lowers the row count for local iteration —
every published number names the count it was measured at.
"""

from __future__ import annotations

import sys

from tests.scale import live_engines as L
from tests.scale.matrix import ARTIFACT_DIR, Matrix

SUITES = ("cdc", "crash", "batch", "scheduler")


def run(suites: tuple[str, ...] = SUITES, rows: int = L.SCALE_ROWS) -> Matrix:
    matrix = Matrix()
    if "cdc" in suites:
        from tests.scale import cdc_cells

        cdc_cells.postgres_cdc(matrix, rows)
        cdc_cells.mysql_cdc(matrix, rows)
        cdc_cells.mongo_cdc(matrix, rows)
    if "crash" in suites:
        from tests.scale import crash_cells

        crash_cells.crash_cells(matrix, rows)
    if "batch" in suites:
        from tests.scale import batch_cells

        batch_cells.batch_mode_cells(matrix, rows)
    if "scheduler" in suites:
        from tests.scale import scheduler_cells

        scheduler_cells.scheduler_cells(matrix, rows)
    return matrix


def main(argv: list[str]) -> int:
    if not L.enabled():
        print("skip: set DATAFLOW_SCALE_MODES=1 to run the live scale matrix")
        return 0
    suites = tuple(a for a in argv if a in SUITES) or SUITES
    print(f"scale matrix: suites={suites} rows={L.SCALE_ROWS}", flush=True)
    matrix = run(suites)
    print("\n" + matrix.markdown())
    counts = matrix.counts()
    # A partial run gets its own artifact: a single-suite re-run must not
    # overwrite the full-matrix evidence with a subset of the cells.
    name = (
        "scale_matrix_modes_schedules.json"
        if suites == SUITES
        else f"scale_matrix_{'_'.join(suites)}.json"
    )
    path = matrix.write_json(ARTIFACT_DIR / name)
    print(f"\ncounts={counts}\nartifact={path}")
    return 1 if counts["fail"] else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
