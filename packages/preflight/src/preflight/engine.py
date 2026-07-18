from __future__ import annotations

from preflight.gates import PREFLIGHT_GATES
from preflight.models import GateResult, GateStatus, PreflightContext, PreflightResult


class PreflightEngine:
    """Runs all preflight gates in order. Stops at first BLOCK — zero rows moved."""

    def __init__(self, fail_fast: bool = True):
        self.fail_fast = fail_fast

    def run(self, ctx: PreflightContext) -> PreflightResult:
        results: list[GateResult] = []
        blockers: list[GateResult] = []

        for i, (gate_id, gate_fn) in enumerate(PREFLIGHT_GATES):
            result = gate_fn(ctx)
            results.append(result)

            if result.status == GateStatus.BLOCK:
                blockers.append(result)
                if self.fail_fast:
                    break

        passed = len(blockers) == 0 and all(
            r.status in (GateStatus.PASS, GateStatus.SKIP) for r in results
        )
        return PreflightResult(passed=passed, gates=results, blockers=blockers)
