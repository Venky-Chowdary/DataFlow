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
                    # Mark remaining gates as skipped so the UI still shows a
                    # complete list instead of "passed then failed" confusion.
                    for skipped_id, _ in PREFLIGHT_GATES[i + 1:]:
                        results.append(
                            GateResult(
                                gate_id=skipped_id,
                                status=GateStatus.SKIP,
                                message="Skipped — earlier gate blocked the transfer",
                                details={"skipped_after": result.gate_id.value},
                            )
                        )
                    break

        passed = len(blockers) == 0 and all(
            r.status in (GateStatus.PASS, GateStatus.SKIP) for r in results
        )
        return PreflightResult(passed=passed, gates=results, blockers=blockers)
