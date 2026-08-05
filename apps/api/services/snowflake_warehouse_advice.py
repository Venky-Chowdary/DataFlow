"""Soft Snowflake warehouse sizing advisor from G7 volume estimates.

Informational only — never a GateId and never blocks Validate/Execute.
G7 remains local staging capacity; this helps operators pick XS–XL before a
cost-surprise cutover. Heuristics are intentionally conservative and honest.
"""

from __future__ import annotations

from typing import Any


def advise_snowflake_warehouse(
    *,
    estimated_bytes: int = 0,
    row_count: int = 0,
    current_warehouse: str = "",
) -> dict[str, Any] | None:
    """Return a soft warehouse size recommendation from estimated payload size.

    Thresholds are order-of-magnitude guides for COPY/INSERT cutovers, not
    Snowflake's official sizing calculator. Credits/time still depend on
    warehouse auto-suspend, clustering, and concurrency.
    """
    est = max(0, int(estimated_bytes or 0))
    rows = max(0, int(row_count or 0))
    if est <= 0 and rows <= 0:
        return None

    # Prefer bytes; fall back to a coarse row×128 estimate already used by G7.
    volume = est if est > 0 else rows * 128
    gib = volume / float(1024**3)

    if gib < 0.5:
        size, credits = "X-Small", "low"
        rationale = "Under ~0.5 GiB estimated — X-Small usually enough for a governed cutover."
    elif gib < 5:
        size, credits = "Small", "moderate"
        rationale = "Roughly 0.5–5 GiB — Small absorbs COPY bursts without long queue wait."
    elif gib < 50:
        size, credits = "Medium", "elevated"
        rationale = "Roughly 5–50 GiB — Medium reduces wall-clock; watch auto-suspend."
    elif gib < 250:
        size, credits = "Large", "high"
        rationale = "Roughly 50–250 GiB — Large recommended; stage + COPY still at-least-once safe."
    else:
        size, credits = "X-Large+", "very high"
        rationale = (
            "Over ~250 GiB estimated — X-Large+ and phased cutover; "
            "publish ThroughputBench before promising wall-clock."
        )

    return {
        "kind": "snowflake_warehouse_advice",
        "recommended_size": size,
        "credit_band": credits,
        "estimated_bytes": volume,
        "estimated_gib": round(gib, 3),
        "row_count": rows,
        "current_warehouse": (current_warehouse or "").strip() or None,
        "honesty": (
            "Soft advisory from volume estimates — not a Snowflake credit quote, "
            "not a new validation gate, and not a substitute for warehouse load history."
        ),
        "message": f"Suggested Snowflake warehouse: {size} ({rationale})",
        "rationale": rationale,
    }
