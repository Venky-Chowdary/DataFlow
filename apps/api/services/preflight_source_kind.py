"""Preflight decisions that do not belong in the preflight god module.

Source kind decides whether a blank cell is SQL NULL. Sample date agreement
decides the one locale a file may adopt when the operator left it on Auto.
Both are pure: they do not run gates or touch a destination.
"""

from __future__ import annotations

from typing import Any


def destination_nullability(columns: Any) -> dict[str, bool]:
    """Live/create-new nullability. Missing columns stay unknown (nullable)."""
    out: dict[str, bool] = {}
    for col in columns or []:
        name = getattr(col, "name", None)
        if not name:
            continue
        out[str(name)] = bool(getattr(col, "nullable", True))
    return out


def resolve_preflight_source_kind(
    source_kind: str | None,
    *,
    source_connector_id: str | None = None,
    source_file_id: str | None = None,
) -> str:
    """File blanks become SQL NULL only for an upload, never for a connector.

    The request model defaults ``source_kind`` to ``file``. A saved source
    connector must not inherit that default, even when a stale upload id is
    sent with it: Validate would accept empty integers that the database
    writer still rejects.
    """
    kind = (source_kind or "").strip().lower()
    connector = (source_connector_id or "").strip()
    # The upload id is part of the call so every router can pass the id it
    # already holds. It is not a selector: a stale upload cannot keep the
    # spreadsheet NULL rule on a connector, and it cannot turn an explicit
    # database kind into a file.
    _ = source_file_id
    if not kind:
        kind = "database" if connector else "file"
    if connector and kind == "file":
        return "database"
    return kind or "file"


def agreed_sample_date_locale(sample_rows: list[dict], columns: list[str]) -> str:
    """One inferred date order when every column agrees and none stay ambiguous.

    A settler in ``dob`` must not rewrite an unrelated column whose slash
    dates are still ambiguous. Empty means the operator locale, or Auto, stays.
    """
    from services.transform_engine import ambiguous_date_columns, infer_date_locale

    per_column = {
        col: infer_date_locale(sample_rows, [col]) for col in columns
    }
    agreed = {loc for loc in per_column.values() if loc}
    if len(agreed) != 1:
        return ""
    if ambiguous_date_columns(sample_rows, columns):
        return ""
    return next(iter(agreed))
