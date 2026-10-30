"""Preflight decisions that do not belong in the preflight god module.

Source kind decides whether a blank cell is SQL NULL. Sample date agreement
decides the one locale a file may adopt when the operator left it on Auto.
Both are pure: they do not run gates or touch a destination.
"""

from __future__ import annotations

from typing import Any


_RECORD_BATCH_FORMATS = frozenset({"s3", "gcs", "adls", "sftp"})


def source_is_record_batch(kind: str | None, db_type: str | None = "") -> bool:
    """A file or object store is a batch of records, not a database extract.

    S3→S3 was judged as a database extract, so one bad cast blocked the load
    instead of quarantining that row (R8-003). CSV uploads already took the
    file path. Object-store formats share it.
    """
    if (kind or "").strip().lower() == "file":
        return True
    return (db_type or "").strip().lower() in _RECORD_BATCH_FORMATS


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


def shape_typed_values(
    source_kind: str | None,
    source_type: str | None = "",
    *,
    source_connector_id: str | None = None,
) -> bool:
    """The value model a transform recipe runs under for this source.

    Execute's runner decides it from the source format
    (:func:`services.shape_apply.is_typed_value_source`); Validate and the
    Transform preview must decide it the same way, or a database source's ''
    and whitespace are previewed as NULL and written as values.
    """
    from services.shape_apply import is_typed_value_source

    kind = resolve_preflight_source_kind(
        source_kind, source_connector_id=source_connector_id
    )
    fmt = str(source_type or "").strip()
    if not fmt and (source_connector_id or "").strip():
        try:
            from services.connector_store import get_connector

            saved = get_connector(str(source_connector_id))
            fmt = str(getattr(saved, "type", "") or "") if saved else ""
        except Exception:  # noqa: BLE001 - an unreadable store is a file-rules preview
            fmt = ""
    if kind == "file" and not (source_connector_id or "").strip():
        return False
    return is_typed_value_source(fmt)
