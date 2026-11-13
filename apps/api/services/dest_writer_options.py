"""One validator for a destination's file-writer options (G7-F2 / G7-F3).

``formula_protection`` (``"off"`` | ``"prefix"``) and ``tsv_null`` (``"empty"`` |
``"\\N"``) ride a destination's ``extra`` (or sit top-level on an endpoint dict).
An unknown value is refused when the request is made — every entry point that
accepts a destination or its extras (transfer JSON and multipart runs, plans,
contracts, preflight, shape and fidelity probes) calls this — never later, when
the writer meets it at run time.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from services.csv_safety import formula_protection_mode
from services.export_dialect import tsv_null_mode

#: Option -> vocabulary check (raises ``ValueError`` naming the option).
WRITER_OPTION_CHECKS: dict[str, Callable[[object], str]] = {
    "formula_protection": formula_protection_mode,
    "tsv_null": tsv_null_mode,
}


#: Object-store / file-server engines whose file format follows the object name.
_OBJECT_FILE_ENGINES = frozenset({
    "s3", "minio", "r2", "gcs", "gcp_storage", "adls", "azure_blob", "azure_blob_storage",
    "azure_data_lake", "azure_data_lake_storage", "sftp", "email", "smtp",
})


def _written_file_format(payload: dict[str, Any]) -> str:
    """The file format an endpoint dict writes, or ``""`` when it cannot be told here.

    file_export: its ``format``. An object store / SFTP / email: an explicit
    ``file_format``, else the object name's extension. A database or an
    ``extra``-only payload: unknown (the writer still refuses at run time).
    """
    nested = payload.get("extra") if isinstance(payload.get("extra"), dict) else {}
    kind = str(payload.get("kind") or "").strip().lower()
    fmt = str(payload.get("format") or payload.get("type") or "").strip().lower()
    if kind == "file_export":
        return fmt.lstrip(".")
    if fmt not in _OBJECT_FILE_ENGINES:
        return ""
    declared = str(payload.get("file_format") or nested.get("file_format") or "").strip().lower()
    if declared:
        return declared.lstrip(".")
    name = str(payload.get("table") or payload.get("collection") or payload.get("key") or "").strip().lower()
    return name.rsplit(".", 1)[-1] if "." in name.rsplit("/", 1)[-1] else ""


def validate_dest_writer_options(*payloads: Any) -> None:
    """Raise ``ValueError`` for an unknown or misplaced writer option in any payload.

    A payload is an endpoint dict (its own keys and its nested ``extra``) or an
    ``extra`` dict; anything else is ignored. Absent options are fine.
    ``tsv_null: "\\N"`` is refused when the payload names a file format other
    than TSV (a CSV/JSON file_export, an object named ``*.csv``).
    """
    from services.export_dialect import tsv_null_marker

    for payload in payloads:
        if not isinstance(payload, dict):
            continue
        nested = payload.get("extra")
        for scope in (payload, nested if isinstance(nested, dict) else {}):
            for key, check in WRITER_OPTION_CHECKS.items():
                value = scope.get(key)
                if value is not None:
                    check(value)
        mode = payload.get("tsv_null") or (nested.get("tsv_null") if isinstance(nested, dict) else None)
        fmt = _written_file_format(payload)
        if mode is not None and fmt:
            tsv_null_marker(fmt, mode)


def checked_dest_payload(value: Any) -> Any:
    """Pydantic ``field_validator`` body: the payload unchanged, or a ``ValueError`` (422)."""
    validate_dest_writer_options(value)
    return value


def refuse_invalid_dest_writer_options(*payloads: Any) -> None:
    """Router helper for payloads parsed by hand (multipart forms): 422 on an unknown option."""
    try:
        validate_dest_writer_options(*payloads)
    except ValueError as exc:
        from fastapi import HTTPException

        raise HTTPException(status_code=422, detail=str(exc)) from exc
