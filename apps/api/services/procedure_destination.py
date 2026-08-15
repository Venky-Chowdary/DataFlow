"""Destination stored-procedure hooks and row-apply — one dest-write owner.

Competitor facts (cite; do not conclude a product is "not good"):
- Informatica CDI SQL transformation (docs, current): connected row-by-row
  CALL with field→IN mapping; unconnected Target Pre-load / Target Post-load;
  optional Continue on SQL Error; SQLError + NumRowsAffected output fields.
- Databricks Lakeflow query-based connectors ingest tables via a cursor
  column. They do not CALL a destination stored procedure as the writer.
- Airbyte / Fivetran: table/stream writers. Dest SP is not a first-class sink.

DataFlow (stricter than Informatica continue-on-error):
- ``before_write`` / ``after_write`` hooks are explicit CALL/EXEC only.
- Row-apply maps declared binds onto row columns. Missing binds quarantine
  the row — never invent, never silent drop.
- A failed CALL quarantines the row and continues the batch (operator can
  replay). We do not offer Informatica "continue on SQL error" as success.
- CDC / SCD2 / mirror cannot use row-apply (procedure is not a table identity).
- SQLite / DuckDB / files / Iceberg / Kafka: dest procedure is refused.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Mapping

from services.procedure_source import (
    CALLABLE_REFUSED_SYNC_MODES,
    PROCEDURE_DIALECTS,
    QUERY_ONLY_DIALECTS,
    CallableSpec,
    ProcedureSourceError,
    compile_callable_sql,
    dialect_of,
    parse_callable_source,
    procedure_params_of,
    procedure_text_of,
)

HOOK_BEFORE = "before_write"
HOOK_AFTER = "after_write"
MODE_TABLE = "table"
MODE_HOOKS = "hooks"
MODE_ROW_APPLY = "row_apply"
DEST_PROCEDURE_MODES = frozenset({MODE_HOOKS, MODE_ROW_APPLY})

REASON_DEST_ENGINE = "dest_procedure_engine_refused"
REASON_DEST_CDC = "dest_procedure_refuses_history_sync"
REASON_UNBOUND = "dest_procedure_unbound_param"
REASON_CALL_FAILED = "dest_procedure_call_failed"
REASON_OK = "dest_procedure_applied"


class ProcedureDestinationError(ValueError):
    """Operator-visible dest-procedure refusal — never invent a CALL."""

    def __init__(self, message: str, *, reason: str = "") -> None:
        super().__init__(message)
        self.reason = reason or "dest_procedure_refused"


@dataclass(frozen=True)
class DestProcedurePlan:
    mode: str
    dialect: str
    row_spec: CallableSpec | None = None
    before_spec: CallableSpec | None = None
    after_spec: CallableSpec | None = None
    param_map: dict[str, str] = field(default_factory=dict)
    notes: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "dialect": self.dialect,
            "row_sql": self.row_spec.sql if self.row_spec else "",
            "row_identifier": self.row_spec.identifier if self.row_spec else "",
            "before_sql": self.before_spec.sql if self.before_spec else "",
            "after_sql": self.after_spec.sql if self.after_spec else "",
            "param_map": dict(self.param_map),
            "notes": list(self.notes),
        }


def dest_write_mode_of(dest: Any) -> str:
    extra = _extra(dest)
    raw = str(extra.get("dest_write_mode") or extra.get("dest_read_mode") or "").strip().lower()
    if raw in {MODE_ROW_APPLY, "procedure", "stored_procedure"}:
        return MODE_ROW_APPLY
    if raw == MODE_HOOKS or extra.get("dest_procedure_before") or extra.get("dest_procedure_after"):
        return MODE_HOOKS
    if extra.get("dest_procedure_call") or extra.get("dest_procedure"):
        return MODE_ROW_APPLY
    return MODE_TABLE


def dest_procedure_param_map(dest: Any) -> dict[str, str]:
    extra = _extra(dest)
    raw = extra.get("dest_procedure_param_map") or extra.get("procedure_param_map") or {}
    if not isinstance(raw, Mapping):
        return {}
    return {str(k).lstrip(":@"): str(v).strip() for k, v in raw.items() if str(v).strip()}


def dest_procedure_sync_refusal(sync_mode: str, dest: Any = None) -> str | None:
    """Row-apply cannot drive CDC/SCD2/mirror. Hooks on a table write are allowed."""
    mode = dest_write_mode_of(dest) if dest is not None else MODE_ROW_APPLY
    if mode != MODE_ROW_APPLY:
        return None
    from services.sync_cursor import normalize_sync_mode

    sync = normalize_sync_mode(sync_mode, default="")
    if sync in CALLABLE_REFUSED_SYNC_MODES:
        return (
            "Destination stored-procedure row-apply is a CALL per row, not a "
            "table identity. CDC / SCD2 / mirror would delete or version rows "
            "the procedure never listed — refuse. Use upsert/append, or table "
            "write with before/after hooks."
        )
    return None


def assert_dest_procedure_sync_allowed(sync_mode: str, dest: Any) -> None:
    reason = dest_procedure_sync_refusal(sync_mode, dest)
    if reason:
        raise ProcedureDestinationError(reason, reason=REASON_DEST_CDC)


def plan_dest_procedure(dest: Any) -> DestProcedurePlan | None:
    """Parse dest extra into a plan, or None when the dest is a normal table."""
    mode = dest_write_mode_of(dest)
    if mode == MODE_TABLE:
        return None
    dialect = dialect_of(dest)
    dialect_n = (dialect or "").strip().lower()
    if dialect_n in QUERY_ONLY_DIALECTS or dialect_n in {
        "csv",
        "json",
        "jsonl",
        "parquet",
        "iceberg",
        "kafka",
        "s3",
        "gcs",
        "mongodb",
        "dynamodb",
    }:
        raise ProcedureDestinationError(
            f"{dialect_n or 'This engine'} cannot execute a destination stored procedure.",
            reason=REASON_DEST_ENGINE,
        )
    if dialect_n and dialect_n not in PROCEDURE_DIALECTS and dialect_n != "generic_sql":
        raise ProcedureDestinationError(
            f"Destination stored procedure is not offered for '{dialect_n}'.",
            reason=REASON_DEST_ENGINE,
        )

    extra = _extra(dest)
    notes = [
        "Informatica Target Pre/Post-load maps to before_write / after_write.",
        "Failed CALL rows are quarantined — not Informatica continue-on-error.",
        "Binds come from declared param_map columns — never invented.",
    ]
    before = _parse_hook(extra.get("dest_procedure_before"), dialect_n, extra, HOOK_BEFORE)
    after = _parse_hook(extra.get("dest_procedure_after"), dialect_n, extra, HOOK_AFTER)
    row_spec = None
    param_map = dest_procedure_param_map(dest)
    if mode == MODE_ROW_APPLY:
        text = str(
            extra.get("dest_procedure_call")
            or extra.get("dest_procedure")
            or procedure_text_of(dest)
            or ""
        ).strip()
        if not text:
            raise ProcedureDestinationError(
                "Destination procedure write needs one CALL / EXEC."
            )
        try:
            row_spec = parse_callable_source(
                text,
                dialect=dialect_n,
                mode="procedure",
                params=_plan_params(text, dest, param_map),
            )
        except ProcedureSourceError as exc:
            raise ProcedureDestinationError(str(exc)) from exc
        if row_spec.verb == "SELECT" and dialect_n not in {
            "postgresql",
            "postgres",
            "pgvector",
            "redshift",
        }:
            raise ProcedureDestinationError(
                "Destination row-apply is CALL/EXEC. SELECT belongs on the source extract."
            )
    return DestProcedurePlan(
        mode=mode,
        dialect=dialect_n,
        row_spec=row_spec,
        before_spec=before,
        after_spec=after,
        param_map=param_map,
        notes=tuple(notes),
    )


def binds_for_row(
    row: Mapping[str, Any],
    *,
    param_map: Mapping[str, str],
    spec: CallableSpec,
) -> tuple[dict[str, Any], list[str]]:
    """Build CALL binds from a row. Returns (binds, missing_bind_names)."""
    binds: dict[str, Any] = {}
    missing: list[str] = []
    needed = set(spec.params.keys()) | set(param_map.keys())
    for name in sorted(needed):
        col = param_map.get(name) or name
        if col in row:
            binds[name] = row.get(col)
            continue
        # Case-insensitive column match — do not invent a value.
        hit = next((k for k in row if str(k).lower() == str(col).lower()), None)
        if hit is not None:
            binds[name] = row.get(hit)
            continue
        missing.append(name)
    return binds, missing


def apply_rows_via_procedure(
    dest: Any,
    records: list[dict[str, Any]],
    *,
    execute_call,
    plan: DestProcedurePlan | None = None,
) -> tuple[int, list[str], dict[str, Any]]:
    """CALL once per row in one dest session. Failed rows quarantine.

    ``execute_call(sql, binds)`` must run on the dest connection. It may
    raise; the row is quarantined and the batch continues.
    """
    planned = plan or plan_dest_procedure(dest)
    if planned is None or planned.mode != MODE_ROW_APPLY or planned.row_spec is None:
        raise ProcedureDestinationError("Destination is not a procedure row-apply.")
    sql, _ = compile_callable_sql(planned.row_spec)
    written = 0
    failed = 0
    quarantine: list[dict[str, Any]] = []
    ddl = [
        f"Dest procedure row-apply {planned.row_spec.identifier} "
        f"({len(records)} incoming rows; failed CALLs quarantine)"
    ]
    if planned.before_spec:
        execute_call(*compile_callable_sql(planned.before_spec))
        ddl.append(f"before_write {planned.before_spec.identifier}")
    for rec in records:
        if not isinstance(rec, dict):
            failed += 1
            quarantine.append(
                {
                    "stage": "dest_procedure",
                    "reason": REASON_UNBOUND,
                    "error": "Row is not an object — cannot bind CALL params.",
                    "source_record": rec,
                }
            )
            continue
        binds, missing = binds_for_row(
            rec, param_map=planned.param_map, spec=planned.row_spec
        )
        for key, val in (planned.row_spec.params or {}).items():
            if key not in binds and val is not None:
                binds[key] = val
                if key in missing:
                    missing.remove(key)
        if missing:
            failed += 1
            quarantine.append(
                {
                    "stage": "dest_procedure",
                    "reason": REASON_UNBOUND,
                    "error": f"CALL binds not present on row: {', '.join(missing)}",
                    "source_record": rec,
                    "missing_binds": missing,
                }
            )
            continue
        try:
            execute_call(sql, binds)
            written += 1
        except Exception as exc:
            failed += 1
            quarantine.append(
                {
                    "stage": "dest_procedure",
                    "reason": REASON_CALL_FAILED,
                    "error": str(exc)[:500],
                    "source_record": rec,
                    "sql": sql,
                }
            )
    if planned.after_spec:
        execute_call(*compile_callable_sql(planned.after_spec))
        ddl.append(f"after_write {planned.after_spec.identifier}")
    summary = {
        "ok": failed == 0,
        "dest_write_mode": MODE_ROW_APPLY,
        "dest_procedure": planned.row_spec.identifier,
        "rows_written": written,
        "rows_affected": written,
        "sql_error": failed > 0,
        "sql_error_count": failed,
        "quarantine_count": len(quarantine),
        "delivery_semantics": "at_least_once_dest_procedure_call",
        "exactly_once_claimed_platform": False,
        "notes": list(planned.notes),
    }
    if quarantine:
        summary["quarantine"] = quarantine
    return written, ddl, summary


def run_dest_hook(spec: CallableSpec | None, execute_call) -> dict[str, Any]:
    if spec is None:
        return {"ran": False}
    sql, binds = compile_callable_sql(spec)
    execute_call(sql, binds)
    return {"ran": True, "identifier": spec.identifier, "sql": sql}


def _parse_hook(
    text: Any,
    dialect: str,
    extra: Mapping[str, Any],
    hook: str,
) -> CallableSpec | None:
    raw = str(text or "").strip()
    if not raw:
        return None
    params = extra.get("dest_procedure_params") if isinstance(extra.get("dest_procedure_params"), Mapping) else {}
    try:
        spec = parse_callable_source(raw, dialect=dialect, mode="procedure", params=params or {})
    except ProcedureSourceError as exc:
        raise ProcedureDestinationError(str(exc)) from exc
    if spec.verb == "SELECT" and dialect not in {"postgresql", "postgres", "pgvector", "redshift"}:
        raise ProcedureDestinationError(
            f"{hook} must be CALL/EXEC — a SELECT hook is not a dest procedure."
        )
    return spec


_BIND_RE = re.compile(r"(?<!:):([A-Za-z_][A-Za-z0-9_]*)|@([A-Za-z_][A-Za-z0-9_]*)")


def _plan_params(text: str, dest: Any, param_map: Mapping[str, str]) -> dict[str, Any]:
    """Parse-time placeholders. Real values come from each row or dest extra."""
    names = [a or b for a, b in _BIND_RE.findall(text or "")]
    out: dict[str, Any] = {n: None for n in names if n}
    for key in param_map:
        out.setdefault(key, None)
    out.update(procedure_params_of(dest))
    return out


def _extra(dest: Any) -> dict[str, Any]:
    if dest is None:
        return {}
    if hasattr(dest, "extra"):
        extra = getattr(dest, "extra", None) or {}
        return dict(extra) if isinstance(extra, Mapping) else {}
    if isinstance(dest, Mapping):
        nested = dest.get("extra") if isinstance(dest.get("extra"), Mapping) else {}
        return {**dict(nested or {}), **{k: v for k, v in dest.items() if k != "extra"}}
    return {}
