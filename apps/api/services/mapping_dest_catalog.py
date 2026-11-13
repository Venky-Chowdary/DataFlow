"""The destination column catalog the Map step picks targets from.

Map used to show the destination as a bare list of names, so remapping
``order_id`` onto ``legacy_order_ref`` meant typing a name and hoping. The
operator needs what Validate already knows: each column's type, whether it is
nullable, its default, whether it is part of the key, and which columns the
write cannot leave empty.

One builder turns an ``introspect_endpoint`` reading into that catalog, so the
picker, the destination panel and Validate's G14 gate read the same catalog.
"Required" is computed by :func:`unfilled_required_columns` with no mappings —
the exact predicate G14 blocks on — never a second copy of the rule.

Honesty rules:

* Nullability that was not read is ``None``, never ``True``. A column whose
  nullability is unmeasured cannot be called optional, and the catalog says
  ``nullability_measured: false`` so the panel shows "unmeasured", not "safe".
* ``can_add_columns`` is the same decision Validate's DDL gate makes: a new
  target name is acceptable on a missing table (CREATE), on a schemaless store,
  or on an existing table whose schema policy permits ADD COLUMN. A policy that
  forbids it (``pause_on_change`` / ``type_locked``) makes a new name an error,
  so the picker must not offer "will create column".
"""

from __future__ import annotations

import re
from typing import Any

from services.destination_requirements_gate import unfilled_required_columns
from services.log_redaction import redact_log_text

CATALOG_VERSION = "dest_column_catalog.v1"

#: Schema policies under which the writer refuses ADD COLUMN (batch_progress).
_NO_ADD_POLICIES = frozenset({"pause_on_change", "type_locked"})

#: Marker ``endpoint_intelligence._attach_db_sample`` appends when its schema read failed.
_SOFT_FAILED_PROBE = "schema probe:"

_ABS_PATH = re.compile(r"(?:[A-Za-z]:)?(?:[\\/][\w.\-~ ]+){2,}[\\/]?")


def safe_catalog_message(message: Any) -> str:
    """Operator-facing text with secrets redacted and filesystem paths removed."""
    text = redact_log_text(str(message or "")).strip()
    text = _ABS_PATH.sub("<path>", text)
    return text[:400]


def _lower(values: Any) -> set[str]:
    return {str(v).strip().lower() for v in (values or []) if str(v).strip()}


def _is_schemaless(dest_type: str) -> bool:
    if not dest_type:
        return False
    try:
        from services.connector_capability_registry import is_schemaless

        return bool(is_schemaless(dest_type))
    except Exception:  # noqa: BLE001 — unknown key means "not proven schemaless"
        return False


def add_column_decision(
    *,
    dest_type: str,
    table_exists: bool | None,
    schema_policy: str = "",
) -> tuple[bool, str]:
    """Whether a target name the destination lacks is acceptable, and why."""
    policy = (schema_policy or "").strip().lower()
    if table_exists is False:
        return True, "The table does not exist yet: every mapped column is created with it."
    if _is_schemaless(dest_type):
        return True, "Schemaless destination: a new field is written with the first row that carries it."
    if table_exists is None:
        return False, "Destination table was not read: a new column cannot be promised until it is."
    if policy in _NO_ADD_POLICIES:
        return False, (
            f"Schema policy '{policy}' forbids ADD COLUMN: map onto an existing column "
            "or change the policy."
        )
    return True, "The writer adds the column (ADD COLUMN) before the first row is written."


def build_destination_column_catalog(
    info: dict[str, Any] | None,
    *,
    dest_type: str = "",
    schema_policy: str = "",
) -> dict[str, Any]:
    """Normalise an ``introspect_endpoint`` reading into the Map picker catalog."""
    info = dict(info or {})
    schema = {str(k): str(v or "") for k, v in dict(info.get("schema") or {}).items()}
    names = [str(c) for c in (info.get("columns") or list(schema.keys())) if str(c)]
    nullability = {
        str(k): bool(v) for k, v in dict(info.get("schema_nullability") or {}).items()
    }
    defaults = {str(k): str(v) for k, v in dict(info.get("schema_defaults") or {}).items()}
    identity = [str(c) for c in (info.get("identity_columns") or [])]
    generated = [str(c) for c in (info.get("generated_columns") or [])]
    pk = _lower(info.get("primary_key_columns"))
    identity_l, generated_l = _lower(identity), _lower(generated)
    nulls_l = {k.lower(): v for k, v in nullability.items()}
    defaults_l = {k.lower(): v for k, v in defaults.items()}
    measured = bool(names) and _lower(names) <= set(nulls_l)
    required = _lower(
        unfilled_required_columns(
            column_nullability=nullability,
            column_defaults=defaults,
            identity_columns=identity,
            generated_columns=generated,
            mappings=[],
        )
    )
    table_exists = info.get("table_exists")
    connected = bool(info.get("connected")) if "connected" in info else bool(names)
    if table_exists is None and names:
        table_exists = True
    if not names and (not connected or _SOFT_FAILED_PROBE in str(info.get("message") or "")):
        # introspect soft-fails a schema read and keeps whatever existence it had
        # guessed from the object list: a probe that failed is not evidence the
        # table is missing, so a "will create" promise would be a false read.
        table_exists = None
    columns: list[dict[str, Any]] = []
    for ordinal, name in enumerate(names):
        key = name.lower()
        nullable = nulls_l.get(key)
        default = defaults_l.get(key)
        columns.append(
            {
                "name": name,
                "ordinal": ordinal,
                "type": schema.get(name, ""),
                "nullable": nullable,
                "default": default,
                "has_default": default is not None,
                "primary_key": key in pk,
                "identity": key in identity_l,
                "generated": key in generated_l,
                # None = unmeasured: never claim "optional" without a reading.
                "required": (key in required) if nullable is not None else None,
            }
        )
    can_add, add_reason = add_column_decision(
        dest_type=dest_type, table_exists=table_exists, schema_policy=schema_policy
    )
    return {
        "version": CATALOG_VERSION,
        "connected": connected,
        "table_exists": table_exists,
        "dest_type": (dest_type or "").lower(),
        "columns": columns,
        "nullability_measured": measured,
        "can_add_columns": can_add,
        "add_columns_reason": add_reason,
        "message": safe_catalog_message(info.get("message") or info.get("error") or ""),
    }


def read_destination_column_catalog(
    endpoint: Any,
    *,
    schema_policy: str = "",
) -> dict[str, Any]:
    """Probe the destination's own catalog (blocking — call off the event loop)."""
    from src.transfer.endpoint_intelligence import introspect_endpoint

    endpoint.extra = {
        **(endpoint.extra or {}),
        "introspect_purpose": "destination",
    }
    info = introspect_endpoint(endpoint)
    dest_type = str(getattr(endpoint, "format", "") or getattr(endpoint, "kind", "") or "")
    return build_destination_column_catalog(
        info, dest_type=dest_type, schema_policy=schema_policy
    )
