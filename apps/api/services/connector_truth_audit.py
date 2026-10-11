"""Code-derived audit of connector capability and form claims."""

from __future__ import annotations

import importlib
import inspect
import json
import logging
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger(__name__)

_API_ROOT = Path(__file__).resolve().parents[1]
_NOT_SUPPORTED_FN = re.compile(r"_not_supported$")

NON_REGISTRY_PATHS: dict[str, tuple[str, str, str, str]] = {
    "generic_sql": (
        "connectors.generic_sql", "read_table_batch",
        "connectors.generic_sql", "write_mapped_rows",
    ),
}
FILE_FORMAT_PATH = (
    "src.transfer.file_stream", "iter_source_rows",
    "src.transfer.adapters", "write_destination_file",
)
FILE_FORMATS = frozenset({
    "csv", "tsv", "json", "jsonl", "ndjson", "xml", "parquet", "orc", "avro", "excel",
})
EXPLICIT_PROBE_BRANCHES = {
    "mongodb": ("src.transfer.adapters", "probe_mongodb"),
}

_WRITE_MODE_FIELDS = (
    "supports_upsert",
    "supports_append",
    "supports_overwrite",
    "supports_merge",
)
_SECRET_FIELD_KEY = re.compile(
    r"(?i)(pass(word|phrase)?|secret|token|api_?key|private_?key|credential|service_?account)"
)


@dataclass(frozen=True)
class TruthViolation:
    rule: str
    connector_id: str
    claim: str
    evidence: str


def callable_reality(module: str | None, fn_name: str) -> dict[str, Any]:
    """Classify a registry-declared reader/writer entrypoint."""
    if not module:
        return {"exists": False, "kind": "absent", "detail": "registry declares no module"}
    if not fn_name:
        return {"exists": False, "kind": "absent", "detail": "registry declares no function"}
    if _NOT_SUPPORTED_FN.search(fn_name):
        return {
            "exists": True,
            "kind": "refusal",
            "target": f"{module}.{fn_name}",
            "detail": f"{fn_name} raises an explicit unsupported-role error",
        }
    try:
        mod = importlib.import_module(module)
    except Exception as exc:
        return {
            "exists": False, "kind": "import_error",
            "target": f"{module}.{fn_name}", "detail": f"{type(exc).__name__}: {exc}"[:200],
        }
    fn = getattr(mod, fn_name, None)
    if fn is None:
        return {
            "exists": False, "kind": "missing_function",
            "target": f"{module}.{fn_name}", "detail": "module has no such attribute",
        }
    try:
        src = inspect.getsource(fn)
    except (OSError, TypeError):
        src = ""
    body = re.sub(r'""".*?"""', "", src, flags=re.S)
    if "NotImplementedError" in body and body.count("\n") < 12:
        return {
            "exists": True, "kind": "stub",
            "target": f"{module}.{fn_name}", "detail": "body raises NotImplementedError",
        }
    return {
        "exists": True, "kind": "real",
        "target": f"{module}.{fn_name}",
        "detail": f"{body.count(chr(10))} source lines",
    }


@lru_cache(maxsize=None)
def driver_role_reality(driver: str) -> tuple[dict[str, Any], dict[str, Any]]:
    """Resolve driver read/write callables through the engine's owned paths."""
    from src.transfer.connector_capabilities import _FILE_CAPS
    from src.transfer.connector_registry import CONNECTOR_MODULES

    file_caps = _FILE_CAPS.get(driver)
    if file_caps is not None:
        reader = (
            callable_reality("src.transfer.file_stream", "iter_source_rows")
            if file_caps.get("file_source")
            else {
                "exists": False,
                "kind": "absent",
                "detail": "file caps declare no file_source",
            }
        )
        writer = (
            callable_reality("src.transfer.adapters", "write_destination_file")
            if file_caps.get("file_export")
            else {
                "exists": False,
                "kind": "absent",
                "detail": "file caps declare no file_export",
            }
        )
        return reader, writer

    reg = CONNECTOR_MODULES.get(driver)
    if reg is not None:
        reader = callable_reality(reg.reader, reg.reader_fn)
        writer = callable_reality(
            reg.writer,
            getattr(reg, "writer_fn", "") or "write_mapped_rows",
        )
        return reader, writer
    fallback = NON_REGISTRY_PATHS.get(driver)
    if fallback is not None:
        reader_module, reader_fn, writer_module, writer_fn = fallback
        return (
            callable_reality(reader_module, reader_fn),
            callable_reality(writer_module, writer_fn),
        )
    absent = {
        "exists": False,
        "kind": "absent",
        "detail": "driver not in CONNECTOR_MODULES",
    }
    return absent.copy(), absent.copy()


def write_mode_downgrade_reason(
    catalog_id: str,
    driver: str,
    caps: dict[str, Any],
) -> str | None:
    """Explain write claims that are not supported by the driver evidence."""
    del catalog_id
    writer = driver_role_reality(driver)[1]
    writer_kind = writer["kind"]
    if writer_kind in {"absent", "missing_function", "refusal", "stub"}:
        return f"No destination writer for {driver}: {writer['detail']}"
    if not caps.get("write"):
        return f"Driver capability table declares no write for {driver}"
    if caps.get("certified_dest") is False:
        return (
            "Destination not certified: writer code exists but no production "
            "destination COUNT proof (certified_dest=False)"
        )
    if writer_kind == "import_error":
        return None
    return None


def _form_fields(form: Any) -> list[Any]:
    return [
        *form.common_fields,
        *(field for mode in form.auth_modes for field in mode.fields),
    ]


def audit_catalog_truth() -> list[TruthViolation]:
    """Return catalog and form claims that contradict code-owned evidence."""
    from services import connector_auth, connector_form_schema
    from services.connector_capability_registry import (
        CAPABILITY_REGISTRY,
        DEFAULT_CAPABILITY,
        get_connector_capability,
    )
    from src.transfer.connector_capabilities import (
        _FILE_CAPS,
        enrich_catalog_entry,
        resolve_driver_type,
    )
    from src.transfer.connector_registry import CONNECTOR_MODULES

    catalog_path = _API_ROOT / "data" / "connector_catalog.json"
    catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
    catalog_entries = {
        str(entry.get("id", "")): entry for entry in catalog.get("connectors", [])
        if entry.get("id")
    }
    connector_ids = set(catalog_entries) | set(CAPABILITY_REGISTRY)
    violations: list[TruthViolation] = []

    for connector_id in sorted(connector_ids):
        driver = resolve_driver_type(connector_id)
        reader, writer = driver_role_reality(driver)
        capability = get_connector_capability(connector_id)
        driver_caps = capability.get("driver_capabilities", {})
        entry = catalog_entries.get(
            connector_id,
            {"id": connector_id, "name": connector_id, "category": "", "status": ""},
        )
        enriched = enrich_catalog_entry(entry)

        if enriched.get("source_ready") and reader["kind"] not in {"real", "import_error"}:
            violations.append(TruthViolation(
                rule="role_source",
                connector_id=connector_id,
                claim="source_ready",
                evidence=f"{reader['kind']}: {reader['detail']}",
            ))
        if enriched.get("dest_ready") and writer["kind"] not in {"real", "import_error"}:
            violations.append(TruthViolation(
                rule="role_dest",
                connector_id=connector_id,
                claim="dest_ready",
                evidence=f"{writer['kind']}: {writer['detail']}",
            ))
        raw_capability = CAPABILITY_REGISTRY.get(connector_id, DEFAULT_CAPABILITY)
        raw_write_claims = [
            field for field in _WRITE_MODE_FIELDS if raw_capability.get(field)
        ]
        has_static_downgrade = bool(raw_capability.get("capability_downgrades"))
        source_dest_only_zeroing = bool(driver_caps.get("source_only")) or bool(
            driver_caps.get("dest_only") and not driver_caps.get("write")
        )
        if (raw_write_claims or has_static_downgrade) and not source_dest_only_zeroing:
            reason = write_mode_downgrade_reason(connector_id, driver, driver_caps)
            if reason:
                downgrade_entries = capability.get("capability_downgrades") or ()
                has_visible_reason = any(
                    isinstance(downgrade, dict)
                    and isinstance(downgrade.get("reason"), str)
                    and downgrade["reason"].strip()
                    for downgrade in downgrade_entries
                )
                visibly_downgraded = (
                    not any(capability.get(field) for field in _WRITE_MODE_FIELDS)
                    and has_visible_reason
                )
                if not visibly_downgraded:
                    violations.append(TruthViolation(
                        rule="write_modes",
                        connector_id=connector_id,
                        claim=", ".join(raw_write_claims) or "capability_downgrades",
                        evidence=reason,
                    ))

    forms = connector_form_schema._load()
    for form_type, form in forms.items():
        driver = resolve_driver_type(form_type)
        for mode in form.auth_modes:
            if mode.value not in connector_auth._KNOWN_AUTH_MODES:
                violations.append(TruthViolation(
                    rule="form_auth_mode",
                    connector_id=form_type,
                    claim=mode.value,
                    evidence="auth mode is absent from services.connector_auth._KNOWN_AUTH_MODES",
                ))
        for field in _form_fields(form):
            if _SECRET_FIELD_KEY.search(field.key) and not field.sensitive:
                violations.append(TruthViolation(
                    rule="form_secret_field",
                    connector_id=form_type,
                    claim=field.key,
                    evidence="secret-like field must have sensitive=True",
                ))
        reg = CONNECTOR_MODULES.get(driver)
        has_probe = reg is not None and reg.probe is not None
        if not has_probe and driver == "mongodb" and reg is not None:
            probe_module, probe_fn = EXPLICIT_PROBE_BRANCHES[driver]
            has_probe = callable_reality(probe_module, probe_fn)["kind"] in {
                "real",
                "import_error",
            }
        if not (
            has_probe
            or driver in NON_REGISTRY_PATHS
            or driver in _FILE_CAPS
        ):
            violations.append(TruthViolation(
                rule="form_probe",
                connector_id=form_type,
                claim=f"probe path for {driver}",
                evidence="resolved driver has no CONNECTOR_MODULES probe or explicit engine path",
            ))

    return violations
