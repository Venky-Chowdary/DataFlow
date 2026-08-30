#!/usr/bin/env python3
"""Track E — code-derived readiness row for every catalogued connector id.

Answers, per catalog id, only from what the code actually contains:

1. reader reality — canonical registry module + function, is it importable, is
   it a ``*_not_supported`` refusal, and does it introspect / stream;
2. writer reality — same, plus the sync modes the capability SSOT declares;
3. dependency — the driver module the connector imports, whether it is
   declared in ``pyproject.toml`` and whether it imports here;
4. how it could be proven — a store this repo can start locally, an emulator,
   or a hosted tenant with credentials;
5. honest status — ``proven-live-here`` (only when the measured artifact from
   ``connector_readiness_live_proof.py`` says so), ``provable-locally-not-yet-run``,
   ``needs-cloud-credentials``, ``partial (...)``, ``stub - not transfer capable``.

Nothing here infers capability from the catalog's own ``status`` field: that
field is marketing input, not evidence.

    cd apps/api && python scripts/connector_readiness_audit.py

Writes ``data/proofs/connector_readiness_audit.json``.
"""

from __future__ import annotations

import argparse
import importlib
import importlib.util
import inspect
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

ARTIFACT = _API_ROOT / "data" / "proofs" / "connector_readiness_audit.json"
LIVE_ARTIFACT = _API_ROOT / "data" / "proofs" / "connector_readiness_live.json"
MODE_ARTIFACT = _API_ROOT / "data" / "proofs" / "connector_readiness_modes.json"

# Drivers this repo can stand up on a laptop from docker-compose.yml (or with no
# service at all, for the file/embedded ones). Anything not listed needs a
# tenant, an account, or a service the compose file does not define.
LOCAL_SERVICE = {
    "postgresql": "docker compose service `postgres` (PostgreSQL 16, wal_level=logical)",
    "mysql": "docker compose service `mysql` (MySQL 8, ROW binlog + GTID)",
    "mongodb": "docker compose service `mongodb` (replica set rs0)",
    "redis": "docker compose service `redis`",
    "sqlserver": "docker compose service `sqlserver` (profile amd64-sql)",
    "elasticsearch": "docker compose service `elasticsearch` (profile search)",
    "sqlite": "embedded file - no service needed",
    "duckdb": "embedded file - no service needed",
    "csv": "local file - no service needed",
    "json": "local file - no service needed",
    "jsonl": "local file - no service needed",
    "ndjson": "local file - no service needed",
    "tsv": "local file - no service needed",
    "xml": "local file - no service needed",
    "parquet": "local file - no service needed",
    "avro": "local file - no service needed",
    "excel": "local file - no service needed",
    "generic_sql": "any of the local SQL engines above via SQLAlchemy URL",
}

EMULATOR_SERVICE = {
    "s3": "docker compose service `minio` (S3-compatible)",
    "minio": "docker compose service `minio`",
    "gcs": "docker compose service `fake-gcs` (fake-gcs-server emulator)",
    "adls": "docker compose service `azurite` (Azure storage emulator)",
    "dynamodb": "docker compose service `dynamodb-local`",
    "bigquery": "docker compose service `bigquery-emulator`",
    "sftp": "any local SSH/SFTP server image",
    "kafka": "no broker in docker-compose.yml",
}

_NOT_SUPPORTED_FN = re.compile(r"_not_supported$")

#: Drivers the engine reaches through its own explicit branches instead of
#: ``CONNECTOR_MODULES``: SQLAlchemy engines share one generic adapter, and file
#: formats are read by the streaming file reader and written by the export
#: adapter. Classifying these as "no reader/writer" would be as dishonest as
#: claiming a stub is live, so the audit resolves them here and says which
#: engine branch owns them.
_NON_REGISTRY_PATHS: dict[str, tuple[str, str, str, str]] = {
    "generic_sql": (
        "connectors.generic_sql", "read_table_batch",
        "connectors.generic_sql", "write_mapped_rows",
    ),
}
_FILE_FORMAT_PATH = (
    "src.transfer.file_stream", "iter_source_rows",
    "src.transfer.adapters", "write_destination_file",
)
_FILE_FORMATS = frozenset({
    "csv", "tsv", "json", "jsonl", "ndjson", "xml", "parquet", "orc", "avro", "excel",
})

#: Drivers that stand in for many different engines behind one adapter. Proving
#: one of them (DuckDB over SQLAlchemy, one REST tap) says nothing about the
#: others, so their catalog ids never inherit ``proven-live-here``.
_SHARED_ADAPTER = {
    "generic_sql": "shared SQLAlchemy adapter - each engine needs its own measured cell",
    "rest_api": "generic REST tap - each API needs its own endpoint, auth and schema",
}


def _declared_dependencies() -> set[str]:
    """Distributions the API declares - requirements.txt is this app's manifest."""
    names: set[str] = set()
    for name in ("requirements.txt", "requirements-rag.txt"):
        path = _API_ROOT / name
        if not path.exists():
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            spec = line.split("#")[0].strip()
            if not spec or spec.startswith("-"):
                continue
            base = re.split(r"[<>=!~\[; ]", spec, maxsplit=1)[0].strip().lower()
            if base:
                names.add(base)
    return names


_DIST_FOR_MODULE = {
    "psycopg2": "psycopg2-binary",
    "pymysql": "pymysql",
    "pymongo": "pymongo",
    "redis": "redis",
    "elasticsearch": "elasticsearch",
    "boto3": "boto3",
    "google.cloud.storage": "google-cloud-storage",
    "azure.storage.blob": "azure-storage-blob",
    "snowflake.connector": "snowflake-connector-python",
    "google.cloud.bigquery": "google-cloud-bigquery",
    "sqlite3": "stdlib",
    "pyodbc": "pyodbc",
    "oracledb": "oracledb",
    "paramiko": "paramiko",
    "requests": "requests",
    "duckdb": "duckdb",
    "sqlalchemy": "sqlalchemy",
}


def _module_importable(module: str) -> bool:
    try:
        return importlib.util.find_spec(module) is not None
    except (ImportError, ModuleNotFoundError, ValueError):
        return False


def _callable_reality(module: str | None, fn_name: str) -> dict[str, Any]:
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


def _modes(driver: str, cap_row: dict[str, Any], writer_kind: str) -> dict[str, Any]:
    """Sync modes the capability SSOT declares for this driver.

    Declared, not measured - the measured cells live in the live artifacts and
    the matrix doc marks a mode green only when a cell proved it.
    """
    if writer_kind not in {"real"}:
        return {"declared": [], "note": "no writer, so no write mode applies"}
    declared: list[str] = []
    if cap_row.get("supports_overwrite"):
        declared.append("full_refresh_overwrite")
    if cap_row.get("supports_append"):
        declared.append("full_refresh_append")
    if cap_row.get("supports_upsert"):
        declared.extend(["incremental_deduped", "upsert"])
    if cap_row.get("supports_cdc"):
        declared.append("cdc (at-least-once upsert)")
    merge_note = ""
    if cap_row.get("supports_merge"):
        try:
            from connectors.merge_registry import MERGE_STRATEGIES

            strat = MERGE_STRATEGIES.get(driver, {}).get("strategy", "")
            merge_note = f"MERGE via {strat}" if strat else "MERGE"
        except Exception:
            merge_note = "MERGE"
    return {
        "declared": declared,
        "merge": merge_note,
        "incremental_append": "full_refresh_append" in declared,
        "note": cap_row.get("cdc_prerequisites", "")[:180],
    }


def _proof_path(driver: str) -> dict[str, str]:
    if driver in LOCAL_SERVICE:
        return {"class": "local", "how": LOCAL_SERVICE[driver]}
    if driver in EMULATOR_SERVICE:
        how = EMULATOR_SERVICE[driver]
        if how.startswith("no "):
            return {"class": "unavailable-locally", "how": how}
        return {"class": "emulator", "how": how}
    return {"class": "hosted", "how": "hosted tenant credentials (no local image for this service)"}


def _live_index() -> dict[tuple[str, str, str], dict[str, Any]]:
    index: dict[tuple[str, str, str], dict[str, Any]] = {}
    for path in (LIVE_ARTIFACT, MODE_ARTIFACT):
        if not path.exists():
            continue
        payload = json.loads(path.read_text(encoding="utf-8"))
        for cell in payload.get("cells", []):
            key = (cell.get("source", ""), cell.get("destination", ""), cell.get("sync_mode", ""))
            index[key] = cell
    return index


def _proven_roles(driver: str, live: dict[tuple[str, str, str], dict[str, Any]]) -> dict[str, list[str]]:
    """Measured pass cells for this driver, in each role.

    Harness store ids are endpoint formats (``duckdb``), so they are resolved
    through the same driver resolver the product uses before matching.
    """
    from src.transfer.connector_capabilities import resolve_driver_type

    as_source = sorted({
        f"{d} ({m})" for (s, d, m), c in live.items()
        if resolve_driver_type(s) == driver and c.get("status") == "pass"
    })
    as_dest = sorted({
        f"{s} ({m})" for (s, d, m), c in live.items()
        if resolve_driver_type(d) == driver and c.get("status") == "pass"
    })
    return {"proven_as_source": as_source, "proven_as_dest": as_dest}


def audit_driver(
    driver: str,
    live: dict[tuple[str, str, str], dict[str, Any]],
    declared_deps: set[str],
) -> dict[str, Any]:
    from services.connector_capability_registry import CAPABILITY_REGISTRY
    from src.transfer.connector_capabilities import _DRIVER_MODULE, driver_available, get_capabilities
    from src.transfer.connector_registry import CONNECTOR_MODULES

    reg = CONNECTOR_MODULES.get(driver)
    cap_row = dict(CAPABILITY_REGISTRY.get(driver, {}))
    caps = get_capabilities(driver)

    fallback = _NON_REGISTRY_PATHS.get(driver)
    if fallback is None and driver in _FILE_FORMATS:
        fallback = _FILE_FORMAT_PATH
    if reg is None and fallback is not None:
        r_mod, r_fn, w_mod, w_fn = fallback
        reader = _callable_reality(r_mod, r_fn)
        writer = _callable_reality(w_mod, w_fn)
        reader["engine_branch"] = "explicit engine branch, not CONNECTOR_MODULES"
        writer["engine_branch"] = "explicit engine branch, not CONNECTOR_MODULES"
    elif reg is None:
        reader = {"exists": False, "kind": "absent", "detail": "driver not in CONNECTOR_MODULES"}
        writer = dict(reader)
    else:
        reader = _callable_reality(reg.reader, reg.reader_fn)
        writer = _callable_reality(reg.writer, getattr(reg, "writer_fn", "") or "write_mapped_rows")

    dep_module = _DRIVER_MODULE.get(driver, "unset")
    dep = {
        "module": dep_module,
        "distribution": _DIST_FOR_MODULE.get(dep_module or "", "unknown") if dep_module else "none",
        "importable_here": _module_importable(dep_module) if dep_module else True,
        "declared_in_requirements": (
            _DIST_FOR_MODULE.get(dep_module or "", "").lower() in declared_deps
            or _DIST_FOR_MODULE.get(dep_module or "", "") == "stdlib"
        ) if dep_module else True,
    }

    proof = _proof_path(driver)
    roles = _proven_roles(driver, live)
    modes = _modes(driver, cap_row, writer["kind"])

    if reader["kind"] not in {"real"} and writer["kind"] not in {"real"}:
        status = "stub - not transfer capable"
    elif roles["proven_as_source"] and roles["proven_as_dest"]:
        status = "proven-live-here"
    elif roles["proven_as_source"] or roles["proven_as_dest"]:
        proven_role = "source" if roles["proven_as_source"] else "destination"
        other = "destination" if proven_role == "source" else "source"
        if reader["kind"] == "real" and writer["kind"] == "real":
            status = f"partial (proven as {proven_role}; {other} role not yet measured)"
        else:
            status = f"partial ({proven_role} only in code)"
    elif reader["kind"] != "real":
        status = "partial (writer only - no reader in code)"
    elif writer["kind"] != "real":
        # Role gap and proof gap are different facts; a source-only connector
        # behind a hosted tenant is not one a client can exercise here either.
        suffix = "" if proof["class"] in {"local", "emulator"} else "; needs tenant credentials"
        status = f"partial (reader only - writer refuses this role{suffix})"
    elif proof["class"] in {"local", "emulator"}:
        status = "provable-locally-not-yet-run"
    else:
        status = "needs-cloud-credentials"

    if not dep["importable_here"] and status == "provable-locally-not-yet-run":
        status = f"provable-locally-not-yet-run (install {dep['distribution']})"

    return {
        "driver_type": driver,
        "in_connector_registry": reg is not None,
        "reader": reader,
        "writer": writer,
        "introspect": bool(caps.get("introspect")),
        "streaming_read": bool(cap_row.get("supports_streaming")),
        "batch_pattern": cap_row.get("pattern", ""),
        "transfer_ready_declared": bool(cap_row.get("transfer_ready")),
        "certification_tier": cap_row.get("tier", ""),
        "dependency": dep,
        "sync_modes": modes,
        "proof": proof,
        **roles,
        "driver_available_here": bool(driver_available(driver)),
        "status": status,
    }


def _measured_endpoint_roles(
    live: dict[tuple[str, str, str], dict[str, Any]],
) -> dict[str, set[str]]:
    """Roles each *endpoint format* was measured in, by the harness' own id.

    Kept apart from the driver-level index: ``duckdb`` resolves to the
    ``generic_sql`` family, so only the raw store id distinguishes the tile that
    was actually exercised from the other tiles sharing that adapter.
    """
    roles: dict[str, set[str]] = {}
    for (s, d, _m), cell in live.items():
        if cell.get("status") != "pass":
            continue
        roles.setdefault(s, set()).add("source")
        roles.setdefault(d, set()).add("destination")
    return roles


def _connector_status(
    cid: str,
    driver: str,
    row: dict[str, Any],
    measured_ids: dict[str, set[str]],
) -> tuple[str, str]:
    """Per-id status: a driver's measured cell does not certify every tile on it.

    Two cases downgrade the driver-level verdict:

    * shared adapters (``generic_sql``, ``rest_api``) - one measured engine is
      not proof for the other 50 tiles pointed at the same code;
    * hosted aliases (``postgresql_rds``) - the engine is the same, but the
      managed service itself was never contacted from this box.

    Neither downgrade applies to the tile the harness actually ran: DuckDB sits
    behind the shared SQLAlchemy adapter and was measured by id in both roles,
    so it keeps the measured verdict.
    """
    from src.transfer.connector_capabilities import enrich_catalog_entry

    status = row["status"]
    own = measured_ids.get(cid, set())
    if {"source", "destination"} <= own:
        return "proven-live-here", "measured by id in both roles"
    if own:
        role = "source" if "source" in own else "destination"
        other = "destination" if role == "source" else "source"
        return (
            f"partial (proven as {role}; {other} role not yet measured)",
            "measured by id in one role",
        )
    shared = _SHARED_ADAPTER.get(driver)
    if shared and status.startswith("proven-live-here"):
        proven = ", ".join(row.get("proven_as_dest", [])[:3]) or "another engine"
        return (
            "provable-locally-not-yet-run" if _proof_path(driver)["class"] in {"local", "emulator"}
            else "needs-cloud-credentials",
            f"{shared}; adapter measured with {proven}",
        )
    try:
        enriched = enrich_catalog_entry({"id": cid, "name": cid, "category": "", "status": ""})
    except Exception:
        enriched = {}
    if enriched.get("is_hosted_alias") and status.startswith("proven-live-here"):
        return (
            "needs-cloud-credentials",
            f"managed alias of {enriched.get('alias_of') or driver}; the engine is proven "
            "locally, the hosted service itself was not contacted",
        )
    return status, shared or ""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default=str(ARTIFACT))
    args = parser.parse_args()

    from src.transfer.connector_capabilities import resolve_driver_type

    catalog = json.loads(
        (_API_ROOT / "data" / "connector_catalog.json").read_text(encoding="utf-8")
    )
    entries = catalog.get("connectors", [])
    live = _live_index()
    measured_ids = _measured_endpoint_roles(live)
    deps = _declared_dependencies()

    driver_rows: dict[str, dict[str, Any]] = {}
    connectors: list[dict[str, Any]] = []
    for entry in entries:
        cid = entry["id"]
        driver = resolve_driver_type(cid)
        if driver not in driver_rows:
            driver_rows[driver] = audit_driver(driver, live, deps)
        row = driver_rows[driver]
        status, note = _connector_status(cid, driver, row, measured_ids)
        connectors.append({
            "id": cid,
            "name": entry.get("name", cid),
            "category": entry.get("category", ""),
            "catalog_status_raw": entry.get("status", ""),
            "driver_type": driver,
            "status": status,
            "note": note,
        })

    totals: dict[str, int] = {}
    for c in connectors:
        totals[c["status"]] = totals.get(c["status"], 0) + 1
    driver_totals: dict[str, int] = {}
    for row in driver_rows.values():
        driver_totals[row["status"]] = driver_totals.get(row["status"], 0) + 1

    out = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "schema_version": 1,
        "catalog_declared_total": catalog.get("total"),
        "catalog_actual_entries": len(entries),
        "measured_cells_considered": len(live),
        "connector_status_totals": dict(sorted(totals.items())),
        "driver_status_totals": dict(sorted(driver_totals.items())),
        "drivers": dict(sorted(driver_rows.items())),
        "connectors": connectors,
    }
    dest = Path(args.out)
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(out, indent=2, default=str) + "\n", encoding="utf-8")
    print(json.dumps({
        "catalog_actual_entries": len(entries),
        "distinct_drivers": len(driver_rows),
        "connector_status_totals": out["connector_status_totals"],
        "driver_status_totals": out["driver_status_totals"],
        "artifact": str(dest),
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
