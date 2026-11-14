"""Certified comes from CI evidence, never from a hand-kept list (CLM-7).

A connector tile used to read "Certified" because its id sat in a frozenset in
``connector_capabilities``. Kafka, the vector stores, Oracle and SQL Server
were badged that way with no CI run behind them, and wire-compatible brands
(MariaDB, DocumentDB, Cosmos DB, the Redis services) inherited the badge from
the engine they imitate.

This module answers one question for a catalog tile: did a CI job that
*proves* its engine run green, with none of that engine's tests skipped?
The answer comes from the evidence document ``services/connector_ci_evidence``
builds out of ``scripts/check_live_skips.py`` results:

* ``data/connector_ci_evidence.json`` is the committed snapshot (written by
  ``scripts/connector_claims_report.py`` from a CI run's live-engine results,
  and naming that run). ``DATAFLOW_CONNECTOR_CI_EVIDENCE`` points a deployment
  at a fresher copy.
* No snapshot, or an engine absent from it, means "not proven". Nothing is
  Certified by default.

An implemented engine without that proof gets the honest tier
``implemented`` ("Implemented — not yet proven"): it can be tried, it is not
offered for production schedules, and it is not counted as proven anywhere.

A wire-compatible brand alias is proven only by evidence recorded under its
own id (``mariadb``, ``documentdb`` ...); the base engine's run says nothing
about the brand.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

EVIDENCE_ENV = "DATAFLOW_CONNECTOR_CI_EVIDENCE"
SNAPSHOT = Path(__file__).resolve().parents[1] / "data" / "connector_ci_evidence.json"

CERTIFIED_TIER = "certified"
IMPLEMENTED_TIER = "implemented"
IMPLEMENTED_LABEL = "Implemented — not yet proven"

#: In-process engines: the api-tests job runs them inside the test process.
FILES_ENGINE = "files"
# The tabular formats api-tests reads and writes in-process. PDF, DOCX and
# HTML extraction depend on OCR / converter packages a run can lack, so their
# tests can skip unseen: they are not counted as proven by this job.
_FILES_PROVEN_FORMATS = frozenset({
    "csv", "tsv", "json", "jsonl", "ndjson", "excel", "parquet", "avro", "orc",
    "xml", "yaml", "fixed_width",
})

# Driver type -> the evidence engine whose CI job proves it. A driver that is
# not listed has no proving job, so it is never proven.
_ENGINE_FOR_DRIVER: dict[str, str] = {
    "postgresql": "postgresql",
    # pgvector is PostgreSQL plus the vector type; the PostgreSQL jobs run its
    # tests (the manifest attributes a pgvector skip to postgresql).
    "pgvector": "postgresql",
    "mysql": "mysql",
    "mongodb": "mongodb",
    "redis": "redis",
    "kafka": "kafka",
    "sqlserver": "sqlserver",
    "mssql": "sqlserver",
    "oracle": "oracle",
    "bigquery": "bigquery",
    "snowflake": "snowflake",
    "gcs": "gcs",
    "adls": "adls",
    "sqlite": "sqlite",
}

_cache: dict[str, Any] = {"key": None, "doc": None}


def evidence_path() -> Path:
    override = (os.environ.get(EVIDENCE_ENV) or "").strip()
    return Path(override) if override else SNAPSHOT


def evidence_snapshot() -> dict[str, Any]:
    """The evidence document in force, or an empty one (nothing proven)."""
    path = evidence_path()
    try:
        key = (str(path), path.stat().st_mtime_ns)
    except OSError:
        return {"engines": {}, "live_verified": [], "source": "", "missing": str(path)}
    if _cache["key"] != key:
        try:
            doc = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            doc = {}
        if not isinstance(doc, dict):
            doc = {}
        doc.setdefault("engines", {})
        _cache["key"], _cache["doc"] = key, doc
    return _cache["doc"]


def engine_for(driver: str) -> str:
    """Evidence engine that proves ``driver``; empty when no CI job proves it."""
    key = (driver or "").strip().lower()
    if key in _ENGINE_FOR_DRIVER:
        return _ENGINE_FOR_DRIVER[key]
    if key in _FILES_PROVEN_FORMATS:
        return FILES_ENGINE
    return ""


def engine_proven(engine: str) -> bool:
    record = (evidence_snapshot().get("engines") or {}).get(engine) or {}
    return bool(engine) and record.get("status") == "ci_green" and bool(record.get("live_verified"))


def alias_proven(catalog_id: str) -> bool:
    """A brand alias's own evidence (recorded under its catalog id)."""
    return engine_proven((catalog_id or "").strip().lower())


def proof_for(catalog_id: str, driver: str, *, alias: bool = False) -> dict[str, Any]:
    """Evidence record for one tile: which engine, its CI status, proven or not."""
    snap = evidence_snapshot()
    engine = (catalog_id or "").strip().lower() if alias else engine_for(driver)
    record = (snap.get("engines") or {}).get(engine) or {}
    status = str(record.get("status") or ("no_ci_job" if not engine or not record else ""))
    proven = engine_proven(engine)
    return {
        "engine": engine or None,
        "status": status,
        "proven": proven,
        "emulated": bool(record.get("emulated")),
        "evidence_source": str(snap.get("source_run") or snap.get("source") or ""),
        "evidence_generated_at": str(snap.get("generated_at") or ""),
    }


def driver_proven(driver: str) -> bool:
    return engine_proven(engine_for(driver))


def proven_and_pending(drivers: list[str]) -> dict[str, list[str]]:
    """Split transfer-ready driver types into proven (CI green) and pending."""
    proven = sorted(d for d in drivers if driver_proven(d))
    pending = sorted(d for d in drivers if d not in set(proven))
    return {"proven": proven, "pending": pending}


def reset_cache() -> None:
    _cache["key"] = None
    _cache["doc"] = None
