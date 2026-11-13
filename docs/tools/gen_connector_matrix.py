#!/usr/bin/env python3
"""Generate the capability matrix in ``docs/reference/connectors.md``.

Every cell is read from code or from the CI configuration, so the page cannot
drift from what the product does:

* read / write / status   ``src.transfer.connector_capabilities`` (declared
                          driver and file tables)
* CDC, upsert, merge      ``services.connector_capability_registry.get_connector_capability``
                          (the single capability function)
* create-privilege probe  ``services.destination_privilege_probe._SUPPORTED``
* incremental rule        ``services.preflight_cursor_gate`` plus the driver's
                          own ``incremental`` declaration
* CI evidence             the workflow files under ``.github/`` and
                          ``apps/api/requirements.txt``

Usage (from the repository root, with the API's Python environment)::

    python docs/tools/gen_connector_matrix.py            # rewrite the matrix
    python docs/tools/gen_connector_matrix.py --check    # exit 1 when stale

The matrix shows *declared* capabilities: what a server reports when every
driver package is installed. A server missing a package reports less; the
catalog API (``GET /api/v1/catalog/connectors``) shows that live view.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import tempfile
from collections import Counter
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

REPO_ROOT = Path(__file__).resolve().parents[2]
API_ROOT = REPO_ROOT / "apps" / "api"
TARGET = REPO_ROOT / "docs" / "reference" / "connectors.md"

BEGIN = "<!-- BEGIN GENERATED: connector-matrix (docs/tools/gen_connector_matrix.py) -->"
END = "<!-- END GENERATED: connector-matrix -->"

PR_CI_FILES = (
    ".github/workflows/ci.yml",
    ".github/actions/ci-services/action.yml",
)
NIGHTLY_FILES = (".github/workflows/full-suite.yml",)
REQUIREMENTS = "apps/api/requirements.txt"

# driver -> (what CI runs, pattern that proves it is still wired). The pattern
# is matched against the PR workflow, then the nightly workflow, then the
# Python requirements, so removing a service from CI changes this page.
_START = r"start_ci_databases\.sh[^\n]*\b{}\b"
CI_MARKERS: dict[str, tuple[str, str, str]] = {
    "postgresql": ("live container", _START.format("postgres(?:-cdc)?"), "workflow"),
    "pgvector": ("live container (PostgreSQL image with pgvector)", _START.format("postgres-cdc"), "workflow"),
    "mysql": ("live container", _START.format("mysql"), "workflow"),
    "mongodb": ("live container", _START.format("mongo"), "workflow"),
    "redis": ("live container", _START.format("redis"), "workflow"),
    "sqlserver": ("live container", r"mssql/server|dataflow-sqlserver", "workflow"),
    "oracle": ("opt-in job, skipped unless ENABLE_ORACLE_CDC is set", r"ENABLE_ORACLE_CDC", "workflow"),
    "kafka": ("live container", r"apache/kafka|redpanda", "workflow"),
    "elasticsearch": ("live container", r"image:[^\n]*elasticsearch|docker run[^\n]*elasticsearch", "workflow"),
    "qdrant": ("live container", r"image:[^\n]*qdrant|docker run[^\n]*qdrant", "workflow"),
    "weaviate": ("live container", r"image:[^\n]*weaviate|docker run[^\n]*weaviate", "workflow"),
    "milvus": ("live container", r"image:[^\n]*milvus|docker run[^\n]*milvus", "workflow"),
    "iceberg": ("live container", r"image:[^\n]*iceberg|docker run[^\n]*iceberg", "workflow"),
    "bigquery": ("emulator container", _START.format("bigquery"), "workflow"),
    "gcs": ("emulator container", _START.format("gcs"), "workflow"),
    "adls": ("emulator container", _START.format("adls"), "workflow"),
    "snowflake": ("in-process emulator (fakesnow)", r"^fakesnow\b", "requirements"),
    "s3": ("in-process emulator (moto)", r"^moto\b", "requirements"),
    "dynamodb": ("in-process emulator (moto)", r"^moto\b", "requirements"),
}
NO_SERVICE_NEEDED = {"sqlite"}


def _bootstrap() -> None:
    """Make the API importable and keep its state out of the repository."""
    for path in (API_ROOT, REPO_ROOT / "packages" / "preflight" / "src"):
        if str(path) not in sys.path:
            sys.path.insert(0, str(path))
    if not (os.environ.get("DATAWRAP_DATA_DIR") or os.environ.get("DATAFLOW_DATA_DIR")):
        os.environ["DATAWRAP_DATA_DIR"] = tempfile.mkdtemp(prefix="datawrap-docs-")
    os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
    os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")


@contextmanager
def _declared_view() -> Iterator[None]:
    """Report capabilities as if every driver package were installed.

    Without this the page would change with whatever happens to be installed on
    the machine that generated it.
    """
    from src.transfer import connector_capabilities as cc

    original = cc.driver_available
    cc.driver_available = lambda *_a, **_k: True  # type: ignore[assignment]
    try:
        yield
    finally:
        cc.driver_available = original  # type: ignore[assignment]


def _read(rel: str) -> str:
    path = REPO_ROOT / rel
    return path.read_text(encoding="utf-8") if path.is_file() else ""


def ci_evidence(driver: str, caps: dict[str, Any], is_file_format: bool) -> str:
    if is_file_format:
        return "no service needed (file fixtures)"
    if driver in NO_SERVICE_NEEDED:
        return "no service needed (file engine)"
    if caps.get("evidence"):
        return f"fixture only (`{caps['evidence']}`)"
    marker = CI_MARKERS.get(driver)
    if marker is None:
        return "not started by any workflow"
    label, pattern, where = marker
    regex = re.compile(pattern, re.MULTILINE)
    if where == "requirements":
        return label if regex.search(_read(REQUIREMENTS)) else "not started by any workflow"
    if any(regex.search(_read(f)) for f in PR_CI_FILES):
        return f"{label}, PR workflow"
    if any(regex.search(_read(f)) for f in NIGHTLY_FILES):
        return f"{label}, nightly workflow only"
    return "not started by any workflow"


def _yes(value: Any) -> str:
    return "yes" if value else "no"


def _incremental(driver: str, caps: dict[str, Any], is_file_format: bool, file_sources: frozenset[str]) -> str:
    if not caps.get("read"):
        return "n/a"
    if caps.get("incremental") is False:
        return "no (declared)"
    if is_file_format or driver in file_sources:
        return "re-read, no cursor"
    return "needs cursor"


def _status(caps: dict[str, Any]) -> str:
    from src.transfer.connector_capabilities import capability_label, effective_status

    label = capability_label(caps)
    status = effective_status(caps)
    notes = []
    if caps.get("certified") is False:
        notes.append("not certified")
    if caps.get("certified_dest") is False:
        notes.append("destination not certified")
    text = f"{label} ({status})"
    return text + ("; " + ", ".join(notes) if notes else "")


def collect_rows() -> list[dict[str, str]]:
    from services import destination_privilege_probe as probe
    from services.connector_capability_registry import get_connector_capability
    from services.preflight_cursor_gate import _FILE_UPSERT_SOURCES
    from src.transfer import connector_capabilities as cc

    rows: list[dict[str, str]] = []
    drivers = sorted(set(cc._DRIVER_CAPS) | {"generic_sql"})
    formats = sorted(cc._FILE_CAPS)
    for driver in drivers + formats:
        is_file = driver in cc._FILE_CAPS
        caps = cc._declared_capabilities(driver)
        record = get_connector_capability(driver)
        writable = bool(caps.get("write"))
        if not writable:
            probe_cell = "n/a"
        else:
            probe_cell = _yes(probe._normalize_engine(driver) in probe._SUPPORTED)
        rows.append(
            {
                "kind": "file format" if is_file else "driver",
                "id": driver,
                "read": _yes(caps.get("read")),
                "write": _yes(writable),
                "incremental": _incremental(driver, caps, is_file, _FILE_UPSERT_SOURCES),
                "cdc": _yes(record.get("supports_cdc")),
                "cdc_sink": _yes(record.get("supports_cdc_sink")),
                "upsert": _yes(record.get("supports_upsert")) if writable else "n/a",
                "merge": str(record.get("merge_strategy") or "") or "-",
                "probe": probe_cell,
                "status": _status(caps),
                "ci": ci_evidence(driver, caps, is_file),
            }
        )
    return rows


def catalog_summary() -> dict[str, Any]:
    from src.transfer import connector_capabilities as cc

    raw = json.loads((API_ROOT / "data" / "connector_catalog.json").read_text(encoding="utf-8"))
    items = raw.get("connectors", raw) if isinstance(raw, dict) else raw
    enriched = [cc.enrich_catalog_entry(dict(item)) for item in items]
    return {
        "tiles": len(enriched),
        "by_status": dict(sorted(Counter(str(e.get("effective_status")) for e in enriched).items())),
        "by_tier": dict(sorted(Counter(str(e.get("certification_tier")) for e in enriched).items())),
        "transfer_ready": sum(1 for e in enriched if e.get("transfer_ready")),
    }


def render() -> str:
    _bootstrap()
    from services.connector_capability_registry import capability_counts

    with _declared_view():
        rows = collect_rows()
        counts = capability_counts()
        catalog = catalog_summary()

    lines: list[str] = []
    lines.append("### Counts")
    lines.append("")
    lines.append("These numbers count different things. Quote the first one as \"connectors that can run a transfer\".")
    lines.append("")
    lines.append("| Count | Value | What it counts |")
    lines.append("|---|---|---|")
    defs = counts["definitions"]
    lines.append(f"| Transfer-live drivers | {counts['transfer_live_drivers']} | {defs['transfer_live_drivers']} |")
    lines.append(
        f"| Transfer-ready catalog drivers | {counts['transfer_ready_catalog_drivers']} "
        f"| {defs['transfer_ready_catalog_drivers']} |"
    )
    lines.append(
        f"| Transfer-ready catalog ids | {counts['transfer_ready_catalog_tiles']} "
        f"| {defs['transfer_ready_catalog_tiles']} |"
    )
    lines.append(
        f"| CDC source engines | {len(counts['cdc_source_engines'])} "
        f"| {defs['cdc_source_engines']} ({', '.join(counts['cdc_source_engines'])}) |"
    )
    status = ", ".join(f"{k}: {v}" for k, v in catalog["by_status"].items())
    tier = ", ".join(f"{k}: {v}" for k, v in catalog["by_tier"].items())
    lines.append(f"| Catalog tiles | {catalog['tiles']} | All tiles in the connector catalog. By status: {status}. |")
    lines.append(
        f"| Catalog tiles by tier | - | {tier}. Tiles whose own record says transfer-ready: "
        f"{catalog['transfer_ready']} (a per-tile answer; the frozen id list above is the shorter one). |"
    )
    lines.append("")

    def table(title: str, kind: str) -> None:
        lines.append(f"### {title}")
        lines.append("")
        lines.append(
            "| Connector | Read | Write | Incremental | CDC source | CDC sink | Upsert | Merge strategy "
            "| Create-privilege probe | Declared status | Engine in CI |"
        )
        lines.append("|---|---|---|---|---|---|---|---|---|---|---|")
        for r in rows:
            if r["kind"] != kind:
                continue
            lines.append(
                f"| `{r['id']}` | {r['read']} | {r['write']} | {r['incremental']} | {r['cdc']} "
                f"| {r['cdc_sink']} | {r['upsert']} | {r['merge']} | {r['probe']} | {r['status']} | {r['ci']} |"
            )
        lines.append("")

    table("Drivers", "driver")
    table("File formats", "file format")
    return "\n".join(lines)


def matrix_ids(markdown: str) -> set[str]:
    """Connector ids listed in the generated block (used by the drift test)."""
    block = markdown[markdown.index(BEGIN) : markdown.index(END)]
    return set(re.findall(r"^\| `([a-z0-9_]+)` \|", block, re.MULTILINE))


def build_document(existing: str) -> str:
    if BEGIN not in existing or END not in existing:
        raise SystemExit(f"{TARGET} is missing the generated-block markers")
    head, _, rest = existing.partition(BEGIN)
    _, _, tail = rest.partition(END)
    return f"{head}{BEGIN}\n\n{render()}\n{END}{tail}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n", 1)[0])
    parser.add_argument("--check", action="store_true", help="exit 1 if the matrix is stale")
    args = parser.parse_args(argv)
    existing = TARGET.read_text(encoding="utf-8")
    updated = build_document(existing)
    if args.check:
        if updated != existing:
            print("docs/reference/connectors.md is stale; run docs/tools/gen_connector_matrix.py")
            return 1
        return 0
    TARGET.write_text(updated, encoding="utf-8")
    print(f"wrote {TARGET.relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
