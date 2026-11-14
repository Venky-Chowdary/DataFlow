#!/usr/bin/env python3
"""Generate the capability matrix in ``docs/reference/connectors.md``.

Every cell is read from code or from the CI evidence snapshot, so the page
cannot drift from what the product does or claims:

* read / write / status   ``src.transfer.connector_capabilities`` (declared
                          driver and file tables)
* CDC, upsert, merge      ``services.connector_capability_registry.get_connector_capability``
                          (the single capability function)
* create-privilege probe  ``services.destination_privilege_probe._SUPPORTED``
* incremental rule        ``services.preflight_cursor_gate`` plus the driver's
                          own ``incremental`` declaration
* tier                    ``connector_capabilities.certification_tier`` (through
                          ``enrich_catalog_entry``): "certified" needs CI proof
* CI evidence             ``services.connector_proof`` over the committed
                          snapshot ``apps/api/data/connector_ci_evidence.json``,
                          the same data the public claims are generated from
                          (``apps/api/scripts/connector_claims_report.py``)

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

# Wording for the tier a driver's own catalog tile gets
# (``connector_capabilities.certification_tier``). "Certified" needs CI proof.
TIER_TEXT = {
    "certified": "Certified",
    "implemented": "Implemented, not yet proven",
    "source_only": "Source only",
    "connect_only": "Connection test only",
    "planned": "Planned",
}

# Wording for an engine's CI status (``services.connector_ci_evidence``).
CI_STATUS_TEXT = {
    "ci_green": "proven",
    "ci_red": "not proven: a proving job did not run green",
    "not_run": "not proven: the proving job did not run (opt-in)",
    "no_ci_result": "not proven: no result recorded for the proving job",
    "no_ci_job": "not proven: no CI job proves it",
}


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


def ci_evidence(driver: str) -> str:
    """CI proof for a driver, from the committed evidence snapshot (W5).

    Only a green run of a job that proves the engine, with none of the
    engine's tests skipped, counts. What the workflows configure never does.
    """
    from services.connector_proof import proof_for

    proof = proof_for(driver, driver)
    text = CI_STATUS_TEXT.get(proof["status"], f"not proven: {proof['status'] or 'no CI job proves it'}")
    if proof["proven"] and proof["emulated"]:
        text += " (against an emulator)"
    if proof["engine"] and proof["engine"] != driver:
        text += f"; engine `{proof['engine']}`"
    return text


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


def _tier(driver: str, caps: dict[str, Any]) -> str:
    """The tier the driver's own catalog tile gets, with the product's notes."""
    from src.transfer.connector_capabilities import effective_status, enrich_catalog_entry

    tier = str(enrich_catalog_entry({"id": driver}).get("certification_tier") or "planned")
    text = TIER_TEXT.get(tier, tier)
    notes = []
    if effective_status(caps) == "beta":
        notes.append("beta, tested against fixtures")
    if caps.get("certified") is False:
        notes.append("not offered as transfer-ready")
    if caps.get("certified_dest") is False:
        notes.append("destination role not offered as transfer-ready")
    return text + ("; " + "; ".join(notes) if notes else "")


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
                "tier": _tier(driver, caps),
                "ci": ci_evidence(driver),
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
    from services.connector_proof import evidence_snapshot, proven_and_pending

    with _declared_view():
        rows = collect_rows()
        counts = capability_counts()
        catalog = catalog_summary()
        split = proven_and_pending(counts["transfer_live_driver_types"])
    snap = evidence_snapshot()
    run = str(snap.get("source_run") or "").rstrip("/").rsplit("/", 1)[-1] or "none"

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
        f"| CI-proven drivers | {len(split['proven'])} | Transfer-live drivers whose proving CI job ran green "
        f"with none of their tests skipped ({', '.join(split['proven']) or 'none'}). The other "
        f"{len(split['pending'])} are implemented but not yet proven. Evidence: CI run {run}, "
        f"recorded {str(snap.get('generated_at') or 'never')[:10]}. |"
    )
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
            "| Create-privilege probe | Tier | CI evidence |"
        )
        lines.append("|---|---|---|---|---|---|---|---|---|---|---|")
        for r in rows:
            if r["kind"] != kind:
                continue
            lines.append(
                f"| `{r['id']}` | {r['read']} | {r['write']} | {r['incremental']} | {r['cdc']} "
                f"| {r['cdc_sink']} | {r['upsert']} | {r['merge']} | {r['probe']} | {r['tier']} | {r['ci']} |"
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
