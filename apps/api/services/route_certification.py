"""Route certification matrix — earned from test results, never from a list.

``PRODUCTION_SKU`` says which source → destination routes we *intend* to sell.
This module says which route × sync-mode cells a test actually proved, by
reading the pytest JUnit XML a run produced.

Tests declare what they proved with :class:`RouteEvidence`, which writes a
``route_cert`` property through pytest's ``record_property``. Declare the cell
at the start of the test so a later skip or failure is still attributed to it,
then call :meth:`RouteEvidence.prove` only after the matching assertions.

Cell status, strongest evidence first:

``failed``      any test for the cell failed or errored
``refused``     a passing test proved the engine refuses the cell, named the
                reason and wrote zero rows
``verified``    a passing test proved every check in :data:`REQUIRED_CHECKS`
``partial``     tests passed but some required checks were never proved
``skipped``     every test for the cell skipped (reasons kept verbatim)
``unverified``  no test evidence at all

Only ``verified`` and ``refused`` are certification. Data cases a cell has not
covered are listed in ``cases_missing``; they do not upgrade or downgrade the
status, they bound what the status means.
"""

from __future__ import annotations

import json
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence

EVIDENCE_PROPERTY = "route_cert"
SCHEMA_VERSION = 1

#: What a transfer cell must prove before it is ``verified``.
REQUIRED_CHECKS: tuple[str, ...] = ("preflight", "write", "independent_readback", "rerun")
#: What a refused cell must prove: the engine wrote nothing.
REFUSAL_CHECKS: tuple[str, ...] = ("zero_writes",)
KNOWN_CHECKS = frozenset(REQUIRED_CHECKS + REFUSAL_CHECKS)

DATA_CASES: tuple[str, ...] = (
    "nulls",
    "numeric_boundaries",
    "timestamps",
    "unicode",
    "duplicate_keys",
    "composite_keys",
    "schema_drift",
    "cancellation",
    "resume",
)

#: Sync modes every PRODUCTION_SKU route is assessed against. A cell without
#: evidence stays ``unverified`` rather than disappearing from the matrix.
ASSESSED_SYNC_MODES: tuple[str, ...] = ("full_refresh_overwrite", "full_refresh_append", "upsert")

STATUSES: tuple[str, ...] = ("failed", "refused", "verified", "partial", "skipped", "unverified")

Route = tuple[str, str, str, str]


def route_label(src_kind: str, src_fmt: str, dst_kind: str, dst_fmt: str) -> str:
    """Same spelling as ``services.sku_honesty.classify_sku_route``."""
    return f"{src_kind}/{src_fmt} → {dst_kind}/{dst_fmt}"


def _ordered(values: Iterable[str], order: Sequence[str]) -> list[str]:
    seen = set(values)
    return [v for v in order if v in seen]


class RouteEvidence:
    """Record, through ``record_property``, what one test proved for one cell."""

    def __init__(
        self,
        record_property: Callable[[str, Any], None],
        route: Sequence[str],
        sync_mode: str,
    ) -> None:
        if len(route) != 4 or not all(route):
            raise ValueError(f"route must be (src_kind, src_fmt, dst_kind, dst_fmt): {route!r}")
        if not sync_mode:
            raise ValueError("sync_mode is required")
        self._record = record_property
        self.route: Route = tuple(str(p) for p in route)  # type: ignore[assignment]
        self.sync_mode = str(sync_mode)
        self._emit(checks=(), cases=())

    def _emit(self, *, checks: Iterable[str], cases: Iterable[str], refusal: str = "") -> None:
        payload: dict[str, Any] = {
            "route": list(self.route),
            "sync_mode": self.sync_mode,
            "checks": list(checks),
            "cases": list(cases),
        }
        if refusal:
            payload["refusal"] = refusal
        self._record(EVIDENCE_PROPERTY, json.dumps(payload, sort_keys=True))

    def prove(self, *checks: str, cases: Iterable[str] = ()) -> None:
        unknown = [c for c in checks if c not in KNOWN_CHECKS]
        if unknown:
            raise ValueError(f"unknown certification check(s): {unknown}")
        cases = tuple(cases)
        bad_cases = [c for c in cases if c not in DATA_CASES]
        if bad_cases:
            raise ValueError(f"unknown data case(s): {bad_cases}")
        self._emit(checks=checks, cases=cases)

    def refused(self, reason: str, *, zero_writes: bool) -> None:
        if not str(reason or "").strip():
            raise ValueError("a refused cell must name its reason")
        self._emit(
            checks=REFUSAL_CHECKS if zero_writes else (),
            cases=(),
            refusal=str(reason).strip(),
        )


MARKER = "route_cert"


def declare_marked_items(items: Iterable[Any]) -> None:
    """Attribute ``@pytest.mark.route_cert(route, sync_mode)`` items at collection.

    Collection-time skips (an absent emulator) never reach the test body, so
    a body-only declaration would leave the cell ``unverified`` instead of
    ``skipped``. Writing the identity into ``user_properties`` here keeps every
    skip on the record.
    """
    for item in items:
        marker = item.get_closest_marker(MARKER)
        if marker is None:
            continue
        route = marker.kwargs.get("route") or (marker.args[0] if marker.args else None)
        mode = marker.kwargs.get("sync_mode") or (marker.args[1] if len(marker.args) > 1 else "")
        if not route or not mode:
            raise ValueError(f"{item.nodeid}: route_cert marker needs route and sync_mode")
        payload = {"route": list(route), "sync_mode": str(mode), "checks": [], "cases": []}
        item.user_properties.append((EVIDENCE_PROPERTY, json.dumps(payload, sort_keys=True)))


def _outcome(case: ET.Element) -> tuple[str, str]:
    for tag, outcome in (("failure", "failed"), ("error", "error"), ("skipped", "skipped")):
        node = case.find(tag)
        if node is not None:
            return outcome, str(node.get("message") or "").strip()
    return "passed", ""


def _skip_reason(message: str) -> str:
    # pytest's JUnit skip message is "<reason>" or "<path>:<line>: <reason>".
    text = message.strip()
    if text.startswith(("Skipped: ", "skipped: ")):
        text = text.split(": ", 1)[1]
    return text


def parse_junit(source: str | Path) -> list[dict[str, Any]]:
    """Testcases from a JUnit XML file that carry route evidence."""
    root = ET.parse(str(source)).getroot()
    out: list[dict[str, Any]] = []
    for case in root.iter("testcase"):
        records: list[dict[str, Any]] = []
        for prop in case.iter("property"):
            if prop.get("name") != EVIDENCE_PROPERTY:
                continue
            try:
                payload = json.loads(prop.get("value") or "")
            except ValueError:
                continue
            route = payload.get("route")
            if not isinstance(route, list) or len(route) != 4 or not payload.get("sync_mode"):
                continue
            records.append(payload)
        if not records:
            continue
        outcome, message = _outcome(case)
        if outcome == "skipped":
            message = _skip_reason(message)
        classname = str(case.get("classname") or "")
        name = str(case.get("name") or "")
        out.append(
            {
                "test": f"{classname}::{name}" if classname else name,
                "outcome": outcome,
                "message": message,
                "records": records,
            }
        )
    return out


def _empty_cell(route: Route, sync_mode: str, in_sku: bool) -> dict[str, Any]:
    return {
        "route": route_label(*route),
        "source_kind": route[0],
        "source_format": route[1],
        "dest_kind": route[2],
        "dest_format": route[3],
        "sync_mode": sync_mode,
        "in_production_sku": in_sku,
        "status": "unverified",
        "checks_proved": [],
        "checks_missing": list(REQUIRED_CHECKS),
        "cases_covered": [],
        "cases_missing": list(DATA_CASES),
        "refusal_reason": None,
        "skip_reasons": [],
        "evidence": [],
    }


def _grade(cell: dict[str, Any], tests: list[dict[str, Any]]) -> None:
    passed = [t for t in tests if t["outcome"] == "passed"]
    failed = [t for t in tests if t["outcome"] in {"failed", "error"}]
    skipped = [t for t in tests if t["outcome"] == "skipped"]

    checks: set[str] = set()
    cases: set[str] = set()
    refusals: list[str] = []
    for test in passed:
        for rec in test["cell_records"]:
            checks.update(rec.get("checks") or ())
            cases.update(c for c in rec.get("cases") or () if c in DATA_CASES)
            if rec.get("refusal"):
                refusals.append(str(rec["refusal"]))

    required = REFUSAL_CHECKS if refusals else REQUIRED_CHECKS
    missing = [c for c in required if c not in checks]
    cell["checks_proved"] = _ordered(checks, REQUIRED_CHECKS + REFUSAL_CHECKS)
    cell["checks_missing"] = missing
    cell["cases_covered"] = _ordered(cases, DATA_CASES)
    cell["cases_missing"] = [c for c in DATA_CASES if c not in cases]
    cell["refusal_reason"] = refusals[0] if refusals else None
    cell["skip_reasons"] = sorted({t["message"] for t in skipped if t["message"]})
    cell["evidence"] = [
        {"test": t["test"], "outcome": t["outcome"], "message": t["message"]} for t in tests
    ]

    if failed:
        cell["status"] = "failed"
    elif passed and not missing:
        cell["status"] = "refused" if refusals else "verified"
    elif passed:
        cell["status"] = "partial"
    elif skipped:
        cell["status"] = "skipped"
    else:
        cell["status"] = "unverified"


def build_route_matrix(
    tests: list[dict[str, Any]],
    *,
    routes: Sequence[Route] | None = None,
    sync_modes: Sequence[str] = ASSESSED_SYNC_MODES,
    source: str = "",
) -> dict[str, Any]:
    """Grade every route × sync-mode cell from parsed JUnit evidence."""
    if routes is None:
        from src.transfer.registry import PRODUCTION_SKU

        routes = PRODUCTION_SKU
    sku = [tuple(r) for r in routes]
    sku_set = set(sku)

    by_cell: dict[tuple[Route, str], list[dict[str, Any]]] = {}
    for test in tests:
        grouped: dict[tuple[Route, str], list[dict[str, Any]]] = {}
        for rec in test["records"]:
            key = (tuple(str(p) for p in rec["route"]), str(rec["sync_mode"]))
            grouped.setdefault(key, []).append(rec)  # type: ignore[arg-type]
        for key, recs in grouped.items():
            by_cell.setdefault(key, []).append({**test, "cell_records": recs})

    keys: list[tuple[Route, str]] = [(r, m) for r in sku for m in sync_modes]
    keys += sorted(k for k in by_cell if k not in set(keys))

    cells = []
    for route, mode in keys:
        cell = _empty_cell(route, mode, route in sku_set)
        _grade(cell, by_cell.get((route, mode), []))
        cells.append(cell)

    outcomes = [t["outcome"] for t in tests]
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": source,
        "required_checks": list(REQUIRED_CHECKS),
        "data_cases": list(DATA_CASES),
        "assessed_sync_modes": list(sync_modes),
        "summary": {
            "cells": len(cells),
            "cells_by_status": {s: sum(1 for c in cells if c["status"] == s) for s in STATUSES},
            "tests_passed": outcomes.count("passed"),
            "tests_failed": outcomes.count("failed") + outcomes.count("error"),
            "tests_skipped": outcomes.count("skipped"),
            "certified_cells": sum(1 for c in cells if c["status"] in {"verified", "refused"}),
        },
        "cells": cells,
    }


def load_route_matrix(path: str | Path) -> dict[str, Any] | None:
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(data, dict) or data.get("schema_version") != SCHEMA_VERSION:
        return None
    return data


def route_certification_rollup(matrix: dict[str, Any] | None, route: str) -> dict[str, Any]:
    """Per-route view for the proof ledger: one entry per assessed sync mode."""
    if not matrix:
        return {"status": "unverified", "modes": [], "generated_at": None}
    modes = [
        {
            "sync_mode": c["sync_mode"],
            "status": c["status"],
            "checks_missing": c["checks_missing"],
            "cases_missing": c["cases_missing"],
            "skip_reasons": c["skip_reasons"],
            "refusal_reason": c["refusal_reason"],
        }
        for c in matrix.get("cells") or []
        if c.get("route") == route
    ]
    return {
        "status": _rollup_status([m["status"] for m in modes]),
        "modes": modes,
        "generated_at": matrix.get("generated_at"),
    }


def _rollup_status(statuses: list[str]) -> str:
    """A route is only ``verified`` when every assessed mode is certified."""
    if not statuses:
        return "unverified"
    if "failed" in statuses:
        return "failed"
    certified = [s for s in statuses if s in {"verified", "refused"}]
    if len(certified) == len(statuses):
        return "verified"
    if certified or "partial" in statuses:
        return "partial"
    if "skipped" in statuses:
        return "skipped"
    return "unverified"
