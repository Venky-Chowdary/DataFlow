"""Which engines CI actually ran green: the only source for "live-verified" (A4).

The connector capability page used to say what CI *configures* (a regex over
the workflow YAML found ``mssql/server`` and printed "live container"), not
what CI *proved*. SQL Server was shown that way while its job skipped every
live test, and Kafka while the only job that starts it, the nightly, was
being cancelled at its time limit.

This module turns the per-job results written by
``scripts/check_live_skips.py`` into one evidence record per engine:

``ci_green``      a job that proves the engine ran with no failure, no
                  unexpected skip and at least its floor of executed tests;
                  no job that proves it is red. Only this is live-verified.
``ci_red``        a job that proves the engine failed or skipped.
``not_run``       the proving jobs did not run (opt-in, e.g. Oracle).
``no_ci_result``  a job is meant to prove it, but no result was supplied
                  (a local docs build, or the nightly has not reported).
``no_ci_job``     no job proves it.

Peers do not count: SQL Server's job also starts PostgreSQL, but only jobs
whose manifest entry ``proves`` an engine speak for it. Standard library
only, so the CI evidence job needs no dependency install.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

MANIFEST = Path(__file__).resolve().parents[1] / "scripts" / "live_engine_jobs.json"
SCHEMA_VERSION = 1
LIVE_VERIFIED = "ci_green"


def load_manifest(path: Path = MANIFEST) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def load_results(paths: Iterable[Path]) -> list[dict[str, Any]]:
    """Result JSONs from ``check_live_skips.py``; a directory is searched recursively."""
    found: list[dict[str, Any]] = []
    for path in paths:
        path = Path(path)
        files = sorted(path.rglob("*.json")) if path.is_dir() else [path]
        for file in files:
            try:
                data = json.loads(file.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                continue  # not a checker result (other artifacts share the folder)
            if isinstance(data, dict) and data.get("schema_version") == SCHEMA_VERSION and data.get("job"):
                found.append(data)
    return found


def _missing_variants(name: str, spec: dict[str, Any], runs: list[dict[str, Any]]) -> list[str]:
    """``job[key=value]`` for each required variant with no green run.

    cdc-sqlserver declares ``required_labels: {"driver": ["pymssql", "pyodbc"]}``:
    production dials SQL Server through pymssql, so a pyodbc-only green does
    not make SQL Server live-verified.
    """
    gaps = []
    for key, values in (spec.get("required_labels") or {}).items():
        for value in values:
            if not any(r["status"] == "green" and r["labels"].get(key) == value for r in runs):
                gaps.append(f"{name}[{key}={value}]")
    return gaps


def engine_evidence(
    manifest: dict[str, Any], results: Iterable[dict[str, Any]]
) -> dict[str, dict[str, Any]]:
    """One evidence record per engine named anywhere in the manifest."""
    jobs = manifest.get("jobs") or {}
    by_job: dict[str, list[dict[str, Any]]] = {}
    for result in results:
        by_job.setdefault(str(result["job"]), []).append(result)

    engines = sorted(
        set(manifest.get("engine_tokens") or {})
        | {e for spec in jobs.values() for e in spec.get("engines") or []}
    )
    out: dict[str, dict[str, Any]] = {}
    for engine in engines:
        proving = [
            name
            for name, spec in sorted(jobs.items())
            if spec.get("mode") != "report" and engine in (spec.get("proves") or [])
        ]
        runs = []
        for name in proving:
            for result in by_job.get(name, []):
                runs.append(
                    {
                        "job": name,
                        "workflow": jobs[name].get("workflow", ""),
                        "status": result.get("status", ""),
                        "executed": (result.get("counts") or {}).get("executed", 0),
                        "unexpected_skips": (result.get("counts") or {}).get("unexpected_skips", 0),
                        "sha": (result.get("run") or {}).get("sha", ""),
                        "run_id": (result.get("run") or {}).get("run_id", ""),
                        "not_run_reason": result.get("not_run_reason", ""),
                        "labels": dict(result.get("labels") or {}),
                    }
                )
        statuses = {r["status"] for r in runs}
        missing = [n for n in proving if n not in by_job]
        complete = []
        for name in proving:
            gaps = _missing_variants(name, jobs[name], [r for r in runs if r["job"] == name])
            missing += gaps
            if name in by_job and not gaps and all(
                r["status"] == "green" for r in runs if r["job"] == name
            ):
                complete.append(name)
        if not proving:
            status = "no_ci_job"
        elif "red" in statuses:
            status = "ci_red"
        elif complete:
            status = LIVE_VERIFIED
        elif "not_run" in statuses:
            status = "not_run"
        else:
            status = "no_ci_result"
        emulated = any(engine in (jobs[name].get("emulated") or []) for name in proving)
        out[engine] = {
            "status": status,
            "live_verified": status == LIVE_VERIFIED,
            "emulated": emulated,
            "proving_jobs": proving,
            "missing_results": missing,
            "drivers": sorted(
                {r["labels"]["driver"] for r in runs if r["status"] == "green" and r["labels"].get("driver")}
            ),
            "runs": runs,
        }
    return out


def evidence_document(
    manifest: dict[str, Any], results: Iterable[dict[str, Any]]
) -> dict[str, Any]:
    from datetime import datetime, timezone

    results = list(results)
    engines = engine_evidence(manifest, results)
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "scripts/check_live_skips.py results; manifest scripts/live_engine_jobs.json",
        "jobs_reported": sorted({str(r["job"]) for r in results}),
        "live_verified": sorted(e for e, v in engines.items() if v["live_verified"]),
        "engines": engines,
        "honesty_note": (
            "live_verified means a CI job that proves the engine ran green in the runs "
            "supplied here. Workflow configuration alone never makes an engine live-verified."
        ),
    }
