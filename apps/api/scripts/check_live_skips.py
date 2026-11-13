#!/usr/bin/env python3
"""Fail a live-engine CI job when one of its tests skipped unexpectedly (A3).

A live-engine job exists to prove an engine. A test that skips there proves
nothing, yet pytest exits 0 and the job goes green: that is how the
cdc-sqlserver job reported success for months with every live test skipped.
This checker reads the job's pytest junit XML and the job's entry in
``scripts/live_engine_jobs.json`` and:

* fails when a skip is not in the job's ``expected_skips`` allowlist (each
  entry names the test, the skip message it must carry, and why);
* fails when a test failed, or when fewer tests executed than the job's
  ``min_executed`` floor (a selection that collapsed to nothing is not green);
* writes a result JSON that the docs build consumes: an engine is
  "live-verified" only when a job listed for it produced ``status: green``
  (``services/connector_ci_evidence.py``).

Modes (per job, in the manifest):

``strict``         every skip must be expected.
``engine_scoped``  only a skip whose message names one of the job's engines
                   must be expected (whole-suite jobs, where SaaS or Oracle
                   skips are not this job's claim).
``report``         everything is counted and published; the job never fails.

Usage (from apps/api)::

    python scripts/check_live_skips.py --job cdc-sqlserver \\
        --junit "$RUNNER_TEMP/live-junit/sqlserver-cdc.xml" \\
        --out "$RUNNER_TEMP/live-engine/cdc-sqlserver.json"
    python scripts/check_live_skips.py --job cdc-oracle --not-run "ENABLE_ORACLE_CDC is not 'true'" \\
        --out "$RUNNER_TEMP/live-engine/cdc-oracle.json"
"""

from __future__ import annotations

import argparse
import fnmatch
import json
import os
import re
import xml.etree.ElementTree as ET  # nosec B405 - parses junit this job just wrote
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

MANIFEST = Path(__file__).resolve().with_name("live_engine_jobs.json")
SCHEMA_VERSION = 1
MODES = ("strict", "engine_scoped", "report")


@dataclass(frozen=True)
class Case:
    """One pytest test as junit recorded it."""

    test: str
    outcome: str  # passed | failed | skipped | xfailed
    message: str = ""


@dataclass
class CheckResult:
    job: str
    mode: str
    engines: list[str]
    ran: bool
    not_run_reason: str = ""
    cases: list[Case] = field(default_factory=list)
    expected: list[dict[str, str]] = field(default_factory=list)
    unexpected: list[dict[str, str]] = field(default_factory=list)
    ignored: list[dict[str, str]] = field(default_factory=list)
    stale_allowlist: list[dict[str, str]] = field(default_factory=list)
    problems: list[str] = field(default_factory=list)
    min_executed: int = 0
    opt_in: str = ""

    def count(self, outcome: str) -> int:
        return sum(1 for c in self.cases if c.outcome == outcome)

    @property
    def executed(self) -> int:
        return self.count("passed") + self.count("failed")

    @property
    def status(self) -> str:
        if not self.ran:
            return "not_run"
        if self.problems or self.unexpected or self.count("failed"):
            return "red"
        return "green"

    @property
    def enforced(self) -> bool:
        return self.mode != "report"

    def exit_code(self) -> int:
        if not self.enforced:
            return 0
        if self.status == "not_run":
            # An opt-in job may legitimately not run; it is then not
            # live-verified, which the evidence file says. Anything else
            # that did not run is a broken job.
            return 0 if self.opt_in else 1
        return 0 if self.status == "green" else 1

    def to_json(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "job": self.job,
            "mode": self.mode,
            "enforced": self.enforced,
            "engines": list(self.engines),
            "status": self.status,
            "ran": self.ran,
            "not_run_reason": self.not_run_reason,
            "opt_in": self.opt_in,
            "counts": {
                "tests": len(self.cases),
                "executed": self.executed,
                "passed": self.count("passed"),
                "failed": self.count("failed"),
                "skipped": self.count("skipped"),
                "xfailed": self.count("xfailed"),
                "expected_skips": len(self.expected),
                "unexpected_skips": len(self.unexpected),
                "other_skips": len(self.ignored),
                "min_executed": self.min_executed,
            },
            "unexpected_skips": self.unexpected,
            "expected_skips": self.expected,
            "stale_allowlist": self.stale_allowlist,
            "problems": self.problems,
            "run": {
                key: os.environ.get(env, "")
                for key, env in (
                    ("sha", "GITHUB_SHA"),
                    ("run_id", "GITHUB_RUN_ID"),
                    ("ref", "GITHUB_REF_NAME"),
                    ("event", "GITHUB_EVENT_NAME"),
                    ("workflow", "GITHUB_WORKFLOW"),
                )
            },
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }


def load_manifest(path: Path = MANIFEST) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if data.get("schema_version") != SCHEMA_VERSION:
        raise ValueError(f"{path}: unsupported schema_version {data.get('schema_version')!r}")
    for name, job in (data.get("jobs") or {}).items():
        if job.get("mode") not in MODES:
            raise ValueError(f"{path}: job {name!r} has mode {job.get('mode')!r}; use one of {MODES}")
        unknown = set(job.get("engines") or []) - set(data.get("engine_tokens") or {})
        if unknown:
            raise ValueError(f"{path}: job {name!r} names engines without tokens: {sorted(unknown)}")
        for entry in job.get("expected_skips") or []:
            missing = [k for k in ("test", "reason", "why") if not str(entry.get(k) or "").strip()]
            if missing:
                raise ValueError(f"{path}: job {name!r} expected_skips entry {entry!r} lacks {missing}")
            re.compile(entry["reason"])
    return data


def node_id(classname: str, name: str) -> str:
    """Rebuild the pytest node id from a junit ``classname`` and ``name``.

    ``tests.test_x.TestY`` + ``test_z[p]`` -> ``tests/test_x.py::TestY::test_z[p]``.
    Module parts are lower-case (``test_*``), class parts start upper-case.
    A collection-level skip has no classname; its name is kept as is.
    """
    if not classname:
        return name
    parts = classname.split(".")
    split = len(parts)
    for i, part in enumerate(parts):
        if part[:1].isupper():
            split = i
            break
    module = "/".join(parts[:split]) + ".py"
    return "::".join([module, *parts[split:], name])


def read_junit(paths: Iterable[Path]) -> list[Case]:
    cases: list[Case] = []
    for path in paths:
        root = ET.parse(path).getroot()  # nosec B314 - CI-generated junit
        for tc in root.iter("testcase"):
            test = node_id(tc.get("classname") or "", tc.get("name") or "")
            failure = tc.find("failure")
            if failure is None:
                failure = tc.find("error")
            skipped = tc.find("skipped")
            if failure is not None:
                cases.append(Case(test, "failed", failure.get("message") or ""))
            elif skipped is not None and skipped.get("type") == "pytest.xfail":
                cases.append(Case(test, "xfailed", skipped.get("message") or ""))
            elif skipped is not None:
                cases.append(Case(test, "skipped", skipped.get("message") or skipped.text or ""))
            else:
                cases.append(Case(test, "passed"))
    return cases


def names_engine(message: str, engines: Iterable[str], tokens: dict[str, list[str]]) -> list[str]:
    """Engines among ``engines`` whose token appears as a word in ``message``.

    Word edges are any non-alphanumeric character, so ``DATAFLOW_PG_TEST_DSN
    unset`` names postgresql and ``pgvector`` does not match ``pg``.
    """
    text = (message or "").lower()
    hit = []
    for engine in engines:
        for token in tokens.get(engine) or []:
            if re.search(rf"(?<![a-z0-9]){re.escape(token.lower())}(?![a-z0-9])", text):
                hit.append(engine)
                break
    return hit


def _match_entry(case: Case, entry: dict[str, str]) -> bool:
    return fnmatch.fnmatchcase(case.test, entry["test"]) and re.search(
        entry["reason"], case.message or ""
    ) is not None


def check(
    job: str,
    cases: list[Case],
    manifest: dict[str, Any],
    *,
    missing_junit: Iterable[str] = (),
    not_run: str = "",
) -> CheckResult:
    jobs = manifest.get("jobs") or {}
    if job not in jobs:
        raise KeyError(f"job {job!r} is not in the live-engine manifest ({sorted(jobs)})")
    spec = jobs[job]
    result = CheckResult(
        job=job,
        mode=spec["mode"],
        engines=list(spec.get("engines") or []),
        ran=not not_run,
        not_run_reason=not_run,
        cases=list(cases),
        min_executed=int(spec.get("min_executed") or 0),
        opt_in=str(spec.get("opt_in") or ""),
    )
    if not_run:
        if not result.opt_in and result.enforced:
            result.problems.append(f"job is not opt-in but did not run: {not_run}")
        return result
    for path in missing_junit:
        result.problems.append(f"junit file missing: {path}")
    if result.executed < result.min_executed:
        result.problems.append(
            f"only {result.executed} tests executed; the job's floor is {result.min_executed}"
        )
    allow = list(spec.get("expected_skips") or [])
    used = [False] * len(allow)
    tokens = manifest.get("engine_tokens") or {}
    for case in cases:
        if case.outcome != "skipped":
            continue
        row = {"test": case.test, "reason": case.message}
        hit = next((i for i, entry in enumerate(allow) if _match_entry(case, entry)), None)
        if hit is not None:
            used[hit] = True
            result.expected.append({**row, "why": allow[hit]["why"]})
            continue
        if result.mode == "engine_scoped" or result.mode == "report":
            named = names_engine(case.message, result.engines, tokens)
            if not named:
                result.ignored.append(row)
                continue
            row["engines"] = ",".join(named)
        result.unexpected.append(row)
    result.stale_allowlist = [
        {"test": e["test"], "reason": e["reason"]} for e, u in zip(allow, used) if not u
    ]
    if result.mode == "report":
        # Report mode publishes engine-attributed skips without failing.
        result.problems = []
    return result


def summary_markdown(result: CheckResult, limit: int = 40) -> str:
    data = result.to_json()
    c = data["counts"]
    lines = [
        f"### Live-engine skips: `{result.job}` ({result.mode})",
        f"- status: **{result.status.upper()}** — engines: {', '.join(result.engines)}",
    ]
    if not result.ran:
        lines.append(f"- not run: {result.not_run_reason}")
        return "\n".join(lines) + "\n"
    lines.append(
        f"- {c['executed']} executed ({c['passed']} passed, {c['failed']} failed), "
        f"{c['skipped']} skipped: {c['expected_skips']} expected, "
        f"{c['unexpected_skips']} unexpected, {c['other_skips']} not this job's engines"
    )
    for problem in result.problems:
        lines.append(f"- problem: {problem}")
    for row in result.unexpected[:limit]:
        lines.append(f"- unexpected skip: `{row['test']}` — {row['reason'][:200]}")
    if len(result.unexpected) > limit:
        lines.append(f"- … {len(result.unexpected) - limit} more in the result JSON")
    for row in result.stale_allowlist:
        lines.append(f"- allowlist entry matched nothing: `{row['test']}` / `{row['reason']}`")
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--job", required=True)
    parser.add_argument("--junit", action="append", default=[], help="junit XML (repeatable)")
    parser.add_argument("--manifest", default=str(MANIFEST))
    parser.add_argument("--out", help="write the result JSON here")
    parser.add_argument("--not-run", default="", help="record that the job did not run, and why")
    args = parser.parse_args(argv)

    manifest = load_manifest(Path(args.manifest))
    paths = [Path(p) for p in args.junit]
    present = [p for p in paths if p.is_file()]
    missing = [str(p) for p in paths if not p.is_file()]
    if not args.not_run and not paths:
        missing.append("(no --junit given)")
    result = check(
        args.job,
        read_junit(present),
        manifest,
        missing_junit=missing,
        not_run=args.not_run,
    )
    if args.out:
        out = Path(args.out)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(result.to_json(), indent=2) + "\n", encoding="utf-8")
    text = summary_markdown(result)
    print(text)
    step_summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if step_summary:
        with open(step_summary, "a", encoding="utf-8") as fh:
            fh.write(text)
    return result.exit_code()


if __name__ == "__main__":
    raise SystemExit(main())
