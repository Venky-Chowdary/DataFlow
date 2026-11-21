#!/usr/bin/env python3
"""Decide whether this PR run may reuse the backend checks of an earlier commit.

A push that changes only the web app (apps/web, packages/design-system, the
root npm manifests, Markdown docs) leaves every backend input byte-identical.
The backend jobs (API test shards, live-engine jobs, image, security, evals)
then prove nothing new, and they take over an hour.

Reuse is allowed only when an ancestor of HEAD (at most 25 commits back) has
every backend check green, and every path changed between that ancestor and
HEAD is web-only. Anything else, including any change under .github/ or
apps/api/, runs the full workflow. The answer goes to $GITHUB_OUTPUT as
reuse=true|false plus the commit reused, and is written to the job summary.

Standard library only. Reads the check runs with the workflow's GITHUB_TOKEN.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import urllib.request

BACKEND_CHECKS = (
    *(f"api-tests ({i}/6)" for i in range(1, 7)),
    "cdc-sqlserver (pymssql)",
    "cdc-sqlserver (pyodbc)",
    "no-config-transfer",
    "mapping-evidence",
    "ai-offline-correctness",
    "pilot-answer-quality",
    "gitops",
    "security",
    "API image (runtime + companion source bundle)",
)
WEB_ONLY = re.compile(
    r"^(apps/web/|packages/design-system/|package\.json$|package-lock\.json$|docs/)"
    r"|^[^/]+\.md$"
)


def _git(*args: str) -> str:
    return subprocess.check_output(["git", *args], text=True).strip()


def _check_runs(repo: str, sha: str, token: str) -> dict[str, str]:
    out: dict[str, str] = {}
    page = 1
    while True:
        req = urllib.request.Request(
            f"https://api.github.com/repos/{repo}/commits/{sha}/check-runs?per_page=100&page={page}",
            headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
        )
        with urllib.request.urlopen(req, timeout=30) as resp:  # nosec B310 - api.github.com
            data = json.load(resp)
        for run in data.get("check_runs", []):
            # Later attempts of one check overwrite earlier ones only when they succeeded.
            if out.get(run["name"]) != "success":
                out[run["name"]] = run.get("conclusion") or run.get("status") or ""
        if len(data.get("check_runs", [])) < 100:
            return out
        page += 1


def decide(repo: str, token: str) -> tuple[bool, str, str]:
    if os.environ.get("GITHUB_EVENT_NAME") != "pull_request":
        return False, "", "not a pull_request run"
    merge = _git("rev-list", "--parents", "-n", "1", "HEAD").count(" ") == 2
    head = _git("rev-parse", "HEAD^2") if merge else _git("rev-parse", "HEAD")
    base = _git("rev-parse", "HEAD^1") if merge else ""
    for sha in _git("rev-list", "--max-count=25", f"{head}~1").split():
        runs = _check_runs(repo, sha, token)
        if not all(runs.get(name) == "success" for name in BACKEND_CHECKS):
            continue
        changed = [p for p in _git("diff", "--name-only", sha, head).splitlines() if p]
        backend = [p for p in changed if not WEB_ONLY.search(p)]
        if backend:
            return False, sha, f"backend paths changed since {sha[:9]}: {', '.join(backend[:8])}"
        if base:
            # The run is on HEAD merged into the base branch: backend changes that
            # reached the base since that commit forked must be tested too.
            fork = _git("merge-base", sha, base)
            moved = [p for p in _git("diff", "--name-only", fork, base).splitlines() if p and not WEB_ONLY.search(p)]
            if moved:
                return False, sha, f"the base branch gained backend changes since {sha[:9]}: {', '.join(moved[:8])}"
        return True, sha, f"only web paths changed since {sha[:9]} ({len(changed)} files); its backend checks are green"
    return False, "", "no ancestor in the last 25 commits has every backend check green"


def main() -> int:
    repo, token = os.environ["GITHUB_REPOSITORY"], os.environ["GITHUB_TOKEN"]
    try:
        reuse, sha, why = decide(repo, token)
    except Exception as exc:  # noqa: BLE001 - any doubt runs everything
        reuse, sha, why = False, "", f"could not decide ({type(exc).__name__}); running everything"
    print(f"reuse={str(reuse).lower()} from={sha} because {why}")
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as fh:
        fh.write(f"reuse={str(reuse).lower()}\nfrom={sha}\n")
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as fh:
            verdict = f"reused from `{sha[:9]}`" if reuse else "run in full"
            fh.write(f"### Backend checks: {verdict}\n- {why}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
