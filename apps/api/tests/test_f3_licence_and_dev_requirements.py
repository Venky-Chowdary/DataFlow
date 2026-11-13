"""F3: test tools stay out of the production install; licences are gated.

* ``requirements.txt`` (what Dockerfile.api installs) carries no test tool;
  ``requirements-dev.txt`` carries them and includes the runtime file.
* Product code never imports a test tool at module level (moto / fakesnow are
  reached only inside guarded optional paths).
* Every CI job that runs pytest installs the dev file.
* ``scripts/licence_scan.py`` classifies licences, honours reviewed
  exceptions, fails on new copyleft / unknown ones and on a stale NOTICE.
"""

from __future__ import annotations

import importlib.util
import json
import re
import sys
from pathlib import Path

import pytest
import yaml

API = Path(__file__).resolve().parents[1]
REPO = API.parents[1]
TEST_TOOLS = {"pytest", "pytest-asyncio", "pytest-timeout", "hypothesis", "moto", "fakesnow", "responses"}


def _load_scanner():
    spec = importlib.util.spec_from_file_location("licence_scan", REPO / "scripts" / "licence_scan.py")
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module  # dataclasses resolve the module by name
    spec.loader.exec_module(module)
    return module


scan = _load_scanner()


def _names(path: Path) -> set[str]:
    out = set()
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.split("#", 1)[0].strip()
        if line and not line.startswith("-"):
            out.add(re.split(r"[<>=!~\[; ]", line, maxsplit=1)[0].lower())
    return out


def test_runtime_requirements_carry_no_test_tool() -> None:
    leaked = _names(API / "requirements.txt") & TEST_TOOLS
    assert not leaked, f"move {sorted(leaked)} to requirements-dev.txt"


def test_dev_requirements_include_runtime_and_the_test_tools() -> None:
    text = (API / "requirements-dev.txt").read_text(encoding="utf-8")
    assert re.search(r"^-r requirements\.txt$", text, re.MULTILINE)
    assert TEST_TOOLS <= _names(API / "requirements-dev.txt")


def test_production_image_installs_runtime_only() -> None:
    dockerfile = (REPO / "Dockerfile.api").read_text(encoding="utf-8")
    assert "requirements-dev" not in dockerfile
    assert "[dev]" not in dockerfile


def test_product_code_never_imports_a_test_tool_at_module_level() -> None:
    top_level = re.compile(r"^(?:import|from)\s+(pytest|hypothesis|responses|moto|fakesnow)\b", re.MULTILINE)
    offenders = []
    for root in ("connectors", "services", "src"):
        for path in (API / root).rglob("*.py"):
            if "/tests/" in path.as_posix():
                continue
            match = top_level.search(path.read_text(encoding="utf-8", errors="replace"))
            if match:
                offenders.append(f"{path.relative_to(API)}: {match.group(0)}")
    assert not offenders, offenders


def _ci_files() -> list[Path]:
    return sorted((REPO / ".github" / "workflows").glob("*.yml"))


def test_every_ci_job_that_runs_pytest_installs_the_dev_requirements() -> None:
    problems = []
    for path in _ci_files():
        jobs = (yaml.safe_load(path.read_text(encoding="utf-8")) or {}).get("jobs") or {}
        for name, job in jobs.items():
            steps = job.get("steps") or []
            runs = "\n".join(str(s.get("run") or "") for s in steps)
            uses = "\n".join(str(s.get("uses") or "") for s in steps)
            runs_pytest = "pytest" in runs or "npm run test:cdc" in runs or "warehouse_sku_report" in runs
            installs_dev = "requirements-dev.txt" in runs or "./.github/actions/ci-services" in uses
            if runs_pytest and not installs_dev:
                problems.append(f"{path.name}:{name}")
    action = (REPO / ".github" / "actions" / "ci-services" / "action.yml").read_text(encoding="utf-8")
    assert "requirements-dev.txt" in action
    assert not problems, f"jobs run pytest without requirements-dev.txt: {problems}"


# ── scanner ────────────────────────────────────────────────────────────────

POLICY = {
    "allowed": ["MIT", "Apache-2.0", "BSD-3-Clause"],
    "exceptions": {"pypi:lgpl-lib": {"licence": "LGPL-2.1", "why": "reviewed"}},
}


@pytest.mark.parametrize(
    ("raw", "spdx"),
    [
        ("MIT License", "MIT"),
        ("Apache Software License", "Apache-2.0"),
        ("Apache License, Version 2.0", "Apache-2.0"),
        ("new BSD", "BSD-3-Clause"),
        ("UNKNOWN", ""),
        ("", ""),
        ("Permission is hereby granted, free of charge, to any person " + "x" * 120, "MIT"),
    ],
)
def test_normalise(raw: str, spdx: str) -> None:
    assert scan.normalise(raw) == spdx


def _pkg(name: str, licence: str, ecosystem: str = "pypi", scope: str = "runtime"):
    return scan.classify(scan.Package(ecosystem, name, "1.0", licence, scope, "test"), POLICY)


@pytest.mark.parametrize(
    ("licence", "status"),
    [
        ("MIT", "allowed"),
        ("MIT OR GPL-3.0", "allowed"),  # dual licence: the permissive option is ours
        ("MIT AND Apache-2.0", "allowed"),
        ("MIT AND LGPL-2.1", "copyleft"),
        ("GPL-3.0", "copyleft"),
        ("AGPL-3.0", "copyleft"),
        ("SSPL-1.0", "copyleft"),
        ("MPL-2.0", "copyleft"),
        ("Other/Proprietary License", "unknown"),
        ("", "unknown"),
    ],
)
def test_classify(licence: str, status: str) -> None:
    assert _pkg("x", licence).status == status


def test_a_new_copyleft_dependency_fails_until_reviewed() -> None:
    problems = scan.violations([_pkg("new-gpl", "GPL-3.0"), _pkg("lgpl-lib", "LGPL-2.1")], POLICY)
    assert len(problems) == 1 and "new-gpl" in problems[0]


def test_a_reviewed_dependency_whose_licence_changed_fails() -> None:
    problems = scan.violations([_pkg("lgpl-lib", "AGPL-3.0")], POLICY)
    assert problems and "changed" in problems[0]


def test_policy_exceptions_need_a_licence_and_a_why(tmp_path: Path) -> None:
    path = tmp_path / "p.json"
    path.write_text(json.dumps({"allowed": [], "exceptions": {"pypi:x": {"licence": "GPL"}}}), encoding="utf-8")
    with pytest.raises(ValueError, match="lacks"):
        scan.load_policy(path)
    path.write_text(json.dumps({"allowed": [], "exceptions": {"x": {"licence": "GPL", "why": "y"}}}), encoding="utf-8")
    with pytest.raises(ValueError, match="pypi: or npm:"):
        scan.load_policy(path)


def test_the_repository_policy_loads_with_no_decision_left_pending() -> None:
    policy = scan.load_policy(REPO / "licence-policy.json")
    assert not [k for k, v in policy["exceptions"].items() if v.get("owner_decision")]
    # Owner decision 3: the proprietary vendor drivers are operator-installed,
    # so they are not dependencies of anything Datawrap ships.
    assert "pypi:hdbcli" not in policy["exceptions"]
    assert "pypi:teradatasql" not in policy["exceptions"]


def test_npm_scan_reads_the_lockfile(tmp_path: Path) -> None:
    lock = {
        "lockfileVersion": 3,
        "packages": {
            "": {"name": "root"},
            "apps/web": {"dependencies": {"react": "^18", "@own/ds": "*"}},
            "node_modules/@own/ds": {"link": True, "resolved": "packages/ds"},
            "node_modules/react": {"version": "18.3.1", "license": "MIT"},
            "node_modules/vite": {"version": "5.0.0", "license": "MIT", "dev": True},
            "node_modules/weird": {"version": "1.0.0"},
        },
    }
    path = tmp_path / "package-lock.json"
    path.write_text(json.dumps(lock), encoding="utf-8")
    rows = {p.name: p for p in scan.scan_npm(path)}
    assert rows["react"].scope == "runtime" and rows["react"].licence == "MIT"
    assert rows["vite"].scope == "dev"
    assert scan.classify(rows["weird"], POLICY).status == "unknown"
    assert "@own/ds" not in rows
    direct = scan.direct_dependencies(tmp_path / "missing.txt", path)
    assert direct == {"npm:react"}


def test_notice_names_every_direct_production_dependency() -> None:
    direct = scan.direct_dependencies()
    gaps = scan.notice_gaps(direct, (REPO / "NOTICE").read_text(encoding="utf-8"))
    assert not gaps, f"regenerate NOTICE (scripts/licence_scan.py --notice NOTICE): {gaps}"
    assert "pypi:fastapi" in direct and "npm:react" in direct


# ── owner decision 1: the project is proprietary ───────────────────────────

_FIRST_PARTY_NPM = ("package.json", "apps/web/package.json", "packages/design-system/package.json")


def test_license_file_states_the_owner_and_all_rights_reserved() -> None:
    text = (REPO / "LICENSE").read_text(encoding="utf-8")
    assert "Copyright (c) 2026 Venky Chowdary. All rights reserved." in text
    assert "written permission" in text and '"AS IS"' in text
    assert "@" not in text, "no e-mail address in LICENSE"


def test_first_party_package_metadata_is_proprietary() -> None:
    for rel in _FIRST_PARTY_NPM:
        data = json.loads((REPO / rel).read_text(encoding="utf-8"))
        assert data.get("license") == "UNLICENSED", rel
        assert data.get("private") is True, rel
    lock = json.loads((REPO / "package-lock.json").read_text(encoding="utf-8"))["packages"]
    for path in ("", "apps/web", "packages/design-system"):
        assert lock[path].get("license") == "UNLICENSED", path or "<root>"
    pyproject = (REPO / "packages" / "preflight" / "pyproject.toml").read_text(encoding="utf-8")
    assert 'license = { text = "Proprietary" }' in pyproject
    assert "License :: Other/Proprietary License" in pyproject


def test_the_gate_recognises_the_projects_own_packages_as_first_party() -> None:
    policy = scan.load_policy(REPO / "licence-policy.json")
    own = scan.classify(scan.Package("npm", "@dataflow/web", "0.1.0", "UNLICENSED", "first-party", "lock"), policy)
    assert own.status == "first_party"
    stranger = scan.classify(scan.Package("npm", "left-pad", "1.0.0", "UNLICENSED", "runtime", "lock"), policy)
    assert stranger.status == "unknown"
    assert scan.violations([own], policy) == []
    rows = {p.name: p for p in scan.scan_npm(REPO / "package-lock.json")}
    for name in ("dataflow", "@dataflow/web", "@dataflow/design-system"):
        assert scan.classify(rows[name], policy).status == "first_party", name
    assert scan.classify(
        scan.Package("pypi", "dataflow-preflight", "0.1.0", "Proprietary", "first-party", "pyproject"), policy
    ).status == "first_party"


def test_notice_and_docs_point_at_the_licence() -> None:
    notice_text = (REPO / "NOTICE").read_text(encoding="utf-8")
    assert "Copyright (c) 2026 Venky Chowdary. All rights reserved." in notice_text
    assert "See LICENSE" in notice_text
    assert "not stated in this repository" not in notice_text
    docs = (REPO / "docs" / "legal" / "third-party-licences.md").read_text(encoding="utf-8")
    assert "LICENSE" in docs and "proprietary" in docs.lower()


# ── owner decision 2: the LGPL drivers are accepted with attribution ───────

_LGPL = ("pypi:paramiko", "pypi:psycopg2-binary", "pypi:pymssql")


def test_lgpl_drivers_are_approved_with_the_decision_recorded() -> None:
    policy = scan.load_policy(REPO / "licence-policy.json")
    for key in _LGPL:
        entry = policy["exceptions"][key]
        assert not entry.get("owner_decision"), key
        assert entry["decision"] == {
            "status": "approved",
            "by": "owner",
            "date": "2026-11-13",
            "terms": "accepted with attribution; used unmodified",
        }, key


SOURCE_DIR = "/usr/share/doc/datawrap/third-party-source"


def _policy_with(notice: str) -> dict:
    return {
        "allowed": [],
        "exceptions": {
            "pypi:x": {
                "licence": "LGPL-2.1",
                "why": "w",
                "notice": notice,
                "decision": {"status": "approved", "by": "owner", "date": "2026-11-13", "terms": "t"},
            }
        },
    }


def test_an_approved_lgpl_exception_must_ship_its_source_in_the_image(tmp_path: Path) -> None:
    path = tmp_path / "p.json"
    # A written offer is an ongoing obligation the owner did not take on.
    for notice in (
        "LGPL-2.1, used unmodified.",
        "LGPL-2.1. Licence text: https://www.gnu.org/licenses/x. Source: https://example. A written offer: ...",
    ):
        path.write_text(json.dumps(_policy_with(notice)), encoding="utf-8")
        with pytest.raises(ValueError, match="source shipped in image"):
            scan.load_policy(path)
    ok = (
        "LGPL-2.1, used unmodified, replaceable. Licence text: https://www.gnu.org/licenses/x. "
        f"Corresponding source shipped in image at {SOURCE_DIR}/x-1.0.tar.gz."
    )
    path.write_text(json.dumps(_policy_with(ok)), encoding="utf-8")
    scan.load_policy(path)


def test_notice_says_where_in_the_image_the_lgpl_source_is() -> None:
    text = (REPO / "NOTICE").read_text(encoding="utf-8")
    assert "written offer" not in text and "three years" not in text
    for name in ("paramiko", "psycopg2-binary", "pymssql"):
        block = text.split(f"{name} (pypi) - ", 1)[1].split("\n\n", 1)[0]
        assert "gnu.org/licenses" in block, name
        assert f"source shipped in image at {SOURCE_DIR}" in block, name
        assert "replace" in block and "unmodified" in block, name
    pymssql = text.split("pymssql (pypi) - ", 1)[1].split("\n\n", 1)[0]
    assert "FreeTDS" in pymssql and "freetds-" in pymssql


def test_mpl_packages_keep_a_source_availability_notice() -> None:
    """MPL-2.0, unmodified: a notice of where the source is available suffices."""
    policy = scan.load_policy(REPO / "licence-policy.json")
    for key in ("pypi:certifi", "pypi:orjson"):
        notice = policy["exceptions"][key]["notice"]
        assert "Source: https://" in notice and "unmodified" in notice, key


def test_notice_attributes_the_libraries_bundled_inside_wheels() -> None:
    text = (REPO / "NOTICE").read_text(encoding="utf-8")
    assert "Libraries bundled inside the shipped Python wheels" in text
    for needle in ("OpenSSL", "MIT Kerberos", "PostgreSQL libpq", "keyutils", "libxcrypt",
                   "Carnegie Mellon University", "The FreeType Project", "Independent JPEG Group"):
        assert needle in text, needle
