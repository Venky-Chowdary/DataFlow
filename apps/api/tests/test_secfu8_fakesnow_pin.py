"""Security follow-up 8: fakesnow is pinned to the release line that matches the installed duckdb.

``requirements.txt`` said ``fakesnow>=0.8.0``. fakesnow 0.11.22 requires
``duckdb~=1.5.6`` and ``sqlglot~=30.22.0`` (the T-SQL playground filter's
parser is pinned to the same sqlglot); fakesnow 0.12 moved to duckdb 2.x. An
unbounded pin lets a fresh image resolve fakesnow 0.12+, which either fails
to resolve against the rest of the file or drags duckdb (every local DuckDB
connector and the playground backstop) and sqlglot to new majors unproven.
"""

from __future__ import annotations

from importlib import metadata
from pathlib import Path

import pytest
from packaging.requirements import Requirement
from packaging.version import Version

_REQUIREMENTS = Path(__file__).resolve().parents[1] / "requirements.txt"


def _requirement(name: str) -> Requirement:
    for raw in _REQUIREMENTS.read_text(encoding="utf-8").splitlines():
        line = raw.split("#", 1)[0].strip()
        if line and Requirement(line).name.lower() == name:
            return Requirement(line)
    raise AssertionError(f"{name} is not declared in requirements.txt")


def test_fakesnow_has_an_upper_bound_below_the_duckdb_2_line():
    spec = _requirement("fakesnow").specifier
    assert not spec.contains(Version("0.12.0")), str(spec)
    assert not spec.contains(Version("1.0.0")), str(spec)


def test_the_pin_admits_the_installed_fakesnow_and_its_duckdb_and_sqlglot():
    try:
        installed = metadata.version("fakesnow")
    except metadata.PackageNotFoundError:  # pragma: no cover - fakesnow is a test dependency
        pytest.skip("fakesnow is not installed")
    assert _requirement("fakesnow").specifier.contains(Version(installed))
    needs = {Requirement(r).name.lower(): Requirement(r) for r in metadata.requires("fakesnow") or [] if "extra ==" not in r}
    for name in ("duckdb", "sqlglot"):
        assert needs[name].specifier.contains(Version(metadata.version(name))), (name, str(needs[name]))
    # The file's own sqlglot pin stays the one fakesnow needs.
    assert _requirement("sqlglot").specifier == needs["sqlglot"].specifier
