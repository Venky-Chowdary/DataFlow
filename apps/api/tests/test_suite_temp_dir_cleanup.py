"""The suite's own data directory does not outlive the test process.

conftest.py gave every pytest process a fresh ``df-suite-data-*`` directory
under the system temp dir and never removed it: a developer machine or a
self-hosted runner collected one per run (about 10,000 directories, ~490 MB,
on one shared machine in a day). It also created one when DATAFLOW_DATA_DIR was
already set. A directory the suite creates is now removed when the process
exits; a directory the caller set is neither created nor removed.
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

API = Path(__file__).resolve().parents[1]

# Read at import, after conftest and before any per-test fixture narrows it.
_PROBE = """
import os
SUITE_DIR = os.environ["DATAFLOW_DATA_DIR"]
def test_report_data_dir():
    open(os.environ["SUITE_DIR_REPORT"], "w").write(SUITE_DIR)
"""


def _run_suite(tmp_path: Path, env_overrides: dict[str, str | None]) -> tuple[Path, set[str]]:
    probe = API / "tests" / f"test_zz_suite_dir_probe_{os.getpid()}.py"
    probe.write_text(_PROBE, encoding="utf-8")
    report = tmp_path / "data_dir.txt"
    temp_root = tmp_path / "tmp"
    temp_root.mkdir()
    env = {**os.environ, "SUITE_DIR_REPORT": str(report), "TMPDIR": str(temp_root)}
    for key, value in env_overrides.items():
        if value is None:
            env.pop(key, None)
        else:
            env[key] = value
    try:
        done = subprocess.run(
            [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", str(probe),
             "--basetemp", str(tmp_path / "bt")],
            cwd=API, env=env, capture_output=True, text=True, timeout=300,
        )
    finally:
        probe.unlink(missing_ok=True)
    assert done.returncode == 0, done.stdout[-2000:] + done.stderr[-2000:]
    leftovers = {p.name for p in temp_root.iterdir() if p.name.startswith("df-suite-data-")}
    return Path(report.read_text(encoding="utf-8")), leftovers


def test_the_suite_data_dir_is_removed_when_the_process_exits(tmp_path):
    used, leftovers = _run_suite(
        tmp_path, {"DATAFLOW_DATA_DIR": None, "DATAWRAP_DATA_DIR": None}
    )
    assert used.name.startswith("df-suite-data-"), used
    assert not used.exists(), f"{used} outlived the test process"
    assert leftovers == set(), leftovers


def test_a_data_dir_the_caller_set_is_used_and_kept(tmp_path):
    mine = tmp_path / "mine"
    mine.mkdir()
    used, leftovers = _run_suite(
        tmp_path, {"DATAFLOW_DATA_DIR": str(mine), "DATAWRAP_DATA_DIR": None}
    )
    assert used == mine and mine.is_dir()
    assert leftovers == set(), leftovers  # none created beside it
