"""The suite never shares stored uploads across test modules or runs.

conftest.py left DATAFLOW_UPLOAD_DIR unset, so platform_config.upload_dir()
was the source tree's apps/api/uploads. A test that stored an upload left it
there for every later test in the process and every later run on the machine
(one checkout held a stale "487df0a033f540c0_orders.csv"), and Pilot then
resolved "orders" to it: test_pilot_colloquial_ops_wave50 routed the plan-only
"trasfer orders to warehouse" to start_dataset_transfer on PR 406 CI.
"""
from __future__ import annotations

from pathlib import Path

API = Path(__file__).resolve().parents[1]


def test_uploads_never_land_in_the_source_tree():
    from services.platform_config import upload_dir

    assert API / "uploads" != upload_dir().resolve()
    assert API not in upload_dir().resolve().parents


def _child_pytest(tmp_path: Path, module: Path, extra_env: dict[str, str]):
    import os
    import subprocess
    import sys

    # A fresh run: not the parent's suite dirs (each run makes its own) and not
    # the parent's CI shard (the child's own split could drop its module).
    env = {k: v for k, v in os.environ.items()
           if k not in {"DATAFLOW_TEST_SHARD", "DATAFLOW_UPLOAD_DIR", "DATAWRAP_UPLOAD_DIR",
                        "DATAFLOW_DATA_DIR"}}
    env.update(extra_env, TMPDIR=str(tmp_path))
    return subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", "-p", "no:randomly",
         str(module), "--basetemp", str(tmp_path / f"bt_{module.stem}")],
        cwd=API, capture_output=True, text=True, timeout=300, env=env,
    )


def test_a_stored_upload_is_not_seen_by_a_later_run(tmp_path):
    """One run stores an upload the way an upload test does; a later run asks
    Pilot the way test_pilot_colloquial_ops_wave50 does. The later run must not
    find the file: that is how a stale upload in the source tree used to route
    a plan-only request to a real transfer. (Within one run, modules share the
    run's upload folder by design; test_pilot_colloquial_ops_wave50 is hermetic
    for that reason.)"""
    import os

    name = f"zqorders{os.getpid()}"  # unique: no leftover upload can match it
    report = tmp_path / "stored.txt"
    store = API / "tests" / f"test_zz_upload_store_{os.getpid()}.py"
    route = API / "tests" / f"test_zz_upload_route_{os.getpid()}.py"
    store.write_text(
        "import os\n"
        "def test_store():\n"
        "    from services.file_parser import store_upload\n"
        f"    stored = store_upload('{name}.csv', b'order_id,sku\\n1,a\\n')\n"
        "    open(os.environ['STORED_REPORT'], 'w').write(stored['path'])\n",
        encoding="utf-8",
    )
    route.write_text(
        "import os\n"
        "os.environ.setdefault('DATAFLOW_PILOT_ENGINE', 'local')\n"
        "def test_route():\n"
        "    from services.platform_config import upload_dir\n"
        "    from src.ai.copilot.tools import infer_tools_from_message\n"
        f"    assert not [p for p in os.listdir(upload_dir()) if '{name}' in p]\n"
        f"    names = [n for n, _ in infer_tools_from_message('trasfer {name} to warehouse')]\n"
        "    assert 'start_dataset_transfer' not in names, names\n",
        encoding="utf-8",
    )
    try:
        first = _child_pytest(tmp_path, store, {"STORED_REPORT": str(report)})
        assert first.returncode == 0, first.stdout[-3000:] + first.stderr[-2000:]
        assert "1 passed" in first.stdout, first.stdout[-3000:]
        stored = Path(report.read_text(encoding="utf-8"))
        assert API not in stored.resolve().parents, stored
        assert not stored.exists(), f"{stored} outlived the run that stored it"
        second = _child_pytest(tmp_path, route, {})
        assert second.returncode == 0, second.stdout[-3000:] + second.stderr[-2000:]
        assert "1 passed" in second.stdout, second.stdout[-3000:]
    finally:
        store.unlink(missing_ok=True)
        route.unlink(missing_ok=True)
