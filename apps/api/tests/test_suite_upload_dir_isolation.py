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


def test_a_stored_upload_is_not_seen_by_another_module(tmp_path):
    """Store 'orders.csv' the way an upload test does, then ask Pilot the way
    test_pilot_colloquial_ops_wave50 does, in a fresh pytest process whose
    two modules run in that order."""
    import os
    import subprocess
    import sys

    name = f"zqorders{os.getpid()}"  # unique: no leftover upload can match it
    first = API / "tests" / f"test_zz_upload_store_{os.getpid()}.py"
    second = API / "tests" / f"test_zz_upload_route_{os.getpid()}.py"
    first.write_text(
        "def test_store():\n"
        "    from services.file_parser import store_upload\n"
        f"    store_upload('{name}.csv', b'order_id,sku\\n1,a\\n')\n",
        encoding="utf-8",
    )
    second.write_text(
        "import os\n"
        "os.environ.setdefault('DATAFLOW_PILOT_ENGINE', 'local')\n"
        "def test_route():\n"
        "    from src.ai.copilot.tools import infer_tools_from_message\n"
        f"    names = [n for n, _ in infer_tools_from_message('trasfer {name} to warehouse')]\n"
        "    assert 'start_dataset_transfer' not in names, names\n",
        encoding="utf-8",
    )
    try:
        done = subprocess.run(
            [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider",
             "-p", "no:randomly", str(first), str(second), "--basetemp", str(tmp_path / "bt")],
            cwd=API, capture_output=True, text=True, timeout=300,
            env={**os.environ, "TMPDIR": str(tmp_path)},
        )
    finally:
        first.unlink(missing_ok=True)
        second.unlink(missing_ok=True)
    assert done.returncode == 0, done.stdout[-3000:] + done.stderr[-2000:]
