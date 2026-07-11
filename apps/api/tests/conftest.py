"""Pytest configuration — isolate tests from live infrastructure."""

import os
import sys
from pathlib import Path

_api_root = Path(__file__).resolve().parents[1]
_src_root = _api_root / "src"

for path in (_src_root, _api_root):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")
