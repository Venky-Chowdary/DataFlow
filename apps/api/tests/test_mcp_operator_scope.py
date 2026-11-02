"""confirm_action does not mutate when no request is bound.

The check runs in a child process so stubbing the copilot package cannot
affect the rest of the suite. The child does not import the agent runtime.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

_CHILD = r"""
import importlib.util
import sys
import types
from pathlib import Path

root = Path(sys.argv[1])
copilot = root / "src" / "ai" / "copilot"

def pkg(name, path):
    mod = types.ModuleType(name)
    mod.__path__ = [str(path)]
    mod.__package__ = name
    sys.modules[name] = mod

pkg("src", root / "src")
pkg("src.ai", root / "src" / "ai")
pkg("src.ai.copilot", copilot)
spec = importlib.util.spec_from_file_location(
    "src.ai.copilot.confirm_ack", copilot / "confirm_ack.py"
)
mod = importlib.util.module_from_spec(spec)
mod.__package__ = "src.ai.copilot"
sys.modules["src.ai.copilot.confirm_ack"] = mod
spec.loader.exec_module(mod)
body = mod.confirm_from_tool("ack_missing", "qe")
assert body["ok"] is False, body
assert "ack_id" in body["error"] or "confirm_action" in body["error"], body
"""


def test_confirm_without_a_request_does_not_mutate() -> None:
    root = Path(__file__).resolve().parents[1]
    proc = subprocess.run(
        [sys.executable, "-c", _CHILD, str(root)],
        cwd=str(root),
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr or proc.stdout
