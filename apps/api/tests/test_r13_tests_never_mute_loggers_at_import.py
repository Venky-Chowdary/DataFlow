"""No test module may change a logger's level or propagation at import time.

pytest imports every module in the shard at collection, so a module-scope
``logging.getLogger("services").setLevel(ERROR)`` muted every ``services``
logger for the whole session: EOS redelivery (``cdc_eos: committed/skip``),
the log-floor test and the wave95 sync-mode warning all failed their caplog
checks in CI shard 4 while the behaviour they prove was intact. Levels belong
in fixtures (``caplog.set_level``, or set-and-restore).
"""

from __future__ import annotations

import re
from pathlib import Path

_MUTATION = re.compile(
    r"^(logging\.getLogger\([^)]*\)\.(setLevel|propagate)"
    r"|logging\.(disable|basicConfig|root\.setLevel)"
    r"|[A-Za-z_]*logger\.(setLevel\(|propagate\s*=))"
)


def test_no_test_module_mutates_logging_at_module_scope():
    offenders = []
    for path in sorted(Path(__file__).resolve().parent.glob("test_*.py")):
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if _MUTATION.match(line):  # column 0 only: module scope
                offenders.append(f"{path.name}:{lineno}: {line.strip()}")
    assert not offenders, "module-scope logger mutation mutes other tests' caplog:\n" + "\n".join(offenders)
