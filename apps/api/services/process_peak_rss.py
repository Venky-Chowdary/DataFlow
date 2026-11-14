"""This process's own peak resident memory.

``getrusage(RUSAGE_SELF).ru_maxrss`` is not that in a child process on Linux:
at exec the kernel folds the old mm's high-water mark (the parent's pages,
after fork or vfork) into the child's ``maxrss``. So a worker spawned by a
large process reads at least the parent's RSS. ``/proc/self/status`` VmHWM
is the process's own high-water mark; ``ru_maxrss`` is the fallback where
procfs is absent (macOS reports it in bytes, Linux in KiB).
"""

from __future__ import annotations

import sys

_STATUS = "/proc/self/status"


def own_peak_rss_bytes() -> int:
    """Peak resident set size of this process in bytes (0 if unknown)."""
    try:
        with open(_STATUS, encoding="ascii", errors="replace") as fh:
            for line in fh:
                if line.startswith("VmHWM:"):
                    return int(line.split()[1]) * 1024
    except (OSError, ValueError, IndexError):
        pass
    try:
        import resource
    except ImportError:  # pragma: no cover - non-POSIX
        return 0
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(peak if sys.platform == "darwin" else peak * 1024)
