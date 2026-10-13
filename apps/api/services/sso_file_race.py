"""Fresh-interpreter entry points for the file-backend SSO race.

The atomic tests start eight processes against one flocked JSON file. They must
use spawn. Fork copies the parent address space, and after the API suite has
imported the process that copy is large enough for a GitHub runner's overcommit
heuristic to refuse it with ``OSError: [Errno 12] Cannot allocate memory``.
The fixture itself is a few bytes of JSON; the process count stays eight.

Spawn on Linux is ``vfork`` plus ``exec`` (``subprocess._USE_VFORK``), so the
child is a new interpreter and the parent's virtual size is not reserved again.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

# Interpreter startup on a loaded runner is slower than fork. This bound waits
# for that startup. It does not change the exactly-one-winner assertion.
RACE_WAIT_SECONDS = 30


def _file_backend_only() -> None:
    return None


def claim_once_child(replay_path: str, queue: Any, barrier: Any) -> None:
    """Claim one shared nonce. Exactly one of eight children may win."""
    from services import sso_state

    parent = Path(replay_path).parent

    def _directory() -> Path:
        return parent

    sso_state.mongo_database = _file_backend_only
    sso_state.data_dir = _directory
    barrier.wait(timeout=RACE_WAIT_SECONDS)
    queue.put(
        sso_state.claim_once(
            "oidc_nonce",
            "shared-token",
            datetime.now(timezone.utc) + timedelta(minutes=5),
        )
    )


def get_state_child(state_path: str, queue: Any, barrier: Any) -> None:
    """Consume one shared state token. Exactly one of eight children may win."""
    from services import sso_state

    sso_state.mongo_database = _file_backend_only
    sso_state.STATE_PATH = Path(state_path)
    barrier.wait(timeout=RACE_WAIT_SECONDS)
    queue.put(sso_state.get_state("shared-state", "oidc") is not None)
