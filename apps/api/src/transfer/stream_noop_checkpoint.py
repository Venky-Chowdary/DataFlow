"""A checkpoint service that persists nothing, for internal staging phases.

Lives in its own module (Phase F8 size freeze) and is re-exported by
``src.transfer.stream`` so every existing import keeps working.
"""

from __future__ import annotations

from typing import Any


class _NoOpCheckpointService:
    """A checkpoint service that does not persist anything.

    Used for temporary staging transfers so the parent job's checkpoint is not
    overwritten by an internal phase.

    Module 14 honesty: this phase is not crash-recoverable on its own.
    """

    failed_saves = 0
    durable = False
    resume_supported = False

    @property
    def has_failed_saves(self) -> bool:
        return False

    def save(self, checkpoint: Any) -> bool:  # noqa: ARG002
        return True

    def require_save(self, checkpoint: Any) -> None:  # noqa: ARG002
        return None

    def load(self, job_id: str) -> Any | None:  # noqa: ARG002
        return None

    def honesty(self) -> dict:
        from services.execution_engine_contract import noop_checkpoint_posture

        return noop_checkpoint_posture()
