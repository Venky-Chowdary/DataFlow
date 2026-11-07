"""D36 — two different one-shot jobs writing one destination table both started.

The idempotency claim only refused a resubmit of the *same* transfer, and the
D104 one-writer rule only covers pipelines. Two one-shot jobs with different
sources (or mappings) onto one table both ran; on Snowflake the table was left
an interleaving of both while one job reported success.

The submit path now also claims the destination object in the same claim store:
the second job is refused with ``DestinationBusy`` (a 409-class
``DuplicateTransferSubmission``) naming the job
that holds the write, and the claim frees itself once that job is terminal.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from services.mongodb_service import MongoDBService  # noqa: E402
from src.transfer.engine import (  # noqa: E402
    DuplicateTransferSubmission,
    UniversalTransferEngine,
)
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402
from tests.test_property2_golden_path_never_blocked import _FakeMongo  # noqa: E402


class _ClaimingMongo(_FakeMongo):
    """Job store plus the process-local claim registry the real service uses."""

    def create_transfer_job(self, job_data: dict) -> str:
        job_id = uuid.uuid4().hex[:24]
        self.jobs[job_id] = {**job_data, "status": "pending"}
        return job_id

    def _status(self, job_id: str) -> str:
        return str((self.jobs.get(job_id) or {}).get("status") or "")

    def claim_job_idempotency(self, *, key, job_id, ttl_seconds=None):
        from services.job_idempotency import claim_expiry

        acquired, holder = MongoDBService._local_claim(
            key, job_id, claim_expiry(ttl_seconds), self._status
        )
        return acquired, holder, self._status(holder) if holder else ""

    def bind_job_idempotency(self, key, from_job_id, to_job_id):
        MongoDBService._local_bind(key, from_job_id, to_job_id)
        return True

    def release_job_idempotency(self, key, job_id=""):
        MongoDBService._local_release(key, job_id)
        return True


@pytest.fixture()
def store(monkeypatch):
    fake = _ClaimingMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(MongoDBService, "_LOCAL_CLAIMS", {})
    return fake


def _request(tmp_path: Path, src_table: str, dst_table: str, **kw) -> TransferRequest:
    return TransferRequest(
        source=EndpointConfig(
            kind="database", format="sqlite", database=str(tmp_path / "src.sqlite"),
            table=src_table,
        ),
        destination=EndpointConfig(
            kind="database", format="sqlite", database=str(tmp_path / "dst.sqlite"),
            table=dst_table,
        ),
        sync_mode="full_refresh_overwrite",
        **kw,
    )


def test_second_one_shot_job_on_same_table_is_refused(tmp_path, store):
    engine = UniversalTransferEngine()
    first = engine._create_pending_job(_request(tmp_path, "orders_a", "orders"))

    with pytest.raises(DuplicateTransferSubmission) as refused:
        # A different source onto the same table — not a resubmit, still a
        # second writer. Case-folded: ORDERS is the same table.
        engine._create_pending_job(_request(tmp_path, "orders_b", "ORDERS"))
    assert type(refused.value).__name__ == "DestinationBusy"
    assert refused.value.existing_job_id == first
    assert "one writer at a time" in str(refused.value)
    assert "'ORDERS'" in str(refused.value)
    # The refused submit left no runnable job and no claim behind.
    assert list(store.jobs) == [first]

    # A different table on the same destination is not a conflict.
    engine._create_pending_job(_request(tmp_path, "orders_b", "orders_archive"))


def test_destination_claim_frees_when_holder_finishes(tmp_path, store):
    engine = UniversalTransferEngine()
    first = engine._create_pending_job(_request(tmp_path, "orders_a", "orders"))
    store.update_job_status(first, "completed")
    engine._release_idempotency(first)

    second = engine._create_pending_job(_request(tmp_path, "orders_b", "orders"))
    assert second != first
    # Even an unreleased claim stops blocking once its job is terminal.
    store.update_job_status(second, "failed")
    third = engine._create_pending_job(_request(tmp_path, "orders_c", "orders"))
    assert third not in {first, second}
