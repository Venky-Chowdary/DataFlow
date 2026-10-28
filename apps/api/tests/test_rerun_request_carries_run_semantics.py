"""D124 (second half) — rerun/retry/resume rebuild the request from the job.

rerun_job, retry_job and resume_job reconstruct the TransferRequest from the
job's persisted ``transfer_request``. ``transfer_request_from_dict`` restored
``source_timezone``, ``duplicate_policy``, ``allow_duplicates`` and
``file_wait_seconds`` but ``transfer_request_to_dict`` never wrote them, so a
rerun of a run that declared them quietly ran with the defaults — a different
run than the one the operator is repeating. (The cursor, key, stream_contracts,
read_options, source_filter, acknowledgments and contract binding already
round-trip; the keyed quiet poll itself is engine-side and identical for every
entry point.)
"""

from __future__ import annotations

import dataclasses

import src.transfer.engine as engine_mod
from services.mongodb_service import MemoryMongoDBService
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import (
    EndpointConfig,
    TransferRequest,
    transfer_request_from_dict,
    transfer_request_to_dict,
)

#: Request fields that are per-attempt bookkeeping, not run semantics.
_NOT_PERSISTED = {"source_content", "rerun_of"}


def _request() -> TransferRequest:
    return TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database="a.db", table="t"),
        destination=EndpointConfig(kind="database", format="sqlite", database="b.db", table="t"),
        sync_mode="incremental_deduped",
        stream_contracts=[{"name": "t", "cursor_field": "seq", "primary_key": ["id"]}],
        source_timezone="Europe/Berlin",
        duplicate_policy="dedupe_skip",
        allow_duplicates=True,
        file_wait_seconds=45,
    )


def test_run_semantics_survive_the_persisted_job_round_trip(monkeypatch):
    store = MemoryMongoDBService()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: store)
    job_id = UniversalTransferEngine()._create_pending_job(_request())

    rebuilt = transfer_request_from_dict(store.get_job(job_id)["transfer_request"])
    assert rebuilt.source_timezone == "Europe/Berlin"
    assert rebuilt.duplicate_policy == "dedupe_skip"
    assert rebuilt.allow_duplicates is True
    assert rebuilt.file_wait_seconds == 45
    assert rebuilt.stream_contracts == _request().stream_contracts


def test_every_request_field_is_persisted_or_deliberately_not():
    stored = transfer_request_to_dict(_request())
    missing = [
        f.name
        for f in dataclasses.fields(TransferRequest)
        if f.name not in stored and f.name not in _NOT_PERSISTED
    ]
    assert missing == [], f"fields a rerun would silently drop: {missing}"
