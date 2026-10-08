"""A CDC job that fails before its first checkpoint still releases its slot.

DEF-B2-008 and DEF-CDC-SLOT-LEAK: a PostgreSQL to Oracle CDC run failed on
its first batch. No checkpoint had promoted ``cdc_slot_name`` onto the job,
so the terminal release returned ``no_slot_name`` and the slot plus its
publication stayed on the source, retaining WAL.
"""

from __future__ import annotations

from typing import Any

import pytest

from connectors.postgresql_change_stream import PostgreSqlChangeStreamCdc
from services import cdc_catchup
from services.cdc_catchup import (
    capture_identity,
    capture_identity_from,
    stamp_capture_identity,
)
from src.transfer import job_failure
from services.error_handling import TransferCancelled


def _pg_reader(slot: str = "df_qa_pg_or_cdc_34f9897e") -> PostgreSqlChangeStreamCdc:
    reader = PostgreSqlChangeStreamCdc.__new__(PostgreSqlChangeStreamCdc)
    reader.slot_name = slot
    reader.publication_name = "df_pub_qa_pg_or_cdc_840b1bc26f"
    return reader


class _Mongo:
    def __init__(self, job: dict[str, Any]) -> None:
        self.job = dict(job)
        self.updates: list[tuple[str, dict[str, Any]]] = []

    def get_job(self, job_id: str) -> dict[str, Any]:
        return dict(self.job)

    def update_job_status(self, job_id: str, status: str, **fields: Any) -> None:
        self.updates.append((status, fields))
        self.job.update(fields)
        self.job["status"] = status


@pytest.fixture
def released(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    calls: list[dict[str, Any]] = []

    def _release(job: dict[str, Any] | None, **kwargs: Any) -> dict[str, Any]:
        calls.append({"job": dict(job or {}), **kwargs})
        return {"released": True}

    monkeypatch.setattr(cdc_catchup, "release_finished_cdc_slot", _release)
    return calls


def test_postgres_reader_names_its_slot_before_the_first_poll() -> None:
    assert capture_identity(_pg_reader()) == {
        "cdc_slot_name": "df_qa_pg_or_cdc_34f9897e",
        "cdc_publication_name": "df_pub_qa_pg_or_cdc_840b1bc26f",
    }
    assert capture_identity(object()) == {}


def test_identity_is_found_on_a_wrapping_exception() -> None:
    inner = ValueError("decimal wire parse failed — refuse invent into NUMBER(38,0)")
    stamp_capture_identity(inner, _pg_reader())
    try:
        try:
            raise inner
        except ValueError as exc:
            raise RuntimeError("write failed") from exc
    except RuntimeError as outer:
        assert capture_identity_from(outer)["cdc_slot_name"] == "df_qa_pg_or_cdc_34f9897e"


def test_first_batch_failure_releases_the_slot_the_job_never_recorded(
    released: list[dict[str, Any]],
) -> None:
    mongo = _Mongo({"id": "job-1", "status": "running", "phase": "load"})
    exc = ValueError("decimal wire parse failed — refuse invent into NUMBER(38,0)")
    stamp_capture_identity(exc, _pg_reader())

    job_failure._fail_runtime_job(mongo, "job-1", exc, request=None)

    assert len(released) == 1
    call = released[0]
    assert call["reason"] == "failed"
    assert call["retriable"] is False
    assert call["job"]["cdc_slot_name"] == "df_qa_pg_or_cdc_34f9897e"
    assert call["job"]["cdc_publication_name"] == "df_pub_qa_pg_or_cdc_840b1bc26f"
    status, fields = mongo.updates[-1]
    assert status == "failed"
    assert fields["cdc_slot_name"] == "df_qa_pg_or_cdc_34f9897e"


def test_a_checkpointed_slot_name_is_not_overwritten(
    released: list[dict[str, Any]],
) -> None:
    mongo = _Mongo({"id": "job-2", "cdc_slot_name": "df_checkpointed_slot"})
    exc = ValueError("decimal wire parse failed — refuse invent into NUMBER(38,0)")
    stamp_capture_identity(exc, _pg_reader("df_other_slot"))

    job_failure._fail_runtime_job(mongo, "job-2", exc, request=None)

    assert released[0]["job"]["cdc_slot_name"] == "df_checkpointed_slot"


def test_retriable_failure_keeps_the_slot_for_resume(
    released: list[dict[str, Any]],
) -> None:
    mongo = _Mongo({"id": "job-3"})
    exc = RuntimeError("CDC catch-up dest COUNT short of the live source image")
    stamp_capture_identity(exc, _pg_reader())

    job_failure._fail_runtime_job(mongo, "job-3", exc, request=None)

    assert released == []
    assert mongo.updates[-1][1]["cdc_slot_name"] == "df_qa_pg_or_cdc_34f9897e"


def test_cancel_before_a_checkpoint_releases_the_named_slot(
    released: list[dict[str, Any]],
) -> None:
    mongo = _Mongo({"id": "job-4"})
    exc = TransferCancelled("cancelled by operator")
    stamp_capture_identity(exc, _pg_reader())

    job_failure._fail_runtime_job(mongo, "job-4", exc, request=None)

    assert [c["reason"] for c in released] == ["cancelled"]
    assert released[0]["job"]["cdc_slot_name"] == "df_qa_pg_or_cdc_34f9897e"


def test_refused_cdc_batch_releases_through_the_shared_helper(
    released: list[dict[str, Any]],
) -> None:
    mongo = _Mongo({"id": "job-5"})
    exc = RuntimeError("write batch blocked")
    stamp_capture_identity(exc, _pg_reader())

    identity = job_failure.release_cdc_capture_after_failure(
        mongo, "job-5", None, exc, retriable=False
    )

    assert identity["cdc_slot_name"] == "df_qa_pg_or_cdc_34f9897e"
    assert released[0]["reason"] == "failed"
    assert released[0]["job"]["cdc_slot_name"] == "df_qa_pg_or_cdc_34f9897e"


def test_non_cdc_failure_has_no_identity(released: list[dict[str, Any]]) -> None:
    identity = job_failure.release_cdc_capture_after_failure(
        _Mongo({"id": "job-6"}), "job-6", None, ValueError("x"), retriable=False
    )
    assert identity == {}
