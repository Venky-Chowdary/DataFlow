"""CDC destination statements: refuse CALL before a reader opens; hooks once.

Named fixture: one PostgreSQL-shaped destination, orders (and customers when
two streams are selected). This is not a live binlog apply. CDC delivery
stays at-least-once upsert. A destination CALL is not executed.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from services.sync_cursor import SyncContract
from src.transfer.cdc_transfer import (
    _run_cdc_multi_stream,
    _run_cdc_shared_multi_table,
    _run_cdc_single_stream,
)
from src.transfer.stream_dest_procedure import (
    CdcDestinationSessionError,
    cdc_destination_hooks,
)


def _pg_dest(**extra: object) -> MagicMock:
    dest = MagicMock()
    dest.format = "postgresql"
    dest.table = "orders"
    dest.collection = ""
    dest.extra = {
        "type": "postgresql",
        **extra,
    }
    return dest


def test_cdc_call_is_refused_before_a_reader_opens() -> None:
    source = MagicMock()
    source.format = "postgresql"
    source.table = "orders"
    source.collection = ""
    dest = _pg_dest(
        dest_write_mode="procedure",
        dest_procedure_call="CALL public.land(:id)",
    )
    opened: list[str] = []

    def _opened(*_a, **_k):
        opened.append("reader")
        raise AssertionError("CDC reader opened after a destination CALL")

    with patch("src.transfer.cdc_transfer.CdcEngine", side_effect=_opened), patch(
        "src.transfer.cdc_transfer.PostgreSqlChangeStreamCdc", side_effect=_opened
    ), pytest.raises(CdcDestinationSessionError, match="CALL"):
        _run_cdc_single_stream(
            source,
            dest,
            [{"source": "id", "target": "id"}],
            {"id": "integer"},
            None,
            sync_mode="cdc",
            stream_contracts=[
                {"name": "orders", "selected": True, "primary_key": "id", "sync_mode": "cdc"}
            ],
        )
    assert opened == []


def test_stream_contract_call_is_refused_before_a_reader_opens() -> None:
    """The CALL lives on the stream, not the endpoint. It still must not upsert."""
    source = MagicMock()
    source.format = "postgresql"
    source.table = "orders"
    source.collection = ""
    dest = _pg_dest()
    opened: list[str] = []

    def _opened(*_a, **_k):
        opened.append("reader")
        raise AssertionError("CDC reader opened after a stream CALL")

    with patch("src.transfer.cdc_transfer.CdcEngine", side_effect=_opened), pytest.raises(
        CdcDestinationSessionError, match="CALL"
    ):
        _run_cdc_single_stream(
            source,
            dest,
            [{"source": "id", "target": "id"}],
            {"id": "integer"},
            None,
            sync_mode="cdc",
            stream_contracts=[
                {
                    "name": "orders",
                    "selected": True,
                    "primary_key": "id",
                    "sync_mode": "cdc",
                    "dest_write_mode": "procedure",
                    "dest_procedure_call": "CALL public.land(:id)",
                }
            ],
        )
    assert opened == []


def test_cdc_insert_is_refused_before_a_shared_reader_opens() -> None:
    source = MagicMock()
    source.format = "postgresql"
    dest = _pg_dest(
        dest_write_mode="query",
        dest_query_sql="INSERT INTO orders (id) VALUES (:id)",
    )
    selected = [
        SyncContract(name="customers", sync_mode="cdc", primary_key="id"),
        SyncContract(name="orders", sync_mode="cdc", primary_key="id"),
    ]
    with pytest.raises(CdcDestinationSessionError, match="INSERT/MERGE"):
        _run_cdc_shared_multi_table(
            source,
            dest,
            [],
            {},
            None,
            sync_mode="cdc",
            stream_contracts=[],
            selected=selected,
            job_id="j-call",
            checkpoint=None,
            checkpoint_service=None,
            backfill_new_fields=False,
            validation_mode="strict",
            limit=0,
        )


def test_multi_stream_call_does_not_fall_back_to_sequential_upsert() -> None:
    source = MagicMock()
    source.format = "postgresql"
    source.table = "customers,orders"
    source.collection = ""
    dest = _pg_dest(
        dest_write_mode="procedure",
        dest_procedure_call="CALL public.land(:id)",
    )
    selected = [
        SyncContract(name="customers", sync_mode="cdc", primary_key="id"),
        SyncContract(name="orders", sync_mode="cdc", primary_key="id"),
    ]
    with patch(
        "src.transfer.cdc_transfer._run_cdc_shared_multi_table",
        side_effect=AssertionError("shared reader started"),
    ), patch(
        "src.transfer.cdc_transfer._run_cdc_multi_stream_sequential",
        side_effect=AssertionError("sequential upsert started"),
    ), pytest.raises(CdcDestinationSessionError, match="CALL"):
        _run_cdc_multi_stream(
            source,
            dest,
            [],
            {},
            None,
            sync_mode="cdc",
            stream_contracts=[
                {"name": "customers", "selected": True, "primary_key": "id"},
                {"name": "orders", "selected": True, "primary_key": "id"},
            ],
            selected=selected,
            job_id="j-replay",
            checkpoint=None,
            checkpoint_service=None,
            backfill_new_fields=False,
            validation_mode="strict",
            limit=0,
        )


def test_hooks_run_once_around_three_change_batches(monkeypatch) -> None:
    """The session is the apply loop, not each batch. Three batches, two hook calls."""
    seen: list[str] = []

    def _hook(_dest, spec) -> None:
        seen.append(spec.identifier)

    monkeypatch.setattr("src.transfer.adapters._run_dest_procedure_hook", _hook)
    dest = _pg_dest(
        dest_write_mode="table",
        dest_procedure_before="CALL public.prep()",
        dest_procedure_after="CALL public.finish()",
    )
    ddl: list[str] = []
    batches = 0
    with cdc_destination_hooks(dest, ddl):
        for _ in range(3):
            batches += 1
    assert batches == 3
    assert seen == ["public.prep", "public.finish"]
    assert any(line.startswith("before_write once") for line in ddl)
    assert any(line.startswith("after_write once") for line in ddl)


def test_before_hook_failure_does_not_run_the_after_hook(monkeypatch) -> None:
    seen: list[str] = []

    def _hook(_dest, spec) -> None:
        seen.append(spec.identifier)
        if spec.identifier.endswith("prep"):
            raise RuntimeError("prep failed")

    monkeypatch.setattr("src.transfer.adapters._run_dest_procedure_hook", _hook)
    dest = _pg_dest(
        dest_write_mode="table",
        dest_procedure_before="CALL public.prep()",
        dest_procedure_after="CALL public.finish()",
    )
    applied = 0
    with pytest.raises(CdcDestinationSessionError, match="prep failed"):
        with cdc_destination_hooks(dest, []):
            applied += 1
    assert applied == 0
    assert seen == ["public.prep"]


def test_failed_apply_still_runs_after_hook_and_keeps_the_apply_error(monkeypatch) -> None:
    seen: list[str] = []

    def _hook(_dest, spec) -> None:
        seen.append(spec.identifier)
        if spec.identifier.endswith("finish"):
            raise RuntimeError("finish failed")

    monkeypatch.setattr("src.transfer.adapters._run_dest_procedure_hook", _hook)
    dest = _pg_dest(
        dest_write_mode="table",
        dest_procedure_before="CALL public.prep()",
        dest_procedure_after="CALL public.finish()",
    )
    with pytest.raises(RuntimeError, match="apply failed"):
        with cdc_destination_hooks(dest, []):
            raise RuntimeError("apply failed")
    assert seen == ["public.prep", "public.finish"]


def test_sequential_cdc_runs_session_hooks_once_for_two_tables(monkeypatch) -> None:
    source = MagicMock()
    source.format = "postgresql"
    source.table = "customers,orders"
    source.collection = ""
    dest = _pg_dest(
        dest_write_mode="table",
        dest_procedure_before="CALL public.prep()",
        dest_procedure_after="CALL public.finish()",
    )
    hooks_during_stream: list[tuple[str, str]] = []
    seen: list[str] = []

    def _hook(_dest, spec) -> None:
        seen.append(spec.identifier)

    def _fake_single(src, destination, mappings, *args, **kwargs):
        extra = destination.extra
        hooks_during_stream.append(
            (extra.get("dest_procedure_before"), extra.get("dest_procedure_after"))
        )
        return 1, ["stream"], {"cdc": {}}, ["id"]

    monkeypatch.setattr("src.transfer.adapters._run_dest_procedure_hook", _hook)
    monkeypatch.setattr("src.transfer.cdc_transfer._run_cdc_single_stream", _fake_single)
    monkeypatch.setattr(
        "src.transfer.cdc_transfer._run_cdc_shared_multi_table",
        MagicMock(side_effect=RuntimeError("force sequential path")),
    )
    selected = [
        SyncContract(name="customers", sync_mode="cdc", primary_key="id"),
        SyncContract(name="orders", sync_mode="cdc", primary_key="id"),
    ]
    _rows, ddl, _summary, _headers = _run_cdc_multi_stream(
        source,
        dest,
        [{"source": "id", "target": "id"}],
        {"id": "integer"},
        None,
        sync_mode="cdc",
        stream_contracts=[
            {"name": "customers", "selected": True, "primary_key": "id", "sync_mode": "cdc"},
            {"name": "orders", "selected": True, "primary_key": "id", "sync_mode": "cdc"},
        ],
        selected=selected,
        job_id="j-hooks",
        checkpoint=None,
        checkpoint_service=None,
        backfill_new_fields=False,
        validation_mode="strict",
        limit=0,
    )
    assert hooks_during_stream == [("", ""), ("", "")]
    assert seen == ["public.prep", "public.finish"]
    assert sum(1 for line in ddl if line.startswith("before_write once")) == 1
    assert sum(1 for line in ddl if line.startswith("after_write once")) == 1
    assert dest.extra["dest_procedure_before"] == "CALL public.prep()"
    assert dest.extra["dest_procedure_after"] == "CALL public.finish()"
