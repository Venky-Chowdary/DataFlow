"""SQL Server CDC snapshot and polling handoffs stay within capture bounds."""

from __future__ import annotations

from contextlib import ExitStack, contextmanager
from typing import Iterator
from unittest.mock import MagicMock, patch

import pytest

from connectors.lsn_guards import lsn_family
from connectors.sqlserver_cdc_native import (
    SqlServerNativeCdc,
    decode_mssql_cdc_token,
)
from services.cdc_cursor_gap import CdcLsnGapError
from services.cdc_exactly_once import batch_lsn


CFG = {
    "host": "localhost",
    "database": "dataflow",
    "username": "sa",
    "password": "not-used-by-fakes",
}
RESUME_LSN = "0000002e000001d80030"
EARLIER_LSN = "0000002e000001d80020"
LATER_LSN = "0000002e000001d80040"


class _HandoffCursor:
    def __init__(
        self,
        *,
        database_max_lsn: str,
        start_lsns: dict[str, str | None],
        min_lsns: dict[str, str | None],
    ) -> None:
        self.database_max_lsn = database_max_lsn
        self.start_lsns = start_lsns
        self.min_lsns = min_lsns
        self._row: tuple[str | None] | None = None

    def execute(self, query: str, params: tuple[str, ...] | None = None) -> None:
        if "fn_cdc_get_max_lsn" in query:
            self._row = (self.database_max_lsn,)
        elif "fn_cdc_get_min_lsn" in query:
            capture = params[0] if params else ""
            self._row = (self.min_lsns.get(capture),)
        elif "cdc.change_tables" in query:
            capture = params[0] if params else ""
            self._row = (self.start_lsns.get(capture),)
        else:
            raise AssertionError(f"unexpected query: {query}")

    def fetchone(self) -> tuple[str | None] | None:
        return self._row


def _reader(*, shared: bool = False) -> SqlServerNativeCdc:
    reader = SqlServerNativeCdc(
        CFG,
        table=["orders", "users"] if shared else "orders",
        primary_key="id",
        primary_keys={"orders": "id", "users": "id"} if shared else None,
        schema="dbo",
        batch_size=2,
    )
    reader.phase = "streaming"
    reader.start_lsn = RESUME_LSN
    reader.capture_instance = "dbo_orders"
    reader._capture_resolved = True
    reader._resume_inclusive = True
    return reader


def _connection(cursor):
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return conn


@contextmanager
def _poll_stubs(
    reader: SqlServerNativeCdc,
    *,
    min_lsn: str,
    max_lsn: str,
) -> Iterator[tuple[MagicMock, MagicMock, MagicMock]]:
    cursor = MagicMock()
    conn = _connection(cursor)
    tvf = MagicMock(return_value="cdc.fn_cdc_get_all_changes_dbo_orders")
    interleave = MagicMock(return_value=iter(()))
    with ExitStack() as stack:
        stack.enter_context(patch.object(reader, "_conn", return_value=conn))
        stack.enter_context(patch.object(reader, "_acquire_cdc_lease"))
        stack.enter_context(
            patch.object(reader, "_capture_instance_exists", return_value=True)
        )
        if reader._shared:
            def resolve_captures(_cur):
                reader._captures = {"orders": "dbo_orders", "users": "dbo_users"}

            stack.enter_context(
                patch.object(
                    reader, "_resolve_all_captures", side_effect=resolve_captures
                )
            )
            stack.enter_context(
                patch.object(reader, "_min_lsn_for", return_value=min_lsn)
            )
            stack.enter_context(
                patch.object(reader, "_max_lsn", return_value=max_lsn)
            )
            stack.enter_context(
                patch.object(reader, "_changes_tvf_for", new=tvf)
            )
        else:
            stack.enter_context(
                patch(
                    "services.cdc_incremental_runner.interleave_incremental_snapshot",
                    new=interleave,
                )
            )
            stack.enter_context(patch.object(reader, "_resolve_capture_instance"))
            stack.enter_context(
                patch.object(reader, "_maybe_record_capture_schema")
            )
            stack.enter_context(patch.object(reader, "_min_lsn", return_value=min_lsn))
            stack.enter_context(
                patch.object(reader, "_max_lsn", return_value=max_lsn)
            )
            stack.enter_context(patch.object(reader, "_changes_tvf", new=tvf))
        yield cursor, tvf, interleave


def _assert_heartbeat(reader: SqlServerNativeCdc, batches: list) -> None:
    assert len(batches) == 1
    assert not batches[0].inserts
    assert (
        decode_mssql_cdc_token(batches[0].resume_token)["lsn"]
        == RESUME_LSN
    )
    assert reader.start_lsn == RESUME_LSN
    assert reader._resume_inclusive is True


def test_native_token_types_digit_only_sqlserver_lsns() -> None:
    reader = _reader()
    token = reader._token(lsn="00000025000004500003", phase="streaming")

    assert lsn_family(batch_lsn(token)) == "mssql_lsn"


def test_poll_emits_distinct_batches_for_distinct_lsns() -> None:
    reader = _reader()
    reader.start_lsn = EARLIER_LSN
    columns = ["__$start_lsn", "__$seqval", "__$operation", "id", "amount"]
    first_seq = "00000000000000000005"
    second_seq = "00000000000000000010"
    cursor = MagicMock()
    cursor.description = [(name,) for name in columns]
    cursor.fetchall.return_value = [
        (bytes.fromhex(EARLIER_LSN), bytes.fromhex(first_seq), 2, 1, "10.00"),
        (bytes.fromhex(LATER_LSN), bytes.fromhex(second_seq), 4, 1, "99.00"),
    ]
    conn = _connection(cursor)
    interleave = MagicMock(return_value=iter(()))

    with ExitStack() as stack:
        stack.enter_context(patch.object(reader, "_conn", return_value=conn))
        stack.enter_context(patch.object(reader, "_acquire_cdc_lease"))
        stack.enter_context(patch.object(reader, "_resolve_capture_instance"))
        stack.enter_context(patch.object(reader, "_maybe_record_capture_schema"))
        stack.enter_context(
            patch.object(reader, "_min_lsn", return_value=EARLIER_LSN)
        )
        stack.enter_context(patch.object(reader, "_max_lsn", return_value=LATER_LSN))
        stack.enter_context(patch.object(reader, "_changes_tvf", return_value="cdc.fn"))
        stack.enter_context(
            patch(
                "services.cdc_incremental_runner.interleave_incremental_snapshot",
                new=interleave,
            )
        )
        batches = list(reader.poll())

    assert len(batches) == 2
    assert batches[0].inserts == [{"id": "1", "amount": "10.00"}]
    assert batches[1].updates == [{"id": "1", "amount": "99.00"}]
    assert batch_lsn(batches[0].resume_token) == f"{EARLIER_LSN}.p{first_seq}"
    assert batch_lsn(batches[1].resume_token) == f"{LATER_LSN}.p{second_seq}"


def test_snapshot_handoff_uses_capture_start_when_database_max_lags() -> None:
    reader = _reader()
    cursor = _HandoffCursor(
        database_max_lsn=EARLIER_LSN,
        start_lsns={"dbo_orders": RESUME_LSN},
        min_lsns={"dbo_orders": None},
    )

    assert reader._snapshot_handoff_lsn(cursor, "dbo_orders") == RESUME_LSN


def test_snapshot_handoff_uses_database_max_when_it_is_newer() -> None:
    reader = _reader()
    cursor = _HandoffCursor(
        database_max_lsn=LATER_LSN,
        start_lsns={"dbo_orders": RESUME_LSN},
        min_lsns={"dbo_orders": None},
    )

    assert reader._snapshot_handoff_lsn(cursor, "dbo_orders") == LATER_LSN


def test_snapshot_handoff_never_falls_below_non_null_capture_min() -> None:
    reader = _reader()
    cursor = _HandoffCursor(
        database_max_lsn=EARLIER_LSN,
        start_lsns={"dbo_orders": RESUME_LSN},
        min_lsns={"dbo_orders": LATER_LSN},
    )

    assert reader._snapshot_handoff_lsn(cursor, "dbo_orders") == LATER_LSN


def test_shared_snapshot_handoff_uses_each_capture_start_when_min_is_null() -> None:
    reader = _reader(shared=True)
    cursor = _HandoffCursor(
        database_max_lsn=EARLIER_LSN,
        start_lsns={"dbo_orders": RESUME_LSN, "dbo_users": EARLIER_LSN},
        min_lsns={"dbo_orders": None, "dbo_users": None},
    )
    conn = _connection(cursor)

    with ExitStack() as stack:
        stack.enter_context(patch.object(reader, "_conn", return_value=conn))
        stack.enter_context(patch.object(reader, "_acquire_cdc_lease"))

        def resolve_captures(_cur):
            reader._captures = {"orders": "dbo_orders", "users": "dbo_users"}

        stack.enter_context(
            patch.object(
                reader, "_resolve_all_captures", side_effect=resolve_captures
            )
        )
        stack.enter_context(patch.object(reader, "_maybe_record_capture_schema"))
        stack.enter_context(
            patch.object(reader, "_iter_snapshot_table", return_value=iter(()))
        )
        batches = list(reader._snapshot_shared())

    assert (
        decode_mssql_cdc_token(batches[-1].resume_token)["lsn"]
        == RESUME_LSN
    )


@pytest.mark.parametrize("shared", [False, True], ids=["single", "shared"])
def test_poll_behind_database_max_emits_non_advancing_heartbeat(
    shared: bool,
) -> None:
    reader = _reader(shared=shared)
    with _poll_stubs(reader, min_lsn=RESUME_LSN, max_lsn=EARLIER_LSN) as (
        cursor,
        tvf,
        interleave,
    ):
        batches = list(reader.poll())

    _assert_heartbeat(reader, batches)
    tvf.assert_not_called()
    interleave.assert_not_called()
    cursor.execute.assert_not_called()


@pytest.mark.parametrize("shared", [False, True], ids=["single", "shared"])
def test_poll_with_null_capture_min_emits_non_advancing_heartbeat(
    shared: bool,
) -> None:
    reader = _reader(shared=shared)
    with _poll_stubs(reader, min_lsn="", max_lsn=LATER_LSN) as (
        cursor,
        tvf,
        interleave,
    ):
        batches = list(reader.poll())

    _assert_heartbeat(reader, batches)
    tvf.assert_not_called()
    interleave.assert_not_called()
    cursor.execute.assert_not_called()


@pytest.mark.parametrize("shared", [False, True], ids=["single", "shared"])
def test_poll_still_raises_for_resume_before_real_capture_min(shared: bool) -> None:
    reader = _reader(shared=shared)
    with _poll_stubs(reader, min_lsn=LATER_LSN, max_lsn=LATER_LSN) as (
        cursor,
        tvf,
        interleave,
    ):
        with pytest.raises(CdcLsnGapError):
            list(reader.poll())

    tvf.assert_not_called()
    interleave.assert_not_called()
    cursor.execute.assert_not_called()
