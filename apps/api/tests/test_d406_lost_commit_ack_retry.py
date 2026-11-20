"""D406: a retry after a lost commit acknowledgement reports what really landed.

The exactly-once SQL apply retries a batch once when its destination
connection drops (``cdc_eos_sa._in_dest_transaction``). If the drop comes
*after* the COMMIT reached the server (the acknowledgement is lost, the
driver raises with ``connection_invalidated``), the retry reads the
destination-owned watermark under lock, finds this batch's LSN committed and
correctly writes nothing again. But it then reported ``already_committed``
with ``rows_written=0`` for rows that this very run had written, so the job
under-counted them (R9: counts must match what landed).

The retry now verifies before it reports: when the watermark it finds is the
exact one the lost commit wrote (same LSN, epoch, apply sequence and
checksum), the batch is reported with the first attempt's counts; any other
watermark is a different writer's and is reported as found.

Live PostgreSQL destination; the fault is injected around a real COMMIT.
"""

from __future__ import annotations

import uuid
from typing import Any

import pytest

from tests.test_cdc_exactly_once_live_engines import (
    ENGINES,
    HEADERS,
    MAPPINGS,
    PK,
    TYPES,
    _batch,
    _count,
    _drop,
    _require,
)


class _LostAckEngine:
    """Delegates to a real engine. Once armed (the schema is prepared), the
    next ``times`` apply transactions COMMIT for real and then raise as a
    driver does when the acknowledgement is lost."""

    def __init__(self, real: Any, times: int) -> None:
        self._real = real
        self.left = times
        self.lost = 0
        self.armed = False

    def __getattr__(self, name: str) -> Any:
        return getattr(self._real, name)

    def begin(self):
        outer = self
        inner = self._real.begin()

        class _Ctx:
            def __enter__(self):
                return inner.__enter__()

            def __exit__(self, exc_type, exc, tb):
                result = inner.__exit__(exc_type, exc, tb)  # the real COMMIT (or rollback)
                if exc_type is None and outer.armed and outer.left > 0:
                    from sqlalchemy.exc import DBAPIError

                    outer.left -= 1
                    outer.lost += 1
                    raise DBAPIError(
                        "COMMIT", None, ConnectionResetError("server closed the connection"),
                        connection_invalidated=True,
                    )
                return result

        return _Ctx()


@pytest.fixture
def lost_ack(monkeypatch):
    import connectors.cdc_eos_sa as eos
    import connectors.generic_sql as gs

    real_engine = gs._engine
    real_prepare = eos._prepare_eos_schema
    engines: list[_LostAckEngine] = []

    def arm(times: int) -> list[_LostAckEngine]:
        def engine(cfg, *a, **kw):
            wrapped = _LostAckEngine(real_engine(cfg, *a, **kw), times)
            engines.append(wrapped)
            return wrapped

        def prepare(engine, *a, **kw):
            out = real_prepare(engine, *a, **kw)
            engine.armed = True  # the next transaction is the batch's apply
            return out

        monkeypatch.setattr(gs, "_engine", engine)
        monkeypatch.setattr(eos, "_prepare_eos_schema", prepare)
        return engines

    return arm


def test_a_landed_commit_with_a_lost_ack_is_counted_once(lost_ack) -> None:
    from connectors.cdc_eos_sql import apply_change_batch_exactly_once

    _require("postgresql")
    table = f"d406ack_{uuid.uuid4().hex[:8]}"
    key = f"k-{table}"
    try:
        engines = lost_ack(times=1)
        rows, _ck, summary, _deleted = apply_change_batch_exactly_once(
            dest_type="postgresql",
            dest_cfg=dict(ENGINES["postgresql"]),
            dest_table=table,
            change=_batch("0/10", inserts=[{"id": "1", "v": "a"}, {"id": "2", "v": "b"}, {"id": "3", "v": "c"}]),
            mappings=MAPPINGS,
            column_types=TYPES,
            headers=HEADERS,
            pk_target_cols=PK,
            cursor_key=key,
        )
        assert sum(e.lost for e in engines) == 1, "the fault was not injected"
        assert _count("postgresql", table) == 3  # applied once, not twice
        assert rows == 3, (rows, summary.get("eos_status"))
        assert summary.get("eos_status") == "applied", summary
    finally:
        _drop("postgresql", [table])


def test_the_retry_does_not_claim_another_writers_commit() -> None:
    """Verify-before-report: a watermark this attempt did not write is not ours."""
    from connectors.cdc_eos_sa import _carry_landed_attempt
    from services.cdc_exactly_once import EosApplyResult

    ours = EosApplyResult(
        status="applied", rows_written=3, committed_lsn="0/10", epoch=4, apply_seq=7, apply_checksum="c1"
    )
    found = EosApplyResult(
        status="already_committed", committed_lsn="0/10", epoch=4, apply_seq=7, apply_checksum="c1",
        already_committed=True,
    )
    assert _carry_landed_attempt(ours, found) is ours
    for other in (
        {"epoch": 5}, {"apply_seq": 8}, {"apply_checksum": "c2"}, {"committed_lsn": "0/11"},
    ):
        theirs = EosApplyResult(**{**found.__dict__, **other})
        assert _carry_landed_attempt(ours, theirs) is theirs
    # A retry that applied (the first commit did not land) reports itself.
    applied = EosApplyResult(status="applied", rows_written=3, committed_lsn="0/10", epoch=4, apply_seq=7)
    assert _carry_landed_attempt(ours, applied) is applied
    assert _carry_landed_attempt(None, found) is found


def test_a_bundle_with_a_lost_ack_reports_every_members_rows(lost_ack) -> None:
    from connectors.cdc_eos_sql import apply_eos_bundle, eos_stream_key
    from services.cdc_exactly_once import EosBundleStream

    _require("postgresql")
    tables = [f"d406b_{uuid.uuid4().hex[:8]}" for _ in range(2)]
    streams = [
        EosBundleStream(
            dest_table=t,
            change=_batch("0/20", inserts=[{"id": f"{i}", "v": t} for i in range(n)]),
            mappings=MAPPINGS,
            column_types=TYPES,
            pk_target_cols=PK,
            stream_key=eos_stream_key(
                dest_type="postgresql", dest_database=str(ENGINES["postgresql"]["database"]),
                dest_object=t, cursor_key=f"k-{t}", stream_name="",
            ),
            headers=HEADERS,
        )
        for t, n in zip(tables, (2, 3))
    ]
    try:
        engines = lost_ack(times=1)
        result = apply_eos_bundle(
            dest_type="postgresql",
            dest_cfg=dict(ENGINES["postgresql"]),
            streams=streams,
            incoming_lsn="0/20",
            bundle_key=f"bundle-{tables[0]}",
        )
        assert sum(e.lost for e in engines) == 1
        assert [_count("postgresql", t) for t in tables] == [2, 3]
        assert result.rows_written == 5, result
        assert result.already_committed is False
    finally:
        _drop("postgresql", tables)
