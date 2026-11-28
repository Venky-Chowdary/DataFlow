"""D426 / D406: the post-commit watermark re-read survives a dropped connection.

An exactly-once CDC batch commits its rows and the destination-owned
watermark in one transaction, then re-reads the watermark before the source
position is acknowledged (``verify_dest_commit``). That re-read swallowed any
error into an empty view, so one dropped connection after a commit that had
landed failed the run with "exactly_once dest commit unverified - watermark
LSN None is behind expected ... Refuse source ack." (QA round 63: the run
after an interrupted run went red although nothing was wrong).

The re-read now retries once on a fresh connection, so a landed commit is
confirmed and the run completes; a watermark that is genuinely unreadable
still fails closed, with a message that names the table and says that the
source was not acknowledged (no internal "None").

Live PostgreSQL destination; the fault is injected around a real connection.
"""

from __future__ import annotations

import uuid
from typing import Any

import pytest

from tests.test_cdc_exactly_once_live_engines import ENGINES, _batch, _count, _drop, _require


class _DropOnConnect:
    """Delegates to a real engine; while armed, the next ``connect`` raises
    the way SQLAlchemy does when the pooled socket died under it."""

    def __init__(self, real: Any, state: dict[str, int]) -> None:
        self._real = real
        self._state = state

    def __getattr__(self, name: str) -> Any:
        return getattr(self._real, name)

    def connect(self):
        if self._state["armed"] > 0:
            from sqlalchemy.exc import DBAPIError

            self._state["armed"] -= 1
            self._state["dropped"] += 1
            raise DBAPIError(
                "SELECT committed_lsn", None,
                ConnectionResetError("server closed the connection unexpectedly"),
                connection_invalidated=True,
            )
        return self._real.connect()


@pytest.fixture
def drop_reread(monkeypatch):
    """Arms one dropped connection for the re-read that follows the next commit."""
    import connectors.cdc_eos_sa as eos
    import connectors.generic_sql as gs

    state = {"armed": 0, "dropped": 0, "arm_next": 0}
    real_engine = gs._engine
    real_txn = eos._in_dest_transaction

    def engine(cfg, *a, **kw):
        return _DropOnConnect(real_engine(cfg, *a, **kw), state)

    def txn(*a, **kw):
        out = real_txn(*a, **kw)
        if state["arm_next"]:
            state["armed"], state["arm_next"] = state["arm_next"], 0
        return out

    monkeypatch.setattr(gs, "_engine", engine)
    monkeypatch.setattr(eos, "_in_dest_transaction", txn)
    return state


def _apply(table: str, key: str, change):
    from connectors.cdc_eos_sql import apply_change_batch_exactly_once

    rows, _ck, summary, deleted = apply_change_batch_exactly_once(
        dest_type="postgresql",
        dest_cfg=dict(ENGINES["postgresql"]),
        dest_table=table,
        change=change,
        mappings=[
            {"source": "id", "target": "id", "confidence": 1.0},
            {"source": "v", "target": "v", "confidence": 1.0},
        ],
        column_types={"id": "string", "v": "string"},
        headers=["id", "v"],
        pk_target_cols=["id"],
        cursor_key=key,
    )
    return rows, summary, deleted


def _values(table: str) -> dict[str, str]:
    from sqlalchemy import text

    from connectors.generic_sql import _engine
    from services.engine_pool import release_engine

    eng = _engine(dict(ENGINES["postgresql"]))
    try:
        with eng.connect() as conn:
            return {str(r[0]): str(r[1]) for r in conn.execute(text(f'SELECT id, v FROM "{table}"'))}
    finally:
        release_engine(eng)


def test_a_dropped_reread_after_a_landed_commit_confirms_it(drop_reread) -> None:
    _require("postgresql")
    table = f"d426rr_{uuid.uuid4().hex[:8]}"
    key = f"k-{table}"
    try:
        # Run 1 lands the first copy.
        rows, summary, _ = _apply(
            table, key, _batch("0/10", inserts=[{"id": "1", "v": "a"}, {"id": "2", "v": "b"}])
        )
        assert rows == 2 and summary.get("eos_status") == "applied"
        # Run 2: update, delete and insert commit, then the re-read loses its socket.
        drop_reread["arm_next"] = 1
        rows, summary, deleted = _apply(
            table,
            key,
            _batch("0/20", updates=[{"id": "1", "v": "a2"}], inserts=[{"id": "3", "v": "c"}], deletes=["2"]),
        )
        assert drop_reread["dropped"] == 1, "the fault was not injected"
        assert summary.get("eos_status") == "applied", summary
        assert summary.get("eos_committed_lsn") == "0/20", summary
        assert (rows, deleted) == (2, 1)
        assert _values(table) == {"1": "a2", "3": "c"}
        # The next run applies its batch on top: nothing skipped, nothing twice.
        rows, summary, _ = _apply(table, key, _batch("0/30", updates=[{"id": "3", "v": "c3"}]))
        assert summary.get("eos_status") == "applied" and rows == 1
        assert _values(table) == {"1": "a2", "3": "c3"}
        assert _count("postgresql", table) == 2
    finally:
        _drop("postgresql", [table])


def test_an_unreadable_watermark_fails_closed_without_internal_none() -> None:
    from services.cdc_exactly_once import (
        REASON_UNVERIFIED,
        DestWmView,
        ExactlyOnceRouteError,
        verify_dest_commit,
    )

    with pytest.raises(ExactlyOnceRouteError) as caught:
        verify_dest_commit(
            dest=DestWmView(),
            expected_lsn="0000003b00004f480003",
            dest_object="r63_cdc_ms_dst",
        )
    message = str(caught.value)
    assert caught.value.reason == REASON_UNVERIFIED
    assert "None" not in message
    assert "r63_cdc_ms_dst" in message
    assert "0000003b00004f480003" in message
    assert "not acknowledged" in message and "no change is lost" in message
    # A watermark that is readable but behind still refuses the ack.
    with pytest.raises(ExactlyOnceRouteError):
        verify_dest_commit(dest=DestWmView(committed_lsn="0/10"), expected_lsn="0/20")
