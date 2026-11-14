"""The duplicates-allowed flag set for one write never reaches the next write.

CI shard 5 (#430, run 43583651666) failed test_qa1 ... create_new_append[key0-pg]
and [key0-mysql]: the operator's key was withheld from a create-new table. An
earlier append create-new (MYSQL-DEF-1 marks it "repeated keys allowed") had
raised inside the writer, and ``_write_batch`` resets the flag only on its
normal return paths, so the flag stayed set for the rest of the thread.
"""

from __future__ import annotations

import pytest


def test_a_writer_that_raises_does_not_leave_duplicates_allowed(monkeypatch):
    import connectors.postgresql_writer as pgw
    from services.duplicate_policy import current_duplicates_allowed
    from src.transfer import stream_write
    from src.transfer.models import EndpointConfig

    def _boom(**_kw):
        assert current_duplicates_allowed(), "the append create-new should allow repeated keys while it writes"
        raise RuntimeError("writer failed mid-batch")

    monkeypatch.setattr(pgw, "write_mapped_rows", _boom)
    dest = EndpointConfig(kind="database", format="postgresql", host="127.0.0.1", port=5432, database="dataflow", table="t_leak")
    assert current_duplicates_allowed() is False
    with pytest.raises(Exception):
        stream_write._write_batch(
            "postgresql", dest, {"host": "127.0.0.1", "port": 5432, "database": "dataflow"},
            "t_leak", ["id"], [["1"]], [{"source": "id", "target": "id"}], {"id": "integer"},
            True, None, 0, 1, 0,
            write_mode="insert", conflict_columns=["id"], sync_mode="full_refresh_append",
            skip_preflight=True,
        )
    assert current_duplicates_allowed() is False
