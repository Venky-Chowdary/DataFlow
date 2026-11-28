"""D335 live scenario shared by the Oracle and SQL Server legs.

PostgreSQL source (key ``id``, cursor ``updated_at`` with microseconds, row 30
NULL except key and cursor), keyed incremental_append + propagate_columns:
run 1 loads 30 rows, run 2 adds id 31 with NO schema change (QA r66), run 3
adds id 32 after ``ADD COLUMN tier INT DEFAULT 1`` (QA r65-r75). Each run must
append only its new ids and never report a doubled watermark row as verified.
"""

from __future__ import annotations

import uuid
from typing import Any, Callable


def _ts(i: int) -> str:
    return f"2026-11-24 10:{i:02d}:00.{(i * 123457) % 1000000:06d}"


def _request(source: Any, dest: Any, table: str) -> Any:
    from src.transfer.models import TransferRequest

    return TransferRequest(
        source=source,
        destination=dest,
        sync_mode="incremental_append",
        stream_contracts=[
            {
                "name": table,
                "cursor_field": "updated_at",
                "cursor_semantics": "modification_timestamp",
                "primary_key": ["id"],
                "sync_mode": "incremental_append",
                "selected": True,
            }
        ],
        schema_policy="propagate_columns",
        validation_mode="strict",
    )


def run_boundary_scenario(
    store: Any,
    *,
    pg_sql: Callable[..., Any],
    pg_endpoint: Callable[[str], Any],
    dest_endpoint: Callable[[str], Any],
    dest_ids: Callable[[str], list[int]],
    drop_dest: Callable[[str], None],
    source_cursor_type: str = "TIMESTAMP(6)",
    ts: Callable[[int], str] = _ts,
    pre_create: Callable[[str], None] | None = None,
) -> None:
    from src.transfer.engine import UniversalTransferEngine

    def _run() -> Any:
        job_id = "d335l" + uuid.uuid4().hex[:12]
        store.update_job_status(job_id, "pending", transfer_request={})
        return UniversalTransferEngine().execute_tracked(
            _request(pg_endpoint(src), dest_endpoint(dst), src), job_id
        )

    src, dst = f"d335s_{uuid.uuid4().hex[:8]}", f"D335D_{uuid.uuid4().hex[:8]}".upper()
    try:
        pg_sql(
            f'CREATE TABLE public."{src}" (id INT PRIMARY KEY, note TEXT, '
            f"updated_at {source_cursor_type} NOT NULL)"
        )
        if pre_create is not None:
            pre_create(dst)
        for i in range(1, 31):
            pg_sql(f'INSERT INTO public."{src}" VALUES (%s, %s, %s)', (i, None if i == 30 else f"n{i}", ts(i)))
        first = _run()
        assert first.success, first.error
        assert dest_ids(dst) == list(range(1, 31))

        pg_sql(f'INSERT INTO public."{src}" VALUES (31, %s, %s)', ("n31", ts(31)))
        second = _run()
        assert dest_ids(dst) == list(range(1, 32)), "run 2 re-appended the watermark row (id 30)"
        assert second.success, second.error

        pg_sql(f'ALTER TABLE public."{src}" ADD COLUMN tier INT DEFAULT 1')
        pg_sql(f'INSERT INTO public."{src}" (id, note, updated_at) VALUES (32, %s, %s)', ("n32", ts(32)))
        third = _run()
        assert dest_ids(dst) == list(range(1, 33)), "run 3 re-appended the watermark row (id 31)"
        assert third.success, third.error
    finally:
        pg_sql(f'DROP TABLE IF EXISTS public."{src}"')
        drop_dest(dst)
