"""D419 scenario shared by the SQL Server live leg and the local legs.

PostgreSQL source (key ``id``), scd2 into ``dest``: run 1 loads 30 rows; run 2
updates id 3, deletes id 6, inserts id 41 (QA r57); run 3 updates id 3 again
and id 7. Each run must be green, report the versions it closed, and leave no
gap or overlap: a closed version's ``valid_to`` equals its successor's
``valid_from`` exactly, and every key has one current version.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Callable


def _request(source: Any, dest: Any, table: str) -> Any:
    from src.transfer.models import TransferRequest

    return TransferRequest(
        source=source,
        destination=dest,
        sync_mode="scd2",
        stream_contracts=[
            {"name": table, "primary_key": ["id"], "sync_mode": "scd2", "selected": True}
        ],
        skip_preflight=True,
        validation_mode="strict",
    )


def _instant(value: Any) -> datetime:
    """A stored stamp as an aware UTC instant (driver datetimes or ISO text)."""
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(str(value).strip().replace(" +", "+").replace("Z", "+00:00"))
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


def run_scd2_history_scenario(
    store: Any,
    *,
    pg_sql: Callable[..., Any],
    pg_endpoint: Callable[[str], Any],
    dest_endpoint: Callable[[str], Any],
    dest_history: Callable[[str], list[tuple[Any, Any, Any, Any]]],
    drop_dest: Callable[[str], None],
) -> None:
    """``dest_history(table)`` returns ``(id, valid_from, valid_to, is_current)`` rows."""
    from src.transfer.engine import UniversalTransferEngine

    tag = uuid.uuid4().hex[:8]
    src, dst = f"d419_src_{tag}", f"d419_dst_{tag}"

    def _run() -> Any:
        job_id = "d419" + uuid.uuid4().hex[:12]
        store.update_job_status(job_id, "pending", transfer_request={})
        return UniversalTransferEngine().execute_tracked(
            _request(pg_endpoint(src), dest_endpoint(dst), src), job_id
        )

    pg_sql(f"CREATE TABLE {src} (id BIGINT PRIMARY KEY, name TEXT, amount NUMERIC(12,2))")
    try:
        for i in range(1, 31):
            pg_sql(f"INSERT INTO {src} VALUES (%s, %s, %s)", (i, f"n{i}", i * 1.25))  # nosec B608
        expected = [(30, 30)]
        first = _run()
        assert first.success, first.error

        pg_sql(f"UPDATE {src} SET name = 'changed-3' WHERE id = 3")  # nosec B608
        pg_sql(f"DELETE FROM {src} WHERE id = 6")  # nosec B608
        pg_sql(f"INSERT INTO {src} VALUES (41, 'n41', 41.00)")  # nosec B608
        second = _run()
        assert second.success, second.error  # D419: "0 versions closed this run, expected 2"
        expected.append((32, 30))

        pg_sql(f"UPDATE {src} SET name = 'changed-3b' WHERE id = 3")  # nosec B608
        pg_sql(f"UPDATE {src} SET amount = 7.77 WHERE id = 7")  # nosec B608
        third = _run()
        assert third.success, third.error
        expected.append((34, 30))

        rows = dest_history(dst)
        current = [r for r in rows if bool(int(r[3]))]
        assert (len(rows), len(current)) == expected[-1], (len(rows), len(current))
        by_key: dict[int, list[tuple[datetime, datetime | None]]] = {}
        for key, vf, vt, _cur in rows:
            by_key.setdefault(int(key), []).append(
                (_instant(vf), None if vt is None else _instant(vt))
            )
        assert len({int(r[0]) for r in current}) == len(current), "a key with two current versions"
        for key, versions in by_key.items():
            versions.sort(key=lambda v: v[0])
            for (vf_prev, vt_prev), (vf_next, _vt_next) in zip(versions, versions[1:]):
                # AC-2: closed and reopened in one run -> no gap, no overlap.
                assert vt_prev == vf_next, (key, vt_prev, vf_next)
        assert len(by_key[3]) == 3 and len(by_key[7]) == 2 and len(by_key[6]) == 1
        assert by_key[6][0][1] is not None, "a key deleted at source stays closed"
    finally:
        pg_sql(f"DROP TABLE IF EXISTS {src}")  # nosec B608
        drop_dest(dst)
