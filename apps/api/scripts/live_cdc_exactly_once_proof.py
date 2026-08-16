"""Live CDC exactly-once proof: real Postgres / MySQL / Oracle / SQL Server.

The dest-owned watermark protocol was only ever exercised against SQLite, where
one process holds one file lock and a transaction cannot be observed by anyone
else. That proves the decision algebra, not the protocol: fencing, redelivery,
crash-before-commit rollback and multi-stream atomicity all depend on the *dest
engine's* transaction semantics (MySQL DDL is non-transactional, Postgres aborts
the whole transaction on any error, both replicate `ON CONFLICT`/`ON DUPLICATE
KEY` differently).

Each scenario applies CDC batches through the product path
(``apply_change_batch_exactly_once`` / ``apply_eos_bundle``) against a live
engine, then reads the destination back with the engine's own COUNT and the
dest-owned watermark row. Nothing here asserts a pass: it records the measured
destination state so a scenario that violates its contract shows up as a gap
rather than being rounded to green.

Usage::

    python scripts/live_cdc_exactly_once_proof.py            # every engine
    python scripts/live_cdc_exactly_once_proof.py postgresql

Artifact: /home/ubuntu/repro/cdc_exactly_once_live_results.json
"""

from __future__ import annotations

import json
import os
import sys
import traceback
import uuid
from typing import Any, Callable

sys.path.insert(0, "/home/ubuntu/repos/DataFlow/apps/api")
os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from connectors.cdc_eos_sa import (  # noqa: E402
    sa_dest_engine_count,
    sa_dest_watermark_view,
)
from connectors.cdc_eos_sql import (  # noqa: E402
    apply_change_batch_exactly_once,
    apply_eos_bundle,
    open_eos_session,
)
from connectors.lsn_guards import DF_LSN_COL  # noqa: E402
from services.cdc_engine import ChangeBatch  # noqa: E402
from services.cdc_exactly_once import (  # noqa: E402
    EosBundleStream,
    EosCrash,
    ExactlyOnceRouteError,
)

ARTIFACT = "/home/ubuntu/repro/cdc_exactly_once_live_results.json"

ENGINES: dict[str, dict[str, Any]] = {
    "postgresql": dict(
        type="postgresql",
        host="localhost",
        port=5433,
        database="dataflow",
        username="postgres",
        password="postgres",
    ),
    "mysql": dict(
        type="mysql",
        host="127.0.0.1",
        port=3307,
        database="dataflow",
        username="root",
        password="dataflow",
    ),
    "oracle": dict(
        type="oracle",
        host="127.0.0.1",
        port=1521,
        database="FREEPDB1",
        username="system",
        password="dataflow",
    ),
    "sqlserver": dict(
        type="sqlserver",
        host="127.0.0.1",
        port=1433,
        database="dataflow",
        username="sa",
        password="DataFlow_CDC_2022!",
    ),
}

_QUOTE = {"mysql": ("`", "`"), "sqlserver": ("[", "]")}


def quoted(engine: str, name: str) -> str:
    open_q, close_q = _QUOTE.get(engine, ('"', '"'))
    return f"{open_q}{name}{close_q}"


def _drop_sql(engine: str, table: str) -> str:
    """Dialect-correct conditional drop.

    Oracle has no ``IF EXISTS`` and SQL Server only gained it in 2016, so the
    harness has to speak each dialect or the clean slate silently fails and the
    next scenario measures the previous scenario's rows.
    """
    q = quoted(engine, table)
    if engine == "oracle":
        return (
            "BEGIN EXECUTE IMMEDIATE 'DROP TABLE ' || '" + q + "'; "
            "EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
        )
    if engine == "sqlserver":
        return f"IF OBJECT_ID('{table}', 'U') IS NOT NULL DROP TABLE {q}"
    return f"DROP TABLE IF EXISTS {q}"


MAPPINGS = [
    {"source": "id", "target": "id", "confidence": 1.0},
    {"source": "v", "target": "v", "confidence": 1.0},
]
TYPES = {"id": "string", "v": "string"}
HEADERS = ["id", "v"]
PK = ["id"]


def batch(lsn: str, *, inserts=None, updates=None, deletes=None) -> ChangeBatch:
    return ChangeBatch(
        inserts=list(inserts or []),
        updates=list(updates or []),
        deletes=list(deletes or []),
        resume_token={"lsn": lsn},
    )


def drop(engine: str, tables: list[str]) -> None:
    """Clean slate per scenario, including the dest-owned watermark rows."""
    from connectors.generic_sql import _engine
    from services.cdc_exactly_once import WATERMARK_TABLE
    from services.engine_pool import release_engine
    from sqlalchemy import text

    eng = _engine(dict(ENGINES[engine]))
    try:
        with eng.begin() as conn:
            for t in tables:
                conn.execute(text(_drop_sql(engine, t)))
            try:
                conn.execute(text(f"DELETE FROM {WATERMARK_TABLE}"))
            except Exception:
                pass
    finally:
        release_engine(eng)


def apply(
    engine: str,
    table: str,
    change: ChangeBatch,
    *,
    key: str,
    fence: int = 0,
    crash_after: str | None = None,
) -> dict[str, Any]:
    rows, _ck, summary, deleted = apply_change_batch_exactly_once(
        dest_type=engine,
        dest_cfg=dict(ENGINES[engine]),
        dest_table=table,
        change=change,
        mappings=MAPPINGS,
        column_types=TYPES,
        headers=HEADERS,
        pk_target_cols=PK,
        cursor_key=key,
        writer_fence=fence,
        crash_after=crash_after,
    )
    return {"rows": rows, "deleted": deleted, "status": summary.get("eos_status")}


def wm(engine: str, key: str, table: str) -> dict[str, Any]:
    from connectors.cdc_eos_sql import eos_stream_key

    stream_key = eos_stream_key(
        dest_type=engine,
        dest_database=str(ENGINES[engine]["database"]),
        dest_object=table,
        cursor_key=key,
        stream_name="",
    )
    view = sa_dest_watermark_view(dict(ENGINES[engine]), stream_key, engine)
    return {
        "committed_lsn": view.committed_lsn,
        "epoch": view.epoch,
        "fence_epoch": view.fence_epoch,
        "phase": view.phase,
        "apply_seq": view.apply_seq,
    }


def count(engine: str, table: str) -> int:
    return sa_dest_engine_count(dict(ENGINES[engine]), table, engine)


def rows_of(engine: str, table: str) -> list[tuple]:
    from connectors.generic_sql import _engine
    from services.engine_pool import release_engine
    from sqlalchemy import text

    q = quoted(engine, table)
    eng = _engine(dict(ENGINES[engine]))
    try:
        with eng.begin() as conn:
            # Every column quoted: the dest tables are created case-sensitively,
            # and Oracle folds a bare ``v`` to ``V`` (ORA-00904).
            cols = ", ".join(quoted(engine, c) for c in ("id", "v", DF_LSN_COL))
            res = conn.execute(
                text(f"SELECT {cols} FROM {q} ORDER BY {quoted(engine, 'id')}")
            )
            return [tuple(r) for r in res]
    finally:
        release_engine(eng)


# --------------------------------------------------------------------- scenarios


def s_clean_apply(engine: str) -> dict[str, Any]:
    """Three batches, monotonic LSNs: dest holds the reduced net effect."""
    t, key = "eos_clean", "clean|" + uuid.uuid4().hex[:6]
    drop(engine, [t])
    apply(engine, t, batch("0/100", inserts=[{"id": "1", "v": "a"}, {"id": "2", "v": "b"}]), key=key)
    apply(engine, t, batch("0/200", updates=[{"id": "1", "v": "a2"}]), key=key)
    apply(engine, t, batch("0/300", inserts=[{"id": "3", "v": "c"}], deletes=["2"]), key=key)
    return {
        "expected_rows": [("1", "a2"), ("3", "c")],
        "dest_rows": [(r[0], r[1]) for r in rows_of(engine, t)],
        "dest_count": count(engine, t),
        "watermark": wm(engine, key, t),
        "expected_lsn": "0/300",
    }


def s_redelivery_same_batch(engine: str) -> dict[str, Any]:
    """At-least-once source redelivery of an already-committed LSN is a no-op."""
    t, key = "eos_redeliver", "redeliver|" + uuid.uuid4().hex[:6]
    drop(engine, [t])
    b = batch("0/100", inserts=[{"id": "1", "v": "a"}])
    first = apply(engine, t, b, key=key)
    second = apply(engine, t, b, key=key)
    third = apply(engine, t, b, key=key)
    return {
        "first_status": first["status"],
        "replay_status": [second["status"], third["status"]],
        "replay_rows_written": [second["rows"], third["rows"]],
        "dest_count": count(engine, t),
        "expected_count": 1,
        "watermark": wm(engine, key, t),
    }


def s_stale_lsn_dropped(engine: str) -> dict[str, Any]:
    """An older LSN arriving after a newer commit must not resurrect old values."""
    t, key = "eos_stale", "stale|" + uuid.uuid4().hex[:6]
    drop(engine, [t])
    apply(engine, t, batch("0/100", inserts=[{"id": "1", "v": "old"}]), key=key)
    apply(engine, t, batch("0/200", updates=[{"id": "1", "v": "new"}]), key=key)
    late = apply(engine, t, batch("0/150", updates=[{"id": "1", "v": "resurrected"}]), key=key)
    return {
        "late_status": late["status"],
        "dest_rows": [(r[0], r[1]) for r in rows_of(engine, t)],
        "expected_rows": [("1", "new")],
        "watermark": wm(engine, key, t),
        "expected_lsn": "0/200",
    }


def s_crash_before_commit(engine: str) -> dict[str, Any]:
    """Crash after apply, before COMMIT: dest rolls back, retry lands once."""
    t, key = "eos_crash_pre", "crashpre|" + uuid.uuid4().hex[:6]
    drop(engine, [t])
    apply(engine, t, batch("0/100", inserts=[{"id": "1", "v": "a"}]), key=key)
    crashed = None
    for point in ("after_apply_before_watermark", "after_watermark_before_commit"):
        try:
            apply(
                engine,
                t,
                batch("0/200", inserts=[{"id": "2", "v": "b"}]),
                key=key,
                crash_after=point,
            )
        except EosCrash:
            crashed = point
        after_crash = {"count": count(engine, t), "watermark": wm(engine, key, t)}
        if after_crash["count"] != 1 or after_crash["watermark"]["committed_lsn"] != "0/100":
            return {
                "crash_point": point,
                "rolled_back": False,
                "after_crash": after_crash,
                "note": "dest kept partial work from an uncommitted batch",
            }
    retry = apply(engine, t, batch("0/200", inserts=[{"id": "2", "v": "b"}]), key=key)
    return {
        "crash_point": crashed,
        "rolled_back": True,
        "retry_status": retry["status"],
        "dest_count": count(engine, t),
        "expected_count": 2,
        "watermark": wm(engine, key, t),
        "expected_lsn": "0/200",
    }


def s_crash_after_commit(engine: str) -> dict[str, Any]:
    """Crash after COMMIT before source ack: replay of the same LSN is a no-op."""
    t, key = "eos_crash_post", "crashpost|" + uuid.uuid4().hex[:6]
    drop(engine, [t])
    apply(engine, t, batch("0/100", inserts=[{"id": "1", "v": "a"}]), key=key)
    b = batch("0/200", updates=[{"id": "1", "v": "b"}])
    crashed = False
    try:
        apply(engine, t, b, key=key, crash_after="after_commit_before_ack")
    except EosCrash:
        crashed = True
    committed = {"count": count(engine, t), "watermark": wm(engine, key, t)}
    replay = apply(engine, t, b, key=key)
    return {
        "crash_raised": crashed,
        "dest_after_crash": committed,
        "replay_status": replay["status"],
        "replay_rows_written": replay["rows"],
        "dest_rows": [(r[0], r[1]) for r in rows_of(engine, t)],
        "expected_rows": [("1", "b")],
        "dest_count": count(engine, t),
        "expected_count": 1,
    }


def s_zombie_fence_refused(engine: str) -> dict[str, Any]:
    """A writer whose lease was stolen (lower fence) cannot commit."""
    t, key = "eos_fence", "fence|" + uuid.uuid4().hex[:6]
    drop(engine, [t])
    apply(engine, t, batch("0/100", inserts=[{"id": "1", "v": "live"}]), key=key, fence=4)
    reason = None
    try:
        apply(engine, t, batch("0/200", inserts=[{"id": "9", "v": "zombie"}]), key=key, fence=2)
    except ExactlyOnceRouteError as exc:
        reason = exc.reason
    return {
        "refused_reason": reason,
        "dest_rows": [(r[0], r[1]) for r in rows_of(engine, t)],
        "expected_rows": [("1", "live")],
        "watermark": wm(engine, key, t),
        "expected_lsn": "0/100",
    }


def s_same_lsn_different_payload(engine: str) -> dict[str, Any]:
    """Same LSN, different payload is a conflict — never a silent overwrite."""
    t, key = "eos_conflict", "conflict|" + uuid.uuid4().hex[:6]
    drop(engine, [t])
    apply(engine, t, batch("0/100", inserts=[{"id": "1", "v": "original"}]), key=key)
    reason, status = None, None
    try:
        out = apply(engine, t, batch("0/100", inserts=[{"id": "1", "v": "tampered"}]), key=key)
        status = out["status"]
    except ExactlyOnceRouteError as exc:
        reason = exc.reason
    return {
        "refused_reason": reason,
        "status": status,
        "dest_rows": [(r[0], r[1]) for r in rows_of(engine, t)],
        "expected_rows": [("1", "original")],
    }


def s_bundle_atomic(engine: str) -> dict[str, Any]:
    """N tables + one LSN in one dest transaction; crash rolls back every one."""
    t1, t2 = "eos_bundle_a", "eos_bundle_b"
    key = "bundle|" + uuid.uuid4().hex[:6]
    drop(engine, [t1, t2])

    def streams(lsn: str) -> list[EosBundleStream]:
        # Distinct PK per LSN: reusing one key would make an upsert look like a
        # missing insert when the count does not grow.
        return [
            EosBundleStream(
                dest_table=t,
                change=batch(lsn, inserts=[{"id": f"{n}-{lsn}", "v": f"{t}-{lsn}"}]),
                mappings=MAPPINGS,
                column_types=TYPES,
                pk_target_cols=PK,
                stream_key=f"{key}|{t}",
                headers=HEADERS,
            )
            for n, t in enumerate((t1, t2), start=1)
        ]

    first = apply_eos_bundle(
        dest_type=engine,
        dest_cfg=dict(ENGINES[engine]),
        streams=streams("0/100"),
        incoming_lsn="0/100",
        bundle_key=key,
    )
    crashed = False
    try:
        apply_eos_bundle(
            dest_type=engine,
            dest_cfg=dict(ENGINES[engine]),
            streams=streams("0/200"),
            incoming_lsn="0/200",
            bundle_key=key,
            crash_after="after_apply_before_watermark",
        )
    except EosCrash:
        crashed = True
    after_crash = {t1: count(engine, t1), t2: count(engine, t2)}
    retry = apply_eos_bundle(
        dest_type=engine,
        dest_cfg=dict(ENGINES[engine]),
        streams=streams("0/200"),
        incoming_lsn="0/200",
        bundle_key=key,
    )
    return {
        "first_rows_written": first.rows_written,
        "crash_raised": crashed,
        "counts_after_crash": after_crash,
        "expected_counts_after_crash": {t1: 1, t2: 1},
        "retry_rows_written": retry.rows_written,
        "counts_final": {t1: count(engine, t1), t2: count(engine, t2)},
        "expected_counts_final": {t1: 2, t2: 2},
    }


def s_open_fence_raises_without_apply(engine: str) -> dict[str, Any]:
    """Open must raise the fence with no data apply and return dest resume."""
    t, key = "eos_open", "open|" + uuid.uuid4().hex[:6]
    drop(engine, [t])
    apply(engine, t, batch("0/100", inserts=[{"id": "1", "v": "a"}]), key=key)
    from connectors.cdc_eos_sql import eos_stream_key

    stream_key = eos_stream_key(
        dest_type=engine,
        dest_database=str(ENGINES[engine]["database"]),
        dest_object=t,
        cursor_key=key,
        stream_name="",
    )
    opened = open_eos_session(
        dest_type=engine,
        dest_cfg=dict(ENGINES[engine]),
        stream_key=stream_key,
        incoming_fence=0,
        job_resume={"lsn": "0/900"},
    )
    # Leased writer: a positive incoming generation must be persisted on dest so
    # the previous lease holder cannot commit afterwards.
    leased = open_eos_session(
        dest_type=engine,
        dest_cfg=dict(ENGINES[engine]),
        stream_key=stream_key,
        incoming_fence=7,
        job_resume={"lsn": "0/100"},
    )
    return {
        "dest_lsn": opened.dest_lsn,
        "expected_dest_lsn": "0/100",
        "fence_raised": opened.fence_raised,
        "fence_epoch": opened.fence_epoch,
        "leased_fence_raised": leased.fence_raised,
        "leased_fence_epoch": leased.fence_epoch,
        "expected_leased_fence_epoch": 7,
        "dest_fence_after_open": wm(engine, key, t)["fence_epoch"],
        "resume_clamped_to_dest": json.loads(json.dumps(opened.resume, default=str)),
        "dest_count_unchanged": count(engine, t),
        "expected_count": 1,
    }


SCENARIOS: dict[str, Callable[[str], dict[str, Any]]] = {
    "clean_apply_net_effect": s_clean_apply,
    "redelivery_same_batch_noop": s_redelivery_same_batch,
    "stale_lsn_dropped": s_stale_lsn_dropped,
    "crash_before_commit_rolls_back": s_crash_before_commit,
    "crash_after_commit_replay_noop": s_crash_after_commit,
    "zombie_fence_refused": s_zombie_fence_refused,
    "same_lsn_different_payload_refused": s_same_lsn_different_payload,
    "bundle_atomic_across_streams": s_bundle_atomic,
    "open_raises_fence_without_apply": s_open_fence_raises_without_apply,
}


def main() -> None:
    engines = sys.argv[1:] or list(ENGINES)
    results: dict[str, Any] = {}
    for engine in engines:
        results[engine] = {}
        for name, fn in SCENARIOS.items():
            try:
                results[engine][name] = fn(engine)
            except Exception as exc:  # report, never hide
                results[engine][name] = {
                    "error": f"{type(exc).__name__}: {exc}",
                    "traceback": traceback.format_exc()[-1500:],
                }
            print(f"{engine} {name}: {json.dumps(results[engine][name], default=str)[:400]}")
    os.makedirs(os.path.dirname(ARTIFACT), exist_ok=True)
    with open(ARTIFACT, "w", encoding="utf-8") as fh:
        json.dump(results, fh, indent=2, default=str)
    print(f"\nartifact: {ARTIFACT}")


if __name__ == "__main__":
    main()
