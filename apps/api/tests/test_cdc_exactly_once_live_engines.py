"""CDC exactly-once against live Postgres / MySQL destinations.

The dest-owned watermark protocol was only ever proven on SQLite, which hides
three things real engines do: MySQL rejects ANSI-quoted identifiers, PostgreSQL
aborts the whole transaction when any statement fails, and MySQL commits
implicitly on DDL. Each of those broke exactly-once on a route the product lists
as wired, so the proofs live here against the engines themselves.

Skips when a container port is unreachable — never green by absence.
"""

from __future__ import annotations

import socket
import sys
import uuid
from pathlib import Path
from typing import Any

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.cdc_eos_sa import (  # noqa: E402
    _quote_char,
    sa_dest_engine_count,
    sa_dest_watermark_view,
)
from connectors.cdc_eos_sql import (  # noqa: E402
    apply_change_batch_exactly_once,
    apply_eos_bundle,
    eos_stream_key,
)
from services.cdc_engine import ChangeBatch  # noqa: E402
from services.cdc_exactly_once import (  # noqa: E402
    EosBundleStream,
    EosCrash,
    decide_eos_apply,
)

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
}

MAPPINGS = [
    {"source": "id", "target": "id", "confidence": 1.0},
    {"source": "v", "target": "v", "confidence": 1.0},
]
TYPES = {"id": "string", "v": "string"}
HEADERS = ["id", "v"]
PK = ["id"]


def _reachable(cfg: dict[str, Any]) -> bool:
    try:
        with socket.create_connection((cfg["host"], cfg["port"]), timeout=1):
            return True
    except OSError:
        return False


def _require(engine: str) -> dict[str, Any]:
    cfg = dict(ENGINES[engine])
    if not _reachable(cfg):
        pytest.skip(f"{engine} at {cfg['host']}:{cfg['port']} unreachable")
    return cfg


def _batch(lsn: str, *, inserts=None, updates=None, deletes=None) -> ChangeBatch:
    return ChangeBatch(
        inserts=list(inserts or []),
        updates=list(updates or []),
        deletes=list(deletes or []),
        resume_token={"lsn": lsn},
    )


def _apply(
    engine: str,
    table: str,
    change: ChangeBatch,
    *,
    key: str,
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
        crash_after=crash_after,
    )
    return {"rows": rows, "deleted": deleted, "status": summary.get("eos_status")}


def _count(engine: str, table: str) -> int:
    return sa_dest_engine_count(dict(ENGINES[engine]), table, engine)


def _wm_lsn(engine: str, table: str, key: str) -> str | None:
    stream_key = eos_stream_key(
        dest_type=engine,
        dest_database=str(ENGINES[engine]["database"]),
        dest_object=table,
        cursor_key=key,
        stream_name="",
    )
    return sa_dest_watermark_view(
        dict(ENGINES[engine]), stream_key, engine
    ).committed_lsn


def _drop(engine: str, tables: list[str]) -> None:
    from connectors.generic_sql import _engine
    from services.engine_pool import release_engine
    from sqlalchemy import text

    q = _quote_char(engine if engine != "postgresql" else "postgresql")
    eng = _engine(dict(ENGINES[engine]))
    try:
        with eng.begin() as conn:
            for t in tables:
                name = f"`{t}`" if q == "`" else f'"{t}"'
                conn.execute(text(f"DROP TABLE IF EXISTS {name}"))
    finally:
        release_engine(eng)


def test_mysql_identifiers_are_backquoted() -> None:
    """ANSI double quotes are a syntax error for MySQL identifiers."""
    assert _quote_char("mysql") == "`"
    assert _quote_char("mariadb") == "`"
    assert _quote_char("sqlserver") == "["
    assert _quote_char("postgresql") == '"'


def test_older_lsn_replay_is_not_a_payload_conflict() -> None:
    """Recovery replays a *range* of older batches; only the same LSN can conflict."""
    action, _fence = decide_eos_apply(
        incoming_lsn="0/150",
        dest_lsn="0/200",
        incoming_checksum="aaa",
        dest_checksum="bbb",
    )
    assert action == "already_committed"


@pytest.mark.parametrize("engine", sorted(ENGINES))
def test_live_apply_replay_and_stale_lsn(engine: str) -> None:
    _require(engine)
    table = "eos_t_apply"
    key = "t-apply|" + uuid.uuid4().hex[:8]
    _drop(engine, [table])

    first = _apply(
        engine, table, _batch("0/100", inserts=[{"id": "1", "v": "a"}]), key=key
    )
    assert first["status"] == "applied"
    assert _count(engine, table) == 1

    replay = _apply(
        engine, table, _batch("0/100", inserts=[{"id": "1", "v": "a"}]), key=key
    )
    assert replay["status"] == "already_committed"
    assert replay["rows"] == 0
    assert _count(engine, table) == 1

    _apply(engine, table, _batch("0/200", updates=[{"id": "1", "v": "b"}]), key=key)
    late = _apply(
        engine,
        table,
        _batch("0/150", updates=[{"id": "1", "v": "resurrected"}]),
        key=key,
    )
    assert late["status"] == "already_committed"
    assert _wm_lsn(engine, table, key) == "0/200"


@pytest.mark.parametrize("engine", sorted(ENGINES))
def test_live_crash_before_commit_leaves_nothing_behind(engine: str) -> None:
    _require(engine)
    table = "eos_t_crash"
    key = "t-crash|" + uuid.uuid4().hex[:8]
    _drop(engine, [table])
    _apply(engine, table, _batch("0/100", inserts=[{"id": "1", "v": "a"}]), key=key)

    with pytest.raises(EosCrash):
        _apply(
            engine,
            table,
            _batch("0/200", inserts=[{"id": "2", "v": "b"}]),
            key=key,
            crash_after="after_watermark_before_commit",
        )
    assert _count(engine, table) == 1
    assert _wm_lsn(engine, table, key) == "0/100"

    retry = _apply(
        engine, table, _batch("0/200", inserts=[{"id": "2", "v": "b"}]), key=key
    )
    assert retry["status"] == "applied"
    assert _count(engine, table) == 2
    assert _wm_lsn(engine, table, key) == "0/200"


@pytest.mark.parametrize("engine", sorted(ENGINES))
def test_live_bundle_crash_rolls_back_every_stream(engine: str) -> None:
    """MySQL commits implicitly on DDL: schema work must stay out of the apply txn."""
    _require(engine)
    t1, t2 = "eos_t_bundle_a", "eos_t_bundle_b"
    key = "t-bundle|" + uuid.uuid4().hex[:8]
    _drop(engine, [t1, t2])

    def streams(lsn: str) -> list[EosBundleStream]:
        return [
            EosBundleStream(
                dest_table=t,
                change=_batch(lsn, inserts=[{"id": f"{n}-{lsn}", "v": t}]),
                mappings=MAPPINGS,
                column_types=TYPES,
                pk_target_cols=PK,
                stream_key=f"{key}|{t}",
                headers=HEADERS,
            )
            for n, t in enumerate((t1, t2), start=1)
        ]

    apply_eos_bundle(
        dest_type=engine,
        dest_cfg=dict(ENGINES[engine]),
        streams=streams("0/100"),
        incoming_lsn="0/100",
        bundle_key=key,
    )
    assert (_count(engine, t1), _count(engine, t2)) == (1, 1)

    with pytest.raises(EosCrash):
        apply_eos_bundle(
            dest_type=engine,
            dest_cfg=dict(ENGINES[engine]),
            streams=streams("0/200"),
            incoming_lsn="0/200",
            bundle_key=key,
            crash_after="after_apply_before_watermark",
        )
    assert (_count(engine, t1), _count(engine, t2)) == (1, 1)

    apply_eos_bundle(
        dest_type=engine,
        dest_cfg=dict(ENGINES[engine]),
        streams=streams("0/200"),
        incoming_lsn="0/200",
        bundle_key=key,
    )
    assert (_count(engine, t1), _count(engine, t2)) == (2, 2)
