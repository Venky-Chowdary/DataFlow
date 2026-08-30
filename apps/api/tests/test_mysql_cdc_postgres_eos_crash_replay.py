"""Named MySQL CDC → Postgres dest-owned exactly-once crash-replay.

``100%`` is not claimed here. PLATFORM_EXACTLY_ONCE_CLAIMED stays False.
Dest COUNT is PostgreSQL ``COUNT(*)``, never a writer ack.

Skips when Postgres (or MySQL binlog) is unreachable — never green by absence.
"""

from __future__ import annotations

import json
import socket
import sys
import uuid
from pathlib import Path
from typing import Any

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.cdc_eos_sa import sa_dest_engine_count, sa_dest_watermark_view
from connectors.cdc_eos_sql import apply_change_batch_exactly_once, eos_stream_key
from connectors.lsn_guards import lsn_family
from services.cdc_engine import ChangeBatch
from services.cdc_exactly_once import PLATFORM_EXACTLY_ONCE_CLAIMED, EosCrash
from services.cdc_named_eos import (
    NAMED_EOS_DEST,
    NAMED_EOS_LSN_FAMILY,
    NAMED_EOS_ROUTE_ID,
    NAMED_EOS_SOURCE,
    crash_replay_artifact_template,
    is_named_dest_owned_eos_route,
    named_eos_eligibility,
)

ARTIFACT = (
    _API_ROOT / "data" / "proofs" / "mysql_cdc_postgres_eos_crash_replay.json"
)

PG = dict(
    type="postgresql",
    host="127.0.0.1",
    port=5432,
    database="dataflow",
    username="dataflow",
    password="dataflow",
)

MAPPINGS = [
    {"source": "id", "target": "id", "confidence": 1.0},
    {"source": "v", "target": "v", "confidence": 1.0},
]
TYPES = {"id": "string", "v": "string"}
HEADERS = ["id", "v"]
PK = ["id"]


def _reachable(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=1.5):
            return True
    except OSError:
        return False


def _batch(lsn: str, *, inserts=None, updates=None) -> ChangeBatch:
    file_name, _, pos = lsn.rpartition(":")
    return ChangeBatch(
        inserts=list(inserts or []),
        updates=list(updates or []),
        deletes=[],
        resume_token={"file": file_name or "mysql-bin.000001", "pos": int(pos or 0)},
    )


def _require_pg() -> dict[str, Any]:
    if not _reachable(str(PG["host"]), int(PG["port"])):
        pytest.skip("Postgres at 127.0.0.1:5432 unreachable — named EOS dest COUNT unmeasured")
    return dict(PG)


def _apply(table: str, change: ChangeBatch, *, key: str, crash_after: str | None = None):
    cfg = _require_pg()
    return apply_change_batch_exactly_once(
        dest_type=NAMED_EOS_DEST,
        dest_cfg=cfg,
        dest_table=table,
        change=change,
        mappings=MAPPINGS,
        column_types=TYPES,
        headers=HEADERS,
        pk_target_cols=PK,
        cursor_key=key,
        crash_after=crash_after,
    )


def _count(table: str) -> int:
    return sa_dest_engine_count(dict(PG), table, NAMED_EOS_DEST)


def _wm(table: str, key: str) -> str | None:
    stream_key = eos_stream_key(
        dest_type=NAMED_EOS_DEST,
        dest_database=str(PG["database"]),
        dest_object=table,
        cursor_key=key,
        stream_name="",
    )
    return sa_dest_watermark_view(dict(PG), stream_key, NAMED_EOS_DEST).committed_lsn


def _drop(table: str) -> None:
    from connectors.generic_sql import _engine
    from services.engine_pool import release_engine
    from sqlalchemy import text

    eng = _engine(dict(PG))
    try:
        with eng.begin() as conn:
            conn.execute(text(f'DROP TABLE IF EXISTS "{table}"'))
    finally:
        release_engine(eng)


def _write_artifact(payload: dict[str, Any]) -> None:
    ARTIFACT.parent.mkdir(parents=True, exist_ok=True)
    ARTIFACT.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def test_platform_exactly_once_stays_false_on_named_route() -> None:
    assert PLATFORM_EXACTLY_ONCE_CLAIMED is False
    blob = named_eos_eligibility()
    assert blob["named_route"] is True
    assert blob["route_id"] == NAMED_EOS_ROUTE_ID
    assert blob["platform_exactly_once_claimed"] is False
    assert blob["exactly_once_active"] is True
    assert blob["eligible"] is True
    assert blob["lsn_family"] == NAMED_EOS_LSN_FAMILY
    template = crash_replay_artifact_template()
    assert template["exactly_once_active"] is False
    assert template["measured"] is False
    assert template["platform_exactly_once_claimed"] is False
    from services.cdc_named_eos import stamp_named_eos_on_summary

    stamped = stamp_named_eos_on_summary(
        {},
        source_type="mysql",
        dest_type="postgresql",
        sync_mode="cdc",
        eos_operator_requested=False,
    )
    assert stamped["named_eos_route"] is True
    assert stamped["named_eos_route_id"] == NAMED_EOS_ROUTE_ID
    assert stamped["exactly_once_active"] is False
    assert stamped["platform_exactly_once_claimed"] is False
    assert is_named_dest_owned_eos_route(
        source_type="mysql", dest_type="postgresql", sync_mode="cdc"
    )
    assert not is_named_dest_owned_eos_route(
        source_type="mysql", dest_type="mysql", sync_mode="cdc"
    )
    assert not is_named_dest_owned_eos_route(
        source_type="postgresql", dest_type="postgresql", sync_mode="cdc"
    )
    lsn = "mysql-bin.000003:4096"
    assert lsn_family(lsn) == NAMED_EOS_LSN_FAMILY


def test_named_mysql_cdc_postgres_crash_replay_dest_count() -> None:
    """Crash before COMMIT leaves dest unchanged; replay lands once.

    MySQL-shaped binlog LSN (file:pos) applied dest-owned into Postgres.
    Full binlog reader loop is additional when MySQL ROW binlog is up —
    this named route's dest proof is dest COUNT + watermark.
    """
    cfg = _require_pg()
    table = "eos_mysql_cdc_" + uuid.uuid4().hex[:8]
    key = f"{NAMED_EOS_ROUTE_ID}|{table}"
    artifact = crash_replay_artifact_template()
    artifact["dest"] = {
        "host": cfg["host"],
        "port": cfg["port"],
        "database": cfg["database"],
        "table": table,
    }
    try:
        _drop(table)
        first_lsn = "mysql-bin.000001:120"
        rows, _ck, summary, _deleted = _apply(
            table, _batch(first_lsn, inserts=[{"id": "1", "v": "a"}]), key=key
        )
        assert summary.get("eos_status") == "applied"
        assert _count(table) == 1
        assert _wm(table, key)
        artifact["scenarios"].append(
            {
                "name": "clean_apply",
                "dest_count": _count(table),
                "watermark": _wm(table, key),
                "eos_status": summary.get("eos_status"),
                "rows_written": rows,
            }
        )

        crash_lsn = "mysql-bin.000001:240"
        with pytest.raises(EosCrash):
            _apply(
                table,
                _batch(crash_lsn, inserts=[{"id": "2", "v": "b"}]),
                key=key,
                crash_after="after_watermark_before_commit",
            )
        after_crash = _count(table)
        wm_after_crash = _wm(table, key)
        assert after_crash == 1, f"crash leaked rows: dest COUNT={after_crash}"
        artifact["scenarios"].append(
            {
                "name": "crash_before_commit",
                "dest_count": after_crash,
                "watermark": wm_after_crash,
                "contract": "dest COUNT unchanged; watermark not advanced",
            }
        )

        rows2, _ck2, summary2, _d2 = _apply(
            table, _batch(crash_lsn, inserts=[{"id": "2", "v": "b"}]), key=key
        )
        assert summary2.get("eos_status") == "applied"
        assert _count(table) == 2
        artifact["scenarios"].append(
            {
                "name": "retry_after_crash",
                "dest_count": _count(table),
                "watermark": _wm(table, key),
                "eos_status": summary2.get("eos_status"),
                "rows_written": rows2,
            }
        )

        rows3, _ck3, summary3, _d3 = _apply(
            table, _batch(crash_lsn, inserts=[{"id": "2", "v": "b"}]), key=key
        )
        assert summary3.get("eos_status") == "already_committed"
        assert rows3 == 0
        assert _count(table) == 2
        artifact["scenarios"].append(
            {
                "name": "crash_after_commit_replay_noop",
                "dest_count": _count(table),
                "watermark": _wm(table, key),
                "eos_status": summary3.get("eos_status"),
                "rows_written": rows3,
            }
        )

        artifact["measured"] = True
        artifact["exactly_once_active"] = True
        artifact["dest_count"] = _count(table)
        artifact["platform_exactly_once_claimed"] = PLATFORM_EXACTLY_ONCE_CLAIMED
        assert artifact["platform_exactly_once_claimed"] is False
        assert artifact["dest_count"] == 2
        assert artifact["exactly_once_active"] is True
    finally:
        try:
            _drop(table)
        except Exception:
            pass
        _write_artifact(artifact)
    assert ARTIFACT.exists()
    saved = json.loads(ARTIFACT.read_text(encoding="utf-8"))
    assert saved["measured"] is True
    assert saved["platform_exactly_once_claimed"] is False
    assert saved["dest_count"] == 2
