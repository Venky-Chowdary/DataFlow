"""D430 — a mirror whose zone-bearing timestamps land as ISO text must verify green.

QA (r67-r75): Oracle → MySQL mirror, TSTZ as ``TIMESTAMP(6) WITH TIME ZONE``
landing in MySQL ``varchar(64)`` as ``2024-11-03T05:37:00-05:00``. Every cell
exact, yet "Checksum mismatch (strict) … value changed: 29 keys" (the non-NULL
TSTZ rows); with LONG_TXT omitted the key census crashed on MySQL 1054.
Overwrite/upsert on the same pair were green.

Root causes:
* The mirror is a staged merge. Its Gate-8 source side is the staging pass's
  write-pass digest, taken on a basis that marks a text column carrying a
  source instant (``VARCHAR(64) CARRYING SOURCE_INSTANT``); the apply's
  active-row digest was taken on the plain mapping types, so the same instant
  hashed as an instant on one side and as ``…-05:00`` text on the other.
* ``_generic_key_census`` selected every mapping, omitted columns included.

Oracle is CI-only here. The same code path is driven with a PostgreSQL
``timestamptz`` source read under a non-UTC session zone (``PGTZ``), so the
driver hands back aware datetimes with an offset exactly as the Oracle reader
does after its ``TO_CHAR`` round trip, mapped to ``VARCHAR(64)`` the way the
plan maps an Oracle TSTZ for MySQL.
"""

from __future__ import annotations

import os
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402
from tests import value_fidelity_route as r  # noqa: E402

# Instants across the America/New_York DST fall-back (2024-11-03 06:00Z).
_INSTANTS = [f"2024-11-03 {4 + i // 10:02d}:{(i * 7) % 60:02d}:{i:02d}.{i:06d}+00" for i in range(1, 30)]


@pytest.fixture()
def fake(tmp_path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    store = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: store)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return store


def _endpoint(kind: str, table: str) -> EndpointConfig:
    cfg = r.dest_config(kind)
    return EndpointConfig(
        kind="database", format=kind, table=table, host=cfg["host"], port=cfg["port"],
        database=cfg["database"], username=cfg["username"], password=cfg["password"],
        schema=str(cfg.get("schema") or ""),
    )


@pytest.fixture()
def tables():
    pg = r.pg_conn()
    r.mysql_conn().close()
    src = "d430_" + uuid.uuid4().hex[:8]
    cur = pg.cursor()
    cur.execute(
        f'CREATE TABLE {src} (id BIGINT PRIMARY KEY, tstz TIMESTAMPTZ, note TEXT, "LONG_TXT" TEXT)'
    )
    for i in range(1, 31):
        stamp = None if i == 30 else _INSTANTS[i - 1]
        cur.execute(
            f"INSERT INTO {src} VALUES (%s, %s, %s, %s)",  # nosec B608
            (i, stamp, f"n{i}", "x" * 64),
        )
    try:
        yield src, src + "_out"
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {src}")  # nosec B608
        pg.close()
        r.drop("mysql", src + "_out")


def _request(src: str, dst: str, *, omit: bool) -> TransferRequest:
    mappings = [
        {"source": "id", "target": "id", "target_type": "BIGINT"},
        {"source": "tstz", "target": "tstz", "target_type": "VARCHAR(64)"},
        {"source": "note", "target": "note", "target_type": "TEXT"},
        {"source": "LONG_TXT", "target": "LONG_TXT", "target_type": "TEXT"},
    ]
    if omit:
        mappings[-1] = {"source": "LONG_TXT", "target": "LONG_TXT", "transform": "omit"}
    return TransferRequest(
        source=_endpoint("postgresql", src),
        destination=_endpoint("mysql", dst),
        mappings=mappings,
        sync_mode="full_refresh_mirror",
        stream_contracts=[
            {"name": src, "primary_key": ["id"], "sync_mode": "full_refresh_mirror", "selected": True}
        ],
        skip_preflight=True,
        validation_mode="strict",
    )


def _run(fake, request: TransferRequest):
    job_id = "d430" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(request, job_id)
    return result, (fake.get_job(job_id) or {}).get("reconciliation") or {}


@pytest.mark.parametrize("zone", ["Asia/Kolkata", "America/New_York"])
@pytest.mark.parametrize("omit", [False, True])
def test_exact_mirror_with_instant_text_carrier_is_green(fake, tables, monkeypatch, zone, omit):
    monkeypatch.setenv("PGTZ", zone)
    src, dst = tables
    result, recon = _run(fake, _request(src, dst, omit=omit))
    assert result.success, result.error
    assert recon.get("checksum_match") is True, recon.get("message")
    rows = dict(r.query("mysql", f"SELECT id, tstz FROM `{dst}`"))  # nosec B608
    assert len(rows) == 30 and rows[30] is None
    # Exact: the text carries the session offset and the same instant.
    from datetime import datetime, timezone

    for i in range(1, 30):
        want = datetime.fromisoformat(_INSTANTS[i - 1].replace("+00", "+00:00"))
        got = datetime.fromisoformat(str(rows[i]).replace("Z", "+00:00"))
        assert got.utcoffset() is not None and got.utcoffset().total_seconds() != 0
        assert got.astimezone(timezone.utc) == want, (i, rows[i])


def test_a_wrong_instant_still_fails_and_names_the_key(fake, tables, monkeypatch):
    """AC-3: a genuine difference in the carried instant fails strict Gate-8."""
    import sqlalchemy as sa

    import services.mirror_engine as mirror_mod

    monkeypatch.setenv("PGTZ", "Asia/Kolkata")
    src, dst = tables
    real_apply = mirror_mod.apply_inferred_deletes_via_staging

    def _apply_then_corrupt(conn, target_qualified, *args, **kwargs):
        out = real_apply(conn, target_qualified, *args, **kwargs)
        # One second off on key 7, same text shape: a real wrong write.
        conn.execute(sa.text(
            f"UPDATE {target_qualified} SET tstz = '2024-11-03T10:19:08+05:30' WHERE id = 7"  # nosec B608
        ))
        return out

    monkeypatch.setattr(mirror_mod, "apply_inferred_deletes_via_staging", _apply_then_corrupt)
    result, recon = _run(fake, _request(src, dst, omit=True))
    assert not result.success
    assert recon.get("checksum_match") is False
    assert "7" in [str(k) for k in (recon.get("culprit_keys") or {}).get("changed", [])], recon.get(
        "message"
    )
    # AC-2: the key census never asks either side for the omitted column.
    assert "LONG_TXT" not in str(recon.get("culprit_keys_unavailable_reason") or "")
    assert "Unknown column" not in str(recon.get("message") or "")


def test_key_census_skips_an_omitted_column(tmp_path):
    """AC-2 on the census itself: the omitted column has no destination carrier."""
    import sqlite3

    from src.transfer.reconcile_step import _generic_key_census

    src, dst = tmp_path / "s.sqlite", tmp_path / "d.sqlite"
    for path, extra in ((src, ', "LONG_TXT" TEXT'), (dst, "")):
        conn = sqlite3.connect(str(path))
        with conn:
            conn.execute(f"CREATE TABLE t (id INTEGER PRIMARY KEY, note TEXT{extra})")
            for i in (1, 2, 3):
                vals = (i, f"n{i}", "x") if extra else (i, f"n{i}" if i != 2 else "wrong")
                conn.execute(f"INSERT INTO t VALUES ({', '.join('?' * len(vals))})", vals)
        conn.close()
    changed, missing, extra, reason = _generic_key_census(
        source_endpoint=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        endpoint=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t"),
        dest_summary={"primary_key_columns": ["id"]},
        mappings=[
            {"source": "id", "target": "id"},
            {"source": "note", "target": "note"},
            {"source": "LONG_TXT", "target": "LONG_TXT", "transform": "omit"},
        ],
    )
    assert reason is None, reason
    assert [str(k) for k in changed] == ["2"] and not missing and not extra
