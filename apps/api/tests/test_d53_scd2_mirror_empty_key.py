"""D53 audit - SCD2 and mirror treat '' as a key, not as "no key".

``apply_scd2`` composed keys with ``conflict_key_wire`` (NULL and '' both ``''``)
and then skipped every key whose parts were all empty, so once the upsert
guard stopped refusing '' (D53) the '' row neither landed nor was rejected - it
vanished. '' is a key like any other: it gets its own current version / mirror
row; only NULL / Missing is "no key" and is quarantined with a visible reason.

SQLite source through the engine into SQLite, PostgreSQL and MySQL.
"""

from __future__ import annotations

import uuid

import pytest

from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import TransferRequest
from tests.test_d53_d70_exact_upsert_keys import (  # noqa: F401
    _Sqlite,
    _bump_source,
    _dest,
    _make_source,
    db,
)


def _run(db, source, mode, **contract):
    job_id = "d53s" + uuid.uuid4().hex[:16]
    db.fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(
        TransferRequest(
            source=source.endpoint(source.src),
            destination=db.endpoint(db.dst),
            sync_mode=mode,
            stream_contracts=[{
                "name": source.src, "primary_key": ["k"], "sync_mode": mode,
                "selected": True, **contract,
            }],
            validation_mode="strict",
            skip_preflight=True,
        ),
        job_id,
    )


@pytest.fixture()
def sqlite_source(db, tmp_path):
    (tmp_path / "x").mkdir()
    source = _Sqlite(tmp_path / "x")
    source.fake, source.src = db.fake, db.src
    return source


def test_scd2_keeps_an_empty_key_as_its_own_history(db, sqlite_source):
    keys = ["", "a", "b"]
    _make_source(sqlite_source, keys)
    first = _run(db, sqlite_source, "scd2")
    assert first.success, first.error
    _bump_source(sqlite_source)
    second = _run(db, sqlite_source, "scd2")
    assert second.success, second.error

    rows = db.fetch(f"SELECT k, v, is_current FROM {db.q(db.dst)}")
    assert len(rows) == 6, rows  # three keys x two versions
    current = {k: v for k, v, cur in rows if cur in (1, True)}
    assert current == {"": "v2_", "a": "v2_a", "b": "v2_b"}


def test_mirror_keeps_an_empty_key_and_soft_deletes_it_when_it_disappears(db, sqlite_source):
    keys = ["", "a", "b"]
    _make_source(sqlite_source, keys)
    first = _run(db, sqlite_source, "full_refresh_mirror", delete_mode="soft")
    assert first.success, first.error
    assert set(_dest(db)) == set(keys)

    sqlite_source.execute(
        f"DELETE FROM {sqlite_source.q(sqlite_source.src)} WHERE k = ''"
    )
    second = _run(db, sqlite_source, "full_refresh_mirror", delete_mode="soft")
    assert second.success, second.error
    flags = dict(db.fetch(f"SELECT k, _deleted FROM {db.q(db.dst)}"))
    assert set(flags) == set(keys)
    assert flags[""] in (1, True) and flags["a"] in (0, False) and flags["b"] in (0, False)
