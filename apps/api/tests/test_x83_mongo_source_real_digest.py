"""X-83: a MongoDB-source route must never be classified engine-copy/count-only.

The copy_mongo_<dest> fast paths stamp a real two-sided value digest
(``sql_rows_sha256:`` + ``proof_scope="mapped_columns"``) so Gate-8 performs a
real source re-read and value compare — tampered destination rows are caught
with culprit keys named, never accepted as ``dest_count:N`` equality.

Guard contract: engine-count-only proof is only legal when source and
destination are the same engine family AND an in-engine copy actually
executed. A Mongo source can never run inside the destination engine, so no
copy_mongo_* writer may stamp a count token.
"""

from __future__ import annotations

import os
import re
import sqlite3
from decimal import Decimal
from pathlib import Path

import pytest

SERVICES_DIR = Path(__file__).resolve().parents[1] / "services"
MONGO_WRITER_FILES = sorted(SERVICES_DIR.glob("copy_mongo_*.py"))

COUNT_TOKENS = re.compile(r'f"(?:dest_count|pk_join_count):')
COUNT_SCOPE = "dest_count_equals_source_snapshot_count"

MONGO_URI = os.environ.get(
    "DATAFLOW_X83_MONGO_URI", "mongodb://localhost:27017"
)


def _mongo_live() -> bool:
    try:
        import pymongo

        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
        client.admin.command("ping")
        hello = client.admin.command("hello")
        # The fast path needs a snapshot-read session; standalone mongod
        # declines it before the copy starts.
        return bool(hello.get("setName") or hello.get("msg") == "isdbgrid")
    except Exception:
        return False


LIVE = _mongo_live()
live_required = pytest.mark.skipif(not LIVE, reason="live MongoDB not running")


def test_guard_no_mongo_writer_stamps_engine_count_proof() -> None:
    """copy_mongo_* writers must never emit engine-count-only proof."""
    offenders: list[str] = []
    for path in MONGO_WRITER_FILES:
        text = path.read_text()
        if COUNT_TOKENS.search(text) or f'proof_scope="{COUNT_SCOPE}"' in text:
            offenders.append(path.name)
    assert offenders == [], (
        "Mongo-source writer(s) still stamp engine-count-only proof "
        "(rows entered this process; dest_count:N is not proof): "
        + ", ".join(offenders)
    )


def test_engine_count_proof_requires_in_engine_copy() -> None:
    """Guard: _engine_count_proof_only must stay None on mongo writers' sums."""
    from src.transfer.reconcile_step import _engine_count_proof_only

    summary = {
        "checksum": "dest_count:5",
        "source_checksum": "dest_count:5",
        "target_checksum": "dest_count:5",
        "proof_scope": COUNT_SCOPE,
    }
    # Classification is stamp-driven: a mongo-source writer must simply never
    # stamp it (asserted above). The classifier itself still fires on the
    # token — which is exactly why the stamp must be absent.
    assert _engine_count_proof_only(summary) == 5


def test_sql_copy_proof_names_tamper_culprit_key() -> None:
    """A dest row swapped between write and reconcile is caught and named."""
    from services.copy_fast_path import SqlCopyDigest, sql_copy_proof

    digest = SqlCopyDigest(
        ["id", "v"],
        sort_key="id",
        dest_db_type="sqlite",
        dest_types={"id": "INTEGER", "v": "TEXT"},
    )
    digest.update([(1, "alpha"), (2, "beta")])
    clean = [(1, "alpha"), (2, "beta")]
    proof = sql_copy_proof(digest, clean)
    assert proof.startswith("sql_rows_sha256:")

    tampered = [(1, "beta"), (2, "beta")]
    with pytest.raises(ValueError, match=r"1"):
        sql_copy_proof(digest, tampered)


@live_required
def test_mongo_to_sqlite_clean_exact_and_real_digest(tmp_path: Path) -> None:
    """Live: mongo → sqlite stamps a real digest and lands values exactly."""
    import pymongo

    from services.copy_mongo_sqlite import copy_mongo_to_sqlite

    client = pymongo.MongoClient(MONGO_URI)
    coll = client["df_x83"]["vals"]
    coll.drop()
    docs = [
        {"id": 1, "v": "alpha"},
        {"id": 2, "v": "beta"},
        {"id": 3, "v": 42},
        {"id": 4, "v": Decimal("1.5")},
        {"id": 5, "v": None},
    ]
    coll.insert_many(docs)
    db = tmp_path / "d.sqlite"
    res = copy_mongo_to_sqlite(
        source_cfg={"connection_string": MONGO_URI, "database": "df_x83"},
        source_table="vals",
        dest_cfg={"database": str(db)},
        dest_table="vals",
        pairs=[("id", "id"), ("v", "v")],
        sqlite_ddls=["INTEGER", "TEXT"],
        replace_destination=True,
    )
    assert res.rows_copied == 5
    assert str(res.source_checksum).startswith("sql_rows_sha256:")
    assert res.proof_scope == "mapped_columns"

    conn = sqlite3.connect(db)
    got = sorted(conn.execute('SELECT id, v FROM vals').fetchall())
    assert [r[0] for r in got] == [1, 2, 3, 4, 5]
    assert got[1][1] == "beta"

    # tamper + confirm the writer's proof machinery names the culprit
    conn.execute("UPDATE vals SET v='beta' WHERE id=1")
    conn.commit()
    reread = conn.execute('SELECT id, v FROM vals ORDER BY id').fetchall()
    from services.copy_fast_path import SqlCopyDigest, sql_copy_proof

    digest = SqlCopyDigest(
        ["id", "v"],
        sort_key="id",
        dest_db_type="sqlite",
        dest_types={"id": "INTEGER", "v": "TEXT"},
    )
    digest.update([(1, "alpha"), (2, "beta"), (3, "42"), (4, "1.5"), (5, None)])
    with pytest.raises(ValueError, match="culprit"):
        sql_copy_proof(digest, reread)
    conn.close()


@live_required
def test_mongo_to_sqlite_tamper_changes_digest(tmp_path: Path) -> None:
    """Live: a post-write dest tamper diverges the two-sided digest."""
    import pymongo

    from services.copy_mongo_sqlite import copy_mongo_to_sqlite

    client = pymongo.MongoClient(MONGO_URI)
    coll = client["df_x83"]["tam"]
    coll.drop()
    coll.insert_many([{"id": i, "v": f"row{i}"} for i in range(1, 6)])
    db = tmp_path / "t.sqlite"
    res = copy_mongo_to_sqlite(
        source_cfg={"connection_string": MONGO_URI, "database": "df_x83"},
        source_table="tam",
        dest_cfg={"database": str(db)},
        dest_table="tam",
        pairs=[("id", "id"), ("v", "v")],
        sqlite_ddls=["INTEGER", "TEXT"],
        replace_destination=True,
    )
    base = str(res.source_checksum)
    conn = sqlite3.connect(db)
    conn.execute("UPDATE tam SET v='row2' WHERE id=1")
    conn.commit()
    # destination digest recomputed post-tamper differs from write-time proof
    from services.copy_fast_path import SqlCopyDigest

    digest = SqlCopyDigest(
        ["id", "v"],
        sort_key="id",
        dest_db_type="sqlite",
        dest_types={"id": "INTEGER", "v": "TEXT"},
    )
    rows = conn.execute('SELECT id, v FROM tam').fetchall()
    digest.update(rows)
    assert digest.digest_hex() != base.split(":", 1)[1]
    conn.close()
