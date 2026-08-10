"""Generator-watermark verification — the defect a row checksum cannot see.

A migration that carries explicit key values leaves the destination's identity
or sequence generator at its pre-migration value. Every row reconciles, and the
first application insert after cutover collides on the primary key. These tests
pin the collision arithmetic, the forward-only repair rule, the fail-closed
answer on engines we cannot read, and the certificate blocker.
"""

from __future__ import annotations

import sqlite3

import pytest

from services.identity_watermark import (
    IdentityWatermark,
    identity_watermark_supported,
    read_identity_watermark,
    repair_identity_watermark,
    verify_identity_watermark,
)
from services.migration_certificate import (
    _identity_blockers,
    physical_state_findings,
)


def test_collides_when_next_value_is_inside_migrated_range():
    assert IdentityWatermark(column="id", next_value=1, max_value=7).collides
    assert IdentityWatermark(column="id", next_value=7, max_value=7).collides
    assert not IdentityWatermark(column="id", next_value=8, max_value=7).collides


def test_unknown_state_never_claims_healthy():
    """Missing numbers are not a pass — they are an unverified answer."""
    unread = IdentityWatermark(column="id", reason="probe failed")
    assert not unread.collides
    assert not unread.available


def test_unsupported_engine_is_reported_not_assumed_fine():
    assert not identity_watermark_supported("snowflake")
    result = verify_identity_watermark(
        "snowflake", {}, table="t", columns=["id"]
    )
    assert result["verified"] is False
    assert result["unverified"] == ["id"]
    assert "snowflake" in result["checked"][0]["reason"]


def test_no_key_columns_verifies_nothing():
    result = verify_identity_watermark("postgresql", {}, table="t", columns=[])
    assert result["checked"] == []
    assert result["verified"] is False


def _sqlite_cfg(tmp_path) -> dict[str, str]:
    db = tmp_path / "idw.db"
    conn = sqlite3.connect(db)
    conn.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)"
    )
    # A migration writes explicit keys; the generator is untouched by design.
    conn.execute("INSERT INTO t (id, name) VALUES (1, 'a'), (2, 'b')")
    conn.execute("UPDATE sqlite_sequence SET seq = 0 WHERE name = 't'")
    conn.commit()
    conn.close()
    return {"type": "sqlite", "database": str(db)}


def test_sqlite_collision_is_detected_and_repaired_forward_only(tmp_path):
    cfg = _sqlite_cfg(tmp_path)
    before = read_identity_watermark("sqlite", cfg, table="t", column="id")
    assert before.available
    assert before.next_value == 1
    assert before.max_value == 2
    assert before.collides

    after = repair_identity_watermark(
        "sqlite", cfg, table="t", watermark=before
    )
    assert after.next_value == 3
    assert after.repaired_to == 3
    assert not after.collides

    # Forward-only: a healthy generator is never rewound or re-stamped.
    reread = read_identity_watermark("sqlite", cfg, table="t", column="id")
    again = repair_identity_watermark("sqlite", cfg, table="t", watermark=reread)
    assert again.next_value == 3
    assert again.repaired_to is None


def test_sqlite_repaired_generator_lets_the_next_insert_succeed(tmp_path):
    cfg = _sqlite_cfg(tmp_path)
    verify_identity_watermark(
        "sqlite", cfg, table="t", columns=["id"], repair=True
    )
    conn = sqlite3.connect(cfg["database"])
    conn.execute("INSERT INTO t (name) VALUES ('next')")
    conn.commit()
    assert conn.execute("SELECT MAX(id) FROM t").fetchone()[0] == 3
    conn.close()


def test_verify_without_repair_reports_the_collision_and_does_not_touch_it(tmp_path):
    cfg = _sqlite_cfg(tmp_path)
    result = verify_identity_watermark(
        "sqlite", cfg, table="t", columns=["id"], repair=False
    )
    assert result["collisions"] == ["id"]
    assert result["repaired"] == []
    assert result["passed"] is False
    conn = sqlite3.connect(cfg["database"])
    seq = conn.execute("SELECT seq FROM sqlite_sequence WHERE name='t'").fetchone()
    conn.close()
    assert seq[0] == 0


def test_non_generated_key_is_unavailable_not_collided(tmp_path):
    db = tmp_path / "plain.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
    conn.execute("INSERT INTO t VALUES (5, 'a')")
    conn.commit()
    conn.close()
    wm = read_identity_watermark(
        "sqlite", {"type": "sqlite", "database": str(db)}, table="t", column="id"
    )
    assert not wm.available
    assert not wm.collides
    assert wm.reason


@pytest.mark.parametrize(
    "recon, expect_blocker",
    [
        ({}, False),
        ({"physical_state": {"identity_watermark": {"collisions": []}}}, False),
        ({"physical_state": {"identity_watermark": {"collisions": ["id"]}}}, True),
    ],
)
def test_certificate_blocks_on_a_colliding_generator(recon, expect_blocker):
    findings = physical_state_findings(recon)
    blockers = _identity_blockers(findings)
    assert bool(blockers) is expect_blocker
    if expect_blocker:
        assert "collide" in blockers[0]


def test_certificate_reports_missing_evidence_rather_than_success():
    findings = physical_state_findings({})
    assert findings["identity_watermark"]["verified"] is False
    assert findings["identity_watermark"]["reason"]


def test_mixed_case_key_is_read_not_folded(tmp_path) -> None:
    """A quoted mixed-case key must not fold into a name the catalog lacks.

    Oracle reported 'column is not a GENERATED AS IDENTITY column' for a live
    identity column purely because the probe upper-cased a quoted "id".
    """
    import sqlite3

    path = str(tmp_path / "mixed.db")
    with sqlite3.connect(path) as conn:
        conn.execute('CREATE TABLE "MixedId" ("Id" INTEGER PRIMARY KEY AUTOINCREMENT)')
        conn.execute('INSERT INTO "MixedId" ("Id") VALUES (7)')
    cfg = {"type": "sqlite", "database": path}

    watermark = read_identity_watermark("sqlite", cfg, table="mixedid", column="id")
    assert watermark.max_value == 7
    assert watermark.available is True
