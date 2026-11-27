"""D394 / trust audit P0-3: a mirror's Gate-8 destination digest is a real re-read.

The Trust Report files a mirror / SCD2 run whose digests matched as a
``readback`` check. Whether that destination digest came from the destination
or from the write pass was unverified (ENGINE_READINESS D394). A destination
trigger that changes every value the writer stores makes the difference: a
write-pass digest would still match the source, a re-read of the destination
cannot.
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest


def _request(db: Path | None, rows: list[tuple[str, str]], *, destination: EndpointConfig | None = None) -> TransferRequest:
    body = "id,name\n" + "\n".join(f"{a},{b}" for a, b in rows)
    return TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        source_content=body.encode("utf-8"),
        source_filename="snapshot.csv",
        destination=destination or EndpointConfig(
            kind="database", format="sqlite", connection_string=str(db), table="mirror_test",
        ),
        sync_mode="full_refresh_mirror",
        skip_preflight=True,
        validation_mode="strict",
        stream_contracts=[{
            "selected": True, "sync_mode": "full_refresh_mirror", "primary_key": "id", "delete_mode": "soft",
        }],
    )


def test_mirror_gate8_digest_reads_the_destination_not_the_write_pass(tmp_path: Path) -> None:
    db = tmp_path / "mirror.db"
    engine = UniversalTransferEngine()
    first = engine.execute_tracked(_request(db, [("1", "Alice"), ("2", "Bob")]), f"d394a_{os.getpid()}")
    assert first.success, first.error
    recon = first.reconciliation or {}
    assert recon.get("passed") is True, recon
    assert recon.get("source_checksum") and recon.get("source_checksum") == recon.get("target_checksum")

    # Every value the writer stores from now on is changed by the destination itself.
    conn = sqlite3.connect(str(db))
    for event in ("INSERT", "UPDATE"):
        conn.execute(
            f"CREATE TRIGGER tamper_{event.lower()} AFTER {event} ON mirror_test "
            "WHEN NEW.name NOT LIKE '%!' BEGIN "
            "UPDATE mirror_test SET name = NEW.name || '!' WHERE id = NEW.id; END"
        )
    conn.commit()
    conn.close()

    second = engine.execute_tracked(
        _request(db, [("1", "Alice2"), ("2", "Bob"), ("3", "Cy")]), f"d394b_{os.getpid()}"
    )
    stored = dict(sqlite3.connect(str(db)).execute("SELECT id, name FROM mirror_test").fetchall())
    assert stored[1] == "Alice2!" and stored[3] == "Cy!", stored
    recon = second.reconciliation or {}
    # A write-pass digest would equal the source's; the destination re-read does not.
    assert recon.get("passed") is not True, recon
    assert recon.get("source_checksum") != recon.get("target_checksum"), recon
    assert "Checksum mismatch" in str(recon.get("message") or ""), recon


def test_mirror_gate8_digest_reads_a_live_postgres_destination(monkeypatch) -> None:
    """The same proof on PostgreSQL: a BEFORE trigger rewrites what the writer stores."""
    import uuid

    import pytest

    psycopg2 = pytest.importorskip("psycopg2")
    from tests.conftest import pg_test_cfg

    cfg = pg_test_cfg()
    try:
        conn = psycopg2.connect(host=cfg["host"], port=cfg["port"], dbname=cfg["database"],
                                user=cfg["username"], password=cfg["password"], connect_timeout=3)
    except psycopg2.Error as exc:
        pytest.skip(f"PostgreSQL not reachable: {type(exc).__name__}")
    conn.autocommit = True
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128")
    table = f"d394_mirror_{uuid.uuid4().hex[:10]}"
    destination = EndpointConfig(
        kind="database", format="postgresql", host=cfg["host"], port=cfg["port"],
        database=cfg["database"], username=cfg["username"], password=cfg["password"],
        table=table, schema="public",
    )
    engine = UniversalTransferEngine()
    try:
        first = engine.execute_tracked(
            _request(None, [("1", "Alice"), ("2", "Bob")], destination=destination), f"d394pg_a_{os.getpid()}"
        )
        assert first.success, first.error
        recon = first.reconciliation or {}
        assert recon.get("passed") is True, recon
        with conn.cursor() as cur:
            cur.execute(
                f"CREATE FUNCTION {table}_bang() RETURNS trigger LANGUAGE plpgsql AS "  # nosec B608
                "$$ BEGIN NEW.name := NEW.name || '!'; RETURN NEW; END $$"
            )
            cur.execute(
                f"CREATE TRIGGER {table}_t BEFORE INSERT OR UPDATE ON {table} "  # nosec B608
                f"FOR EACH ROW WHEN (NEW.name NOT LIKE '%!') EXECUTE FUNCTION {table}_bang()"
            )
        second = engine.execute_tracked(
            _request(None, [("1", "Alice2"), ("2", "Bob"), ("3", "Cy")], destination=destination),
            f"d394pg_b_{os.getpid()}",
        )
        with conn.cursor() as cur:
            cur.execute(f"SELECT id::text, name FROM {table}")  # nosec B608
            stored = dict(cur.fetchall())
        assert stored["1"] == "Alice2!" and stored["3"] == "Cy!", stored
        recon = second.reconciliation or {}
        assert recon.get("passed") is not True, recon
        assert recon.get("source_checksum") != recon.get("target_checksum"), recon
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608
            cur.execute(f"DROP FUNCTION IF EXISTS {table}_bang()")  # nosec B608
        conn.close()
