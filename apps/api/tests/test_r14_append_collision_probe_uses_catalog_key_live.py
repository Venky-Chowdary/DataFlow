"""The append key-collision probe keys on the real identity, never a column named ``id``.

Rfam ``clan``: the primary key is ``clan_acc``; ``id`` is a non-unique label.
An incremental append whose delta repeats an ``id`` already at the destination
was refused at Validate ("Destination already stores these keys ... on id"):
``destination_key_collision_probe._identity_pairs`` never received the source
catalog key for append modes and fell back to the name heuristic. The probe
now uses the contract key, else the source catalog primary / unique key, else
no key probe; a genuine primary-key collision is still refused.

Live MySQL -> PostgreSQL through ``execute_tracked``, incremental append.
"""

from __future__ import annotations

import pytest

from tests import r13_mapping_harness as h
from tests.r13_mapping_harness import isolated  # noqa: F401 - fixture

pytestmark = pytest.mark.timeout(300)


def _contract(table: str) -> list[dict]:
    # An API-created append schedule: no key declared on the contract.
    return [{"name": table, "sync_mode": "incremental_append", "primary_key": "",
             "cursor_field": "updated_at", "cursor_semantics": "modification_timestamp",
             "selected": True}]


def _run(fake, table, out):
    return h.execute(
        fake, h.endpoint("mysql", table), h.endpoint("postgresql", out),
        sync_mode="incremental_append", stream_contracts=_contract(table),
    )


def test_append_probe_keys_on_the_catalog_pk_not_a_column_named_id(isolated):  # noqa: F811
    if not h._port_up(3306) or not h._port_up(5432):
        pytest.skip("live MySQL / PostgreSQL not reachable")
    table = f"clan_{h.token()}"
    out = f"{table}_d"
    try:
        h.run_sql("mysql", f"CREATE TABLE {table} (clan_acc VARCHAR(7) NOT NULL PRIMARY KEY, "
                  "id VARCHAR(40) NOT NULL, description VARCHAR(100), updated_at DATETIME(6)) "
                  "DEFAULT CHARSET=utf8mb4")
        first = [("CL00001", "tRNA", "", "2024-01-01"), ("CL00002", "東京 ✓", None, "2024-01-02")]
        h.run_sql("mysql", f"INSERT INTO {table} VALUES (%s, %s, %s, %s)",  # nosec B608
                  params=first)
        assert (r1 := _run(isolated, table, out)).success, r1.error

        # A new clan whose label repeats an existing one: not a collision.
        h.run_sql("mysql", f"INSERT INTO {table} VALUES ('CL00003', 'tRNA', '  ', "  # nosec B608
                  "'2024-02-01')")
        r2 = _run(isolated, table, out)
        assert r2.success, r2.error
        got = h.run_sql("postgresql", f"SELECT clan_acc, id FROM {out} ORDER BY clan_acc")  # nosec B608
        assert got == [("CL00001", "tRNA"), ("CL00002", "東京 ✓"), ("CL00003", "tRNA")]

        # The real key again (a re-emitted row past the watermark): refused at Validate.
        h.run_sql("mysql", f"UPDATE {table} SET description = 'moved', "  # nosec B608
                  "updated_at = '2024-03-01' WHERE clan_acc = 'CL00001'")
        r3 = _run(isolated, table, out)
        assert not r3.success
        assert "clan_acc" in (r3.error or ""), r3.error
        assert len(h.run_sql("postgresql", f"SELECT 1 FROM {out}")) == 3  # nosec B608
    finally:
        h.drop("mysql", table)
        h.drop("postgresql", out)


def test_no_key_append_into_a_destination_unique_key_is_refused_at_validate(isolated):  # noqa: F811
    """No contract or source key; the destination enforces UNIQUE(code) (not its PK)."""
    if not h._port_up(3306) or not h._port_up(5432):
        pytest.skip("live MySQL / PostgreSQL not reachable")
    table = f"uq_{h.token()}"
    out = f"{table}_d"
    try:
        h.run_sql("mysql", f"CREATE TABLE {table} (code VARCHAR(10), note VARCHAR(20), "
                  "updated_at DATETIME(6)) DEFAULT CHARSET=utf8mb4")
        h.run_sql("postgresql", f"CREATE TABLE {out} (code VARCHAR(10) UNIQUE, note VARCHAR(20), "
                  "updated_at TIMESTAMP)",
                  f"INSERT INTO {out} VALUES ('a', 'stored', '2024-01-01')")  # nosec B608
        h.run_sql("mysql", f"INSERT INTO {table} VALUES (%s, %s, %s)",  # nosec B608
                  params=[("a", "again", "2024-02-01"), ("東京", "", "2024-02-02")])
        run = _run(isolated, table, out)
        assert not run.success
        assert "code" in (run.error or "") and "Blocking issues detected" in run.error, run.error
        assert h.run_sql("postgresql", f"SELECT COUNT(*) FROM {out}")[0][0] == 1  # nosec B608
    finally:
        h.drop("mysql", table)
        h.drop("postgresql", out)
