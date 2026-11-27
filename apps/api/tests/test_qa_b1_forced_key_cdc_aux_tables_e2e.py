"""QA batch 1 (job 6b01c15c…): a CDC Execute to MySQL failed with the primary-key
error although the source table has a primary key.

On a server that forces a PRIMARY KEY (Aiven MySQL ``sql_require_primary_key``;
MariaDB ``innodb_force_primary_key``) every table Datawrap creates must declare
one. The keyed data table did; three tables Datawrap creates around it did not:

* the exactly-once CDC apply table compared the key to the column list byte for
  byte, so a key spelled ``id`` beside a column ``Id`` declared no key at all
  (``_ensure_dest_table``) — a keyless apply table, refused by such a server;
* the destination quarantine table ``<table>_df_quarantine`` (written when any
  row of the run is held out) was created keyless;
* the mirror key-staging table was a keyless ``CREATE TABLE ... AS SELECT``.

Live half: a MySQL-family server that forces a key
(``DATAFLOW_TEST_MYSQL_REQUIRE_PK_*``; locally a private MariaDB with
``innodb_force_primary_key=ON``).
"""

from __future__ import annotations

import uuid

import pytest

from connectors.cdc_eos_sa import _resolve_key_spelling
from services.cdc_exactly_once import ExactlyOnceRouteError
from tests.test_d396_require_primary_key_surrogate_e2e import (  # noqa: F401
    _pk,
    _q,
    _require_pk_server,
    _source,
    client,
)


def test_key_spelling_folds_only_where_identifiers_are_case_insensitive():
    assert _resolve_key_spelling("mysql", ["Id", "name"], ["id"], "t") == ["Id"]
    assert _resolve_key_spelling("mssql", ["ORDER_ID"], ["order_id"], "t") == ["ORDER_ID"]
    assert _resolve_key_spelling("postgresql", ["id", "Id"], ["Id"], "t") == ["Id"]
    with pytest.raises(ExactlyOnceRouteError):
        _resolve_key_spelling("postgresql", ["Id", "name"], ["id"], "t")
    with pytest.raises(ExactlyOnceRouteError):
        _resolve_key_spelling("mysql", ["name"], ["id"], "t")


def _engine(cfg):
    import sqlalchemy as sa

    return sa.create_engine(
        f"mysql+pymysql://{cfg['username']}:{cfg['password']}@{cfg['host']}:{cfg['port']}/"
        f"{cfg['database']}"
    )


def test_live_cdc_apply_table_declares_the_key_whatever_its_case():
    import sqlalchemy as sa

    from connectors.cdc_eos_sa import _ensure_dest_table

    cfg = _require_pk_server()
    t = f"qab1_cdc_{uuid.uuid4().hex[:8]}"
    eng = _engine(cfg)
    try:
        with eng.begin() as conn:
            _ensure_dest_table(conn, "mysql", t, ["Id", "name"], ["id"])
        assert _pk(cfg, t) == ["Id"]
    finally:
        with eng.begin() as conn:
            conn.execute(sa.text(f"DROP TABLE IF EXISTS `{t}`"))
        eng.dispose()


def test_live_quarantine_table_is_keyed_on_the_finding_id(monkeypatch):
    from services.dest_quarantine import write_dest_quarantine
    from src.transfer.models import EndpointConfig

    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128")
    cfg = _require_pk_server()
    t = f"qab1_q_{uuid.uuid4().hex[:8]}"
    endpoint = EndpointConfig(
        kind="database", format="mysql", table=t, host=cfg["host"], port=cfg["port"],
        database=cfg["database"], username=cfg["username"], password=cfg["password"],
    )
    try:
        out = write_dest_quarantine(
            endpoint,
            [{"row": 2, "column": "d", "value": "0500-01-01", "reason": "out of range"},
             {"row": 5, "column": "d", "value": "0400-01-01", "reason": "out of range"}],
            job_id="qab1",
        )
        assert out["ok"] and out["rows_written"] == 2, out
        assert _pk(cfg, f"{t}_df_quarantine") == ["_df_qid"]
    finally:
        _q(cfg, f"DROP TABLE IF EXISTS `{t}_df_quarantine`")


def test_live_mirror_stages_its_keys_in_a_keyed_table(client):
    from tests import r4_route_harness as h
    from tests.test_d396_require_primary_key_surrogate_e2e import _stage

    dest = _require_pk_server()
    t = _source(keyed=True)
    try:
        ok, out = _stage(client, t, "mirror", dest, primary_key="id")
        assert ok, out
        h.confirm(client, out["ack_id"], reason="qab1-mirror-1")
        h.execute("postgresql", f'DELETE FROM "{t}" WHERE id = 3')
        ok, out = _stage(client, t, "mirror", dest, primary_key="id")
        assert ok, out
        h.confirm(client, out["ack_id"], reason="qab1-mirror-2")
        rows = _q(dest, f"SELECT id FROM `{t}_d` WHERE COALESCE(_deleted, 0) = 0 ORDER BY id")
        assert rows == [(1,), (2,), (4,)]
        assert not [r for r in _q(dest, "SHOW TABLES") if r[0].startswith("_df_mirrorkeys_")]
    finally:
        h.drop_table("postgresql", t)
        _q(dest, f"DROP TABLE IF EXISTS `{t}_d`")


def test_live_mirror_key_staging_is_keyed_and_absorbs_repeated_keys():
    import sqlalchemy as sa

    from services.mirror_engine import _create_pk_staging

    cfg = _require_pk_server()
    suffix = uuid.uuid4().hex[:8]
    dest, stg = f"qab1_m_{suffix}", f"_df_mirrorkeys_qab1_{suffix}"
    eng = _engine(cfg)
    try:
        with eng.connect() as conn:
            conn.execute(sa.text(f"CREATE TABLE `{dest}` (id INT PRIMARY KEY, v TEXT)"))
            assert _create_pk_staging(conn, f"`{stg}`", f"`{dest}`", "`id`", "mysql") is True
            conn.commit()
            insert = sa.text(
                f"INSERT INTO `{stg}` (`id`) VALUES (:p0) ON DUPLICATE KEY UPDATE `id` = `id`"
            )
            conn.execute(insert, [{"p0": 1}, {"p0": 2}, {"p0": 1}])
            conn.commit()
        assert _pk(cfg, stg) == ["id"]
        assert _q(cfg, f"SELECT id FROM `{stg}` ORDER BY id") == [(1,), (2,)]
    finally:
        _q(cfg, f"DROP TABLE IF EXISTS `{stg}`")
        _q(cfg, f"DROP TABLE IF EXISTS `{dest}`")
        eng.dispose()


def test_staging_stays_a_plain_copy_where_no_key_is_forced():
    class _Conn:
        sql: list[str] = []

        class connection:  # noqa: N801 — DBAPI shape
            @staticmethod
            def cursor():
                class C:
                    def execute(self, q):
                        self.q = q

                    def fetchone(self):
                        return ("10.11.14-MariaDB",) if "VERSION" in self.q else (0,)

                    def close(self):
                        pass

                return C()

        def execute(self, stmt, *a):
            self.sql.append(str(stmt))

    from services.mirror_engine import _create_pk_staging

    conn = _Conn()
    assert _create_pk_staging(conn, "`s`", "`d`", "`id`", "mysql") is False
    assert "PRIMARY KEY" not in conn.sql[-1]
