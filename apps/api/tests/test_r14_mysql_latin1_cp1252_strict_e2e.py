"""QA MySQL D1: strict checksum on ``latin1_swedish_ci`` text holding 0x80-0x9F.

QA (build 8f7d246d): Rfam ``family`` (latin1_swedish_ci, 4,227 rows) copied in
TABLE mode to PostgreSQL and to MySQL failed strict validation and the
destination was dropped; the same table in QUERY mode (``SELECT * FROM
family``) passed. The failing cells hold bytes 0x80-0x9F.

MySQL's ``latin1`` is Windows-1252 with five extra mappings: 0x81, 0x8D, 0x8F,
0x90 and 0x9D (undefined in cp1252) decode to U+0081, U+008D, U+008F, U+0090
and U+009D. Every read path (the copy, the source digest, the destination
digest) must see exactly those characters.

The fixture is built from raw bytes (``UNHEX``) so the test does not depend on
the client's own charset tables: 0x80 (€), 0x92 (’), 0x8F (U+008F), 0x9D
(U+009D), 0x85 (…), 0x96 (–), 0xE9 (é), NULL, '', whitespace. Route: MCP
``start_transfer`` -> ``/api/v1/copilot/confirm`` -> job, table mode and query
mode, -> PostgreSQL and -> MySQL, strict, run 1 and run 2 into the created
table, MySQL and MariaDB sources.
"""

from __future__ import annotations

import os
import uuid
from typing import Any

import pytest

from tests import r4_route_harness as h

# MySQL latin1 = cp1252 + {81, 8D, 8F, 90, 9D} -> U+00xx.
_MYSQL_UNDEFINED = {0x81, 0x8D, 0x8F, 0x90, 0x9D}


def _mysql_latin1(raw: bytes) -> str:
    return "".join(chr(b) if b in _MYSQL_UNDEFINED else bytes([b]).decode("cp1252")
                   for b in raw)


# (key, hex bytes of `note` or None for NULL, hex of `title`)
_ROWS: list[tuple[str, str | None, str]] = [
    ("k01", "80", "45757261"),                       # '€' / 'Eura'
    ("k02", "4974927320666F6F", "92"),               # "It’s foo" / '’'
    ("k03", "8F", "419D42"),                         # U+008F / 'A' U+009D 'B'
    ("k04", "9D", "8F9D8081"),                       # all four, plus 0x81
    ("k05", None, "4E554C4C"),                       # NULL / 'NULL'
    ("k06", "", "656D707479"),                       # '' / 'empty'
    ("k07", "2020", "092009"),                       # spaces / tab space tab
    ("k08", "436166E9", "4AFC7267656E"),             # 'Café' / 'Jürgen'
    ("k09", "8586878889", "8A8B8C8E"),               # …†‡ˆ‰ / Š‹ŒŽ
    ("k10", "9192939495969798", "999A9B9C9E9F"),     # ‘’“”•–—˜ / ™š›œžŸ
    ("k11", "8D90", "84"),                           # U+008D U+0090 / „
    ("k12", "A0FF", "20E92D"),                       # NBSP ÿ / ' é-'
]

_DDL = """
CREATE TABLE {t} (
  `id` varchar(8) NOT NULL,
  `note` text CHARACTER SET latin1 COLLATE latin1_swedish_ci,
  `title` varchar(40) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL,
  `tiny` tinytext CHARACTER SET latin1 COLLATE latin1_swedish_ci,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci
"""


def _expected() -> dict[str, tuple]:
    out = {}
    for key, note, title in _ROWS:
        n = None if note is None else _mysql_latin1(bytes.fromhex(note))
        t = _mysql_latin1(bytes.fromhex(title))
        out[key] = (key, n, t, t)
    return out


# --------------------------------------------------------------- engines


def _mariadb_conn():
    pymysql = pytest.importorskip("pymysql")
    pw = os.environ.get("DATAFLOW_TEST_MARIADB_PASSWORD", "")
    port = int(os.environ.get("DATAFLOW_TEST_MARIADB_PORT", "3307"))
    if not pw or not h.port_up(port):
        pytest.skip("MariaDB not configured (DATAFLOW_TEST_MARIADB_PASSWORD, :3307)")
    return pymysql.connect(
        host=os.environ.get("DATAFLOW_TEST_MARIADB_HOST", "127.0.0.1"), port=port,
        user=os.environ.get("DATAFLOW_TEST_MARIADB_USER", "dataflow"), password=pw,
        database=os.environ.get("DATAFLOW_TEST_MARIADB_DB", "dataflow"),
        charset="utf8mb4", autocommit=True, connect_timeout=5,
    )


_SOURCES = {"mysql": h.mysql_conn, "mariadb": _mariadb_conn}


def _cfg(engine: str) -> dict[str, Any]:
    _SOURCES[engine]().close()  # skips when the engine is absent
    if engine == "mysql":
        return dict(host="127.0.0.1", port=3306,
                    database=os.environ.get("MYSQL_DATABASE", "dataflow"),
                    username=os.environ.get("MYSQL_USER", "dataflow"),
                    password=os.environ["MYSQL_PASSWORD"])
    return dict(host=os.environ.get("DATAFLOW_TEST_MARIADB_HOST", "127.0.0.1"),
                port=int(os.environ.get("DATAFLOW_TEST_MARIADB_PORT", "3307")),
                database=os.environ.get("DATAFLOW_TEST_MARIADB_DB", "dataflow"),
                username=os.environ.get("DATAFLOW_TEST_MARIADB_USER", "dataflow"),
                password=os.environ["DATAFLOW_TEST_MARIADB_PASSWORD"])


def _connector(kind: str, cfg: dict[str, Any], role: str) -> str:
    from services.connector_store import create_connector

    payload = {"name": f"r14-{kind}-{role}-{uuid.uuid4().hex[:6]}", "type": kind,
               "role": role, "workspace_id": "", "ssl": False, **cfg}
    return str(create_connector(payload).id)


def _pg_cfg() -> dict[str, Any]:
    h.pg_conn().close()
    return dict(host=h.PG["host"], port=h.PG["port"], database=h.PG["database"],
                username=h.PG["username"], password=h.PG["password"], schema="public")


def _sql(engine: str, statement: str, args: Any = None) -> list[tuple]:
    conn = _SOURCES[engine]()
    try:
        with conn.cursor() as cur:
            cur.execute(statement, args)
            return list(cur.fetchall() or [])
    finally:
        conn.close()


def _seed(engine: str, table: str) -> None:
    _sql(engine, _DDL.format(t=f"`{table}`"))
    for key, note, title in _ROWS:
        note_sql = "NULL" if note is None else ("''" if note == "" else f"UNHEX('{note}')")
        _sql(engine, f"INSERT INTO `{table}` (id, note, title, tiny) VALUES "
                     f"(%s, {note_sql}, UNHEX('{title}'), UNHEX('{title}'))", (key,))


def _source_cells(engine: str, table: str) -> dict[str, tuple]:
    return {r[0]: tuple(r) for r in
            _sql(engine, f"SELECT id, note, title, tiny FROM `{table}`")}


def _dest_cells(kind: str, table: str) -> dict[str, tuple]:
    q = h.quote(kind, table)
    rows = h.query(kind, f"SELECT {h.quote(kind, 'id')}, {h.quote(kind, 'note')}, "
                         f"{h.quote(kind, 'title')}, {h.quote(kind, 'tiny')} FROM {q}")
    return {r[0]: tuple(r) for r in rows}


# ------------------------------------------------------------- the route


@pytest.fixture(params=["engine_digest_off", "engine_digest_on"])
def client(request, tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("DATAFLOW_TRAINING", "off")
    monkeypatch.delenv("DATAWRAP_ENGINE_DIGEST", raising=False)
    if request.param == "engine_digest_on":
        monkeypatch.setenv("DATAFLOW_ENGINE_DIGEST", "1")
    else:
        monkeypatch.delenv("DATAFLOW_ENGINE_DIGEST", raising=False)
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def _stage(client, args: dict) -> str:
    ok, staged = h.mcp_call(client, "start_transfer", {
        "sync_mode": "full_refresh_overwrite", "primary_key": "id",
        "validation_mode": "strict", **args})
    assert ok, {"error": staged}
    assert not h.blockers_of(staged), h.blockers_of(staged)
    assert staged.get("ack_id"), staged
    return staged["ack_id"]


def test_fixture_bytes_read_back_as_mysql_latin1():
    """The fixture itself: the server decodes the bytes the way the test expects."""
    token = uuid.uuid4().hex[:8]
    t = f"r14l1chk_{token}"
    try:
        _seed("mysql", t)
        assert _source_cells("mysql", t) == _expected()
    finally:
        _sql("mysql", f"DROP TABLE IF EXISTS `{t}`")


@pytest.mark.parametrize("dest", ["postgresql", "mysql"])
@pytest.mark.parametrize("engine", ["mysql", "mariadb"])
@pytest.mark.parametrize("mode", ["table", "query"])
def test_latin1_0x80_0x9f_strict_run1_and_run2(client, engine, mode, dest):
    token = uuid.uuid4().hex[:8]
    src_t, dst_t = f"r14l1_{token}", f"r14l1d_{token}"
    try:
        _seed(engine, src_t)
        src = _connector("mariadb" if engine == "mariadb" else "mysql", _cfg(engine), "source")
        dst = _connector(dest, _pg_cfg() if dest == "postgresql" else _cfg("mysql"),
                         "destination")
        source = ({"source_table": src_t} if mode == "table"
                  else {"source_query": f"SELECT * FROM `{src_t}`"})
        args = {"source_connector_id": src, "dest_connector_id": dst,
                "dest_table": dst_t, **source}

        h.confirm(client, _stage(client, args), reason="r14-latin1")
        assert _dest_cells(dest, dst_t) == _expected()

        # Run 2 into the table run 1 created: NULL -> 0x80, a cell -> NULL, new row.
        _sql(engine, f"UPDATE `{src_t}` SET note = UNHEX('809D') WHERE id = 'k05'")
        _sql(engine, f"UPDATE `{src_t}` SET note = NULL WHERE id = 'k01'")
        _sql(engine, f"INSERT INTO `{src_t}` (id, note, title, tiny) VALUES "
                     f"('k99', UNHEX('8F92'), UNHEX('9D'), '')")
        h.confirm(client, _stage(client, args), reason="r14-latin1-run2")
        assert _dest_cells(dest, dst_t) == _source_cells(engine, src_t)
        assert len(_dest_cells(dest, dst_t)) == len(_ROWS) + 1
    finally:
        _sql(engine, f"DROP TABLE IF EXISTS `{src_t}`")
        h.drop_table(dest, dst_t)


# ------------------------------------------------------------- the owners


@pytest.mark.parametrize("engine", ["mysql", "mariadb"])
def test_mysql_latin1_codec_matches_the_server_for_every_byte(engine):
    """All 256 bytes, decoded by the server and by the one codec, agree."""
    from services.encoding_capacity import MYSQL_LATIN1_CODEC, decode_mysql_latin1

    raw = bytes(range(256))
    server = _sql(engine, "SELECT CONVERT(CAST(UNHEX(%s) AS CHAR CHARACTER SET latin1) "
                          "USING utf8mb4)", (raw.hex(),))[0][0]
    assert decode_mysql_latin1(raw) == server == _mysql_latin1(raw)
    assert server.encode(MYSQL_LATIN1_CODEC) == raw


def test_mysql_latin1_capacity_holds_what_the_server_stores():
    from services.encoding_capacity import bind_unicode_text, cell_fits_capacity, classify_capacity

    cap = classify_capacity("mysql", "VARCHAR(255) COLLATE latin1_swedish_ci")
    for text in ("€", "’", "\u008f", "A\u009dB", "\u0081\u008d\u0090", "Café ÿ"):
        assert cell_fits_capacity(text, cap), text
        assert bind_unicode_text(text, engine="mysql", dest_type="TINYTEXT CHARACTER SET latin1") == text
    for text in ("\u0080", "東京", "Ā"):
        assert not cell_fits_capacity(text, cap), text


def test_a_mysql_collation_never_reaches_postgresql_ddl():
    from services.dest_dialect_facts import _collation_compatible_with_dest
    from services.encoding_capacity import classify_capacity
    from services.type_system import ddl_type

    for coll in ("latin1_swedish_ci", "latin2_general_ci", "cp1250_czech_cs", "ascii_bin",
                 "utf8mb4_0900_ai_ci", "big5_chinese_ci"):
        assert not _collation_compatible_with_dest("postgresql", coll), coll
        assert not _collation_compatible_with_dest("redshift", coll), coll
    for coll in ("en_US.utf8", "und-x-icu", "C.UTF-8", "ucs_basic"):
        assert _collation_compatible_with_dest("postgresql", coll), coll
    assert _collation_compatible_with_dest("mysql", "latin1_swedish_ci")
    assert _collation_compatible_with_dest("sqlserver", "Latin1_General_CI_AS")
    for inferred in ("tinytext COLLATE latin1_swedish_ci", "varchar(40) COLLATE latin1_swedish_ci"):
        assert "COLLATE" not in ddl_type("postgresql", inferred)
    # A PG column's encoding is the database's, never its collation's name.
    assert classify_capacity("postgresql", 'VARCHAR(255) COLLATE "latin1_swedish_ci"').form == "utf8"


def test_c1_controls_block_except_mysql_latin1s_own_five():
    from services.data_integrity import _check_encoding_anomalies, mysql_latin1_exempt_controls

    mojibake = [{"note": "It\u00e2\u0080\u0099s"}, {"note": "esc\u009b[31m"}, {"note": "x\u0080y"}]
    for src, types in (("postgresql", {"note": "text"}), ("mongodb", {"note": "VARCHAR"}),
                       ("csv", {"note": "VARCHAR"}),
                       ("mysql", {"note": "TEXT COLLATE utf8mb4_0900_ai_ci"})):
        exempt = mysql_latin1_exempt_controls(src, types)
        out = _check_encoding_anomalies(mojibake, validation_mode="strict", exempt_controls=exempt)
        assert out["blocks_transfer"] is True, (src, out)
        out = _check_encoding_anomalies([{"note": "A\u009dB"}], validation_mode="strict",
                                        exempt_controls=exempt)
        assert out["blocks_transfer"] is True, (src, "U+009D outside MySQL latin1")
    for types in ({"note": "TEXT COLLATE latin1_swedish_ci"}, {"note": "LONGTEXT"}):
        exempt = mysql_latin1_exempt_controls("mariadb", types)
        ok = _check_encoding_anomalies([{"note": "A\u009dB"}, {"note": "\u008f\u0081\u0090\u008d"}],
                                       validation_mode="strict", exempt_controls=exempt)
        assert ok["blocks_transfer"] is False and ok["warnings"], ok
        # Not one of the five: still blocks even in a latin1 column.
        bad = _check_encoding_anomalies([{"note": "x\u0080y"}], validation_mode="strict",
                                        exempt_controls=exempt)
        assert bad["blocks_transfer"] is True
    c0 = _check_encoding_anomalies([{"note": "bell\u0007"}], validation_mode="strict")
    assert c0["blocks_transfer"] is True, c0


def test_postgres_source_with_c1_mojibake_is_still_blocked_in_strict(client):
    t = f"r14c1pg_{uuid.uuid4().hex[:8]}"
    h.execute("postgresql", f'CREATE TABLE "{t}" (id int PRIMARY KEY, note text)')
    h.execute("postgresql", f"INSERT INTO \"{t}\" VALUES (1, E'It\\u00e2\\u0080\\u0099s'), (2, 'ok')")
    src = _connector("postgresql", _pg_cfg(), "source")
    dst = _connector("mysql", _cfg("mysql"), "destination")
    try:
        ok, out = h.mcp_call(client, "start_transfer", {
            "source_connector_id": src, "dest_connector_id": dst, "source_table": t,
            "dest_table": t, "sync_mode": "full_refresh_overwrite", "primary_key": "id",
            "validation_mode": "strict"})
        assert not ok and ("U+0080" in str(out) or "Invisible" in str(out)
                           or "control" in str(out)), out
    finally:
        h.drop_table("postgresql", t)
        h.drop_table("mysql", t)
