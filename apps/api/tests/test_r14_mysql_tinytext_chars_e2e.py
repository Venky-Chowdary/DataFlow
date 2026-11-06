"""QA MySQL D12: latin1 TINYTEXT -> PostgreSQL VARCHAR(255) refused 3 values.

QA (jobs 6ae6a289…, …602): Rfam ``literature_reference`` (latin1) -> PG. The
TINYTEXT columns were created ``VARCHAR(255)`` and three values were refused
as not fitting. A TINYTEXT is 255 *bytes in its own charset*: 255 characters
in latin1 (where '’' and '–' are one byte each) and 63..255 characters in
utf8mb4. Every destination that counts characters (PostgreSQL, MySQL, Oracle
CHAR semantics) therefore needs exactly 255 characters. The refusal came from
the carrier: the source's MySQL collation was copied onto the PG column
(``VARCHAR(255) COLLATE "latin1_swedish_ci"``) and the fit check then read it
as ISO-8859-1, which has no '’'.

Values here are the worst case of each charset: 255 latin1 characters made of
cp1252 punctuation (765 UTF-8 bytes), 85 three-byte and 63 four-byte
characters in utf8mb4 (255 and 252 bytes), plus NULL, '' and whitespace, run 1
and run 2 into the created table, MySQL and MariaDB sources.
"""

from __future__ import annotations

import uuid

import pytest

from tests import r4_route_harness as h
from tests.test_r14_mysql_latin1_cp1252_strict_e2e import (
    _cfg,
    _connector,
    _pg_cfg,
    _sql,
)

_DDL = """
CREATE TABLE {t} (
  `pmid` int NOT NULL,
  `title` tinytext CHARACTER SET latin1 COLLATE latin1_swedish_ci,
  `journal` tinytext CHARACTER SET latin1 COLLATE latin1_swedish_ci,
  `author` tinytext CHARACTER SET utf8mb4,
  PRIMARY KEY (`pmid`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1
"""

_LATIN1_255 = ("’–“”…€" * 43)[:255]          # 255 chars, 765 UTF-8 bytes
_ROWS = [
    (1, _LATIN1_255, "Nucleic Acids Res", "東" * 85),        # 255 utf8mb4 bytes
    (2, "It’s a test – “quoted”", "J. Mol. Biol.", "😀" * 63),  # 252 bytes
    (3, ("é" * 255), "Café", "ü" * 127),
    (4, None, "", "  "),
    (5, "   ", None, None),
]


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("DATAFLOW_TRAINING", "off")
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def _cells(kind: str, table: str, source: bool, engine: str = "mysql") -> dict:
    sql = "SELECT pmid, title, journal, author FROM "
    if source:
        return {r[0]: tuple(r) for r in _sql(engine, sql + f"`{table}`")}
    q = ", ".join(h.quote(kind, c) for c in ("pmid", "title", "journal", "author"))
    return {int(r[0]): (int(r[0]), *r[1:]) for r in
            h.query(kind, f"SELECT {q} FROM {h.quote(kind, table)}")}


@pytest.mark.parametrize("dest", ["postgresql", "mysql"])
@pytest.mark.parametrize("engine", ["mysql", "mariadb"])
def test_tinytext_worst_case_values_fit_run1_and_run2(client, engine, dest):
    token = uuid.uuid4().hex[:8]
    src_t, dst_t = f"r14lr_{token}", f"r14lrd_{token}"
    try:
        _sql(engine, _DDL.format(t=f"`{src_t}`"))
        for row in _ROWS:
            _sql(engine, f"INSERT INTO `{src_t}` VALUES (%s, %s, %s, %s)", row)
        assert _sql(engine, f"SELECT LENGTH(title), CHAR_LENGTH(title) FROM `{src_t}` "
                            "WHERE pmid = 1")[0] == (255, 255)
        src = _connector("mariadb" if engine == "mariadb" else "mysql", _cfg(engine), "source")
        dst = _connector(dest, _pg_cfg() if dest == "postgresql" else _cfg("mysql"),
                         "destination")
        args = {"source_connector_id": src, "dest_connector_id": dst, "dest_table": dst_t,
                "source_table": src_t, "sync_mode": "full_refresh_overwrite",
                "primary_key": "pmid", "validation_mode": "strict"}
        for run in (1, 2):
            ok, staged = h.mcp_call(client, "start_transfer", args)
            assert ok, {"error": staged}
            assert not h.blockers_of(staged), h.blockers_of(staged)
            h.confirm(client, staged["ack_id"], reason=f"r14-d12-run{run}")
            assert _cells(dest, dst_t, False) == _cells(engine, src_t, True, engine)
            if run == 1:
                _sql(engine, f"UPDATE `{src_t}` SET title = %s WHERE pmid = 4",
                     (_LATIN1_255[::-1],))
        if dest == "postgresql":
            coll = h.query(dest, "SELECT collation_name, character_maximum_length FROM "
                                 "information_schema.columns WHERE table_name = "
                                 f"'{dst_t}' AND column_name = 'title'")[0]
            assert coll == (None, 255), coll
    finally:
        _sql(engine, f"DROP TABLE IF EXISTS `{src_t}`")
        h.drop_table(dest, dst_t)


def test_tinytext_is_255_characters_on_every_character_counting_destination():
    from services.type_system import ddl_type, parse_string_carrier_width

    for inferred in ("tinytext", "TINYTEXT CHARACTER SET latin1 COLLATE latin1_swedish_ci",
                     "tinytext COLLATE utf8mb4_0900_ai_ci"):
        assert parse_string_carrier_width(inferred) == 255
        assert ddl_type("postgresql", inferred) == "VARCHAR(255)"
        assert ddl_type("oracle", inferred) == "VARCHAR2(255 CHAR)"
        assert ddl_type("mysql", inferred).startswith("VARCHAR(255)")
