"""Exit rehearsal on live PostgreSQL: the shapes review 3 (H2, M7 and the digest Lows) found failing.

Each honest table must be proven in both extract formats: a nullable jsonb /
array column holding a NULL, '' next to NULL in text, text uniformly full of
; | and tabs, a one-row jsonb table, an empty bytea, a timestamp before year
1000 and top-level jsonb strings. What cannot be proven is named with the
true reason (a JSON null document, a jsonb number a double cannot hold, a text
column the engine wrote as a document), and what preflight still blocks names
the column and its type plainly. Skipped when no PostgreSQL is reachable.
"""

from __future__ import annotations

from tests.exit_background_guard import _no_exit_background_work_outlives_a_test  # noqa: F401

import uuid

import pytest

from tests.test_exit_rehearsal_e2e import _run, world  # noqa: F401

psycopg2 = pytest.importorskip("psycopg2")

_J = '\'{"a":1,"b":[1,2,3],"c":{"d":null,"e":"x"},"f":true}\''

#: table -> (columns, rows): each must be proven, in JSONL and in CSV.
HONEST = {
    "docnull": ("id BIGINT PRIMARY KEY, jb JSONB, ia INT[]", ["1, '{\"a\": 1}', '{1,2}'", "2, NULL, NULL"]),
    "txt": ("id BIGINT PRIMARY KEY, t TEXT", ["1, ''", "2, NULL", "3, 'x'"]),
    "delims": ("id BIGINT PRIMARY KEY, s TEXT, p TEXT, tb TEXT",
               ["g, 'a;b;c;d', 'a|b|c|d', E'a\\tb\\tc' FROM generate_series(1, 50) g --"]),
    "json1": ("id BIGINT PRIMARY KEY, jb JSONB", [f"1, {_J}"]),
    "bytes": ("id BIGINT PRIMARY KEY, b BYTEA", ["1, ''", "2, '\\x01'", "3, NULL"]),
    "ancient": ("id BIGINT PRIMARY KEY, ts TIMESTAMP", ["1, '0001-01-01 00:00:00'", "2, '0999-12-31 10:00:00.5'",
                                                         "3, '2026-01-01 00:00:00'"]),
    "jbstr": ("id BIGINT PRIMARY KEY, jb JSONB", ["1, '\"str\"'", "2, '[]'", "3, '{}'", "4, 'true'", "5, '1.10'"]),
}
#: table -> (columns, rows, what the not_proven reason must say).
NAMED = {
    "jbnulldoc": ("id BIGINT PRIMARY KEY, jb JSONB", ["1, 'null'", "2, '1'"],
                  "jb holds 1 JSON null document(s), which the extract cannot tell from SQL NULL"),
    "jbbig": ("id BIGINT PRIMARY KEY, jb JSONB", ["1, '{\"n\": 12345678901234567890.123}'"],
              "jb holds a JSON number a double cannot hold exactly"),
    "textjson": ("id BIGINT PRIMARY KEY, t TEXT", ["1, '{\"a\": 1,  \"a\": 2}'", "2, '{\"b\":1}'"],
                 "t is text, but the engine read its content as JSON"),
}
#: table -> (columns, rows, the column and types the failure must name).
BLOCKED = {
    "padded": ("id BIGINT PRIMARY KEY, c CHAR(5)", ["1, 'ab'"], "column c (CHAR(5) to VARCHAR)"),
    "net": ("id BIGINT PRIMARY KEY, ip INET", ["1, '10.0.0.1'"], "column ip (INET to VARCHAR)"),
    "mac": ("id BIGINT PRIMARY KEY, m MACADDR", ["1, '08:00:2b:01:02:03'"], "column m (MACADDR to VARCHAR)"),
    "wide": ("id BIGINT PRIMARY KEY, n NUMERIC(50,0)", ["1, 12345678901234567890123456789012345678901234567890"],
             "column n (DECIMAL(50,0) to"),
}


@pytest.fixture()
def pg(world, monkeypatch):  # noqa: F811
    from services import audit_log, schedule_runner
    from tests.test_parity_live_pg import _pg_cfg

    cfg = _pg_cfg()
    try:
        conn = psycopg2.connect(host=cfg["host"], port=cfg["port"], dbname=cfg["database"],
                                user=cfg["username"], password=cfg["password"], connect_timeout=3)
    except psycopg2.Error as exc:
        pytest.skip(f"PostgreSQL not reachable for the live rehearsal test: {type(exc).__name__}")
    conn.autocommit = True
    monkeypatch.setattr(audit_log, "STORE_PATH", world["tmp"] / "audit.jsonl")
    saved = {"name": "pg", "type": "postgresql", "host": cfg["host"], "port": cfg["port"],
             "database": cfg["database"], "username": cfg["username"], "password": cfg["password"],
             "schema": "public"}
    monkeypatch.setattr(schedule_runner, "_resolve_connector_for_schedule", lambda _c, _s: dict(saved))
    created: list[str] = []

    def make(groups: dict) -> dict[str, str]:
        sfx = uuid.uuid4().hex[:8]
        names = {g: f"r3h2_{g}_{sfx}" for g in groups}
        with conn.cursor() as cur:
            for g, spec in groups.items():
                ddl, rows = spec[0], spec[1]
                cur.execute(f"CREATE TABLE {names[g]} ({ddl})")  # nosec B608 - generated test name
                created.append(names[g])
                for r in rows:
                    sql = (f"INSERT INTO {names[g]} SELECT {r}" if " FROM generate_series" in r
                           else f"INSERT INTO {names[g]} VALUES ({r})")
                    cur.execute(sql)  # nosec B608
        return names

    yield make
    with conn.cursor() as cur:
        for name in created:
            cur.execute(f"DROP TABLE IF EXISTS {name}")  # nosec B608
    conn.close()


def _rehearse(names: dict[str, str], fmt: str) -> dict[str, dict]:
    from services.exit_inventory import declare

    system = declare({"name": f"review 3 {fmt}", "source": {"connector_id": "pg"}, "extract_format": fmt,
                      "tables": [{"name": n} for n in names.values()], "exit_window_hours": 24},
                     workspace_id="ws1", actor="owner@example.com")
    out = _run(system)
    by_name = {t["name"]: t for t in out["tables"]}
    return {g: by_name[n] for g, n in names.items()}


@pytest.mark.parametrize("fmt", ["jsonl", "csv"])
def test_h2_honest_postgres_shapes_are_proven(pg, fmt):
    tables = _rehearse(pg(HONEST), fmt)
    wrong = {g: (t["status"], t["reason"]) for g, t in tables.items() if t["status"] != "proven"}
    assert not wrong, wrong
    assert tables["delims"]["rows_extracted"] == 50 and tables["json1"]["rows_extracted"] == 1


@pytest.mark.parametrize("fmt", ["jsonl", "csv"])
def test_m7_and_lows_what_cannot_be_proven_is_named_with_the_true_reason(pg, fmt):
    tables = _rehearse(pg(NAMED), fmt)
    for group, (_ddl, _rows, said) in NAMED.items():
        table = tables[group]
        assert table["status"] == "not_proven" and said in table["reason"], (group, table["status"], table["reason"])
        assert "is json: the driver hands back" not in table["reason"]


def test_h2_a_type_preflight_still_blocks_is_named_plainly(pg):
    tables = _rehearse(pg({g: spec[:2] for g, spec in BLOCKED.items()}), "jsonl")
    for group, (_ddl, _rows, said) in BLOCKED.items():
        table = tables[group]
        assert table["status"] == "failed" and said in table["reason"], (group, table["reason"])
        assert "see the server log" not in table["reason"]
