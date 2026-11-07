"""A whitespace-only MySQL text value lands exactly and compares exactly.

Schedules-agent finding on the latin1 Rfam ``clan`` table: mappings posted
as plain ``{source, target, confidence}`` through ``/api/v1/transfer/execute``
(also REST schedules) failed Gate-8 with "source='' dest=' '" for a
``previous_id`` TINYTEXT of one space. The writer stored ' ' exactly; the
read-back's source side was rewritten by a name-triggered ``trim_id``
(``transform_engine.infer_transform_for_mapping``: any target ending in
``id``), which ``resolve_transform`` inferred for the untyped mapping.

Covered here, live MySQL 8 -> PostgreSQL and -> MySQL, run 1 and run 2:
TINYTEXT, TEXT, VARCHAR and CHAR, each under a PAD SPACE collation
(``latin1_swedish_ci``, ``utf8mb4_bin``) and a NO PAD one
(``utf8mb4_0900_ai_ci``, ``utf8mb4_0900_bin``), id-named and plain columns,
values ' ', '  ', 'x ', ' x', '', NULL. MySQL CHAR strips trailing spaces on
read, so CHAR is compared with what the server returns.
"""

from __future__ import annotations

import uuid

import pytest

from tests import r4_route_harness as h
from tests.test_r14_mysql_latin1_cp1252_strict_e2e import _cfg, _pg_cfg, _sql

_COLLATIONS = {
    "pad": ("latin1", "latin1_swedish_ci"),
    "padbin": ("utf8mb4", "utf8mb4_bin"),
    "nopad": ("utf8mb4", "utf8mb4_0900_ai_ci"),
    "nopadbin": ("utf8mb4", "utf8mb4_0900_bin"),
}
_TYPES = {"tt": "tinytext", "tx": "text", "vc": "varchar(20)", "ch": "char(5)"}
_VALUES = [" ", "  ", "x ", " x", "", None]


def _columns() -> list[tuple[str, str]]:
    out = []
    for tkey, ddl in _TYPES.items():
        for ckey, (cs, coll) in _COLLATIONS.items():
            for suffix in ("_id", "_v"):
                out.append((f"{tkey}_{ckey}{suffix}", f"{ddl} CHARACTER SET {cs} COLLATE {coll}"))
    return out


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


@pytest.mark.parametrize("dest", ["postgresql", "mysql"])
def test_whitespace_only_text_lands_and_compares_exactly(client, dest):
    from services.connector_store import create_connector

    cols = _columns()
    t = f"r14ws_{uuid.uuid4().hex[:8]}"
    _sql("mysql", f"CREATE TABLE `{t}` (pk int PRIMARY KEY, "
                  + ", ".join(f"`{n}` {d}" for n, d in cols) + ") DEFAULT CHARSET=latin1")
    for i, v in enumerate(_VALUES, start=1):
        _sql("mysql", f"INSERT INTO `{t}` VALUES (%s{', %s' * len(cols)})", (i, *([v] * len(cols))))
    src = create_connector({"name": f"ws-src-{t}", "type": "mysql", "role": "source",
                            "workspace_id": "", "ssl": False, **_cfg("mysql")})
    dst = create_connector({"name": f"ws-dst-{t}", "type": dest, "role": "destination",
                            "workspace_id": "", "ssl": False,
                            **(_pg_cfg() if dest == "postgresql" else _cfg("mysql"))})
    dest_t = f"{t}_d"
    names = ["pk", *[n for n, _ in cols]]
    try:
        for run in (1, 2):
            resp = client.post("/api/v1/transfer/execute", json={
                "source": {"kind": "database", "format": "mysql", "connector_id": src.id,
                           "table": t},
                "destination": {"kind": "database", "format": dest, "connector_id": dst.id,
                                "table": dest_t, **({"schema": "public"} if dest == "postgresql" else {})},
                "mappings": [{"source": c, "target": c, "confidence": 0.99} for c in names],
                "sync_mode": "full_refresh_overwrite", "validation_mode": "strict"})
            assert resp.status_code == 200, resp.text
            job = h.wait_job(resp.json()["job_id"])
            assert job.get("status") == "completed", (run, job.get("error"))
            want = {r[0]: tuple(r) for r in _sql("mysql", f"SELECT {', '.join(f'`{c}`' for c in names)} FROM `{t}`")}
            q = ", ".join(h.quote(dest, c) for c in names)
            got = {int(r[0]): (int(r[0]), *[
                # PostgreSQL CHAR(n) pads on read and compares padding-blind —
                # the same value MySQL CHAR returns trimmed.
                (v.rstrip(" ") if isinstance(v, str) and names[i + 1].startswith("ch_")
                 and dest == "postgresql" else v)
                for i, v in enumerate(r[1:])]) for r in h.query(
                    dest, f"SELECT {q} FROM {h.quote(dest, dest_t)}")}
            diff = {k: [(names[i], want[k][i], got.get(k, ())[i:i+1]) for i in range(len(names)) if got.get(k, ())[i:i+1] != (want[k][i],)] for k in want}
            assert got == want, (run, {k: v for k, v in diff.items() if v})
            if run == 1:
                _sql("mysql", f"UPDATE `{t}` SET `tt_pad_id` = ' ', `vc_nopad_id` = '   ' WHERE pk = 5")
    finally:
        _sql("mysql", f"DROP TABLE IF EXISTS `{t}`")
        h.drop_table(dest, dest_t)


def test_an_id_named_text_mapping_is_not_trimmed():
    from services.transform_resolver import resolve_transform

    for col in ("previous_id", "clan_acc_id", "rfamid"):
        m = {"source": col, "target": col, "confidence": 0.99}
        assert resolve_transform(m, column_types={}) == "none"
        assert resolve_transform(m, column_types={col: "TINYTEXT"},
                                 dest_types={col: "VARCHAR(255)"}) == "none"
