"""QA round 12, bugs 1 and 2: Rfam ``family`` MySQL/MariaDB -> PostgreSQL, strict.

QA on 8f7d246d: the public Rfam MySQL ``family`` table (4,227 rows) copied to
PostgreSQL with strict checksum validation failed ("no differing cell in 50
sampled rows") and the destination table was dropped. ``clan_membership`` from
the same server passed. ``SELECT * FROM family`` in query mode failed the same
way.

The public server is not reachable from CI, so ``family`` is recreated here
with Rfam's real DDL on the local MySQL (:3306) and MariaDB (:3307) servers and
filled with values of the shapes Rfam holds: ``double(5,2)`` cutoffs, five-place
``double(10,5)`` statistics, ``tinyint(1)`` flags, ``int unsigned``, ``bigint``
counts, multi-line unicode ``longtext``, ``datetime`` and ``timestamp``, NULL,
'' and whitespace. The route is QA's: MCP ``start_transfer`` ->
``/api/v1/copilot/confirm`` -> job, table mode and query mode, then run 2 into
the table DataFlow created. Every cell is compared afterwards.

The run is repeated with ``DATAFLOW_ENGINE_DIGEST`` unset (CI) and set, since
production's setting is not known.
"""

from __future__ import annotations

import math
import os
import random
import uuid
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from tests import r4_route_harness as h

_ROWS = 2400

_FAMILY_DDL = """
CREATE TABLE {t} (
  `rfam_acc` varchar(7) NOT NULL,
  `rfam_id` varchar(40) NOT NULL,
  `auto_wiki` int(10) unsigned NOT NULL,
  `description` varchar(75) DEFAULT NULL,
  `author` tinytext,
  `seed_source` tinytext,
  `gathering_cutoff` double(5,2) DEFAULT NULL,
  `trusted_cutoff` double(5,2) DEFAULT NULL,
  `noise_cutoff` double(5,2) DEFAULT NULL,
  `comment` longtext,
  `previous_id` tinytext,
  `cmbuild` tinytext,
  `cmcalibrate` tinytext,
  `cmsearch` tinytext,
  `num_seed` bigint(20) DEFAULT NULL,
  `num_full` bigint(20) DEFAULT NULL,
  `num_genome_seq` bigint(20) DEFAULT NULL,
  `num_refseq` bigint(20) DEFAULT NULL,
  `type` varchar(50) DEFAULT NULL,
  `structure_source` tinytext,
  `number_of_species` bigint(20) DEFAULT NULL,
  `number_3d_structures` int(11) DEFAULT NULL,
  `num_pseudonokts` int(11) DEFAULT NULL,
  `tax_seed` mediumtext,
  `ecmli_lambda` double(10,5) DEFAULT NULL,
  `ecmli_mu` double(10,5) DEFAULT NULL,
  `ecmli_cal_db` mediumint(9) DEFAULT '0',
  `ecmli_cal_hits` mediumint(9) DEFAULT '0',
  `maxl` int(9) DEFAULT '0',
  `clen` int(9) DEFAULT '0',
  `match_pair_node` tinyint(1) DEFAULT '0',
  `hmm_tau` double(10,5) DEFAULT NULL,
  `hmm_lambda` double(10,5) DEFAULT NULL,
  `created` datetime NOT NULL,
  `updated` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`rfam_acc`),
  UNIQUE KEY `rfam_acc` (`rfam_acc`),
  KEY `rfam_id` (`rfam_id`),
  KEY `fk_family_wikitext1_idx` (`auto_wiki`),
  CONSTRAINT `fk_family_wikitext1` FOREIGN KEY (`auto_wiki`) REFERENCES {w} (`auto_wiki`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

# Rfam's ``family.auto_wiki`` references ``wikitext``; the parent comes along so
# the source carries the same foreign key QA's did.
_WIKI_DDL = """
CREATE TABLE {w} (
  `auto_wiki` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `title` varchar(150) NOT NULL,
  PRIMARY KEY (`auto_wiki`),
  UNIQUE KEY `title` (`title`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""
_WIKI_IDS = [1, 2, 3, 1302, 65535, 16777215, 2147483648, 4294967295]

_COLUMNS = [
    "rfam_acc", "rfam_id", "auto_wiki", "description", "author", "seed_source",
    "gathering_cutoff", "trusted_cutoff", "noise_cutoff", "comment", "previous_id",
    "cmbuild", "cmcalibrate", "cmsearch", "num_seed", "num_full", "num_genome_seq",
    "num_refseq", "type", "structure_source", "number_of_species",
    "number_3d_structures", "num_pseudonokts", "tax_seed", "ecmli_lambda", "ecmli_mu",
    "ecmli_cal_db", "ecmli_cal_hits", "maxl", "clen", "match_pair_node", "hmm_tau",
    "hmm_lambda", "created", "updated",
]

_TEXTS = [
    None, "", "   ", " padded ", "Zoë ✓ 日本語 🚀", "ünïcødé — ‘quotes’",
    "line one\nline two\r\nline three\ttab", "Griffiths-Jones SR; Bateman A",
    "Published; PMID:12345678", "trailing space ",
]
_TYPES = ["Gene; rRNA;", "Gene; tRNA;", "Cis-reg; riboswitch;", "Intron;", "", None]


def _double(rng: random.Random, places: int, hi: float) -> float | None:
    roll = rng.random()
    if roll < 0.08:
        return None
    if roll < 0.20:
        return float(rng.randint(0, int(hi)))  # 38.00 — a whole number in a double(m,2)
    if roll < 0.24:
        return -round(rng.uniform(0, hi), places)
    return round(rng.uniform(0, hi), places)


def _rows(seed: int) -> list[tuple]:
    rng = random.Random(seed)
    base = datetime(2013, 10, 3, 20, 41, 44)
    rows = []
    for i in range(_ROWS):
        acc = f"RF{i:05d}"
        comment = rng.choice(_TEXTS)
        if i % 7 == 0:
            comment = ("The 5S ribosomal RNA (5S rRNA) is a component of the large "
                       "ribosomal subunit — ✓ ünïcødé.\n\n" * rng.randint(1, 30))
        rows.append((
            acc, f"fam_{i}_{rng.randint(0, 9)}", rng.choice(_WIKI_IDS),
            rng.choice(_TEXTS), rng.choice(_TEXTS), rng.choice(_TEXTS),
            _double(rng, 2, 999), _double(rng, 2, 999), _double(rng, 2, 999),
            comment, rng.choice(_TEXTS),
            "cmbuild -F CM SEED", "cmcalibrate --mpi CM",
            "cmsearch --cpu 4 --verbose --nohmmonly -T 30.00 -Z 742849.287494 CM SEQDB",
            rng.choice([None, rng.randint(0, 10**6)]), rng.randint(0, 9 * 10**12),
            rng.randint(0, 10**9), rng.choice([None, 0, rng.randint(1, 10**9)]),
            rng.choice(_TYPES), rng.choice(["Published; PMID:9223489", "", None]),
            rng.randint(0, 10**6), rng.choice([None, 0, rng.randint(1, 50)]),
            rng.choice([None, 0, 3]), rng.choice(_TEXTS),
            _double(rng, 5, 99999), _double(rng, 5, 99999),
            rng.randint(-8388608, 8388607), rng.randint(0, 8388607),
            rng.randint(0, 10**6), rng.randint(0, 10**4), rng.choice([0, 1, None]),
            _double(rng, 5, 99999), _double(rng, 5, 99999),
            base + timedelta(seconds=rng.randint(0, 10**8)),
            base + timedelta(seconds=rng.randint(0, 10**8)),
        ))
    return rows


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


def _utc(conn):
    with conn.cursor() as cur:
        cur.execute("SET time_zone = '+00:00'")
    return conn


_SOURCES = {
    "mysql": lambda: _utc(h.mysql_conn()),
    "mariadb": lambda: _utc(_mariadb_conn()),
}


def _source_cfg(engine: str) -> dict[str, Any]:
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

    payload = {"name": f"r12-{kind}-{role}-{uuid.uuid4().hex[:6]}", "type": kind,
               "role": role, "workspace_id": "", "ssl": False, **cfg}
    return str(create_connector(payload).id)


def _pg_cfg() -> dict[str, Any]:
    h.pg_conn().close()
    return dict(host=h.PG["host"], port=h.PG["port"], database=h.PG["database"],
                username=h.PG["username"], password=h.PG["password"], schema="public")


def _seed(engine: str, table: str, rows: list[tuple]) -> None:
    conn = _SOURCES[engine]()
    try:
        with conn.cursor() as cur:
            wiki = f"`{table}_wiki`"
            cur.execute(_WIKI_DDL.format(w=wiki))
            cur.executemany(f"INSERT INTO {wiki} (auto_wiki, title) VALUES (%s, %s)",
                            [(i, f"Wiki_{i}") for i in _WIKI_IDS])
            cur.execute(_FAMILY_DDL.format(t=f"`{table}`", w=wiki))
            marks = ", ".join(["%s"] * len(_COLUMNS))
            cur.executemany(
                f"INSERT INTO `{table}` ({', '.join(f'`{c}`' for c in _COLUMNS)}) "
                f"VALUES ({marks})", rows)
    finally:
        conn.close()


def _source_sql(engine: str, statement: str) -> None:
    conn = _SOURCES[engine]()
    try:
        with conn.cursor() as cur:
            cur.execute(statement)
    finally:
        conn.close()


def _norm(value: Any) -> Any:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, Decimal):
        value = float(value)
    if isinstance(value, float):
        return round(value, 5)
    if isinstance(value, datetime):
        return value.replace(tzinfo=None) if value.tzinfo is None else \
            value.astimezone(tz=None).replace(tzinfo=None)
    return value


def _source_cells(engine: str, table: str) -> dict[str, tuple]:
    conn = _SOURCES[engine]()
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT {', '.join(f'`{c}`' for c in _COLUMNS)} FROM `{table}`")
            return {r[0]: tuple(_norm(v) for v in r) for r in cur.fetchall()}
    finally:
        conn.close()


def _dest_cells(table: str) -> dict[str, tuple]:
    conn = h.pg_conn()
    try:
        cur = conn.cursor()
        cur.execute("SET TIME ZONE 'UTC'")
        cur.execute(f'SELECT {", ".join(chr(34) + c + chr(34) for c in _COLUMNS)} '
                    f'FROM "{table}"')
        out = {}
        for r in cur.fetchall():
            cells = []
            for c, v in zip(_COLUMNS, r):
                if isinstance(v, datetime) and v.tzinfo is not None:
                    v = v.replace(tzinfo=None) - (v.utcoffset() or timedelta(0))
                cells.append(_norm(v))
            out[r[0]] = tuple(cells)
        return out
    finally:
        conn.close()


def _assert_same(engine: str, src_t: str, dst_t: str) -> None:
    src, dst = _source_cells(engine, src_t), _dest_cells(dst_t)
    assert sorted(dst) == sorted(src)
    bad = [(k, c, src[k][i], dst[k][i]) for k in src for i, c in enumerate(_COLUMNS)
           if src[k][i] != dst[k][i]
           and not (isinstance(src[k][i], float) and isinstance(dst[k][i], float)
                    and math.isclose(src[k][i], dst[k][i]))]
    assert not bad, bad[:10]


def _pg_exists(table: str) -> bool:
    return bool(h.query("postgresql", f"SELECT to_regclass('public.\"{table}\"') IS NOT NULL")[0][0])


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
        "sync_mode": "full_refresh_overwrite", "primary_key": "rfam_acc",
        "validation_mode": "strict",
        # Synthetic counts trip the PII heuristics; QA's run acknowledged them too.
        "pii_acknowledgement": {"approved_by": "qa-lead", "reason": "synthetic Rfam fixture"},
        **args})
    assert ok, {"error": staged}
    assert not h.blockers_of(staged), h.blockers_of(staged)
    assert staged.get("ack_id"), staged
    return staged["ack_id"]


@pytest.mark.parametrize("engine", ["mysql", "mariadb"])
@pytest.mark.parametrize("mode", ["table", "query"])
def test_rfam_family_strict_checksum_run1_and_run2(client, engine, mode):
    token = uuid.uuid4().hex[:8]
    src_t, dst_t = f"r12fam_{token}", f"r12famd_{token}"
    try:
        _seed(engine, src_t, _rows(seed=12))
        src = _connector("mariadb" if engine == "mariadb" else "mysql",
                         _source_cfg(engine), "source")
        dst = _connector("postgresql", _pg_cfg(), "destination")
        source = ({"source_table": src_t} if mode == "table"
                  else {"source_query": f"SELECT * FROM `{src_t}`"})
        args = {"source_connector_id": src, "dest_connector_id": dst,
                "dest_table": dst_t, **source}

        h.confirm(client, _stage(client, args), reason="r12-family")
        _assert_same(engine, src_t, dst_t)

        # Run 2 into the table run 1 created: value -> NULL, NULL -> value,
        # '' -> unicode, whole-number doubles, a flipped flag, a new row.
        _source_sql(engine, f"UPDATE `{src_t}` SET comment = NULL, gathering_cutoff = 38, "
                            f"match_pair_node = 1 - COALESCE(match_pair_node, 0) "
                            f"WHERE rfam_acc = 'RF00001'")
        _source_sql(engine, f"UPDATE `{src_t}` SET description = '東京 é', hmm_tau = 0.5 "
                            f"WHERE rfam_acc = 'RF00002'")
        _source_sql(engine, f"INSERT INTO `{src_t}` (rfam_acc, rfam_id, auto_wiki, "
                            f"description, trusted_cutoff, created) VALUES "
                            f"('RF99999', 'nouveau', 4294967295, '  ', 25.00, "
                            f"'2026-03-29 01:30:15')")
        h.confirm(client, _stage(client, args), reason="r12-family-run2")
        _assert_same(engine, src_t, dst_t)
        assert len(_dest_cells(dst_t)) == _ROWS + 1
    finally:
        _source_sql(engine, f"DROP TABLE IF EXISTS `{src_t}`, `{src_t}_wiki`")
        h.drop_table("postgresql", dst_t)


# ------------------------------------------- a validation failure keeps data


def _skew_one_source_cell(monkeypatch, *, key: str, column: str) -> None:
    """Make the source digest render one cell of one row differently.

    This is the shape QA hit: every row lands, the 50-row key-aligned sample
    is clean (the row is far outside it), and only the whole-population
    digests disagree because one side canonicalized a value differently.
    """
    import src.transfer.stream as stream

    real = stream.row_fingerprints

    def skewed(rows, columns=None, **kwargs):
        out = []
        for row_key, fingerprint in real(rows, columns, **kwargs):
            if f"rfam_acc={key}" in fingerprint:
                fingerprint = fingerprint.replace(f"{column}=", f"{column}=~", 1)
            out.append((row_key, fingerprint))
        return out

    monkeypatch.setattr(stream, "row_fingerprints", skewed)


def _confirm_failed(client, ack_id: str) -> dict:
    resp = client.post("/api/v1/copilot/confirm", json={"ack_id": ack_id, "reason": "r12"})
    assert resp.status_code == 200, resp.text
    job = h.wait_job(str(resp.json().get("job_id") or ""))
    assert str(job.get("status")) == "failed", {
        "status": job.get("status"), "error": job.get("error")}
    return job


@pytest.mark.parametrize("engine,mode", [("mysql", "table"), ("mariadb", "query")])
def test_failed_validation_keeps_destination_and_names_the_column(
    client, monkeypatch, engine, mode,
):
    token = uuid.uuid4().hex[:8]
    src_t, dst_t = f"r12keep_{token}", f"r12keepd_{token}"
    try:
        _seed(engine, src_t, _rows(seed=7)[:1200])
        src = _connector(engine, _source_cfg(engine), "source")
        dst = _connector("postgresql", _pg_cfg(), "destination")
        source = ({"source_table": src_t} if mode == "table"
                  else {"source_query": f"SELECT * FROM `{src_t}`"})
        args = {"source_connector_id": src, "dest_connector_id": dst,
                "dest_table": dst_t, **source}

        with monkeypatch.context() as patch:
            _skew_one_source_cell(patch, key="RF01000", column="ecmli_mu")
            job = _confirm_failed(client, _stage(client, args))

        text = str(job.get("error") or job.get("message") or "")
        # The destination this run created is kept, with every row, and the
        # job says where.
        assert _pg_exists(dst_t), text
        assert len(_dest_cells(dst_t)) == 1200
        assert f"Data kept at public.{dst_t}" in text, text
        assert "was dropped" not in text, text
        # The report names the column the digests disagree on, and does not
        # claim the census could not run or that every column mismatched.
        recon = job.get("reconciliation") or {}
        assert recon.get("digest_differing_columns") == ["ecmli_mu"], recon.get("message")
        assert "differ on column(s) ecmli_mu" in text, text
        assert "requires both sides readable" not in text, text
        assert "L4 localized mismatched columns" not in text, text

        # Re-running into the kept table is safe: the overwrite replaces it.
        h.confirm(client, _stage(client, args), reason="r12-keep-rerun")
        _assert_same(engine, src_t, dst_t)
    finally:
        _source_sql(engine, f"DROP TABLE IF EXISTS `{src_t}`, `{src_t}_wiki`")
        h.drop_table("postgresql", dst_t)
