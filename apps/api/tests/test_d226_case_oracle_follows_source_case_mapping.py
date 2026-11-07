"""D226 - the transform proof's SQL oracle and a source whose UPPER differs.

QA (prod 087ac4bf): a TimescaleDB source, recipe step ``case upper`` on a
column holding 'ß'. The job wrote 'STRAßE' (the engine's one-to-one Unicode
mapping, ``sql_upper``) and then failed its own transform proof: "recipe
output disagrees with the independent SQL oracle". The Timescale image is
built on musl, whose UPPER maps ß -> ẞ; glibc keeps ß and ICU expands it to
SS. The oracle recomputed UPPER on the source server, so the disagreement was
the server's case table, not the step.

The oracle now probes the source's UPPER/LOWER once per proof. A mode the
server maps differently is reported as an uncovered step ("recipe
re-applied"), never as proven and never as a failed step.
"""

from __future__ import annotations

import contextlib
import os
import socket
import uuid

import pytest

from src.transfer import reconcile_step as rs
from src.transfer.reconcile_step import _recipe_step_sql, _source_unfaithful_case_modes


class _Server:
    def __init__(self, upper, lower):
        self.upper, self.lower = upper, lower

    @contextlib.contextmanager
    def engine(self, _type, _cfg):
        server = self

        class _Conn:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def execute(self, stmt):
                probe = stmt.compile().params["probe"]

                class _R:
                    def one(self_inner):
                        return (server.upper(probe), server.lower(probe))

                return _R()

        class _Eng:
            def connect(self):
                return _Conn()

        yield _Eng()


def _musl_upper(text):
    from services.transform_engine import sql_upper

    return sql_upper(text).replace("ß", "ẞ")


def _patch(monkeypatch, server):
    import services.dest_precount as dp

    monkeypatch.setattr(dp, "warehouse_sql_engine", server.engine)


def test_musl_upper_is_not_a_faithful_oracle_for_upper(monkeypatch):
    from services.transform_engine import sql_lower

    _patch(monkeypatch, _Server(_musl_upper, sql_lower))
    assert _source_unfaithful_case_modes("postgresql", {}, {"upper", "lower"}) == {"upper"}


def test_matching_server_keeps_the_oracle(monkeypatch):
    from services.transform_engine import sql_lower, sql_upper

    _patch(monkeypatch, _Server(sql_upper, sql_lower))
    assert _source_unfaithful_case_modes("postgresql", {}, {"upper", "lower"}) == frozenset()


def test_probe_failure_leaves_the_step_uncovered(monkeypatch):
    import services.dest_precount as dp

    def _boom(*_a, **_k):
        raise RuntimeError("down")

    monkeypatch.setattr(dp, "warehouse_sql_engine", _boom)
    assert _source_unfaithful_case_modes("postgresql", {}, {"upper"}) == {"upper"}


def test_unfaithful_mode_has_no_sql_and_the_other_mode_keeps_it():
    flagged = frozenset({"upper"})
    assert _recipe_step_sql("case", {"mode": "upper"}, '"n"', {}, src_type="postgresql",
                            unfaithful_case_modes=flagged) is None
    assert _recipe_step_sql("case", {"mode": "lower"}, '"n"', {}, src_type="postgresql",
                            unfaithful_case_modes=flagged) == 'LOWER("n")'


# --------------------------------------------------------------------------- #
# Live: an Alpine (musl) TimescaleDB source, the engine route QA ran.
# --------------------------------------------------------------------------- #

_TS_PORT = int(os.environ.get("DATAFLOW_TEST_TIMESCALE_PORT", "5435"))
_TS_PW = os.environ.get("DATAFLOW_TEST_TIMESCALE_PASSWORD", "")


def _ts_up() -> bool:
    if not _TS_PW:
        return False
    with contextlib.suppress(OSError), socket.create_connection(("127.0.0.1", _TS_PORT), 1):
        return True
    return False


def _ts_cfg() -> dict:
    return dict(host="127.0.0.1", port=_TS_PORT,
                database=os.environ.get("DATAFLOW_TEST_TIMESCALE_DB", "dataflow"),
                username=os.environ.get("DATAFLOW_TEST_TIMESCALE_USER", "dataflow"),
                password=_TS_PW)


def _ts_exec(sql: str, rows=None) -> None:
    import psycopg2

    c = _ts_cfg()
    conn = psycopg2.connect(host=c["host"], port=c["port"], dbname=c["database"],
                            user=c["username"], password=c["password"])
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.executemany(sql, rows) if rows else cur.execute(sql)
    conn.close()


@pytest.mark.skipif(not _ts_up(), reason="TimescaleDB not configured (DATAFLOW_TEST_TIMESCALE_*)")
def test_timescale_sharp_s_upper_verifies_and_names_the_uncovered_step(monkeypatch, tmp_path):
    from services.shape_models import ShapeRecipe
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest
    from tests import r4_route_harness as h
    from tests.typed_fidelity_helpers import pg_endpoint

    h.isolate(tmp_path, monkeypatch)
    tok = uuid.uuid4().hex[:6]
    src_t, dst_t = f"d226_src_{tok}", f"d226_dst_{tok}"
    _ts_exec(f"CREATE TABLE {src_t} (id INT NOT NULL, ts TIMESTAMPTZ NOT NULL, name TEXT, "
             "PRIMARY KEY (id, ts))")
    _ts_exec(f"SELECT create_hypertable('{src_t}', 'ts')")
    _ts_exec(f"INSERT INTO {src_t} VALUES (%s, %s, %s)", [
        (1, "2024-01-01T00:00:00Z", "straße"), (2, "2024-01-02T00:00:00Z", "ß"),
        (3, "2024-01-03T00:00:00Z", "abc"), (4, "2024-01-04T00:00:00Z", None)])
    c = _ts_cfg()
    source = EndpointConfig(kind="database", format="timescaledb", schema="public",
                            table=src_t, **c)
    recipe = {"steps": [{"op": "case", "column": "name", "options": {"mode": "upper"}}]}
    request = TransferRequest(
        source=source, destination=pg_endpoint(dst_t),
        mappings=[{"source": x, "target": x, "approved": True, "confidence": 0.99}
                  for x in ("id", "ts", "name")],
        sync_mode="full_refresh_overwrite", validation_mode="strict",
        shape_recipe=recipe, approved_shape_recipe_hash=ShapeRecipe.parse(recipe).recipe_hash,
    )
    try:
        server_upper = rs._source_unfaithful_case_modes("postgresql", {**c, "type": "postgresql"},
                                                        {"upper"})
        result = UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])
        assert result.success, result.error
        proof = (result.reconciliation or {}).get("transform_applied") or {}
        assert proof.get("status") == "verified", proof
        if server_upper:
            assert proof.get("oracle_uncovered_steps") == ["step 1 (case)"], proof
        got = h.query("postgresql", f'SELECT id, name FROM "{dst_t}" ORDER BY id')
        assert [tuple(r) for r in got] == [(1, "STRAßE"), (2, "ß"), (3, "ABC"), (4, None)]
    finally:
        _ts_exec(f"DROP TABLE IF EXISTS {src_t}")
        h.drop_table("postgresql", dst_t)
