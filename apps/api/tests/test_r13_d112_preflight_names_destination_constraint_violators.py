"""D112 — Validate names the rows the destination's own constraints will refuse.

QA (Neon PG -> Neon PG, incremental_upsert strict): a destination CHECK
(amt >= 0) and FOREIGN KEY (pid -> parent) were invisible to preflight —
green gates, then raw CheckViolation / ForeignKeyViolation after Confirm, and
g22 said "No foreign keys were declared" although the destination had one.

On 3e258e90f the CHECK probe (constraint_check) and the FK orphan probe
(constraint_fk) already block at Validate and g22 no longer claims an
undeclared FK. The CHECK refusal still named the 4th source row "row 3"
(the probe's 0-based index); every refusal now counts rows from 1.
"""

from __future__ import annotations

import os
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402
from tests.test_property2_golden_path_never_blocked import _FakeMongo  # noqa: E402


class _Mongo(_FakeMongo):
    def update_job_fields(self, job_id, fields):
        self.jobs.setdefault(job_id, {}).update(fields or {})
        return True


def _ep(table):
    return EndpointConfig(kind="database", format="postgresql", host="localhost", port=5432,
                          database="dataflow", username="dataflow", password="dataflow",
                          schema="public", table=table)


@pytest.mark.parametrize("bad", ["check", "fk"])
def test_validate_names_the_rows_the_destination_refuses(bad, monkeypatch):
    try:
        import psycopg2

        conn = psycopg2.connect(host="localhost", port=5432, database="dataflow", user="dataflow",
                                password="dataflow", connect_timeout=3)
        conn.autocommit = True
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"PostgreSQL unavailable: {exc}")
    fake = _Mongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    tag = uuid.uuid4().hex[:6]
    src, dst, par = f"r13_d112_s_{tag}", f"r13_d112_d_{tag}", f"r13_d112_p_{tag}"
    rows = [(1, 5, 1), (2, 6, 2), (3, 7, 1), (4, -1, 1) if bad == "check" else (4, 8, 999)]
    try:
        with conn.cursor() as cur:
            cur.execute(f'CREATE TABLE public."{par}" (id int PRIMARY KEY)')
            cur.execute(f'INSERT INTO public."{par}" VALUES (1), (2)')
            cur.execute(f'CREATE TABLE public."{src}" (id int PRIMARY KEY, amt numeric(10,2), pid int, '
                        "updated_at timestamp NOT NULL DEFAULT now())")
            cur.executemany(f'INSERT INTO public."{src}" (id, amt, pid) VALUES (%s, %s, %s)', rows)
            cur.execute(f'CREATE TABLE public."{dst}" (id int PRIMARY KEY, '
                        f'amt numeric(10,2) CHECK (amt >= 0), pid int REFERENCES public."{par}"(id), '
                        "updated_at timestamp)")
            cur.execute(f'INSERT INTO public."{dst}" (id, amt, pid) VALUES (1, 1, 1)')
        job_id = uuid.uuid4().hex[:24]
        req = TransferRequest(
            source=_ep(src), destination=_ep(dst),
            mappings=[{"source": c, "target": c, "confidence": 0.99, "approved": True}
                      for c in ("id", "amt", "pid", "updated_at")],
            sync_mode="incremental_deduped", validation_mode="strict", skip_preflight=False,
            stream_contracts=[{"name": src, "primary_key": "id", "cursor_field": "updated_at",
                               "cursor_semantics": "modification_timestamp",
                               "sync_mode": "incremental_deduped", "selected": True}],
        )
        result = UniversalTransferEngine().execute_tracked(req, job_id)
        assert result.success is False
        gates = {g.get("id"): g for g in (fake.jobs[job_id].get("preflight") or {}).get("gates") or []}
        if bad == "check":
            assert gates["constraint_check"]["status"] == "block"
            assert "row 4:" in gates["constraint_check"]["message"]
            assert "'amt': '-1.00'" in gates["constraint_check"]["message"]
        else:
            assert gates["constraint_fk"]["status"] == "block"
            assert "999" in gates["constraint_fk"]["message"]
        assert "No foreign keys were declared" not in str(gates.get("g22_dest_referential_integrity"))
        with conn.cursor() as cur:  # nothing written after the refusal
            cur.execute(f'SELECT id, amt, pid FROM public."{dst}"')
            assert [tuple(r) for r in cur.fetchall()] == [(1, 1, 1)]
    finally:
        with conn.cursor() as cur:
            for t in (dst, src, par):
                cur.execute(f'DROP TABLE IF EXISTS public."{t}" CASCADE')
        conn.close()
