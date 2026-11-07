"""E1-021 / E2-002 — a SUCCESSFUL overwrite keeps the destination's constraints.

QA (build 6a2bae9aaa00): existing MySQL table with PK, UNIQUE, NOT NULL, CHECK,
FOREIGN KEY and AUTO_INCREMENT; overwrite from a PostgreSQL fixture holding an
orphan FK row and a negative amount. 7 rows loaded, 0 rejected, the table was
recreated without any key or constraint.

On 3e258e90f MySQL already empties the table in place (D195) and the engine
refuses the two violators row by row. MariaDB did not: its CHECK refusal is
ER_CONSTRAINT_FAILED (4025), which PyMySQL raises as OperationalError and whose
text matched no row-contract signature, so the whole job failed on the raw
driver error instead of quarantining the one row. This pins both engines: the
DDL is byte-identical (bar the AUTO_INCREMENT counter), the violators are
quarantined naming the column, and the valid rows land — twice.
"""

from __future__ import annotations

import os
import re
import uuid
from decimal import Decimal

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402
from tests.test_property2_golden_path_never_blocked import _FakeMongo  # noqa: E402

_SRC_ROWS = [
    (1, "a@x", "A", 10, 1), (2, "b@x", "B", 20, 1), (3, "c@x", "", 30, 2),
    (4, "d@x", "東京", -5, 1),  # CHECK (amount >= 0)
    (5, "e@x", "E", 50, 999),  # orphan FK
    (6, "f@x", "  ", 60, 2), (7, "g@x", None, 70, 1),
]


class _Mongo(_FakeMongo):
    def update_job_fields(self, job_id, fields):
        self.jobs.setdefault(job_id, {}).update(fields or {})
        return True


def _ddl(cur, table: str) -> str:
    cur.execute(f"SHOW CREATE TABLE `{table}`")
    return re.sub(r"AUTO_INCREMENT=\d+ ", "", cur.fetchone()[1])


@pytest.mark.parametrize("dest_format,port", [("mysql", 3306), ("mariadb", 3307)])
def test_successful_overwrite_keeps_constraints_and_refuses_violators(dest_format, port, monkeypatch):
    try:
        import psycopg2
        import pymysql

        pg = psycopg2.connect(host="localhost", port=5432, database="dataflow", user="dataflow",
                              password="dataflow", connect_timeout=3)
        pg.autocommit = True
        my = pymysql.connect(host="localhost", port=port, user="dataflow", password="dataflow",
                             database="dataflow", autocommit=True, connect_timeout=3)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"engine unavailable: {exc}")
    fake = _Mongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    tag = uuid.uuid4().hex[:8]
    src, dst, par = f"r13_e1021_s_{tag}", f"r13_e1021_d_{tag}", f"r13_e1021_p_{tag}"
    try:
        with pg.cursor() as cur:
            cur.execute(f'CREATE TABLE public."{src}" (id int, email text, name text, amount numeric(10,2), pid int)')
            cur.executemany(f'INSERT INTO public."{src}" VALUES (%s,%s,%s,%s,%s)', _SRC_ROWS)
        with my.cursor() as cur:
            cur.execute(f"CREATE TABLE `{par}` (id INT PRIMARY KEY)")
            cur.execute(f"INSERT INTO `{par}` VALUES (1), (2)")
            cur.execute(
                f"CREATE TABLE `{dst}` (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, "
                "email VARCHAR(50) NOT NULL UNIQUE, name VARCHAR(50), "
                "amount DECIMAL(10,2) NOT NULL CHECK (amount >= 0), pid INT, "
                f"CONSTRAINT fk_{tag} FOREIGN KEY (pid) REFERENCES `{par}`(id))"
            )
            cur.execute(f"INSERT INTO `{dst}` (email, name, amount, pid) VALUES ('old@x', 'OLD', 1, 1)")
            before = _ddl(cur, dst)
        req = TransferRequest(
            source=EndpointConfig(kind="database", format="postgresql", host="localhost", port=5432,
                                  database="dataflow", username="dataflow", password="dataflow",
                                  schema="public", table=src),
            destination=EndpointConfig(kind="database", format=dest_format, host="localhost", port=port,
                                       database="dataflow", username="dataflow", password="dataflow",
                                       schema="dataflow", table=dst),
            mappings=[{"source": c, "target": c} for c in ("id", "email", "name", "amount", "pid")],
            sync_mode="full_refresh_overwrite",
            skip_preflight=True,
            validation_mode="balanced",
        )
        for _run in (1, 2):
            job_id = uuid.uuid4().hex[:24]
            result = UniversalTransferEngine().execute_tracked(req, job_id)
            job = fake.jobs.get(job_id, {})
            assert result.success is True, result.error
            assert job.get("status") == "completed_with_quarantine"
            details = job.get("rejected_details") or []
            assert sorted(int(d.get("row")) for d in details) == [4, 5]
            check = next(d for d in details if int(d.get("row")) == 4)
            assert check.get("column") in {"amount", "*"}
            if dest_format == "mariadb":
                assert check.get("column") == "amount"
            with my.cursor() as cur:
                assert _ddl(cur, dst) == before
                cur.execute(f"SELECT id, email, name, amount, pid FROM `{dst}` ORDER BY id")
                landed = cur.fetchall()
            assert [r[0] for r in landed] == [1, 2, 3, 6, 7]
            assert [r[2] for r in landed] == ["A", "B", "", "  ", None]
            assert landed[0][3] == Decimal("10.00")
    finally:
        with pg.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{src}"')
        with my.cursor() as cur:
            for t in (f"{dst}_df_quarantine", f"{dst}__df_bak", dst, par):
                cur.execute(f"DROP TABLE IF EXISTS `{t}`")
        pg.close()
        my.close()
