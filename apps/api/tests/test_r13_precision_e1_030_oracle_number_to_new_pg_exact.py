"""E1-030 - Oracle NUMBER keeps every digit in a new PostgreSQL NUMERIC.

QA (earlier list): 3.1415926535897932384626433832795028842 landed as
3.141592653589793 and a ~38-digit value with an exponent became 1e126, while
the job completed with a read-back fingerprint OK.

Not reproducible on 3e258e90f (round 13): the Oracle reader fetches NUMBER as
an exact Decimal (``connectors.oracle_numbers``) and a bare / unconstrained
NUMBER is created as unconstrained ``numeric``. This live e2e guards the
whole route: real Oracle -> real PostgreSQL, preflight on, strict, two runs
into the table DataFlow created, every cell compared as an exact Decimal.
"""

from __future__ import annotations

import os
import socket
import uuid
from decimal import Decimal

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from tests.helpers.live_env import pg_creds, pg_up  # noqa: E402

ORA = dict(
    host="127.0.0.1", port=1521,
    database=os.getenv("DATAFLOW_ORACLE_SERVICE") or "FREEPDB1",
    username=os.getenv("DATAFLOW_ORACLE_USER") or "system",
    password=os.getenv("DATAFLOW_ORACLE_PASSWORD") or "oracle",
)
ROWS = [
    (1, "3.1415926535897932384626433832795028842", "1.2345678901234567890123456789012345678E+125",
     "99999999999.999999", "12345678901234567890123456789012345678", "a"),
    (2, "-0.000000000000000000000000000000000000012345678901234567890123456789",
     "9.9999999999999999999999999999999999999E+125", "-0.000001",
     "-99999999999999999999999999999999999999", None),
    (3, None, None, None, None, " "),
    (4, "0", "1E-130", "0", "0", "Łukasz 山田 🎉"),
]
COLS = ["ID", "PI", "BIGX", "AMT", "N38"]


def _oracle_up() -> bool:
    try:
        with socket.create_connection((ORA["host"], ORA["port"]), timeout=1):
            pass
    except OSError:
        return False
    try:
        import oracledb

        oracledb.connect(user=ORA["username"], password=ORA["password"],
                         dsn=f"{ORA['host']}:{ORA['port']}/{ORA['database']}").close()
        return True
    except Exception:  # noqa: BLE001 - listener up, service not open yet
        return False


@pytest.mark.timeout(180)
def test_oracle_number_lands_exact_in_new_postgres_numeric(monkeypatch):
    if not (_oracle_up() and pg_up()):
        pytest.skip("live Oracle and PostgreSQL required")
    import oracledb
    import psycopg2

    from tests.test_property2_golden_path_never_blocked import _FakeMongo
    import src.transfer.engine as engine_mod
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    class _Mongo(_FakeMongo):
        def update_job_fields(self, *a, **k):
            return None

    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: _Mongo())
    src_t = ("R13E1030_" + uuid.uuid4().hex[:8]).upper()
    dst_t = "r13e1030_" + uuid.uuid4().hex[:8]
    pg = pg_creds()
    oc = oracledb.connect(user=ORA["username"], password=ORA["password"],
                          dsn=f"{ORA['host']}:{ORA['port']}/{ORA['database']}")
    pc = psycopg2.connect(host=pg["host"], port=pg["port"], dbname=pg["database"],
                          user=pg["username"], password=pg["password"])
    pc.autocommit = True
    try:
        cur = oc.cursor()
        cur.execute(f"CREATE TABLE {src_t} (ID NUMBER(10,0) PRIMARY KEY, PI NUMBER, BIGX NUMBER, "
                    "AMT NUMBER(18,6), N38 NUMBER(38,0), NOTE VARCHAR2(50 CHAR))")
        for row in ROWS:
            cur.execute(f"INSERT INTO {src_t} VALUES (:1, TO_NUMBER(:2), TO_NUMBER(:3), TO_NUMBER(:4), "
                        "TO_NUMBER(:5), :6)", list(row))
        oc.commit()
        dec = oc.cursor()
        dec.outputtypehandler = lambda c, m: (
            c.var(Decimal, arraysize=c.arraysize)
            if m.type_code is oracledb.DB_TYPE_NUMBER else None
        )
        dec.execute(f"SELECT {', '.join(COLS)}, NOTE FROM {src_t} ORDER BY ID")
        source = [tuple(r) for r in dec.fetchall()]

        req = TransferRequest(
            source=EndpointConfig(kind="database", format="oracle", schema=ORA["username"].upper(),
                                  table=src_t, **ORA),
            destination=EndpointConfig(kind="database", format="postgresql", host=pg["host"],
                                       port=pg["port"], database=pg["database"], username=pg["username"],
                                       password=pg["password"], schema="public", table=dst_t),
            sync_mode="full_refresh_overwrite", validation_mode="strict",
        )
        for run in (1, 2):
            res = UniversalTransferEngine().execute_tracked(req, uuid.uuid4().hex[:24])
            assert res.success, (run, res.error)
            with pc.cursor() as pcur:
                pcur.execute(f'SELECT "ID", "PI", "BIGX", "AMT", "N38", "NOTE" FROM public."{dst_t}" ORDER BY 1')
                landed = [tuple(r) for r in pcur.fetchall()]
            assert len(landed) == len(source), run
            for s, d in zip(source, landed):
                for name, sv, dv in zip(COLS + ["NOTE"], s, d):
                    if isinstance(sv, Decimal) or isinstance(dv, Decimal):
                        assert (sv is None and dv is None) or Decimal(sv) == Decimal(dv), (run, s[0], name, sv, dv)
                    else:
                        assert sv == dv, (run, s[0], name, sv, dv)
        with pc.cursor() as pcur:
            pcur.execute("SELECT column_name, data_type, numeric_precision FROM information_schema.columns "
                         "WHERE table_name = %s AND column_name IN ('PI', 'BIGX')", (dst_t,))
            assert {r[0]: (r[1], r[2]) for r in pcur.fetchall()} == {
                "PI": ("numeric", None), "BIGX": ("numeric", None)}
    finally:
        try:
            oc.cursor().execute(f"DROP TABLE {src_t} PURGE")
        except Exception:  # noqa: BLE001 - best-effort cleanup
            pass
        with pc.cursor() as pcur:
            pcur.execute(f'DROP TABLE IF EXISTS public."{dst_t}"')
        oc.close()
        pc.close()
