"""Live yaml / fixed-width → Postgres, independent dest COUNT.

Catalog tiles existed; the engine said not yet live. This file is the
proof that a 2-row YAML sequence and a self-describing fixed-width file
land with dest COUNT matching the source, on a connection the writer
never used.
"""

from __future__ import annotations

import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.fixed_width_layout import layout_header_line  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402
from tests.helpers.live_env import pg_creds, pg_up  # noqa: E402
from tests.test_yaml_fixed_width_source import FWF_TWO  # noqa: E402

pytestmark = pytest.mark.skipif(not pg_up(), reason="Postgres not authenticated")


def _pg_dest(table: str) -> EndpointConfig:
    creds = pg_creds()
    return EndpointConfig(
        kind="database",
        format="postgresql",
        host=str(creds["host"]),
        port=int(creds["port"]),
        database=str(creds["database"]),
        username=str(creds["username"]),
        password=str(creds["password"]),
        schema="public",
        table=table,
    )


def _pg_count(table: str) -> int:
    import psycopg2

    creds = pg_creds()
    conn = psycopg2.connect(
        host=creds["host"],
        port=creds["port"],
        dbname=creds["database"],
        user=creds["username"],
        password=creds["password"],
        connect_timeout=5,
    )
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_name = %s",
                (table,),
            )
            if int(cur.fetchone()[0]) == 0:
                return 0
            cur.execute(f'SELECT COUNT(*) FROM public."{table}"')
            return int(cur.fetchone()[0])
    finally:
        conn.close()


def _pg_drop(table: str) -> None:
    import psycopg2

    creds = pg_creds()
    conn = psycopg2.connect(
        host=creds["host"],
        port=creds["port"],
        dbname=creds["database"],
        user=creds["username"],
        password=creds["password"],
        connect_timeout=5,
    )
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{table}" CASCADE')
    finally:
        conn.close()


def _pg_amounts(table: str) -> list[str]:
    import psycopg2

    creds = pg_creds()
    conn = psycopg2.connect(
        host=creds["host"],
        port=creds["port"],
        dbname=creds["database"],
        user=creds["username"],
        password=creds["password"],
        connect_timeout=5,
    )
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f'SELECT amount FROM public."{table}" ORDER BY id::text')
            return [str(row[0]) for row in cur.fetchall()]
    finally:
        conn.close()


def _run_file(fmt: str, filename: str, content: bytes, table: str) -> object:
    request = TransferRequest(
        source=EndpointConfig(kind="file", format=fmt),
        destination=_pg_dest(table),
        source_content=content,
        source_filename=filename,
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        mappings=[
            {"source": "id", "target": "id"},
            {"source": "amount", "target": "amount"},
        ],
    )
    return UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:16])


YAML_LIVE = b'- id: "1"\n  amount: "1000.00"\n- id: "2"\n  amount: "2000.50"\n'


def test_yaml_to_postgres_dest_count() -> None:
    table = f"yaml_src_{uuid.uuid4().hex[:10]}"
    _pg_drop(table)
    try:
        result = _run_file("yaml", "ledger.yaml", YAML_LIVE, table)
        assert result.success, getattr(result, "error", result)
        assert _pg_count(table) == 2
        assert _pg_amounts(table) == ["1000.00", "2000.50"]
    finally:
        _pg_drop(table)


def test_fixed_width_to_postgres_dest_count() -> None:
    table = f"fwf_src_{uuid.uuid4().hex[:10]}"
    _pg_drop(table)
    try:
        result = _run_file("fixed_width", "ledger.fwf", FWF_TWO, table)
        assert result.success, getattr(result, "error", result)
        assert _pg_count(table) == 2
        assert _pg_amounts(table) == ["1000.00", "2000.50"]
        assert layout_header_line((("id", 8), ("amount", 16))).startswith("#layout:")
    finally:
        _pg_drop(table)
