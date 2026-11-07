"""Brand leaks QA saw on screen (production 557f556c), checked through the
routes that render them.

* Validate's upsert-key blocker read "DataFlow would run: CREATE UNIQUE INDEX…"
  — run here through the real preflight against a live PostgreSQL destination
  (an engine whose upsert needs a unique index, so the DDL is disclosed).
* The MCP page's config snippet key is covered by the web test
  ``apps/web/src/lib/mcpClientConfig.brand.test.ts``.
* "admin@dataflow.app" in MCP callers is the deployment's bootstrap admin
  identity (DATAFLOW_ADMIN_EMAIL on the hosted service), not a repository
  seed — see ``test_no_default_seed_carries_the_old_brand_email``.
"""

from __future__ import annotations

import json
import re
import uuid
from pathlib import Path

import pytest

psycopg2 = pytest.importorskip("psycopg2")
REPO = Path(__file__).resolve().parents[3]


@pytest.fixture
def pg_dest():
    from conftest import pg_test_cfg

    cfg = pg_test_cfg()
    try:
        conn = psycopg2.connect(
            host=cfg["host"], port=cfg["port"], dbname=cfg["database"],
            user=cfg["username"], password=cfg["password"], connect_timeout=3,
        )
    except psycopg2.OperationalError as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    conn.autocommit = True
    table = f"r7_brand_{uuid.uuid4().hex[:8]}"
    with conn.cursor() as cur:
        # Existing table, no unique key, no duplicates; unicode/NULL/blank values.
        cur.execute(f"CREATE TABLE {table} (id INTEGER, v TEXT)")
        cur.executemany(
            f"INSERT INTO {table} VALUES (%s, %s)",
            [(1, "Zoë"), (2, None), (3, ""), (4, "  ")],
        )
    try:
        yield {"type": "postgresql", **cfg}, table
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
        conn.close()


def test_validate_upsert_key_blocker_names_datawrap(pg_dest):
    from services.preflight_service import run_file_preflight

    cfg, table = pg_dest
    res = run_file_preflight(
        columns=["id", "v"],
        column_types={"id": "integer", "v": "string"},
        row_count=None,
        mappings=[{"source": "id", "target": "id"}, {"source": "v", "target": "v"}],
        destination_connected=True,
        source_kind="database",
        source_format="sqlite",
        source_table="src_t",
        destination_table=table,
        destination_table_exists=True,
        destination_db_type="postgresql",
        destination_config=cfg,
        sync_mode="incremental_upsert",
        stream_contracts=[{"selected": True, "primary_key": "id", "cursor_column": "v"}],
    )
    gate = next(g for g in res["gates"] if g.get("id") == "g_upsert_dest_key")
    assert gate["status"] == "block", gate
    assert "Datawrap would run: CREATE UNIQUE INDEX" in gate["message"]
    text = json.dumps(res, default=str)
    assert not re.search(r"DataFlow|Dataflow", text), re.findall(r".{60}Data[Ff]low.{40}", text)[:3]


def test_no_default_seed_carries_the_old_brand_email():
    """No seed, default config or deploy file ships admin@dataflow.app."""
    hits = []
    for top in ("apps/api/services", "apps/api/src", "apps/api/connectors", "deploy",
                "scripts", "docker-compose.yml", "docker-compose.prod.yml", "railway.toml",
                "apps/web/src"):
        root = REPO / top
        paths = [root] if root.is_file() else list(root.rglob("*")) if root.exists() else []
        for p in paths:
            if not p.is_file() or p.suffix in {".pyc", ".png", ".jpg", ".woff2"} or ".test." in p.name:
                continue
            try:
                if "@dataflow.app" in p.read_text(encoding="utf-8", errors="ignore"):
                    hits.append(str(p.relative_to(REPO)))
            except OSError:
                continue
    assert not hits, hits
