"""D319: one bad value must not silently turn a whole CSV column into TEXT.

QA (SFTP CSV -> new PostgreSQL, CAST_AND_CONTINUE): row 15 held qty
'NOTANUMBER' and d '2024-13-45'. The job created qty as TEXT for every row
and listed only d as an issue.

Decision (R3, R8): type inference keeps every value, so a column whose
sampled values are not all of one type is written as text - the established
behaviour of pandas, Fivetran and Airbyte file sources, and lossless. What
was wrong is that it was silent. The plan now says so for each such column:
how many values are not of the column's otherwise uniform type, which ones,
and the remedy (remap to that type and accept cast & continue, so the rows
are quarantined instead). An impossible calendar day keeps its date column
(QA browser 3) and still needs the operator's decision, as before.

Route: MCP start_transfer on a real (test) SFTP server -> PostgreSQL. The
operator's other choice is pinned on the engine: qty BIGINT and d DATE under
the quarantine posture land 29 rows, keep both types, and quarantine row 15
listing both issues.
"""

from __future__ import annotations

import uuid

import pytest

import services.config  # noqa: F401 - settings load before the harness blanks SEED_DEMO
from tests._r4_temporal_harness import (  # noqa: F401 - fixtures
    connector,
    pg_exec,
    stage,
    tag,
    transfer_client,
)


def _csv() -> str:
    lines = ["id,qty,d,name"]
    for i in range(1, 31):
        if i == 15:
            lines.append(f"{i},NOTANUMBER,2024-13-45,n{i}")
        else:
            lines.append(f"{i},{i * 10},2024-01-{(i % 28) + 1:02d},n{i}")
    return "\n".join(lines) + "\n"


def _warnings(staged) -> list[dict]:
    out = staged.output or {}
    if not out and staged.error:
        out = {}
    pf = out.get("preflight") or (out.get("plan") or {}).get("preflight") or {}
    return [w for w in (pf.get("warnings") or []) if isinstance(w, dict)]


@pytest.mark.parametrize("mode", ["balanced", "strict"])
def test_plan_discloses_the_column_one_value_turned_into_text(transfer_client, tmp_path, mode):
    pytest.importorskip("paramiko")
    from tests.helpers.live_env import pg_up
    from tests.sftp_test_server import start_sftp_server

    if not pg_up():
        pytest.skip("live PostgreSQL not configured")
    root = tmp_path / "sftp"
    root.mkdir()
    (root / "bad.csv").write_text(_csv(), encoding="utf-8")
    server, runner = start_sftp_server(str(root))
    scfg = server.endpoint_config("/bad.csv")
    src = connector("sftp", "source", type="sftp", host=scfg["host"], port=scfg["port"],
                    username=scfg["username"], password=scfg["password"],
                    database=scfg["database"], host_key=scfg["host_key"])
    dst = connector("postgresql", "destination")
    table = tag("d319")
    try:
        staged = stage(source_connector_id=src, source_table="bad.csv",
                       dest_connector_id=dst, dest_table=table,
                       sync_mode="full_refresh_overwrite", validation_mode=mode)
        mixed = [w for w in _warnings(staged) if w.get("id") == "mixed_type_text"]
        assert len(mixed) == 1, (mixed, staged.error)
        details = mixed[0]["details"]
        assert details["column"] == "qty" and details["dominant_type"] == "INTEGER", details
        assert details["stray_values"] == 1 and details["examples"] == ["NOTANUMBER"], details
        text = mixed[0]["message"]
        assert "qty" in text and "NOTANUMBER" in text and "cast & continue" in text, text
        # The bad date is still the operator's decision, as before.
        assert "d VARCHAR → DATE" in str(staged.error) or "(d)" in str(staged.error), staged.error
    finally:
        runner.stop()
        pg_exec(f"DROP TABLE IF EXISTS {table}")


def test_typed_columns_quarantine_the_row_listing_both_issues(monkeypatch):
    """The operator keeps qty an integer: BIGINT + DATE under quarantine."""
    from tests.helpers.live_env import pg_creds, pg_up

    if not pg_up():
        pytest.skip("live PostgreSQL not configured")
    import psycopg2

    import services.mongodb_service as mongo_mod
    import src.transfer.engine as engine_mod
    from services.mongodb_service import MemoryMongoDBService
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    store = MemoryMongoDBService()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: store)
    monkeypatch.setattr(mongo_mod, "get_mongodb_service", lambda: store)
    p = pg_creds()
    table = f"d319e_{uuid.uuid4().hex[:8]}"
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        source_content=_csv().encode(), source_filename="bad.csv",
        destination=EndpointConfig(kind="database", format="postgresql", host=str(p["host"]),
                                   port=int(p["port"]), database=str(p["database"]),
                                   username=str(p["username"]), password=str(p["password"]),
                                   table=table),
        mappings=[{"source": c, "target": c} for c in ("id", "qty", "d", "name")],
        column_types={"id": "BIGINT", "qty": "BIGINT", "d": "DATE", "name": "TEXT"},
        sync_mode="full_refresh_overwrite", validation_mode="balanced", skip_preflight=True,
    )
    job_id = uuid.uuid4().hex[:24]
    store.update_job_status(job_id, "pending", transfer_request={})
    pg = psycopg2.connect(host=p["host"], port=int(p["port"]), user=p["username"],
                          password=p["password"], dbname=p["database"])
    pg.autocommit = True
    try:
        result = UniversalTransferEngine().execute_tracked(request, job_id)
        assert result.success, result.error
        job = store.get_job(job_id) or {}
        with pg.cursor() as cur:
            cur.execute("SELECT column_name, data_type FROM information_schema.columns "
                        "WHERE table_name = %s", (table,))
            types = dict(cur.fetchall())
            cur.execute(f"SELECT COUNT(*) FROM {table} WHERE id = 15")  # nosec B608
            assert cur.fetchone()[0] == 0
            cur.execute(f"SELECT COUNT(*) FROM {table}")  # nosec B608
            assert cur.fetchone()[0] == 29
        assert types["qty"] == "bigint" and types["d"] == "date", types
        rejected = (job.get("destination_summary") or {}).get("rejected_details") or []
        issues = sorted((int(d.get("row") or 0), str(d.get("column"))) for d in rejected)
        assert issues == [(15, "d"), (15, "qty")], issues
    finally:
        with pg.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
        pg.close()


@pytest.mark.parametrize(
    "values, source_type, expect",
    [
        ([str(i) for i in range(29)] + ["NOTANUMBER"], "VARCHAR", "INTEGER"),
        ([f"{i}.5" for i in range(19)] + ["n/a?"], "VARCHAR", "DECIMAL"),
        # A genuinely mixed code column is text, not an integer column with strays.
        ([str(i) for i in range(6)] + ["A1", "B2", "C3", "D4"], "VARCHAR", None),
        # Plain text, and a column whose source declares its own type.
        (["alpha", "beta", "gamma"], "VARCHAR", None),
        ([str(i) for i in range(29)] + ["NOTANUMBER"], "INTEGER", None),
    ],
)
def test_disclosure_only_for_a_typed_column_with_a_few_strays(values, source_type, expect):
    from services.mixed_type_disclosure import mixed_type_text_warnings

    rows = [{"c": v} for v in values]
    found = mixed_type_text_warnings(
        [{"source": "c", "target": "c", "source_type": source_type, "target_type": "TEXT"}], rows
    )
    assert [w["details"]["dominant_type"] for w in found] == ([expect] if expect else []), found
