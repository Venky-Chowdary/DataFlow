"""Provisional D204 — a CSV with non-ASCII text into a NEW SQL Server table was
blocked on run 1 as a fidelity collapse.

Root cause: the upload route (MCP ``start_dataset_transfer``) ran Map without
binding the source engine. With no engine bound, the file's ``VARCHAR`` was
taken for a code-page column, so the ``NVARCHAR(MAX)`` carrier DataFlow
creates on SQL Server was graded a national-charset *invent*
(``precision_collapse``); every text column then needed a signed Migration
Risk Contract and G4 refused the load. A CSV is UTF-8 text: the national
carrier keeps every scalar it holds.

QA's routes, end to end on a live SQL Server:

* upload: MCP ``start_dataset_transfer`` -> ``confirm_action`` -> job;
* SFTP: MCP ``start_transfer`` (real SFTP test server) -> ``/api/v1/copilot/confirm``
  -> job (did not share the defect; kept as a guard).

Values carry Latin-1 (é, ß), CJK (中文), astral emoji (😀) and math letters,
plus empty and whitespace cells and an all-ASCII column. Run 1 creates the
table; run 2 upserts into the table DataFlow created; every value must
round-trip exactly and the text columns must be national (NVARCHAR).
"""

from __future__ import annotations

import pytest

from tests._r4_temporal_harness import (  # noqa: F401 - fixtures
    MSSQL,
    connector,
    connector_name,
    mcp,
    mssql_exec,
    run,
    tag,
    transfer_client,
    up,
    wait_job,
)

_ROWS = [
    ("1", "café", "Straße", "plain"),
    ("2", "中文", "naïve ✓", "ascii only"),
    ("3", "emoji 😀", "𝔘𝔫𝔦𝔠𝔬𝔡𝔢", "x"),
    ("4", "", "  ", "y"),
    ("5", "Grüße é ß 中文 😀", "end", "z"),
]
_RUN2 = [
    ("1", "café updated ✓", "Straße", "plain"),
    *_ROWS[1:],
    ("6", "ünïcödé 中文 😀", "Ærø", "w"),
    ("7", "plain", "ascii", "v"),
]


def _csv(rows) -> str:
    return "id,name,city,note\n" + "".join(",".join(r) + "\n" for r in rows)


@pytest.fixture
def mssql_up():
    pytest.importorskip("pyodbc")
    if not (MSSQL["password"] and up(MSSQL["host"], MSSQL["port"])):
        pytest.skip("SQL Server not reachable")


def _image(table: str) -> dict[int, tuple]:
    rows = mssql_exec(f"SELECT id, name, city, note FROM dbo.[{table}]")  # nosec B608
    return {int(r[0]): tuple(r[1:]) for r in rows}


def _assert_round_trip(table: str, rows) -> None:
    got = _image(table)
    want = {int(r[0]): tuple(r[1:]) for r in rows}
    assert set(got) == set(want), (sorted(got), sorted(want))
    for key, row in want.items():
        for col, (g, w) in enumerate(zip(got[key], row)):
            if w == "":
                # A CSV empty field: DataFlow's file policy decides NULL or '';
                # the contract under test is the non-ASCII text.
                assert g in ("", None), (key, col, g)
            else:
                assert g == w, (key, col, g, w)


def _text_columns_are_national(table: str) -> None:
    types = {
        str(n): str(t).lower()
        for n, t in mssql_exec(
            "SELECT c.name, ty.name FROM sys.columns c JOIN sys.types ty "
            "ON ty.user_type_id = c.user_type_id WHERE c.object_id = OBJECT_ID(?)",
            f"dbo.{table}",
        )
    }
    for col in ("name", "city", "note"):
        assert types[col] in {"nvarchar", "nchar", "ntext"}, types


def _drop(table: str) -> None:
    try:
        mssql_exec(f"DROP TABLE IF EXISTS dbo.[{table}]")
    except Exception:  # noqa: BLE001, S110 - best-effort cleanup
        pass


def _upload_run(client, dataset: str, dest: str, table: str, mode: str, key: str = "") -> dict:
    args = {"dataset_name": dataset, "dest_connector_name": connector_name(dest),
            "dest_table": table, "sync_mode": mode}
    if key:
        args["primary_key"] = key
    failed, staged = mcp(client, "start_dataset_transfer", args)
    assert failed is False, staged
    failed, confirmed = mcp(client, "confirm_action", {"ack_id": staged["ack_id"], "reason": "d204"})
    assert failed is False, confirmed
    job = wait_job(confirmed["job_id"])
    assert str(job.get("status")) == "completed", (job.get("status"), job.get("error"))
    return job


def test_upload_csv_non_ascii_into_new_sqlserver_table_then_run2(transfer_client, mssql_up):
    dataset, table = tag("d204u"), tag("d204")
    dest = connector("sqlserver", "destination")
    path = transfer_client.uploads / f"{dataset}.csv"
    try:
        path.write_text(_csv(_ROWS), encoding="utf-8")
        _upload_run(transfer_client, dataset, dest, table, "full_refresh_overwrite")
        _text_columns_are_national(table)
        _assert_round_trip(table, _ROWS)

        # Run 2 into the table DataFlow created: keyed upsert.
        path.write_text(_csv(_RUN2), encoding="utf-8")
        _upload_run(transfer_client, dataset, dest, table, "upsert", key="id")
        _assert_round_trip(table, _RUN2)
    finally:
        _drop(table)


def test_sftp_csv_non_ascii_into_new_sqlserver_table_then_run2(transfer_client, mssql_up, tmp_path):
    pytest.importorskip("paramiko")
    from tests.sftp_test_server import start_sftp_server

    root = tmp_path / "sftp"
    root.mkdir()
    (root / "unicode.csv").write_text(_csv(_ROWS), encoding="utf-8")
    server, runner = start_sftp_server(str(root))
    scfg = server.endpoint_config("/unicode.csv")
    src = connector(
        "sftp", "source", type="sftp", host=scfg["host"], port=scfg["port"],
        username=scfg["username"], password=scfg["password"],
        database=scfg["database"], host_key=scfg["host_key"],
    )
    dst = connector("sqlserver", "destination")
    table = tag("d204s")
    try:
        run(transfer_client, source_connector_id=src, source_table="unicode.csv",
            dest_connector_id=dst, dest_table=table,
            sync_mode="full_refresh_overwrite", primary_key="id")
        _text_columns_are_national(table)
        _assert_round_trip(table, _ROWS)

        (root / "unicode.csv").write_text(_csv(_RUN2), encoding="utf-8")
        run(transfer_client, source_connector_id=src, source_table="unicode.csv",
            dest_connector_id=dst, dest_table=table, sync_mode="upsert", primary_key="id")
        _assert_round_trip(table, _RUN2)
    finally:
        runner.stop()
        _drop(table)


@pytest.mark.parametrize("src_format", ["csv", "postgresql"])
def test_studio_plan_mapping_binds_the_source_engine(tmp_path, monkeypatch, src_format):
    """Sibling route: the Studio plan's Map (``run_plan_mapping``) had the same
    unbound source engine, so a CSV / PostgreSQL VARCHAR into a new SQL Server
    table needed a Risk Contract for the NVARCHAR carrier."""
    from services.transfer_plan_service import run_plan_mapping
    from services.transfer_plan_store import create_plan

    monkeypatch.setattr("services.transfer_plan_store.STORE_PATH", tmp_path / "plans.json")
    monkeypatch.setattr("services.audit_log.STORE_PATH", tmp_path / "audit.jsonl")
    kind = "file" if src_format == "csv" else "database"
    plan_id = create_plan({
        "name": "d204-plan",
        "source": {"kind": kind, "format": src_format, "connector_id": "src"},
        "destination": {"kind": "database", "format": "sqlserver", "connector_id": "dst",
                        "table": "d204_new", "extra": {"table_exists": False}},
        "source_columns": ["id", "name"],
        "source_schema": {"id": "INTEGER", "name": "VARCHAR(64)"},
        "target_columns": [],
        "target_schema": {},
        "policies": {"sync_mode": "full_refresh_overwrite"},
    }).id
    result = run_plan_mapping(plan_id, use_llm=False,
                              source_samples={"id": ["1", "2"], "name": ["café", "中文 😀"]})
    name = next(m for m in result["mappings"] if m["source"] == "name")
    assert str(name.get("target_type") or "").upper().startswith("NVARCHAR"), name
    assert not name.get("requires_risk_contract"), (name.get("fidelity_reason"),
                                                    name.get("create_new_risks"))
