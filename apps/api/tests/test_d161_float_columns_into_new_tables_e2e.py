"""D161: a DOUBLE / FLOAT column into a NEW table is not held at G4.

QA (rounds 20 and 26): a ``ratio`` column (multiples of 1/7, -1.5e-10, NULL,
1e-300) from an SFTP CSV, and an Elasticsearch ``double``, into a new SQL
Server table: G4 blocked at mapping confidence 0.70 ("create-new type risk:
ieee_float_artifact") although the plan called the mapping lossless, and a
signed contract did not clear it. The values are binary doubles already: each
cell is the shortest spelling of an IEEE double, so the double carrier holds
every one of them exactly.
"""

from __future__ import annotations

import math

import pytest

from tests._r4_temporal_harness import (  # noqa: F401 - fixtures
    MSSQL,
    confirm,
    connector,
    mssql_exec,
    mysql_exec,
    pg_exec,
    stage,
    staged_summary,
    tag,
    transfer_client,
    up,
)

RATIOS = [i / 7 for i in range(1, 25)] + [-1.5e-10, None, 1e-300, 1.7976931348623157e308]


def _csv() -> str:
    lines = ["id,ratio"]
    for i, r in enumerate(RATIOS, start=1):
        lines.append(f"{i},{'' if r is None else repr(r)}")
    return "\n".join(lines) + "\n"


def _read(dest: str, out: str) -> dict[int, float | None]:
    if dest == "sqlserver":
        rows = mssql_exec(f"SELECT id, ratio FROM dbo.[{out}]")  # nosec B608
    elif dest == "mysql":
        rows = mysql_exec(f"SELECT id, ratio FROM `{out}`")  # nosec B608
    else:
        rows = pg_exec(f'SELECT id, ratio FROM public."{out}"')  # nosec B608
    return {int(i): (None if r is None else float(r)) for i, r in rows}


def _drop(dest: str, out: str) -> None:
    if dest == "sqlserver":
        mssql_exec(f"DROP TABLE IF EXISTS dbo.[{out}]")
    elif dest == "mysql":
        mysql_exec(f"DROP TABLE IF EXISTS `{out}`")
    else:
        pg_exec(f'DROP TABLE IF EXISTS public."{out}"')


@pytest.mark.parametrize("mode", ["strict", "balanced"])
@pytest.mark.parametrize("dest", ["sqlserver", "mysql", "postgresql"])
def test_sftp_csv_float_column_into_new_table(transfer_client, tmp_path, dest, mode):
    pytest.importorskip("paramiko")
    if dest == "sqlserver":
        pytest.importorskip("pyodbc")
        if not (MSSQL["password"] and up(MSSQL["host"], MSSQL["port"])):
            pytest.skip("SQL Server not reachable")
    from services.connector_store import create_connector
    from tests.sftp_test_server import start_sftp_server

    root = tmp_path / "sftp"
    root.mkdir()
    (root / "q20_seed.csv").write_text(_csv(), encoding="utf-8")
    server, runner = start_sftp_server(str(root))
    cfg = server.endpoint_config("/q20_seed.csv")
    out = tag("d161")
    try:
        source = str(create_connector({
            "name": tag("d161-sftp"), "type": "sftp", "role": "source",
            "host": cfg["host"], "port": cfg["port"], "username": cfg["username"],
            "password": cfg["password"], "database": cfg["database"],
            "host_key": cfg["host_key"], "workspace_id": "",
        }).id)
        staged = stage(
            source_connector_id=source, source_table="q20_seed.csv",
            dest_connector_id=connector(dest, "destination"), dest_table=out,
            sync_mode="full_refresh_overwrite", validation_mode=mode,
        )
        summary = staged_summary(staged)
        assert staged.success and not summary["blockers"], summary
        job = confirm(transfer_client, staged)
        assert str(job.get("status") or "") == "completed", job.get("error") or job.get("message")
        got = _read(dest, out)
        for i, want in enumerate(RATIOS, start=1):
            if want is None:
                assert got[i] is None, (i, got[i])
            else:
                assert got[i] == want or math.isclose(got[i], want, rel_tol=0, abs_tol=0), (i, got[i], want)
    finally:
        runner.stop()
        _drop(dest, out)


# ---------------------------------------------------------------------------
# Unit: the G4 confidence the create-new stamp gives a float column
# ---------------------------------------------------------------------------


def _stamped(source_db: str, source_type: str, dest: str, samples: list) -> dict:
    from services.create_new_risk_stamp import apply_create_new_risk_stamps

    row = {
        "source": "ratio", "target": "ratio", "source_type": source_type,
        "target_type": "", "create_new": True,
        "assignment_strategy": "identity_passthrough", "confidence": 0.95,
    }
    return apply_create_new_risk_stamps(
        [row], dest, source_samples={"ratio": samples}, dest_table_exists=False,
        source_db_type=source_db,
    )[0]


_SEVENTHS = [repr(i / 7) for i in range(1, 20)] + ["-1.5e-10", None, "1e-300"]


@pytest.mark.parametrize("dest", ["sqlserver", "mysql", "postgresql", "sqlite"])
@pytest.mark.parametrize(
    ("source_db", "source_type"),
    [("elasticsearch", "double"), ("opensearch", "double"), ("qdrant", "float"),
     ("sftp", "DOUBLE"), ("csv", "DOUBLE")],
)
def test_a_double_column_into_a_new_table_clears_the_floor(source_db, source_type, dest):
    row = _stamped(source_db, source_type, dest, _SEVENTHS)
    kinds = [r.get("kind") for r in row.get("create_new_risks") or []]
    assert "ieee_float_artifact" not in kinds, kinds
    assert float(row["confidence"]) >= 0.85, row["confidence"]


def test_text_a_double_would_round_still_holds_the_column():
    """``42.100000000000001`` is not the double it becomes (42.1): kept held."""
    row = _stamped("csv", "DOUBLE", "postgresql", ["111.89999999999999", "42.100000000000001"])
    assert "ieee_float_artifact" in [r.get("kind") for r in row.get("create_new_risks") or []]
    assert float(row["confidence"]) < 0.85


def test_a_single_precision_carrier_still_holds_the_column():
    """MySQL bare FLOAT is binary32: it would round a double's digits."""
    from services.decimal_observe import ieee_float_create_new_risk, observe_numeric_samples

    risk = ieee_float_create_new_risk(
        observe_numeric_samples(_SEVENTHS), target_type="FLOAT", dest_db="mysql",
        samples=_SEVENTHS,
    )
    assert risk and risk["kind"] == "ieee_float_artifact"
