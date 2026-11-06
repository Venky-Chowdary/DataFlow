"""E2-018 guard — an edited quarantine replay into MySQL lands the corrected cells.

QA (build 6a2bae9aaa00): PostgreSQL -> MySQL, two quarantined rows, replay with
row 2500 amount="1234.50" and row 2900 code="TOOLN" completed, reconciliation
passed and quarantine closed, yet both corrected cells were NULL in MySQL.

Not reproduced on 3e258e90f (upsert / append / overwrite parents, row-payload
and finding edits). This locks the MySQL route, which was never verified: the
corrected values must be what MySQL holds, the untouched rows unchanged.
"""

from __future__ import annotations

import uuid
from decimal import Decimal

import pytest

from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest
from tests.test_quarantine_replay_live_matrix import (
    _client,
    _dest,
    _mysql_cfg,
    _mysql_exec,
    _pg_cfg,
    _pg_exec,
)


def _finding_edits(quarantine: list[dict]) -> list[dict]:
    rows = []
    for d in quarantine:
        d = dict(d)
        vals = dict(d.get("values") or {})
        if str(vals.get("id")) == "2500":
            d["value"], vals["amount"] = "1234.50", "1234.50"
        if str(vals.get("id")) == "2900":
            d["value"], vals["code"] = "TOOLN", "TOOLN"
        d["values"] = vals
        rows.append(d)
    return rows


@pytest.mark.parametrize("parent_mode", ["incremental_deduped", "full_refresh_append"])
@pytest.mark.parametrize("edit_style", ["row_payload", "finding"])
def test_edited_replay_into_mysql_lands_the_corrected_cells(parent_mode, edit_style):
    pg, my = _pg_cfg(), _mysql_cfg()
    if not pg or not my:
        pytest.skip("PostgreSQL and MySQL are both required")
    tag = uuid.uuid4().hex[:8]
    src, dst = f"r13_e2018_s_{tag}", f"r13_e2018_d_{tag}"
    _pg_exec(pg, [
        f'CREATE TABLE "{src}" (id int PRIMARY KEY, code text, amount numeric(12,2))',
        f"INSERT INTO \"{src}\" VALUES (2400, 'OKAY', 1.5), (2500, 'OK', 99999.99), "
        f"(2900, 'TOOLONG', 2.0), (3000, '', NULL)",
    ])
    _mysql_exec(my, [f"CREATE TABLE `{dst}` (id INT PRIMARY KEY, code VARCHAR(5), amount DECIMAL(6,2))"])
    try:
        src_ep = EndpointConfig(
            kind="database", format="postgresql", host=str(pg["host"]), port=int(pg.get("port") or 5432),
            database=pg["database"], username=pg["username"], password=pg["password"],
            schema="public", table=src,
        )
        result = UniversalTransferEngine().execute(
            TransferRequest(
                source=src_ep,
                destination=_dest("mysql", my, dst),
                mappings=[{"source": c, "target": c} for c in ("id", "code", "amount")],
                sync_mode=parent_mode,
                skip_preflight=True,
                validation_mode="balanced",
                stream_contracts=[
                    {"name": src, "primary_key": "id", "sync_mode": parent_mode, "selected": True}
                ],
            )
        )
        assert result.success, result.error
        client = _client()
        q = client.get(f"/api/v1/connectors/jobs/{result.job_id}/quarantine")
        assert q.status_code == 200, q.text
        quarantine = q.json().get("quarantine") or []
        assert len(quarantine) == 2
        rows = (
            [{"id": 2500, "amount": "1234.50"}, {"id": 2900, "code": "TOOLN"}]
            if edit_style == "row_payload"
            else _finding_edits(quarantine)
        )
        replay = client.post(
            f"/api/v1/connectors/jobs/{result.job_id}/quarantine/replay", json={"rows": rows}
        )
        assert replay.status_code == 200, replay.text
        assert replay.json().get("success") is True, replay.json()
        landed = _mysql_exec(my, [f"SELECT id, code, amount FROM `{dst}` ORDER BY id"])[0]
        assert [tuple(r) for r in landed] == [
            (2400, "OKAY", Decimal("1.50")),
            (2500, "OK", Decimal("1234.50")),
            (2900, "TOOLN", Decimal("2.00")),
            (3000, "", None),
        ]
    finally:
        _pg_exec(pg, [f'DROP TABLE IF EXISTS "{src}"'])
        _mysql_exec(my, [f"DROP TABLE IF EXISTS `{dst}`", f"DROP TABLE IF EXISTS `{dst}_df_quarantine`"])
