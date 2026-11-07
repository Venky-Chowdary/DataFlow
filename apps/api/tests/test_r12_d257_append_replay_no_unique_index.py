"""D257: quarantine replay on an append route writes the way the route wrote.

Replay forced a keyed upsert onto every parent, so on a full_refresh_append
table that legitimately holds two id=3 rows the writer tried
CREATE UNIQUE INDEX uidx_<table>_id and the edited row never landed. On an
append table without duplicates it would have added the constraint quietly.
"""
from __future__ import annotations

import pytest

from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest
from tests.test_quarantine_replay_live_matrix import (
    _client,
    _dest,
    _pg_cfg,
    _pg_exec,
    _sid,
)

_CSV = (
    b"id,age\n1,30\n2,31\n3,32\n3,33\n4,34\n11,not-a-number\n12,36\n"
)


def test_append_replay_inserts_and_adds_no_constraint():
    cfg = _pg_cfg()
    if not cfg:
        pytest.skip("PostgreSQL unavailable")
    table = _sid("d257")
    drops = [
        f'DROP TABLE IF EXISTS "{table}_df_quarantine"',
        f'DROP TABLE IF EXISTS "{table}"',
    ]
    _pg_exec(cfg, drops)
    try:
        result = UniversalTransferEngine().execute(
            TransferRequest(
                source=EndpointConfig(kind="file", format="csv"),
                destination=_dest("postgresql", cfg, table),
                source_filename="bad.csv",
                source_content=_CSV,
                sync_mode="full_refresh_append",
                skip_preflight=True,
                validation_mode="balanced",
                mappings=[
                    {"source": "id", "target": "id", "confidence": 0.95},
                    {"source": "age", "target": "age", "confidence": 0.95,
                     "target_type": "integer"},
                ],
                column_types={"id": "string", "age": "string"},
            )
        )
        assert result.success, result.destination_summary
        count = f'SELECT COUNT(*) FROM "{table}"'
        assert int(_pg_exec(cfg, [count])[0][0][0]) == 6

        client = _client()
        q = client.get(f"/api/v1/connectors/jobs/{result.job_id}/quarantine")
        assert q.status_code == 200, q.text
        rows = []
        for d in q.json().get("quarantine") or []:
            d = dict(d)
            d["value"] = "35"
            d["values"] = {**(d.get("values") or {}), "age": "35"}
            rows.append(d)
        assert rows
        replay = client.post(
            f"/api/v1/connectors/jobs/{result.job_id}/quarantine/replay",
            json={"rows": rows},
        )
        assert replay.status_code == 200, replay.text
        assert replay.json().get("success") is True, replay.json()

        assert int(_pg_exec(cfg, [count])[0][0][0]) == 7
        landed = _pg_exec(
            cfg, [f"SELECT age FROM \"{table}\" WHERE id::text = '11'"]
        )[0]
        assert [int(r[0]) for r in landed] == [35]
        dup3 = _pg_exec(cfg, [f"SELECT COUNT(*) FROM \"{table}\" WHERE id::text = '3'"])
        assert int(dup3[0][0][0]) == 2
        uniq = _pg_exec(
            cfg,
            [
                "SELECT COUNT(*) FROM pg_index i JOIN pg_class t "
                "ON t.oid = i.indrelid WHERE i.indisunique AND t.relname = "
                f"'{table}'"
            ],
        )
        assert int(uniq[0][0][0]) == 0
    finally:
        _pg_exec(cfg, drops)
