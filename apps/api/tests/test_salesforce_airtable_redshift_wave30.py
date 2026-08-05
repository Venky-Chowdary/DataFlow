"""Salesforce / Airtable Gate-8 verify routing + Redshift MERGE SQL shape."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_verify_target_routes_salesforce_and_airtable():
    from services.reconciliation import verify_target

    with patch(
        "services.reconciliation.verify_salesforce_object",
        return_value=(3, "sf"),
    ) as sf:
        assert verify_target(
            "salesforce",
            {"password": "tok"},
            schema="",
            table_name="Account",
            fallback_rows=-1,
            fallback_checksum="",
        ) == (3, "sf")
        assert sf.call_args.kwargs["object_name"] == "Account"

    with patch(
        "services.reconciliation.verify_airtable_table",
        return_value=(2, "at"),
    ) as at:
        assert verify_target(
            "airtable",
            {"database": "appBase", "password": "pat"},
            schema="",
            table_name="Tasks",
            fallback_rows=-1,
            fallback_checksum="",
        ) == (2, "at")
        assert at.call_args.kwargs["base_id"] == "appBase"
        assert at.call_args.kwargs["table_name"] == "Tasks"


def test_salesforce_reconcile_sample_meta_on_writeresult():
    from connectors.writer_common import WriteResult

    r = WriteResult(
        ok=True,
        rows_written=1,
        table_name="Account",
        target_schema="",
        checksum="x",
        chunks_completed=1,
        meta={
            "reconcile_sample": [{"Id": "001", "Name": "Acme"}],
            "written_ids": ["001"],
        },
    )
    assert r.meta["written_ids"] == ["001"]


def test_redshift_merge_sql_null_safe_and_returns_empty_on_success():
    from connectors.postgresql_writer import _redshift_merge_upsert

    class _Frag:
        def __init__(self, text: str = ""):
            self.text = text

        def format(self, *args: object, **kwargs: object) -> "_Frag":
            # Support both positional and named .format used by psycopg2.sql
            try:
                if kwargs:
                    return _Frag(self.text.format(**{k: str(v) for k, v in kwargs.items()}))
            except Exception:
                pass
            return _Frag(self.text + " " + " ".join(str(a) for a in args))

        def join(self, parts: object) -> "_Frag":
            return _Frag(self.text.join(str(p) for p in parts))

        def __str__(self) -> str:
            return self.text

    class _SQL:
        @staticmethod
        def SQL(text: str) -> _Frag:
            return _Frag(text)

        @staticmethod
        def Identifier(name: str) -> str:
            return name

        @staticmethod
        def Placeholder() -> str:
            return "%s"

    class _Cur:
        def __init__(self) -> None:
            self.queries: list[str] = []

        def execute(self, query: object, params: object = None) -> None:
            self.queries.append(str(query))

        def executemany(self, query: object, params: object = None) -> None:
            self.queries.append(f"MANY:{query}")

        def fetchone(self):
            return None

    cur = _Cur()
    out = _redshift_merge_upsert(
        cur,
        _SQL,
        schema="public",
        table_name="orders",
        target_cols=["id", "amount"],
        conflict_cols=["id"],
        batch=[(1, "10.00")],
    )
    assert out == []
    joined = " ".join(cur.queries).upper()
    assert "MERGE INTO" in joined
    assert "IS NULL" in joined
    assert "WHEN MATCHED THEN UPDATE" in joined
    assert "WHEN NOT MATCHED THEN INSERT" in joined


def test_run_reconciliation_salesforce_sample_verified():
    from src.transfer.models import EndpointConfig
    from src.transfer.reconcile_step import run_reconciliation

    sample = [{"Id": "001", "Name": "Acme"}]
    with patch(
        "src.transfer.reconcile_step.resolve_connector_config",
        return_value={"type": "salesforce"},
    ), patch(
        "src.transfer.reconcile_step.verify_target",
        return_value=(-1, ""),
    ), patch(
        "src.transfer.reconcile_step.read_target_sample",
        return_value=sample,
    ):
        report = run_reconciliation(
            endpoint=EndpointConfig(
                kind="database", format="salesforce", table="Account"
            ),
            records=sample,
            columns=["Id", "Name"],
            rows_written=1,
            writer_checksum="w",
            dest_summary={
                "table": "Account",
                "sync_mode": "incremental_append",
                "reconcile_sample": sample,
                "source_row_count": 1,
            },
            mappings=[
                {"source": "Id", "target": "Id"},
                {"source": "Name", "target": "Name"},
            ],
            validation_mode="balanced",
        )
    assert report["passed"] is True
    assert report["phase"] == "post_write_sample_verified"
