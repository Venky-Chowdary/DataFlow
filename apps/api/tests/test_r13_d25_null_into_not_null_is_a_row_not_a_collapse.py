"""D25 — one NULL into a NOT NULL text column is a row finding, not a collapse.

QA: with signed CAST_AND_CONTINUE contracts on ``amt``/``qty``, a single NULL
``cust`` into ``cust TEXT NOT NULL`` failed the whole job as "fidelity collapse
... 3 column(s) (amt DECIMAL(14,2) -> DECIMAL(10,2), qty BIGINT -> INT4, cust
TEXT -> TEXT)". The G3 finding was "NOT NULL contract: cust rejects NULL", but
the root-cause headline relabelled it as a type-path collapse and listed the
columns the contracts had already cleared. Expected: the refusal names the
NULL and the column; with a contract on ``cust`` the row is quarantined like
the overflowing ``qty`` row and the other 198 land.

Live PostgreSQL -> existing PostgreSQL table, preflight ON, strict.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from services.root_cause_engine import build_root_causes
from tests import r13_mapping_harness as h
from tests.r13_mapping_harness import isolated  # noqa: F401 - fixture

pytestmark = pytest.mark.timeout(300)

_G3_NOT_NULL = {
    "id": "g3_schema_contract",
    "status": "block",
    "message": "1 type coercion issue(s): NOT NULL contract: cust → cust (TEXT) rejects NULL "
               "— 1 empty/null sample value(s)",
    "details": {
        "issues": ["NOT NULL contract: cust → cust (TEXT) rejects NULL — 1 empty/null sample value(s)"],
        "issues_detail": [
            {"source": "amt", "target": "amt", "source_type": "DECIMAL(14,2)",
             "target_type": "DECIMAL(10,2)", "severity": "warn", "fidelity_collapse": True},
            {"source": "cust", "target": "cust", "source_type": "TEXT", "target_type": "TEXT",
             "severity": "block", "failure_class": "EMPTY_VALUE_NOT_NULLABLE"},
        ],
    },
}


def test_not_null_finding_is_not_titled_a_fidelity_collapse():
    roots = build_root_causes({"gates": [_G3_NOT_NULL], "blockers": []})
    assert not any(r.kind == "fidelity_collapse" for r in roots), [
        (r.kind, r.title, r.summary) for r in roots
    ]


def _signed(col, src, dst, transform):
    from services.migration_risk_contract import create_migration_risk_contract

    contract = create_migration_risk_contract(
        column=col, source_type=src, destination_type=dst, approved_by="qa@example.com",
        reason="hold bad rows out", execution_policy="CAST_AND_CONTINUE", transform=transform,
    ).to_dict()
    return {"source": col, "target": col, "source_type": src, "target_type": dst,
            "transform": transform, "confidence": 0.99, "risk_acknowledged": True,
            "risk_contract": contract}


def _mappings(sign_cust: bool):
    cust = (
        _signed("cust", "TEXT", "TEXT", "none") if sign_cust
        else {"source": "cust", "target": "cust", "source_type": "TEXT",
              "target_type": "TEXT", "confidence": 0.99}
    )
    return [
        {"source": "id", "target": "id", "source_type": "INTEGER",
         "target_type": "INTEGER", "confidence": 0.99},
        _signed("amt", "NUMERIC(14,2)", "NUMERIC(10,2)", "decimal"),
        _signed("qty", "BIGINT", "INTEGER", "integer"),
        cust,
    ]


def test_null_row_refused_by_name_then_quarantined_under_contract(isolated):  # noqa: F811
    table = f"d25_{h.token()}"
    out = f"{table}_d"
    rows = [(i, Decimal(i) + Decimal("0.25"), 10**12 if i == 120 else i,
             None if i == 33 else ("東京" if i == 5 else ("" if i == 6 else f"c{i}")))
            for i in range(1, 201)]
    try:
        h.run_sql(
            "postgresql",
            f"CREATE TABLE {table} (id INT PRIMARY KEY, amt NUMERIC(14,2), qty BIGINT, cust TEXT)",
            f"CREATE TABLE {out} (id INT PRIMARY KEY, amt NUMERIC(10,2), qty INT4, cust TEXT NOT NULL)",
        )
        h.run_sql("postgresql", f"INSERT INTO {table} VALUES (%s,%s,%s,%s)", params=rows)  # nosec B608

        def run(sign_cust):
            return h.execute(
                isolated, h.endpoint("postgresql", table), h.endpoint("postgresql", out),
                sync_mode="full_refresh_append", mappings=_mappings(sign_cust),
            )

        refused = run(False)
        assert not refused.success
        assert "fidelity collapse" not in (refused.error or "").lower(), refused.error
        assert "cust" in refused.error and "NULL" in refused.error, refused.error

        loaded = run(True)
        assert loaded.success, loaded.error
        ids = {r[0] for r in h.run_sql("postgresql", f"SELECT id FROM {out}")}  # nosec B608
        assert len(ids) == 198 and 33 not in ids and 120 not in ids
        assert dict(h.run_sql("postgresql", f"SELECT id, cust FROM {out} WHERE id IN (5, 6)")) == {  # nosec B608
            5: "東京", 6: "",
        }
    finally:
        h.drop("postgresql", table)
        h.drop("postgresql", out)
