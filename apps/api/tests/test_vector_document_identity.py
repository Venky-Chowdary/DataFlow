"""QA MX2-17 — vector document identity comes from the contract PK.

``employees(employee_id)`` has no literal ``id`` column: every record fell
back to a content-hash document id, and two records whose embedded text was
identical produced the same id inside one ``INSERT ... ON CONFLICT DO UPDATE``
— PostgreSQL refuses to touch the same row twice and the whole job failed at
5% with zero rows committed.

Two guarantees, matrix-wide (pgvector/milvus/pinecone/qdrant/weaviate all
share ``vectorize_records``):
1. ``identity_columns`` resolves source_id from the declared PK, so document
   identity is the row's real key — identical content under different PKs no
   longer collides.
2. The batch dedupes identical ids so the upsert statement can never be asked
   to affect one row twice — last wins, same as DO UPDATE semantics.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.vectorization import (  # noqa: E402
    vector_identity_columns,
    vectorize_records,
)

_EMPLOYEES = [
    {"employee_id": "1", "name": "Ann", "notes": "identical free-text note body"},
    {"employee_id": "2", "name": "Bob", "notes": "identical free-text note body"},
]


def test_contract_pk_drives_document_id() -> None:
    rows = vectorize_records(
        _EMPLOYEES,
        identity_columns=["employee_id"],
        model="hash/32",
        skip_chunking=True,
    )
    assert len(rows) == 2
    assert {r["source_id"] for r in rows} == {"1", "2"}
    assert len({r["id"] for r in rows}) == 2


def test_identical_content_without_pk_still_dedupes() -> None:
    rows = vectorize_records(_EMPLOYEES, model="hash/32", skip_chunking=True)
    # Same embedded content + no PK → same hash id; dedupe keeps the batch
    # insertable instead of erroring inside ON CONFLICT.
    assert len(rows) == 1


def test_composite_identity_joins_components() -> None:
    recs = [
        {"region": "us", "seq": "1", "notes": "body text for the record"},
        {"region": "eu", "seq": "1", "notes": "body text for the record"},
    ]
    rows = vectorize_records(
        recs,
        identity_columns=["region", "seq"],
        model="hash/32",
        skip_chunking=True,
    )
    assert len(rows) == 2
    assert {r["source_id"] for r in rows} == {"us\x1f1", "eu\x1f1"}
    assert len({r["id"] for r in rows}) == 2


def test_identity_column_resolves_mapped_source_name() -> None:
    """Dest pk ``employee_id`` mapped from source ``emp_id`` uses the source key."""
    recs = [{"emp_id": "7", "name": "Renamed pk example row content"}]
    cols = vector_identity_columns(
        ["employee_id"],
        [{"source": "emp_id", "target": "employee_id"}],
        recs,
    )
    assert cols == ["emp_id"]
    rows = vectorize_records(
        recs, identity_columns=cols, model="hash/32", skip_chunking=True
    )
    assert rows[0]["source_id"] == "7"
