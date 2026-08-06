"""WriteResult must carry full rejected_details — never truncate before DLQ."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.writer_common import build_mapped_rows_with_details


def test_map_rejects_preserve_full_detail_list_size():
    """Sanity: many transform rejects stay attached (cap must not be 100)."""
    headers = ["id", "n"]
    rows = [[str(i), "not-an-int"] for i in range(1, 151)]
    mappings = [
        {"source": "id", "target": "id", "transform": "none"},
        {
            "source": "n",
            "target": "n",
            "transform": "to_integer",
            "target_type": "integer",
        },
    ]
    _mapped, _errs, rejected = build_mapped_rows_with_details(
        headers=headers,
        data_rows=rows,
        mappings=mappings,
        target_cols=["id", "n"],
        column_types={"id": "string", "n": "integer"},
        dest_types={"id": "string", "n": "integer"},
        error_policy="quarantine",
        dest_kind="sqlite",
        destination_pk_columns=["id"],
    )
    assert len(rejected) >= 150


def test_quarantine_detail_does_not_invent_id_pk_without_contract():
    """Without mapping/contract PK, do not invent id as quarantine identity."""
    headers = ["name", "n"]
    rows = [["alpha", "nope"]]
    mappings = [
        {"source": "name", "target": "name", "transform": "none"},
        {
            "source": "n",
            "target": "n",
            "transform": "to_integer",
            "target_type": "integer",
        },
    ]
    _mapped, _errs, rejected = build_mapped_rows_with_details(
        headers=headers,
        data_rows=rows,
        mappings=mappings,
        target_cols=["name", "n"],
        column_types={"name": "string", "n": "integer"},
        dest_types={"name": "string", "n": "integer"},
        error_policy="quarantine",
        dest_kind="sqlite",
        destination_pk_columns=None,
    )
    assert rejected
    for d in rejected:
        assert d.get("primary_key") in (None, [], ())
