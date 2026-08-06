"""Enterprise honesty: CDC quarantine accumulate, vector PK id, create-new cap."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_cdc_merge_accumulates_rejected_details_across_batches():
    from src.transfer.cdc_transfer import CdcState, _merge_cdc_dest_summary

    state = CdcState()
    with patch("services.quarantine_dlq.persist_rejected_rows") as persist:
        s1 = _merge_cdc_dest_summary(
            state,
            {
                "rejected_details": [{"row": 1, "reason": "bad-a"}],
                "rejected_rows": 1,
            },
            job_id="job-1",
        )
        s2 = _merge_cdc_dest_summary(
            state,
            {
                "rejected_details": [{"row": 2, "reason": "bad-b"}],
                "rejected_rows": 1,
            },
            job_id="job-1",
        )
    assert persist.call_count == 2
    assert len(s2["rejected_details"]) == 2
    assert {d["reason"] for d in s2["rejected_details"]} == {"bad-a", "bad-b"}
    assert s1["rejected_rows"] == 1
    assert s2["rejected_rows"] == 2
    assert state.last_dest_summary["rejected_details_total"] == 2


def test_cdc_merge_refuses_watermark_when_dlq_persist_fails():
    import pytest
    from src.transfer.cdc_transfer import CdcState, _merge_cdc_dest_summary

    state = CdcState()
    with patch(
        "services.quarantine_dlq.persist_rejected_rows",
        side_effect=RuntimeError("dlq down"),
    ):
        with pytest.raises(RuntimeError, match="refuse watermark"):
            _merge_cdc_dest_summary(
                state,
                {"rejected_details": [{"row": 1, "reason": "x"}], "rejected_rows": 1},
                job_id="job-x",
            )


def test_vectorize_preserves_source_pk_as_id():
    from services.vectorization import vectorize_records

    rows = vectorize_records(
        [{"id": "cust-42", "content": "hello world document text"}],
        content_column="content",
        embedding_column=None,
        model="hash/32",
        skip_chunking=True,
    )
    assert len(rows) == 1
    assert rows[0]["id"] == "cust-42"
    assert rows[0]["source_id"] == "cust-42"


def test_vectorize_stamps_content_truncate_metadata():
    from services.vectorization import CONTENT_STORE_LIMIT, vectorize_records

    long = "x" * (CONTENT_STORE_LIMIT + 50)
    rows = vectorize_records(
        [{"id": "1", "content": long, "embedding": [0.1, 0.2, 0.3]}],
        content_column="content",
        embedding_column="embedding",
    )
    assert rows[0]["metadata"].get("_df_content_truncated") is True
    assert rows[0]["metadata"].get("_df_content_original_len") == len(long)
    assert len(rows[0]["content"]) == CONTENT_STORE_LIMIT


def test_create_compatible_new_confidence_capped_for_review():
    from services.semantic_mapper import _apply_create_new_risk_stamps

    out = _apply_create_new_risk_stamps(
        [
            {
                "source": "extra_col",
                "target": "extra_col",
                "confidence": 0.92,
                "assignment_strategy": "create_compatible_new",
                "create_new": True,
                "requires_review": True,
                "score_gap": 0.0,
                "source_type": "VARCHAR",
                "target_type": "TEXT",
            }
        ],
        "postgresql",
    )
    assert out[0]["requires_review"] is True
    assert float(out[0]["confidence"]) <= 0.84


def test_identity_passthrough_create_new_under_g4_floor():
    from services.semantic_mapper import map_columns

    mappings = map_columns(
        ["id", "email"],
        [],
        source_schemas=[
            {"name": "id", "inferred_type": "TEXT", "samples": ["a"]},
            {"name": "email", "inferred_type": "TEXT", "samples": ["a@b.com"]},
        ],
        destination_db_type="postgresql",
        destination_table_exists=False,
    )
    assert all(m["assignment_strategy"] == "identity_passthrough" for m in mappings)
    assert all(m.get("requires_review") is True for m in mappings)
    assert all(float(m["confidence"]) <= 0.84 for m in mappings)


def test_vectorize_metadata_only_rows_get_distinct_ids():
    from services.vectorization import vectorize_records

    rows = vectorize_records(
        [
            {"content": "", "name": "a", "score": 1},
            {"content": "", "name": "b", "score": 2},
        ],
        content_column="content",
        embedding_column=None,
        model="hash/32",
        skip_chunking=True,
        metadata_columns=["name", "score"],
    )
    # Empty content → sparse metadata path; ids must not collide.
    assert len(rows) == 2
    assert rows[0]["embedding"] is None
    assert rows[0]["id"] != rows[1]["id"]


def test_datetime_date_only_does_not_invent_utc_z():
    from services.transform_engine import apply_transform

    out, err = apply_transform("2024-06-15", "datetime")
    assert err is None
    assert out is not None
    assert not str(out).endswith("Z")
    assert "T00:00:00" in str(out)


def test_url_email_iban_transforms_fail_closed_on_garbage():
    from services.transform_engine import apply_transform

    out, err = apply_transform("not-a-url", "url")
    assert out is None
    assert err and "Invalid url" in err

    out, err = apply_transform("not-an-email", "email")
    assert out is None
    assert err and "Invalid email" in err

    out, err = apply_transform("XX00", "iban")
    assert out is None
    assert err and "Invalid iban" in err

    out, err = apply_transform("https://example.com/a", "url")
    assert err is None
    assert out == "https://example.com/a"
