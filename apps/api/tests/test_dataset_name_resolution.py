"""Uploaded dataset names must resolve to the file, not an industry template.

``sample_payments`` used to miss the exact stem (underscore vs space) and then
the token ``payment`` selected the empty Financial Services template. Listed
stems such as ``scale_100k`` came back as not found.
"""

from __future__ import annotations

from services.dataset_resolve import pick_dataset
from src.ai.training.universal_data_feeder import UniversalSchema


def _upload(name: str, columns: list[str], rows: int, file_type: str = "csv") -> UniversalSchema:
    return UniversalSchema(
        name=name,
        source="upload",
        columns=columns,
        samples={col: ["v"] for col in columns},
        row_count=rows,
        file_type=file_type,
    )


def _finance() -> UniversalSchema:
    return UniversalSchema(
        name="Financial Services",
        source="industry",
        columns=["transaction_id", "account_number", "amount"],
        samples={"amount": ["sample_currency_amount"]},
        row_count=0,
        industry="finance",
        file_type="",
    )


def _catalog() -> list[UniversalSchema]:
    return [
        _finance(),
        _upload("sample_payments", ["CUST_ID", "AMT", "TXN_DT"], 10, "csv"),
        _upload("sample_payments", ["CUST_ID", "AMT"], 3, "tsv"),
        _upload("scale_100k", ["id", "email", "amount"], 100_000),
        _upload("mapping_golden", ["source", "target"], 108, "json"),
        _upload("sample_schema_types", ["id", "payload"], 5),
        _upload("sample_logistics", ["CUST_ID", "TRACK_NO"], 5),
        _upload("orders", ["order_id", "email"], 4),
        _upload("customers", ["customer_id", "email"], 4),
    ]


def test_underscore_stem_binds_the_upload_not_the_finance_template() -> None:
    chosen = pick_dataset(_catalog(), "sample_payments")
    assert chosen is not None
    assert chosen.source == "upload"
    assert chosen.row_count == 10
    assert chosen.columns[0] == "CUST_ID"


def test_file_type_suffix_selects_the_thinner_sibling() -> None:
    chosen = pick_dataset(_catalog(), "sample_payments.tsv")
    assert chosen is not None
    assert chosen.file_type == "tsv"
    assert chosen.row_count == 3


def test_listed_stems_resolve() -> None:
    for name, rows in (
        ("scale_100k", 100_000),
        ("mapping_golden", 108),
        ("sample_schema_types", 5),
        ("sample-logistics", 5),
    ):
        chosen = pick_dataset(_catalog(), name)
        assert chosen is not None, name
        assert chosen.source == "upload"
        assert chosen.row_count == rows


def test_payment_keyword_prefers_the_upload_over_the_empty_template() -> None:
    chosen = pick_dataset(_catalog(), "payment")
    assert chosen is not None
    assert chosen.source == "upload"
    assert chosen.name == "sample_payments"
    assert chosen.row_count == 10


def test_finance_with_no_upload_still_returns_the_industry_template() -> None:
    chosen = pick_dataset([_finance()], "finance")
    assert chosen is not None
    assert chosen.source == "industry"
    assert chosen.row_count == 0


def test_ambiguous_keyword_stays_unbound() -> None:
    assert pick_dataset(_catalog(), "email") is None


def test_other_job_spill_name_stays_unbound() -> None:
    assert pick_dataset([_finance()], "products") is None
