"""A 50-row DECIMAL(4,2) sample must not become the column's domain."""

from __future__ import annotations

from connectors.mongodb_writer import _widen_sampled_mongo_decimals
from connectors.writer_common import fits_decimal, parse_decimal_precision_scale
from services.decimal_observe import upgrade_sampled_decimal_stamp


def test_file_sample_decimal_opens_to_full_integer_capacity():
    stamp = upgrade_sampled_decimal_stamp(
        "DECIMAL(4,2)",
        src_type="DECIMAL(4,2)",
        dest_db="mysql",
        source_db="csv",
        samples=["1.25", "2.50"],
    )
    assert stamp.upper().startswith("DECIMAL(38,")
    assert fits_decimal("1001.25", 38, 2, dest_db="mysql")
    sqlserver = upgrade_sampled_decimal_stamp(
        "DECIMAL(4,2)",
        src_type="DECIMAL(4,2)",
        dest_db="sqlserver",
        source_db="sftp",
    )
    assert "38" in sqlserver


def test_declared_relational_precision_is_not_widened():
    assert (
        upgrade_sampled_decimal_stamp(
            "DECIMAL(10,2)",
            src_type="NUMERIC(10,2)",
            dest_db="mysql",
            source_db="postgresql",
        )
        == "DECIMAL(10,2)"
    )


def test_mongo_writer_does_not_keep_a_sample_decimal_ceiling():
    types = _widen_sampled_mongo_decimals(
        {"amount": "DECIMAL(4,2)"},
        [
            {
                "source": "amount",
                "target": "amount",
                "source_type": "DECIMAL(4,2)",
                "source_db": "mongodb",
                "target_type": "DECIMAL(4,2)",
            }
        ],
        {},
    )
    parsed = parse_decimal_precision_scale(types["amount"], dest_db="mongodb")
    assert parsed is None or parsed[0] >= 38


def test_mongo_writer_leaves_a_date_target_alone():
    types = _widen_sampled_mongo_decimals(
        {"created_at": "DATE", "amount": "DECIMAL(4,2)"},
        [
            {
                "source": "created_at",
                "target": "created_at",
                "source_type": "TEXT",
                "source_db": "csv",
                "target_type": "DATE",
            },
            {
                "source": "amount",
                "target": "amount",
                "source_type": "DECIMAL(4,2)",
                "source_db": "csv",
                "target_type": "DECIMAL(4,2)",
            },
        ],
        {},
    )
    assert types["created_at"] == "DATE"
    parsed = parse_decimal_precision_scale(types["amount"], dest_db="mongodb")
    assert parsed is None or parsed[0] >= 38


def test_mongo_writer_keeps_an_operator_decimal_ceiling():
    types = _widen_sampled_mongo_decimals(
        {"amount": "DECIMAL(4,2)"},
        [
            {
                "source": "amount",
                "target": "amount",
                "source_type": "DECIMAL(4,2)",
                "source_db": "csv",
                "target_type": "DECIMAL(4,2)",
                "user_override": True,
            }
        ],
        {},
    )
    assert types["amount"] == "DECIMAL(4,2)"
