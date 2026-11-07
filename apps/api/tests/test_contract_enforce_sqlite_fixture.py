"""Named fixture: signed sqlite contract blocks drifted Execute schema.

Not a live-matrix green. File→sqlite endpoints only.
"""

from __future__ import annotations

import pytest

from services.data_contract import (
    ColumnRule,
    ContractEnforcer,
    ContractStatus,
    ContractViolation,
    DataContract,
)
from src.transfer.models import EndpointConfig, TransferRequest


def _request() -> TransferRequest:
    return TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        destination=EndpointConfig(kind="database", format="sqlite", table="orders"),
    )


def _signed() -> DataContract:
    return DataContract(
        name="sqlite-orders",
        status=ContractStatus.SIGNED,
        source={"format": "csv"},
        destination={"format": "sqlite"},
        columns=[
            ColumnRule(
                source_name="id",
                target_name="id",
                source_type="INTEGER",
                target_type="INTEGER",
                nullable=False,
                primary_key=True,
            ),
            ColumnRule(
                source_name="email",
                target_name="email",
                source_type="TEXT",
                target_type="TEXT",
                nullable=True,
            ),
        ],
        mappings=[
            {"source": "id", "target": "id", "confidence": 1.0},
            {"source": "email", "target": "email", "confidence": 1.0},
        ],
    )


def test_signed_sqlite_contract_passes_matching_schema():
    enforcer = ContractEnforcer(_signed())
    enforcer.enforce(
        _request(),
        sample_schema={"id": "INTEGER", "email": "TEXT"},
        require_signed=True,
    )


def test_signed_sqlite_contract_blocks_missing_required_column():
    enforcer = ContractEnforcer(_signed())
    with pytest.raises(ContractViolation) as exc:
        enforcer.enforce(
            _request(),
            sample_schema={"email": "TEXT"},
            require_signed=True,
        )
    assert exc.value.violations[0]["rule"] == "required_column"


def test_unsigned_sqlite_contract_blocks_when_require_signed():
    contract = _signed()
    contract.status = ContractStatus.DRAFT
    enforcer = ContractEnforcer(contract)
    with pytest.raises(ContractViolation) as exc:
        enforcer.enforce(
            _request(),
            sample_schema={"id": "INTEGER", "email": "TEXT"},
            require_signed=True,
        )
    assert exc.value.violations[0]["rule"] == "contract_not_signed"
