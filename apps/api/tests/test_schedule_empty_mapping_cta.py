"""Empty-mapping parks must name Transfer Studio, not a phantom Validate button."""

from __future__ import annotations

from services.failure_retry_policy import DETERMINISTIC, classify_failure
from services.schedule_mapping_contract import (
    EMPTY_MAPPING_CODE,
    EMPTY_MAPPING_CORRECTIVE,
    EMPTY_MAPPING_REFUSAL,
    is_empty_mapping_refusal,
)


def test_empty_mapping_message_is_recognised():
    assert is_empty_mapping_refusal(EMPTY_MAPPING_REFUSAL) is True
    assert is_empty_mapping_refusal("connection reset") is False


def test_classify_empty_mapping_names_studio_not_job_validate():
    result = classify_failure(error=EMPTY_MAPPING_REFUSAL, phase="validate", rows_written=0)
    assert result.kind == DETERMINISTIC
    assert "Transfer Studio" in result.corrective_action
    assert "Open Validate for this job" not in result.corrective_action


def test_corrective_copy_refuses_a_signature():
    assert "signature" in EMPTY_MAPPING_CORRECTIVE.lower()
    assert EMPTY_MAPPING_CODE == "EMPTY_MAPPING_CONTRACT"
