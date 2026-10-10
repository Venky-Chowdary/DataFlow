from __future__ import annotations

from pathlib import Path

import pytest

from connectors.sdk import get_descriptor, list_descriptors
from tests.connector_certification.cases import build_certification_cases
from tests.connector_certification.harness import certify


@pytest.mark.parametrize(
    "connector_id",
    [descriptor.id for descriptor in list_descriptors()],
)
def test_every_registered_sdk_connector_has_synthetic_certification(
    connector_id: str,
    tmp_path: Path,
) -> None:
    cases = build_certification_cases(tmp_path)
    assert connector_id in cases, f"registered connector {connector_id!r} has no certification case"
    report = certify(cases[connector_id])
    descriptor = get_descriptor(connector_id)
    assert descriptor is not None
    assert not report.failed_steps, report.to_dict()
    for step in report.steps:
        if step.status == "skip":
            assert descriptor.certification_skips[step.name] == step.reason
