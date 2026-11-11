"""Pilot ground truth — uploaded datasets belong to a workspace (audit H4).

The dataset feeder listed every file in a set of hard-coded directories —
including ``apps/api/tests/fixtures`` — to every caller: a viewer in workspace
B was shown a row of workspace A's payroll upload, a per-job ``hydrate_``
spill was offered as a dataset, and the production image would present the
repository's test fixtures as "your datasets" while never reading the
configured upload directory.
"""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path

import pytest

from tests.helpers import pilot_truth as pt

PAYROLL = b"employee,ssn,salary\nAlice Roe,123-45-6789,182000\nBob Poe,987-65-4321,91000\n"
SECRETS = ("123-45-6789", "182000", "Alice Roe")


@contextmanager
def acting_as(workspace: str, actor: str, role: str = "admin"):
    from services.workspace_filter import WorkspaceFilter, bound_workspace_filter
    from src.ai.copilot.tool_permissions import caller_role

    with bound_workspace_filter(WorkspaceFilter.only(workspace), stamp=workspace, actor=actor), caller_role(role):
        yield


def _uploads(datasets: list[dict]) -> list[str]:
    return sorted(d["name"] for d in datasets if d["source"] == "upload")


@pytest.fixture
def upload_dir(monkeypatch, tmp_path):
    """An empty upload directory configured the way a deployment configures it."""
    from services import file_parser
    from src.ai.copilot.data_analyst import get_data_analyst

    pt.reset_workspace(monkeypatch)
    root = tmp_path / "uploads"
    root.mkdir()
    monkeypatch.setenv("DATAFLOW_UPLOAD_DIR", str(root))
    monkeypatch.delenv("DATAFLOW_PILOT_DATASET_DIRS", raising=False)
    monkeypatch.setattr(file_parser, "UPLOAD_DIR", root)
    monkeypatch.setattr(file_parser, "REGISTRY_PATH", tmp_path / "upload_registry.json")
    monkeypatch.setattr(file_parser, "_file_registry", {})
    analyst = get_data_analyst()
    monkeypatch.setattr(analyst, "feeder", type(analyst.feeder)())
    monkeypatch.setattr(analyst, "_cache", {})
    return root


@pytest.fixture
def payroll_in_a(upload_dir):
    from services.file_parser import store_upload

    with acting_as("ws-A", "alice@a.example"):
        record = store_upload("payroll_acme.csv", PAYROLL)
    assert Path(record["path"]).parent == upload_dir and record["workspace_id"] == "ws-A"
    return record


def test_the_feeder_reads_the_configured_upload_directory_and_no_test_fixtures(upload_dir):
    from src.ai.training.universal_data_feeder import UniversalDataFeeder

    dirs = [Path(d).resolve() for d in UniversalDataFeeder().upload_dirs]
    assert dirs == [upload_dir.resolve()], dirs
    assert not any("fixtures" in str(d) for d in dirs)


def test_an_empty_workspace_has_no_uploaded_datasets(upload_dir):
    from src.ai.copilot.data_analyst import get_data_analyst

    assert _uploads(get_data_analyst().list_datasets()) == []
    response = pt.ask(pt.agent(), "list my datasets")
    for fixture in ("sample_payments", "sample_retail", "rbac_route_permissions", "mapping_golden"):
        assert fixture not in response.answer


def test_the_owning_workspace_sees_its_upload(payroll_in_a):
    from src.ai.copilot.data_analyst import get_data_analyst

    with acting_as("ws-A", "alice@a.example"):
        names = _uploads(get_data_analyst().list_datasets())
        response = pt.ask(pt.agent(), "analyze the payroll_acme dataset")
    assert len(names) == 1 and names[0].endswith("payroll_acme")
    assert "3 columns" in response.answer and "ssn" in response.answer


@pytest.mark.parametrize(
    "question",
    ["list my datasets", "analyze the payroll_acme dataset", "search my data for salary", "what datasets have pii"],
)
def test_another_workspace_never_sees_the_upload(payroll_in_a, question):
    from src.ai.copilot.data_analyst import get_data_analyst

    with acting_as("ws-B", "bob@b.example", role="viewer"):
        assert _uploads(get_data_analyst().list_datasets()) == []
        response = pt.ask(pt.agent(), question)
    # Pilot may echo the name the caller typed ("dataset 'payroll_acme' not
    # found"); it must not show the stored upload or anything read from it.
    assert payroll_in_a["file_id"] not in response.answer, response.answer
    if "payroll" not in question:
        assert "payroll" not in response.answer.lower(), response.answer
    for secret in (*SECRETS, "3 columns", "ssn"):
        assert secret not in response.answer, response.answer


def test_a_same_named_upload_in_another_workspace_is_analyzed_from_its_own_bytes(payroll_in_a):
    from services.file_parser import store_upload

    with acting_as("ws-B", "bob@b.example"):
        store_upload("payroll_acme.csv", b"employee,grade\nZed Quill,7\n")
        response = pt.ask(pt.agent(), "analyze the payroll_acme dataset")
    assert "2 columns" in response.answer and "grade" in response.answer
    for secret in SECRETS:
        assert secret not in response.answer


def test_a_job_hydrate_spill_is_not_a_dataset(upload_dir):
    from services.transfer_file_staging import is_transfer_staging_file
    from src.ai.copilot.data_analyst import get_data_analyst

    (upload_dir / "hydrate_0123456789ab_orders_export.csv").write_text("id,card_number\n1,4111111111111111\n")
    (upload_dir / "xfer_ab12_orders.csv").write_text("id,card_number\n2,4222222222222222\n")
    assert is_transfer_staging_file("hydrate_0123456789ab_orders_export.csv")
    assert _uploads(get_data_analyst().list_datasets()) == []
    response = pt.ask(pt.agent(), "analyze hydrate_0123456789ab_orders_export")
    assert "4111111111111111" not in response.answer


def test_a_file_with_no_upload_record_is_hidden_from_a_workspace_bound_caller(upload_dir):
    from src.ai.copilot.data_analyst import get_data_analyst

    (upload_dir / "dropped_by_hand.csv").write_text("a,b\n1,2\n")
    analyst = get_data_analyst()
    assert _uploads(analyst.list_datasets()) == ["dropped_by_hand"], "single-tenant callers keep seeing it"
    analyst.feeder.invalidate_cache()
    with acting_as("ws-B", "bob@b.example"):
        assert _uploads(analyst.list_datasets()) == []
