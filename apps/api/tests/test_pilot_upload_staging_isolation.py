"""Transfer job spills must never surface as Pilot datasets.

The transfer engine spills each job's file source into the shared upload
directory as ``xfer_<token>_<name>``. The Pilot data feeder scans that same
directory, so without a filter one workspace's staged source file becomes a
queryable "dataset" for every Pilot user, and a dataset lookup resolves against
a name the asker never uploaded (``compare orders and products`` silently
succeeding against another job's ``products`` spill instead of failing closed).
"""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.transfer_file_staging import (  # noqa: E402
    TRANSFER_STAGING_PREFIX,
    is_transfer_staging_file,
)
from src.ai.copilot.data_analyst import CopilotDataAnalyst  # noqa: E402
from src.ai.training.universal_data_feeder import UniversalDataFeeder  # noqa: E402

_CSV = b"id,name\n1,widget\n"


def _uploads(tmp_path: Path) -> Path:
    d = tmp_path / "uploads"
    d.mkdir()
    return d


def test_staging_prefix_classifier() -> None:
    assert is_transfer_staging_file(f"{TRANSFER_STAGING_PREFIX}abc_products.csv")
    assert not is_transfer_staging_file("products.csv")
    assert not is_transfer_staging_file("")


def test_job_spill_is_not_a_dataset(tmp_path: Path) -> None:
    up = _uploads(tmp_path)
    (up / "xfer_deadbeef_products.csv").write_bytes(_CSV)
    (up / "my_orders.csv").write_bytes(_CSV)

    feeder = UniversalDataFeeder(upload_dirs=[str(up)])
    names = [n for n, _ in feeder.list_dataset_names()]
    assert "my_orders" in names
    assert not [n for n in names if n.startswith(TRANSFER_STAGING_PREFIX)]

    scanned = [s.name for s in feeder.scan_uploads()]
    assert scanned == ["my_orders"]


def test_dataset_lookup_fails_closed_on_other_jobs_spill(tmp_path: Path) -> None:
    up = _uploads(tmp_path)
    (up / "xfer_deadbeef_products.csv").write_bytes(_CSV)

    analyst = CopilotDataAnalyst()
    analyst.feeder = UniversalDataFeeder(upload_dirs=[str(up)])
    assert analyst.resolve_dataset("products") is None
