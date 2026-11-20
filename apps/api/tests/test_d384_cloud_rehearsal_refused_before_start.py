"""D384: a Cloud exit rehearsal with nowhere to put its extract says so before it starts.

QA (Nov 16): exs_d83afe4bdf3a -> rehearsal exr_1acc64d8bb57 "failed: 1 of 1
table(s) could not be extracted ... see the server log ... Reference
7a328b24". The extract job targets a file export, which Datawrap Cloud
refuses unless the deployment has an export bucket.

8252bd3b5 raised the Cloud refusal inside ``extract_request``. That runs in
``start``'s per-table ``try``, whose ``except`` passes the error to
``safe_error``, which keeps only the class name and answers "see the server
log for the details": the refusal never reached the operator, and a
rehearsal record was created that could only fail. The refusal also told the
operator to "send the rows to an S3 ... or SFTP destination", which a
rehearsal does not let them choose.

Now ``start`` checks first (R8: a knowable refusal before any work): no
rehearsal is recorded, the system is not left claimed, and the 422 names
the real reason and the remedy an administrator can apply. With an export
bucket the rehearsal runs (test_exit_rehearsal_e2e covers that path).
"""

from __future__ import annotations

import pytest

from tests.exit_background_guard import _no_exit_background_work_outlives_a_test  # noqa: F401
from tests.test_exit_rehearsal_e2e import _system, world  # noqa: F401 - fixture


@pytest.fixture
def cloud_without_bucket(monkeypatch):
    monkeypatch.setenv("DATAWRAP_LOCAL_FILE_DESTINATIONS", "off")
    # The fixture's SQLite source stays readable: this test is about the extract.
    monkeypatch.setenv("DATAWRAP_SQLITE_DESTINATION", "on")
    for name in ("DATAWRAP_EXPORT_BUCKET", "DATAFLOW_EXPORT_BUCKET"):
        monkeypatch.delenv(name, raising=False)


def test_start_refuses_with_the_real_reason_and_records_nothing(world, cloud_without_bucket):
    from services import exit_store
    from services.exit_inventory import ExitInvalid, rehearsals_of
    from services.exit_rehearsal import start

    system = _system()
    with pytest.raises(ExitInvalid) as raised:
        start(system, actor="owner@example.com")
    message = str(raised.value)
    assert "see the server log" not in message
    assert "export bucket" in message and "DATAWRAP_EXPORT_BUCKET" in message
    # No remedy the rehearsal cannot offer.
    assert "SFTP" not in message
    assert rehearsals_of(system["id"]) == []
    stored = exit_store.get(exit_store.SYSTEMS, system["id"])
    assert not stored.get("running_rehearsal_id")  # the system is not left claimed


def test_the_route_answers_422_with_that_sentence(world, cloud_without_bucket):
    from fastapi.testclient import TestClient

    from src.main import app

    system = _system()
    res = TestClient(app).post(f"/api/v1/exit/systems/{system['id']}/rehearsals")
    assert res.status_code == 422, res.text
    assert "export bucket" in res.json()["detail"]
    assert res.json()["error"]["reference"]
