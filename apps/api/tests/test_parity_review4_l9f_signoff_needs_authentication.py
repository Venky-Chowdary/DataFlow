"""Re-review 4, 9f (Low): sign-off says plainly that it needs authentication.

With authentication off every caller is "anonymous": the first role signed, and the second was
refused with "One person signs one role; ask another approver", which nobody could act on, leaving
a half-signed report. Sign-off now refuses an anonymous signer up front with a reason that names
the cause; the one-person-one-role rule is unchanged.
"""

from __future__ import annotations

import pytest

from services.audit_log import actor_from_request
from tests.test_parity_review_f5_signoff_integrity import _open_for_signing, store  # noqa: F401


def test_the_auth_off_actor_is_anonymous():
    class _Req:
        class state:  # noqa: N801 - mimics request.state
            user = None

    assert actor_from_request(_Req()) == "anonymous"


@pytest.mark.parametrize("actor", ["anonymous", ""])
def test_an_anonymous_signer_is_refused_before_any_role_is_recorded(store, actor):  # noqa: F811
    from services.parity_signoff import sign_off
    from services.parity_state import ParityForbidden

    job_id = _open_for_signing(store)
    with pytest.raises(ParityForbidden, match="needs authentication"):
        sign_off(job_id, role="engineering", actor=actor, signer_role="admin")
    assert not (store.get_job(job_id).get("parity") or {}).get("approvals")


def test_one_person_still_signs_one_role(store):  # noqa: F811
    from services.parity_signoff import sign_off
    from services.parity_state import ParityConflict

    job_id = _open_for_signing(store)
    sign_off(job_id, role="engineering", actor="eng@example.com", signer_role="admin")
    with pytest.raises(ParityConflict, match="One person signs one role"):
        sign_off(job_id, role="data_owner", actor="eng@example.com", signer_role="admin")
