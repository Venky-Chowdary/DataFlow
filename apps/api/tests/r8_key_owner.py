"""Give fixture API keys an accountable creator (round 8).

A key whose creator has no account and no membership is refused
(``services.effective_principal.api_key_creator_is_accountable``). Fixtures
that mint keys for made-up creators make those creators stored platform-admin
accounts, which keeps each key's own role in charge, as before.
"""

from __future__ import annotations


def ensure_key_owners(*emails: str, role: str = "admin") -> None:
    from services import user_store

    for email in emails:
        if user_store.get_user_record(email) is None:
            user_store.create_user(
                email=email, role=role, password="Key-Owner-Password-2026!", created_by="fixture"
            )
