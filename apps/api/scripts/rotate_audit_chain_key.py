"""Rotate the audit chain's signing key — platform admins only, audited (QA r12 #3).

    python scripts/rotate_audit_chain_key.py --actor admin@example.com \\
        --reason "Quarterly rotation per security policy"

Starts a new current key in the deployment's persisted keyring
(``audit_chain_keys``, see services/audit_chain_keystore.py). Every earlier key
stays in the keyring, retired, so the records it signed keep verifying.

* ``--actor`` must be a platform admin of this deployment; anyone else is
  refused, and the refusal is itself written to the audit chain.
* ``--reason`` (at least 10 characters) is recorded with the rotation.
* A deployment that pins ``DATAFLOW_AUDIT_HMAC_KEY`` signs with that key, so a
  keyring rotation would change nothing: the script refuses and says how to
  rotate a pinned key (new value in ``..._AUDIT_HMAC_KEY``, old value appended
  to ``..._AUDIT_HMAC_RETIRED_KEYS``).

The success event is ``audit.chain_key.rotated``; output names key ids only,
never key material. The Mongo URI comes from ``--uri`` or, like the API, from
``MONGODB_URI`` / ``MONGO_URL``; ``--db`` defaults to ``datatransfer``.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

API = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(API))

MIN_REASON = 10
ROTATED = "audit.chain_key.rotated"
DENIED = "audit.chain_key.rotate.denied"


def _audit(action: str, actor: str, details: dict, *, outcome: str) -> None:
    from services.audit_log import append_audit_event

    append_audit_event(
        action=action,
        resource="audit_chain_keys",
        actor=actor,
        details=details,
        outcome=outcome,
        principal={"actor_type": "user", "actor_id": actor, "auth_method": "cli"},
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--actor", required=True, help="e-mail of the platform admin rotating")
    parser.add_argument("--reason", required=True, help=f"why (at least {MIN_REASON} characters)")
    parser.add_argument("--uri", default="", help="Mongo URI (default: MONGODB_URI / MONGO_URL env)")
    parser.add_argument("--db", default="datatransfer", help="database name (default: datatransfer)")
    args = parser.parse_args(argv)

    actor = args.actor.strip().lower()
    reason = " ".join(args.reason.split())
    if len(reason) < MIN_REASON:
        print(f"error: --reason must be at least {MIN_REASON} characters", file=sys.stderr)
        return 2

    from services import mongodb_service
    from services.platform_config import mongodb_uri

    svc = mongodb_service.MongoDBService(args.uri or mongodb_uri())
    svc.db_name = args.db
    if not svc.connect():
        print("error: cannot reach MongoDB; the keyring lives there", file=sys.stderr)
        return 2
    mongodb_service._mongodb_service = svc

    from services import audit_chain_keys
    from services.audit_chain_keystore import KeystoreUnavailable
    from src.services.auth_service import lookup_user

    user = lookup_user(actor) or {}
    if str(user.get("role") or "") != "admin":
        _audit(DENIED, actor or "unknown", {"reason": reason, "why": "not a platform admin"},
               outcome="denied")
        print(f"error: {actor!r} is not a platform admin of this deployment; refused (audited)",
              file=sys.stderr)
        return 3
    if audit_chain_keys.describe()["source"] == "configured":
        print(
            "error: this deployment pins DATAFLOW_AUDIT_HMAC_KEY, which signs every record; "
            "rotate it by setting a new value and appending the old one to "
            "DATAFLOW_AUDIT_HMAC_RETIRED_KEYS. Nothing changed.",
            file=sys.stderr,
        )
        return 4
    try:
        previous = audit_chain_keys.describe()["current_key_id"]
        status = audit_chain_keys.rotate_signing_key(actor=actor)
    except KeystoreUnavailable as exc:
        print(f"error: audit keyring unavailable: {exc}", file=sys.stderr)
        return 2
    summary = {
        "previous_key_id": previous,
        "new_key_id": status["current_key_id"],
        "keys_in_keyring": len(status["keys"]),
        "reason": reason,
    }
    _audit(ROTATED, actor, summary, outcome="succeeded")
    print(json.dumps({"rotated": True, "actor": actor, **summary}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
