#!/usr/bin/env python3
"""Break-glass: remove an account's second factor from the API host (QA S7).

For the one case the REST reset cannot cover — the only platform admin (for
example the environment bootstrap admin) lost both their authenticator and
their recovery codes. Run it on a machine with the API's environment (the same
metadata database and DATAFLOW_* settings)::

    python scripts/mfa_reset.py --email admin@example.com \\
        --reason "SR-1042: phone lost, codes not stored; verified in person" \\
        --operator "jane.ops"

It removes the factor, pending setup and recovery codes, clears a lockout,
revokes the account's sessions, and writes ``auth.mfa.reset`` to the audit
chain with the operator, the reason and ``via: cli``. Nothing is printed
about the secret. Prefer ``POST /api/v1/iam/mfa/reset`` whenever another
platform admin can sign in.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--email", required=True, help="account whose second factor is reset")
    parser.add_argument("--reason", required=True, help="why (at least 10 characters; audited)")
    parser.add_argument("--operator", required=True, help="who is running this (audited)")
    args = parser.parse_args(argv)

    reason = " ".join(args.reason.split())
    operator = " ".join(args.operator.split())
    if len(reason) < 10:
        parser.error("--reason must be at least 10 characters")
    if not operator:
        parser.error("--operator is required")

    from services import mfa
    from services.audit_coverage import record_security_event
    from services.user_store import normalize_email
    from src.services.auth_service import revoke_sessions_for_email

    email = normalize_email(args.email)
    had = mfa.remove_factor(email, clear_lockout=True)
    revoked = revoke_sessions_for_email(email)
    record_security_event(
        "auth.mfa.reset",
        outcome="succeeded",
        actor=f"cli:{operator}"[:254],
        resource="scripts/mfa_reset.py",
        level="warn",
        details={"target": email, "reason": reason, "via": "cli", "had_factor": had, "sessions_revoked": revoked},
    )
    print(f"two-factor reset for {email}: had_factor={had} sessions_revoked={revoked} (audited as auth.mfa.reset)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
