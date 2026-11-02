"""Claims the first-party generator may never invent.

The product bar is the same as the transfer engine: silent invention is a
data-loss class failure. The pointer-generator can only *copy* evidence or
emit closed glue. These patterns are a second lock on the three claims that
keep getting asked as if they were shipped:

* **dbt** — not a product capability. A fluent sentence that names it is a lie.
* **SSH / tunnel** — not a shipped connection path. Wave3 OFF is intentional.
* **exactly-once** — CDC default is at-least-once upsert on ``_df_lsn`` until
  a named matrix proves otherwise. Generating "exactly-once" from
  "at-least-once" evidence is the failure mode this gate exists for.

If the *evidence* already contains the token (for example a passage that says
"we do not claim exactly-once"), restating it is allowed. Inventing it is not.
"""

from __future__ import annotations

import re

# Each pair is (surface pattern, stable claim id). The id is what tests and
# logs report; the pattern is what the decoder must not emit unless evidence
# already matches it.
FORBIDDEN_CLAIM_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"\bdbt\b", re.I), "dbt"),
    (
        re.compile(
            r"\bssh(?:\s+tunnels?)?\b"
            r"|\btunnels?\b"
            r"|\btunnel(?:ing|led)\b",
            re.I,
        ),
        "ssh",
    ),
    (re.compile(r"\bexactly[\s-]?once\b", re.I), "exactly-once"),
)


def invented_claims(text: str, evidence: str) -> tuple[str, ...]:
    """Claim ids that appear in ``text`` but not in ``evidence``."""
    body = text or ""
    ev = evidence or ""
    found: list[str] = []
    for pattern, claim_id in FORBIDDEN_CLAIM_PATTERNS:
        if pattern.search(body) and not pattern.search(ev):
            found.append(claim_id)
    return tuple(found)


def claims_are_grounded(text: str, evidence: str) -> bool:
    """Whether ``text`` introduces no forbidden claim absent from evidence."""
    return not invented_claims(text, evidence)
