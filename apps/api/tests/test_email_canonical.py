"""P2-1 — canonical_email: the dedupe key for signups.

J.Doe+x@gmail.com collides with jdoe@gmail.com; +tag folding applies to
providers that support sub-addressing only; case folds everywhere.
"""

from __future__ import annotations

from services.email_canonical import canonical_email


def test_gmail_dots_and_plus_fold():
    assert canonical_email("J.Doe+x@gmail.com") == "jdoe@gmail.com"


def test_googlemail_aliases_gmail():
    assert canonical_email("j.doe+x@googlemail.com") == "jdoe@gmail.com"


def test_case_folds_everywhere():
    assert canonical_email("Ada@EXAMPLE.com") == "ada@example.com"


def test_plus_fold_on_supporting_providers():
    for domain in ("outlook.com", "yahoo.com", "icloud.com", "proton.me",
                   "fastmail.com", "zoho.com"):
        assert canonical_email(f"ada+tag@{domain}") == f"ada@{domain}", domain


def test_plus_not_folded_on_other_domains():
    # corp.example is not a known plus-addressing provider — the tag stays
    assert canonical_email("ada+tag@corp.example") == "ada+tag@corp.example"


def test_dots_kept_on_non_gmail():
    assert canonical_email("a.da@outlook.com") == "a.da@outlook.com"


def test_malformed_returns_input():
    assert canonical_email("not-an-email") == "not-an-email"
