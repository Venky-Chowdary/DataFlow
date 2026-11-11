"""Re-review 3, M2: an error keeps its class, its engine code and a fixed sentence — no value.

``safe_result`` stripped value shapes it knew, and let others through
(MySQL ``Incorrect integer value: 'alice@example.com'``, ``Incorrect datetime
value: '1985-02-30 …'``, ``Invalid timestamp: '… SSN 123-45-6789'``). Fix
results now come from an allowlist: exception class + SQLSTATE / errno /
ORA- / Msg number + a fixed message per code; the full text is logged only.
"""

from __future__ import annotations

import logging

import pytest

from services.parity_errors import safe_error

CASES = [
    ("(1366, \"Incorrect integer value: 'alice@example.com' for column 'id' at row 1\")", "MySQL 1366",
     ["alice@example.com"]),
    ("(1292, \"Incorrect datetime value: '1985-02-30 00:00:00' for column 'dob' at row 7\")", "MySQL 1292",
     ["1985-02-30"]),
    ("Invalid timestamp: '2024-13-45 SSN 123-45-6789'", "", ["123-45-6789", "2024-13-45"]),
    ("ORA-01400: cannot insert NULL into (\"S\".\"T\".\"C\")", "ORA-01400", []),
    ("invalid input syntax for type integer: \"4111111111111111\"", "22P02", ["4111111111111111"]),
    ("duplicate key value violates unique constraint \"t_pkey\"\nDETAIL:  Key (email)=(bob@example.com) already exists.",
     "23505", ["bob@example.com"]),
    ("Conversion failed when converting the nvarchar value 'Jane Doe' to data type int.", "Msg 245", ["Jane Doe"]),
    ("new row violates check constraint \"c\" DETAIL: Failing row contains (5, secret).", "23514", ["secret"]),
    ("re-sync job abc failed: Value 'carol@example.com' for column email exceeds 10 chars", "",
     ["carol@example.com"]),
    ("psycopg2.errors.StringDataRightTruncation: value too long for type character varying(5) CONTEXT: COPY t, "
     "line 3, column note: \"dave@example.com\"", "22001", ["dave@example.com"]),
]


@pytest.mark.parametrize("text,code,values", CASES)
def test_only_class_code_and_a_fixed_sentence_survive(text, code, values, caplog):
    with caplog.at_level(logging.WARNING, logger="services.parity_errors"):
        out = safe_error(text, what="The widen column")
    for value in values:
        assert value not in out, out
    if code:
        assert code in out
    assert out.startswith("The widen column failed")
    assert text[:40] in caplog.text  # the full text is in the server log


def test_an_exception_keeps_its_class():
    class UniqueViolation(Exception):
        pass

    out = safe_error(UniqueViolation("duplicate key value violates unique constraint \"u\" DETAIL: Key (e)=(x@y.z)"),
                     what="The re-sync start")
    assert out == "The re-sync start failed (UniqueViolation 23505): a unique key already holds this value (duplicate key)."
