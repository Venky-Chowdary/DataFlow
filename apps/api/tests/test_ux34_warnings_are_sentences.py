"""UX-34: a structured warning reached the screen as a Python dict repr."""

from services.warning_text import warning_text


def test_a_dict_warning_reads_as_its_message():
    w = {"id": "expected_file_not_arrived", "severity": "warn", "message": "The 06:00 file has not arrived yet."}
    assert warning_text(w) == "The 06:00 file has not arrived yet."


def test_a_dict_without_a_message_becomes_clauses_not_a_repr():
    text = warning_text({"id": "x", "column": "email", "rows_affected": 3})
    assert text == "column: email; rows affected: 3"
    assert "{" not in text and "'" not in text


def test_strings_and_lists_pass_through():
    assert warning_text("  plain  ") == "plain"
    assert warning_text(["a", {"message": "b"}]) == "a, b"
    assert warning_text(None) == ""


def test_integrity_summary_never_carries_a_repr(monkeypatch):
    import services.data_integrity as di

    src = open(di.__file__, encoding="utf-8").read()
    assert "all_warnings.append(str(w))" not in src
    assert "warning_text(w)" in src
