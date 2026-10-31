"""D212 follow-up: the code-page sink collapse is value-aware for unmeasured sources.

f6631888 made every source that does not declare a single-byte charset a
collapse on an existing SQL Server code-page carrier, even when every sampled
value encodes in the page (scenario matrix: ASCII 'ABCDEFGHIJ' VARCHAR -> TEXT
blocked). An unmeasured source (an Oracle VARCHAR2, a MySQL column with no
stated charset, an unbound engine) is a collapse only when a value it holds is
outside the destination page; a source measured Unicode stays a collapse on an
unread page; a declared single-byte source never is.
"""

from __future__ import annotations

import pytest

from services.type_system import code_page_sink_would_collapse


@pytest.mark.parametrize("source_db", ["", "oracle", "mysql"])
def test_unmeasured_source_is_judged_by_its_values(source_db):
    kw = dict(dest_db="sqlserver", source_db=source_db)
    assert not code_page_sink_would_collapse("VARCHAR(10)", "TEXT", **kw)
    assert not code_page_sink_would_collapse(
        "VARCHAR(10)", "VARCHAR(10)", population=["ABCDEFGHIJ", "café", None], **kw)
    assert code_page_sink_would_collapse(
        "VARCHAR(10)", "VARCHAR(10)", population=["東京"], **kw)
    assert code_page_sink_would_collapse(
        "VARCHAR(10)", "VARCHAR(10)", population=["🚀"], **kw)
    assert code_page_sink_would_collapse(
        "VARCHAR(10)", "VARCHAR(10)", population=["hello​world"], **kw)


def test_measured_unicode_source_on_an_unread_page_stays_a_collapse():
    assert code_page_sink_would_collapse("VARCHAR(10)", "VARCHAR(10)",
                                         dest_db="sqlserver", source_db="postgresql")
    assert code_page_sink_would_collapse(
        "VARCHAR(10) CHARACTER SET utf8mb4", "VARCHAR(10)",
        dest_db="sqlserver", source_db="mysql")
    # ...but measured values that all fit the page clear it (DEF-R20-001).
    assert not code_page_sink_would_collapse(
        "VARCHAR(10)", "VARCHAR(10)", dest_db="sqlserver", source_db="postgresql",
        population=["12345"])


def test_declared_single_byte_source_and_national_or_utf8_targets_never_collapse():
    assert not code_page_sink_would_collapse(
        "VARCHAR(10) CHARACTER SET latin1", "VARCHAR(10)", dest_db="sqlserver",
        source_db="mysql", population=["café"])
    for target in ("NVARCHAR(10)", "VARCHAR(10) COLLATE Latin1_General_100_CI_AS_SC_UTF8"):
        assert not code_page_sink_would_collapse(
            "VARCHAR2(10)", target, dest_db="sqlserver", source_db="oracle",
            population=["東京"])
