"""A resumed pass that wrote only the tail must re-read every SQL duplex
source: its write-pass digest covers the tail, Gate-8 compares the whole
destination, so staying on it fails a complete table (D213 SQL Server, then
Oracle -> PostgreSQL)."""

import pytest

from services.source_reread import reread_loop_supports


@pytest.mark.parametrize("src", ["oracle", "sqlserver", "mssql", "postgresql", "mysql"])
def test_partial_write_pass_rereads_every_sql_duplex_source(src: str) -> None:
    assert reread_loop_supports(src, partial_write_pass=True)


def test_oracle_outside_resume_stays_on_write_pass() -> None:
    assert not reread_loop_supports("oracle", partial_write_pass=False)
