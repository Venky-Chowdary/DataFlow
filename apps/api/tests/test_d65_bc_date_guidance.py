"""D65 — a BC date blocked with misleading guidance (or not at all).

PostgreSQL renders ``0044-03-15 BC``. Read as AD that is year 44, so MySQL's
range message blamed "year 44" (a year the source never held) and SQL Server,
whose range includes 44 AD, accepted the wire and failed later at the engine.
"""

from __future__ import annotations

import pytest

from connectors.sql_temporal import coerce_sql_temporal


@pytest.mark.parametrize(
    ("engine", "base"),
    [("mysql", "DATE"), ("mysql", "DATETIME"), ("sqlserver", "DATE")],
)
def test_bc_date_is_refused_as_bc_with_the_real_reason(engine, base):
    with pytest.raises(ValueError) as refused:
        coerce_sql_temporal("0044-03-15 BC", base, engine=engine)
    text = str(refused.value)
    assert "BC date" in text
    assert "no BC era" in text
    assert "year 44" not in text


def test_engine_with_native_bc_support_keeps_the_literal():
    assert coerce_sql_temporal("0044-03-15 BC", "DATE", engine="postgresql") == (
        "0044-03-15 BC"
    )
