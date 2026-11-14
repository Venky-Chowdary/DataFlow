"""UX-34: the event log users read showed "postgresql 127.0.0.1:5432"."""

from types import SimpleNamespace

from services.engine_names import engine_display_name
from src.transfer.source_peek import source_wait_message
from src.transfer.stream import _first_page_message


def test_first_page_line_names_engine_and_database_not_the_address():
    run = SimpleNamespace(cfg=SimpleNamespace(src_type="postgresql", src_cfg={"host": "10.0.0.5", "port": 5432, "database": "sales"}))
    msg = _first_page_message(run)
    assert msg.startswith("Reading from the source (PostgreSQL · sales)")
    assert "10.0.0.5" not in msg and "5432" not in msg


def test_wait_line_names_engine_and_database_not_the_address():
    msg = source_wait_message("mysql", {"host": "db.internal", "port": 3306, "database": "w"}, "Analyzing source table")
    assert "(MySQL · w)" in msg and "db.internal" not in msg and "3306" not in msg


def test_engine_names():
    assert engine_display_name("sqlserver") == "SQL Server"
    assert engine_display_name("unknown_engine") == "unknown_engine"
