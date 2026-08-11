"""Drift must address the column the destination actually stores.

Reflection normalises Oracle's ``LABEL`` to ``label``; quoting that spelling
back asks for a different, case-sensitive column (ORA-00904) and adding it
leaves a column the client's own ``SELECT extra`` cannot see.
"""

from sqlalchemy.dialects import oracle, postgresql
from sqlalchemy.sql.elements import quoted_name

from connectors.generic_sql import _resolve_physical_column_idents
from connectors.schema_drift import _quote_added_columns, existing_column_index


class _Dialect:
    def __init__(self, name: str) -> None:
        self.name = name

    @staticmethod
    def denormalize_name(name: str) -> str:
        return oracle.dialect().denormalize_name(name)


class _Engine:
    def __init__(self, name: str) -> None:
        self.dialect = _Dialect(name)


def test_existing_column_index_matches_folded_catalog_name() -> None:
    index = existing_column_index("oracle", ["label", "id"])
    assert index["LABEL".casefold()] == "label"
    assert index["label"] == "label"


def test_existing_column_index_keeps_postgres_case_distinct() -> None:
    index = existing_column_index("postgresql", ["Foo"])
    assert index == {"Foo": "Foo"}
    assert "foo" not in index


def test_added_columns_follow_a_folded_table() -> None:
    assert _quote_added_columns(oracle.dialect(), ["id", "label"]) is None


def test_added_columns_stay_quoted_beside_quoted_lowercase_columns() -> None:
    stored_lower = quoted_name("label", True)
    assert _quote_added_columns(oracle.dialect(), ["id", stored_lower]) is True


def test_added_columns_stay_quoted_on_a_non_folding_dialect() -> None:
    assert _quote_added_columns(postgresql.dialect(), ["id", "label"]) is True


def test_added_columns_quote_when_the_table_is_unread() -> None:
    assert _quote_added_columns(oracle.dialect(), []) is True


def test_resolve_columns_binds_the_stored_spelling(monkeypatch) -> None:
    engine = _Engine("oracle")
    monkeypatch.setattr(
        "connectors.generic_sql._stored_column_spellings",
        lambda *_args, **_kw: {"id": "ID", "label": "LABEL"},
    )
    assert _resolve_physical_column_idents(engine, "T", "S", ["id", "label"]) == {
        "id": "ID",
        "label": "LABEL",
    }


def test_resolve_columns_folds_a_column_drift_will_add(monkeypatch) -> None:
    engine = _Engine("oracle")
    monkeypatch.setattr(
        "connectors.generic_sql._stored_column_spellings",
        lambda *_args, **_kw: {"id": "ID"},
    )
    assert _resolve_physical_column_idents(engine, "T", "S", ["id", "extra"]) == {
        "id": "ID",
        "extra": "EXTRA",
    }


def test_resolve_columns_keeps_lowercase_beside_a_lowercase_table(monkeypatch) -> None:
    engine = _Engine("oracle")
    monkeypatch.setattr(
        "connectors.generic_sql._stored_column_spellings",
        lambda *_args, **_kw: {"id": "id"},
    )
    assert _resolve_physical_column_idents(engine, "T", "S", ["id", "extra"]) == {}


def test_resolve_columns_leaves_non_folding_dialects_alone() -> None:
    assert _resolve_physical_column_idents(_Engine("postgresql"), "T", "S", ["a"]) == {}


def test_resolve_columns_keeps_map_names_when_the_catalog_is_unreadable(
    monkeypatch,
) -> None:
    engine = _Engine("oracle")
    monkeypatch.setattr(
        "connectors.generic_sql._stored_column_spellings",
        lambda *_args, **_kw: {},
    )
    assert _resolve_physical_column_idents(engine, "T", "S", ["id"]) == {}
