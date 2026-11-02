"""Host-fact skips — capability probes, not product assertions."""

from __future__ import annotations

import pytest

from connectors.pgvector_writer import (
    _exec_schema_table,
    pgvector_extension_unavailable_reason,
)
from tests.host_facts import listed_mysql_collations, require_pgvector


def test_vector_control_error_is_named_host_fact():
    err = Exception(
        'could not open extension control file '
        '"/usr/share/postgresql/16/extension/vector.control": No such file or directory'
    )
    named = pgvector_extension_unavailable_reason(err)
    assert named is not None
    assert "vector.control" in named
    assert "listening :5432 is not proof" in named
    assert pgvector_extension_unavailable_reason(Exception("syntax error")) is None


def test_listed_mysql_collations_drops_names_the_host_does_not_ship():
    class _Cur:
        def execute(self, sql, params):
            self.params = params
            self.sql = sql

        def fetchall(self):
            return [("utf8mb4_bin",), ("utf8mb4_unicode_ci",)]

    present = listed_mysql_collations(
        _Cur(),
        (
            "utf8mb4_bin",
            "utf8mb4_unicode_ci",
            "utf8mb4_0900_ai_ci",
            "utf8mb4_uca1400_ai_ci",
        ),
    )
    assert present == {"utf8mb4_bin", "utf8mb4_unicode_ci"}
    assert "utf8mb4_0900_ai_ci" not in present


def test_require_pgvector_skips_when_the_host_has_no_extension(monkeypatch):
    monkeypatch.setattr("tests.host_facts.pgvector_extension_available", lambda: False)
    with pytest.raises(pytest.skip.Exception, match="pgvector extension unavailable"):
        require_pgvector()


def test_exec_schema_table_raises_named_missing_vector_control():
    class _Cur:
        def execute(self, *a, **k):
            raise Exception(
                'could not open extension control file '
                '"/usr/share/postgresql/16/extension/vector.control": No such file or directory'
            )

    with pytest.raises(RuntimeError, match="listening :5432 is not proof"):
        _exec_schema_table(_Cur(), "public", "t", 4)
