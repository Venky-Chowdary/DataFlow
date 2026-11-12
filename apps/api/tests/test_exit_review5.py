"""Review 5 of the exit rehearsal's value proof: unit level, no database needed.

* C-1: a connector saved under a hosted or aliased type (``supabase``,
  ``amazon_rds_postgresql``, ``azure_sql_database``, ``amazon_rds_oracle``,
  ``amazon_rds_mysql``, ``mariadb``) is the same engine on the wire, so every
  engine-specific rule (the driver folds, the exact tokens, the server-text
  read) answers exactly as it does for the canonical name.
"""

from __future__ import annotations

import pytest

from services.exit_completeness import digest_plan, profile_rows, server_text_columns

ALIASES = {
    "postgresql": ["postgres", "supabase", "neon", "amazon_rds_postgresql", "alloydb"],
    "sqlserver": ["mssql", "sql_server", "azure_sql_database", "amazon_rds_sqlserver"],
    "oracle": ["amazon_rds_oracle"],
    "mysql": ["amazon_rds_mysql", "azure_database_for_mysql", "mariadb"],
}
#: canonical engine -> declared types whose verdict is engine-specific (a fold, a token, a server-text read).
SHAPES = {
    "postgresql": ["INTEGER[]", "TIME", "TIMETZ", "INTERVAL", "JSONB", "JSON", "NUMERIC", "TIMESTAMPTZ",
                   "TIMESTAMP"],
    "sqlserver": ["DATETIME2", "DATETIME2(7)", "TIME", "DATETIMEOFFSET(7)", "SQL_VARIANT", "DATETIME2(6)"],
    "oracle": ["TIMESTAMP(9)", "TIMESTAMP(6) WITH TIME ZONE", "INTERVAL DAY(2) TO SECOND(6)", "DATE", "NUMBER"],
    "mysql": ["FLOAT", "DOUBLE", "DATETIME(6)"],
}


def _verdicts(engine: str, ddls: list[str]) -> dict[str, tuple]:
    from services.exit_value_fidelity import adapter_fold, server_text_kind
    from services.exit_value_tokens import token_kind

    types = {"id": "BIGINT", **{f"c{i}": d for i, d in enumerate(ddls)}}
    cols = list(types)
    prof = profile_rows([{c: None for c in cols}], cols, ["id"], None, types=types, engine=engine)
    exact, typed, uncompared = digest_plan(cols, types, engine)
    return {d: (adapter_fold(d, engine), server_text_kind(d, engine), token_kind(d, engine),
                prof.uncompared.get(f"c{i}"), f"c{i}" in exact, f"c{i}" in uncompared,
                f"c{i}" in server_text_columns(cols, types, engine))
            for i, d in enumerate(ddls)}


@pytest.mark.parametrize("canonical, alias", [(c, a) for c, names in ALIASES.items() for a in names])
def test_c1_an_aliased_connector_type_gets_the_canonical_engines_verdicts(canonical, alias):
    expected = _verdicts(canonical, SHAPES[canonical])
    assert any(v[0] or v[1] or v[2] for v in expected.values()), "the shapes must exercise an engine rule"
    assert _verdicts(alias, SHAPES[canonical]) == expected


def test_c1_the_rehearsal_reads_the_source_engine_through_the_family_resolver():
    """verify_table and the declared-types read name the engine by its family, never by the raw tile id."""
    import ast
    from pathlib import Path

    tree = ast.parse((Path(__file__).resolve().parents[1] / "services/exit_rehearsal.py").read_text("utf-8"))
    for fn in (n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)
               and n.name in {"verify_table", "_declared_types"}):
        text = ast.unparse(fn)
        assert "family_or_kind" in text, fn.name
