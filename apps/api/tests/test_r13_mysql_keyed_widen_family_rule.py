"""MySQL family rule for widening an indexed column (property-8 regression).

On the round-13 head ``test_property8_collation_equality`` and
``test_property8_unicode_form`` failed on MySQL 8 with 1170 "BLOB/TEXT column
'code' used in key specification without a key length". Bisected to the
D200/D107 commit (ebc0759c8): a create-new mapping's stamped ``VARCHAR(32)``
stopped being a ceiling, so right after creating ``code VARCHAR(32)`` with its
carried UNIQUE the writer asked ``widen_existing_columns_native`` to MODIFY it
to LONGTEXT. MySQL refuses a LOB in a key; MariaDB silently switches the key
to HASH. The widen then dropped the carried ``utf8mb4_bin`` on MODIFY (1062
duplicate 'alpha').

The rule, in ``connectors/schema_drift`` for every MySQL-family widen: an
indexed column is sized through the create path's key rule
(``mysql_key_compatible_types``) to the widest full-value VARCHAR the 3072-byte
key budget allows, never a LOB; and a MODIFY restates the column's own
CHARACTER SET / COLLATE unless the candidate names one.
"""

from __future__ import annotations

from connectors.schema_drift import widen_existing_columns_native


class _Cursor:
    """information_schema answers for one table: ``code VARCHAR(32) utf8mb4_bin NOT NULL``
    under a UNIQUE key, ``note VARCHAR(32)`` unindexed."""

    def __init__(self, indexed=("code",)):
        self.executed: list[str] = []
        self._rows: list[tuple] = []
        self._indexed = set(indexed)

    def execute(self, sql, params=None):
        self.executed.append(" ".join(sql.split()))
        flat = sql.lower()
        if "information_schema.columns" in flat and "character_maximum_length" in flat:
            self._rows = [("code", "varchar", 32, None, None), ("note", "varchar", 32, None, None)]
        elif "information_schema.columns" in flat and "collation_name" in flat:
            self._rows = [("NO", None, "", "utf8mb4", "utf8mb4_bin")]
        elif "information_schema.columns" in flat:
            self._rows = [("NO", None, "")]
        elif "information_schema.statistics" in flat:
            self._rows = [("PRIMARY", "id")] + [(f"{c}_key", c) for c in sorted(self._indexed)]
        else:
            self._rows = []

    def fetchall(self):
        return list(self._rows)

    def fetchone(self):
        return self._rows[0] if self._rows else None


def _ddl(cur: _Cursor) -> list[str]:
    return [s for s in cur.executed if s.upper().startswith("ALTER TABLE")]


def test_indexed_text_widen_is_sized_to_the_key_budget_and_keeps_its_collation():
    cur = _Cursor(indexed=("code",))
    widen_existing_columns_native(
        cur, "mysql", "dataflow", "t", ["code"], ["LONGTEXT"],
        backfill=True, source_types={"code": "TEXT"},
    )
    alters = _ddl(cur)
    assert len(alters) == 1, cur.executed
    assert "VARCHAR(768)" in alters[0].upper(), alters[0]
    assert "LONGTEXT" not in alters[0].upper()
    assert "COLLATE utf8mb4_bin" in alters[0] and "CHARACTER SET utf8mb4" in alters[0]
    assert alters[0].rstrip().upper().endswith("NOT NULL") or "NOT NULL" in alters[0]


def test_unindexed_text_widen_still_becomes_the_lob_with_its_collation():
    cur = _Cursor(indexed=())
    widen_existing_columns_native(
        cur, "mysql", "dataflow", "t", ["note"], ["LONGTEXT"],
        backfill=True, source_types={"note": "TEXT"},
    )
    alters = _ddl(cur)
    assert len(alters) == 1 and "LONGTEXT" in alters[0].upper(), cur.executed
    assert "COLLATE utf8mb4_bin" in alters[0]


def test_indexed_column_already_at_its_budget_is_kept_and_reported():
    cur = _Cursor(indexed=("code",))
    cur_big = cur

    def execute(sql, params=None, _orig=cur_big.execute):
        _orig(sql, params)
        if "character_maximum_length" in sql.lower():
            cur_big._rows = [("code", "varchar", 768, None, None)]

    cur_big.execute = execute  # type: ignore[method-assign]
    suppressed: dict[str, str] = {}
    widen_existing_columns_native(
        cur_big, "mysql", "dataflow", "t", ["code"], ["LONGTEXT"],
        backfill=True, source_types={"code": "TEXT"}, suppressed_out=suppressed,
    )
    assert _ddl(cur_big) == []
    assert list(suppressed) == ["code"]
