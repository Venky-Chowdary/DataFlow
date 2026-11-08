"""A failed Postgres COPY upsert must not hide the first error.

MariaDB → Postgres upsert staged with COPY. The merge failed, the
transaction aborted, and the values fallback then raised
``current transaction is aborted`` instead of the original failure.
"""

from __future__ import annotations

import pytest

from connectors import postgresql_writer as writer


def test_aborted_upsert_reraises_the_copy_error(monkeypatch: pytest.MonkeyPatch) -> None:
    order: list[str] = []

    class _Conn:
        def rollback(self) -> None:
            order.append("rollback")

    class _Cur:
        def __init__(self) -> None:
            self.connection = _Conn()

        def execute(self, statement, **_kwargs) -> None:
            command = str(statement)
            if command.startswith("SAVEPOINT"):
                order.append("savepoint")
                return
            if command.startswith("ROLLBACK TO SAVEPOINT"):
                order.append("rollback_to")
                return
            order.append("execute")
            raise RuntimeError("duplicate key value violates unique constraint")

    def _values(*_args, **_kwargs) -> None:
        order.append("values")
        raise RuntimeError("current transaction is aborted, commands ignored until end of transaction block")

    class _Ident:
        def __init__(self, *_args: object) -> None:
            pass

    class _Sql:
        def __init__(self, _text: str) -> None:
            self.text = _text

        def __str__(self) -> str:
            return self.text

        def format(self, *_args: object) -> "_Sql":
            return self

    class _Mod:
        SQL = _Sql
        Identifier = _Ident

    monkeypatch.setattr(writer, "_execute_values_insert", _values)
    with pytest.raises(RuntimeError, match="duplicate key value"):
        writer._copy_upsert_batch(
            _Cur(),
            _Mod,
            schema="public",
            table_name="orders",
            target_cols=["id", "qty"],
            conflict_cols=["id"],
            batch=[(1, 2)],
            insert_sql="INSERT",
        )
    assert "rollback" not in order
    assert order.index("rollback_to") < order.index("values")


@pytest.fixture
def upsert_world(monkeypatch):
    class Connection:
        def __init__(self):
            self.pending = [(10, 20)]
            self.stages = {}
            self.aborted = False
            self.saved = None
            self.rollbacks = 0

        def rollback(self):
            self.pending.clear()
            self.aborted = False
            self.rollbacks += 1

    class Cursor:
        def __init__(self):
            self.connection = Connection()
            self.fail_at = None
            self.fail_once = False
            self.recovery_fails = False
            self.fallback_fails = False
            self.values_calls = 0
            self.commands = []

        def fail(self, phase):
            if self.fail_at == phase:
                if self.fail_once:
                    self.fail_at = None
                self.connection.aborted = True
                raise RuntimeError(f"{phase} unavailable")

        def execute(self, statement):
            command = str(statement)
            self.commands.append(command)
            if command.startswith("ROLLBACK TO SAVEPOINT"):
                if self.recovery_fails:
                    raise RuntimeError("savepoint recovery unavailable")
                pending, stages = self.connection.saved
                self.connection.pending = list(pending)
                self.connection.stages = dict(stages)
                self.connection.aborted = False
                return
            if self.connection.aborted:
                raise RuntimeError("current transaction is aborted")
            if command.startswith("SAVEPOINT"):
                self.fail("savepoint")
                self.connection.saved = (
                    list(self.connection.pending), dict(self.connection.stages),
                )
            elif command.startswith("CREATE TEMP TABLE"):
                self.fail("create")
                self.connection.stages[command.split()[3]] = []
            elif command.startswith("INSERT INTO"):
                self.connection.pending.extend(next(iter(self.connection.stages.values())))
                self.fail("merge")
            elif command.startswith("DROP TABLE"):
                self.fail("drop")
                self.connection.stages.pop(command.split()[-1])
            elif command.startswith("RELEASE SAVEPOINT"):
                self.fail("release")
                self.connection.saved = None

    class Statement(str):
        def format(self, *args):
            return Statement(super().format(*args))

        def join(self, parts):
            return Statement(super().join(parts))

    class SqlModule:
        SQL = Statement
        Identifier = Statement

    def copy_rows(cursor, stage, columns, rows):
        cursor.connection.stages[stage].extend(rows)
        cursor.fail("copy")

    def values(cursor, statement, rows):
        assert not cursor.connection.aborted
        cursor.values_calls += 1
        cursor.connection.pending.extend(rows)
        if cursor.fallback_fails:
            cursor.connection.aborted = True
            raise RuntimeError("fallback constraint failure")

    monkeypatch.setattr(writer, "_copy_rows_temp", copy_rows)
    monkeypatch.setattr(writer, "_execute_values_insert", values)
    cursor = Cursor()

    def execute():
        return writer._copy_upsert_batch(
            cursor, SqlModule, schema="public", table_name="orders",
            target_cols=["id", "qty"], conflict_cols=["id"],
            batch=[(1, 2)], insert_sql="INSERT",
        )

    return cursor, execute


@pytest.mark.parametrize("failure", [None, "create", "copy", "merge", "drop"])
def test_copy_fallback_preserves_earlier_transaction_writes(upsert_world, failure):
    cursor, execute = upsert_world
    cursor.fail_at = failure

    assert execute() == 1
    assert cursor.connection.pending == [(10, 20), (1, 2)]
    assert cursor.connection.stages == {}
    assert cursor.connection.saved is None
    assert not cursor.connection.aborted
    assert cursor.connection.rollbacks == 0
    assert cursor.values_calls == (0 if failure is None else 1)


def test_failed_savepoint_creation_never_attempts_fallback(upsert_world):
    cursor, execute = upsert_world
    cursor.fail_at = "savepoint"

    with pytest.raises(RuntimeError, match="savepoint unavailable"):
        execute()

    assert cursor.connection.pending == [(10, 20)]
    assert cursor.values_calls == 0
    assert cursor.connection.rollbacks == 0


def test_failed_savepoint_recovery_preserves_original_error(upsert_world):
    cursor, execute = upsert_world
    cursor.fail_at = "merge"
    cursor.recovery_fails = True

    with pytest.raises(RuntimeError, match="merge unavailable") as caught:
        execute()

    assert str(caught.value.__cause__) == "savepoint recovery unavailable"
    assert cursor.values_calls == 0
    assert cursor.connection.aborted
    assert cursor.connection.rollbacks == 0


def test_failed_values_fallback_is_not_acknowledged(upsert_world):
    cursor, execute = upsert_world
    cursor.fail_at = "drop"
    cursor.fallback_fails = True

    with pytest.raises(RuntimeError, match="fallback constraint failure"):
        execute()

    assert cursor.connection.pending[0] == (10, 20)
    assert cursor.connection.stages == {}
    assert cursor.values_calls == 1
    assert cursor.connection.aborted
    assert cursor.connection.rollbacks == 0
    assert not any(command.startswith("RELEASE SAVEPOINT") for command in cursor.commands)


def test_failed_savepoint_release_is_not_acknowledged(upsert_world):
    cursor, execute = upsert_world
    cursor.fail_at = "release"

    with pytest.raises(RuntimeError, match="release unavailable"):
        execute()

    assert cursor.connection.pending[0] == (10, 20)
    assert cursor.connection.stages == {}
    assert cursor.connection.aborted
    assert cursor.connection.rollbacks == 0


def test_first_release_failure_does_not_retry_the_write_strategy(upsert_world):
    cursor, execute = upsert_world
    cursor.fail_at = "release"
    cursor.fail_once = True

    with pytest.raises(RuntimeError, match="release unavailable"):
        execute()

    assert cursor.values_calls == 0
    assert cursor.connection.pending == [(10, 20), (1, 2)]
    assert cursor.connection.stages == {}
    assert cursor.connection.aborted
    assert cursor.connection.rollbacks == 0
