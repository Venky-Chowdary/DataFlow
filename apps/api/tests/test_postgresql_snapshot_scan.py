"""PostgreSQL held scans must fetch through a transaction-scoped server cursor."""

from itertools import islice

import pytest

from connectors import postgresql_reader as reader
from connectors.sql_snapshot_scan import close_table_scan


@pytest.fixture
def scan_world(monkeypatch):
    class Cursor:
        def __init__(self, connection, name):
            self.connection = connection
            self.name = name
            self.description = None
            self.commands = []
            self.sizes = []
            self.closed = False
            self.rows = iter(range(5))

        def execute(self, statement, params=()):
            from psycopg2 import sql

            self.commands.append((statement, params))
            head = statement.seq[0] if isinstance(statement, sql.Composed) else statement
            if isinstance(head, sql.SQL) and head.string.startswith("DECLARE") and self.connection.fail_execute:
                raise RuntimeError("scan execution failed")
            if self.name:
                assert not self.connection.autocommit
                assert len(self.commands) == 1
                if self.connection.fail_execute:
                    raise RuntimeError("scan execution failed")

        def fetchone(self):
            return (5,)

        def fetchmany(self, size):
            self.sizes.append(size)
            if self.connection.fail_fetch:
                raise RuntimeError("scan fetch failed")
            self.description = [("id",), ("payload",)]
            return [(value, f"row-{value}") for value in islice(self.rows, size)]

        def close(self):
            self.closed = True

    class Connection:
        def __init__(self):
            self.autocommit = True
            self.closed = False
            self.cursors = []
            self.options = []
            self.fail_execute = False
            self.fail_fetch = False

        def cursor(self, name=None, **options):
            cursor = Cursor(self, name)
            self.cursors.append(cursor)
            self.options.append(options)
            return cursor

        def close(self):
            self.closed = True

    connection = Connection()
    import services.source_snapshot as snapshots

    monkeypatch.setattr(snapshots, "get_source_snapshot_conn", lambda: None)
    monkeypatch.setattr(reader, "get_connection", lambda **kwargs: connection)
    monkeypatch.setattr(reader, "_order_by_columns", lambda *args, **kwargs: [])
    monkeypatch.setattr(reader, "_select_list", lambda *args, **kwargs: None)

    def read(state, **kwargs):
        return reader.read_table_scan_batch(
            host="unused", port=5432, database="unused", username="unused",
            password="", schema="public", connection_string="", ssl=False,
            table="records", scan_state=state, **kwargs,
        )

    return connection, snapshots, read


@pytest.mark.parametrize("borrowed", [False, True])
def test_scan_uses_one_named_cursor_with_bounded_pages(scan_world, monkeypatch, borrowed):
    connection, snapshots, read = scan_world
    if borrowed:
        connection.autocommit = False
        monkeypatch.setattr(snapshots, "get_source_snapshot_conn", lambda: connection)
    state = {}
    try:
        first = read(state, limit=2)
        second = read(state, offset=2, limit=3)
        empty = read(state, offset=5, limit=3)

        assert first.headers == ["id", "payload"]
        assert first.rows == [["0", "row-0"], ["1", "row-1"]]
        assert second.rows == [["2", "row-2"], ["3", "row-3"], ["4", "row-4"]]
        assert empty.headers == first.headers
        assert empty.rows == []
        assert first.total_rows == 5
        assert len(connection.cursors) == 2
        catalog, stream = connection.cursors
        assert catalog.name is None
        assert catalog.closed
        assert stream.name
        assert len(stream.commands) == 1
        assert stream.sizes == [2, 3, 3]
        assert stream.closed
        assert connection.options[1] == {"scrollable": False, "withhold": False}
        assert connection.closed is (not borrowed)
        assert state == {}
    finally:
        close_table_scan(state)


@pytest.mark.parametrize("failure", ["execute", "fetch"])
@pytest.mark.parametrize("borrowed", [False, True])
def test_failed_server_scan_releases_only_owned_resources(scan_world, monkeypatch, failure, borrowed):
    connection, snapshots, read = scan_world
    if borrowed:
        connection.autocommit = False
        monkeypatch.setattr(snapshots, "get_source_snapshot_conn", lambda: connection)
    setattr(connection, f"fail_{failure}", True)
    state = {}

    with pytest.raises(RuntimeError, match=f"scan {failure if failure == 'fetch' else 'execution'} failed"):
        read(state, limit=2)

    assert connection.cursors
    assert all(cursor.closed for cursor in connection.cursors)
    assert connection.closed is (not borrowed)
    assert state == {}


@pytest.mark.parametrize("borrowed", [False, True])
def test_later_fetch_failure_does_not_reopen_or_leak_scan(scan_world, monkeypatch, borrowed):
    connection, snapshots, read = scan_world
    if borrowed:
        connection.autocommit = False
        monkeypatch.setattr(snapshots, "get_source_snapshot_conn", lambda: connection)
    state = {}
    first = read(state, limit=2)
    assert len(first.rows) == 2
    connection.fail_fetch = True

    with pytest.raises(RuntimeError, match="scan fetch failed"):
        read(state, offset=2, limit=2)

    assert len(connection.cursors) == 2
    assert len(connection.cursors[1].commands) == 1
    assert all(cursor.closed for cursor in connection.cursors)
    assert connection.closed is (not borrowed)
    assert state == {}


def test_filtered_scan_publishes_the_same_order_and_bound_it_executes(scan_world, monkeypatch):
    from psycopg2 import sql

    connection, snapshots, read = scan_world

    def quoted(identifier, context):
        return ".".join('"' + name.replace('"', '""') + '"' for name in identifier.strings)

    monkeypatch.setattr(sql.Identifier, "as_string", quoted)
    monkeypatch.setattr(reader, "_order_by_columns", lambda *args, **kwargs: ["id", "version"])
    state = {}
    try:
        read(state, limit=2, filter_column="watermark", filter_after="7")
        catalog, stream = connection.cursors
        count, count_params = catalog.commands[0]
        query, scan_params = stream.commands[0]

        assert count.as_string(catalog) == 'SELECT COUNT(*) FROM "public"."records" WHERE "watermark" >= %s'
        assert query.as_string(stream) == (
            'SELECT * FROM "public"."records" WHERE "watermark" >= %s '
            'ORDER BY "watermark", "id", "version"'
        )
        assert count_params == scan_params == ("7",)
        assert state["order_cols"] == ["watermark", "id", "version"]
    finally:
        close_table_scan(state)


@pytest.mark.parametrize("borrowed", [False, True])
def test_redshift_declares_plain_cursor_and_fetches_without_reexecution(scan_world, monkeypatch, borrowed):
    from psycopg2 import sql

    connection, snapshots, read = scan_world
    if borrowed:
        connection.autocommit = False
        monkeypatch.setattr(snapshots, "get_source_snapshot_conn", lambda: connection)
    monkeypatch.setattr(sql.Identifier, "as_string", lambda identifier, context: '"' + identifier.strings[0] + '"')
    state = {}
    try:
        first = read(state, limit=2, source_driver="redshift")
        second = read(state, offset=2, limit=3, source_driver="redshift")
        catalog, stream = connection.cursors
        declaration, params = catalog.commands[-1]

        assert declaration.as_string(catalog) == (
            f'DECLARE "{stream.name}" CURSOR FOR SELECT * FROM "public"."records" ORDER BY 1'
        )
        assert params == ()
        assert stream.commands == []
        assert stream.sizes == [2, 3]
        assert first.headers == ["id", "payload"]
        assert len(first.rows) + len(second.rows) == 5
        assert catalog.closed
    finally:
        close_table_scan(state)
    assert connection.closed is (not borrowed)


@pytest.mark.parametrize("borrowed", [False, True])
def test_failed_redshift_declaration_closes_both_cursors(scan_world, monkeypatch, borrowed):
    connection, snapshots, read = scan_world
    if borrowed:
        connection.autocommit = False
        monkeypatch.setattr(snapshots, "get_source_snapshot_conn", lambda: connection)
    connection.fail_execute = True
    state = {}

    with pytest.raises(RuntimeError, match="scan execution failed"):
        read(state, limit=2, source_driver="redshift")

    assert len(connection.cursors) == 2
    assert all(cursor.closed for cursor in connection.cursors)
    assert connection.closed is (not borrowed)
    assert state == {}