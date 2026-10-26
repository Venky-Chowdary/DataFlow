"""CI-3: a per-test FAKESNOW_DB_PATH override must rebind the shared patch.

The refcounted fakesnow patch used to pin ``db_path`` at first install, so a
test that overrode FAKESNOW_DB_PATH was silently served the previous
catalog — order-dependent shared state. The patch must be dropped and
re-installed when the requested catalog path differs.
"""

import pytest

pytest.importorskip("fakesnow")

import connectors.snowflake_conn as sc  # noqa: E402
from connectors.snowflake_conn import _fakesnow_db_path  # noqa: E402


def _connect():
    return sc.get_connection(
        account="localhost",
        username="u",
        password="p",
        database="dataflow",
        schema="public",
        warehouse="",
        connection_string="",
    )


def _reset_product_patch():
    with sc._fakesnow_lock:
        sc._fakesnow_refcount = 0
        if sc._fakesnow_patch_cm is not None:
            try:
                sc._fakesnow_patch_cm.__exit__(None, None, None)
            except Exception:
                pass
            sc._fakesnow_patch_cm = None
        sc._fakesnow_installed_db_path = None


@pytest.fixture(autouse=True)
def _clean_patch_state():
    _reset_product_patch()
    yield
    _reset_product_patch()


def test_fakesnow_patch_rebinds_when_db_path_changes(monkeypatch, tmp_path):
    cat_a = tmp_path / "cat_a"
    cat_b = tmp_path / "cat_b"
    monkeypatch.setenv("FAKESNOW_DB_PATH", str(cat_a))

    conn_a = _connect()
    conn_a.cursor().execute("CREATE TABLE marker_a (id int)")
    conn_a.cursor().execute("INSERT INTO marker_a VALUES (1)")

    # Hold the first patch open (refcount > 0) and switch catalogs.
    monkeypatch.setenv("FAKESNOW_DB_PATH", str(cat_b))
    assert _fakesnow_db_path() == str(cat_b)
    conn_b = _connect()
    # Catalog B must NOT see A's table — this connect bound the new path.
    with pytest.raises(Exception):
        conn_b.cursor().execute("SELECT * FROM marker_a")
    conn_b.cursor().execute("CREATE TABLE marker_b (id int)")

    # Back on A: marker_a still there, marker_b is not.
    monkeypatch.setenv("FAKESNOW_DB_PATH", str(cat_a))
    conn_a2 = _connect()
    assert conn_a2.cursor().execute("SELECT * FROM marker_a").fetchone() == (1,)
    with pytest.raises(Exception):
        conn_a2.cursor().execute("SELECT * FROM marker_b")

    for c in (conn_a, conn_b, conn_a2):
        try:
            c.close()
        except Exception:
            pass


def test_installed_db_path_tracked(monkeypatch, tmp_path):
    monkeypatch.setenv("FAKESNOW_DB_PATH", str(tmp_path / "cat_x"))
    conn = _connect()
    assert sc._fakesnow_installed_db_path == str(tmp_path / "cat_x")
    conn.close()
