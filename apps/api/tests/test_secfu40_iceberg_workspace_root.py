"""Follow-up review 4 (H): the Iceberg local root is divided per workspace.

``iceberg_catalog.iceberg_local_root`` (``DATAFLOW_ICEBERG_ROOT``, else the data
folder's ``iceberg`` area under confinement) was one folder for every
workspace. The jail only kept paths under it, so workspace B's local SQLite
catalog could name workspace A's catalog file (absolute ``catalog_path`` or
``sqlite:///`` URI), warehouse or metadata file inside it and read A's tables
(probes rev34/p2 and rev33/p10).

Now the root is partitioned like the data area (``embedded_volume``):
workspace W's files live in ``<root>/_workspaces/<W>/`` and the binding is the
same one (the job's workspace, else the stored connector's, else the
request's). A workspace-bound caller opens only its own area: catalog file,
warehouse, metadata, manifest and data files, at save, in the guarded FileIO
(scope captured when the FileIO is built, so pyiceberg's pool threads keep
it), ``dest_precount``'s local COUNT and the Iceberg writer. Relative names are
anchored in the own area. Files in the flat part of the root (from before the
partition) open for an unbound / platform caller, or for the one workspace an
operator assigned them to (``scripts/iceberg_assign_flat_root.py``); never for
an arbitrary workspace. (Since follow-up review 5, "platform" means an explicit
``platform_iceberg_scope()`` or an administrator's all-workspace view, not
"no workspace bound": see test_secfu44.) With auth off (the single local operator) nothing
changes.
"""

from __future__ import annotations

import threading
from pathlib import Path

import pytest

WS_A = "ws-alpha"
WS_B = "ws-bravo"


@pytest.fixture
def root(tmp_path, monkeypatch):
    for prefix in ("DATAFLOW", "DATAWRAP"):
        for name in ("ICEBERG_ROOT", "CONFINE_LOCAL_FILES", "REQUIRE_AUTH", "ALLOW_PLATFORM_AWS_CREDENTIALS",
                     "ALLOW_PLATFORM_GCP_AZURE_CREDENTIALS"):
            monkeypatch.delenv(f"{prefix}_{name}", raising=False)
    jail = tmp_path / "iceroot"
    jail.mkdir()
    monkeypatch.setenv("DATAFLOW_ICEBERG_ROOT", str(jail))
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setattr(
        "socket.socket.connect", lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("a test dialled"))
    )
    return jail.resolve()


def _area(root: Path, workspace: str) -> Path:
    return root / "_workspaces" / workspace


def _bound(workspace: str):
    from services.export_paths import connector_export_scope

    return connector_export_scope(workspace)


def _table(catalog_db: Path, warehouse: Path, marker: str):
    """A real local table, written without any guard (the files a workspace already has)."""
    import pyarrow as pa
    from pyiceberg.catalog.sql import SqlCatalog

    catalog_db.parent.mkdir(parents=True, exist_ok=True)
    warehouse.mkdir(parents=True, exist_ok=True)
    catalog = SqlCatalog("dataflow", uri=f"sqlite:///{catalog_db}", warehouse=warehouse.as_uri())
    catalog.create_namespace("ns")
    table = catalog.create_table("ns.t", schema=pa.schema([("s", pa.string())]))
    table.append(pa.table({"s": [marker, marker + "-2"]}))
    return catalog.load_table("ns.t")


@pytest.fixture
def victim(root):
    """Workspace A's table, in A's area."""
    area = _area(root, WS_A)
    table = _table(area / "victim" / "cat.db", area / "victim", "A-SECRET")
    data_file = next(iter(table.scan().plan_files())).file.file_path
    return {"area": area, "cat": area / "victim" / "cat.db", "wh": area / "victim",
            "meta": table.metadata_location, "data": data_file}


def _rows(endpoint):
    from connectors.iceberg_catalog import load_catalog

    return load_catalog(endpoint).load_table(("ns", "t")).scan().to_arrow().column("s").to_pylist()


def _sql(cs: str, warehouse: str = "mine", **extra) -> dict:
    return {"connection_string": cs, "extra": {"catalog_type": "sql", **extra}, "table": "t", "schema": "ns",
            "warehouse": warehouse}


# --- workspace B cannot reach workspace A's area ---------------------------------------------


def test_b_cannot_open_as_catalog_file_by_absolute_uri(victim):
    with _bound(WS_B), pytest.raises(ValueError, match="(?i)another workspace") as caught:
        _rows(_sql(f"sqlite:///{victim['cat']}"))
    assert str(victim["area"]) not in str(caught.value) and WS_A not in str(caught.value)


def test_b_cannot_name_as_catalog_file_as_catalog_path(victim):
    endpoint = {"extra": {"catalog_type": "sql", "catalog_path": str(victim["cat"])}, "table": "t",
                "schema": "ns", "warehouse": "mine"}
    with _bound(WS_B), pytest.raises(ValueError, match="(?i)another workspace"):
        _rows(endpoint)


@pytest.mark.parametrize("spelling", ["path", "uri"])
def test_b_cannot_name_as_warehouse(victim, spelling):
    from connectors.iceberg_catalog import parse_iceberg_catalog_config

    warehouse = str(victim["wh"]) if spelling == "path" else victim["wh"].as_uri()
    with _bound(WS_B), pytest.raises(ValueError, match="(?i)another workspace"):
        parse_iceberg_catalog_config({"warehouse": warehouse, "table": "t"})  # filesystem catalog
    with _bound(WS_B), pytest.raises(ValueError, match="(?i)another workspace"):
        parse_iceberg_catalog_config(_sql("sqlite:///mine/cat.db", warehouse=warehouse))


def test_relative_names_land_in_the_own_area(victim, root):
    """The reviewer's p2: the same relative ``catalog_path`` names B's own file, not A's."""
    from connectors.iceberg_catalog import parse_iceberg_catalog_config
    from pyiceberg.exceptions import NoSuchTableError

    with _bound(WS_B):
        props = parse_iceberg_catalog_config(_sql("sqlite:///victim/cat.db", warehouse="victim"))["properties"]
        assert props["uri"] == f"sqlite:///{_area(root, WS_B) / 'victim' / 'cat.db'}"
        assert props["warehouse"] == (_area(root, WS_B) / "victim").as_uri()
        (_area(root, WS_B) / "victim").mkdir(parents=True, exist_ok=True)
        with pytest.raises(NoSuchTableError):
            _rows(_sql("sqlite:///victim/cat.db", warehouse="victim"))


def test_registered_metadata_of_another_workspace_is_refused(victim):
    """The reviewer's p10, bound: B registers A's metadata file in its own local catalog."""
    from connectors.iceberg_catalog import load_catalog

    with _bound(WS_B):
        catalog = load_catalog(_sql("sqlite:///mine/cat.db", warehouse="mine"))
        catalog.create_namespace("ns")
        leaked: list = []
        with pytest.raises(ValueError, match="(?i)iceberg"):
            catalog.register_table(("ns", "stolen"), victim["meta"])
            leaked = catalog.load_table(("ns", "stolen")).scan().to_arrow().column("s").to_pylist()
        assert "A-SECRET" not in leaked


def test_the_file_io_keeps_its_workspace_in_a_pool_thread(victim):
    from connectors.iceberg_credential_guard import guarded_file_io

    with _bound(WS_B):
        io = guarded_file_io({})
        with pytest.raises(ValueError):
            io.new_input(victim["data"]).open()
    outcome: list = []

    def read() -> None:  # pyiceberg reads data files in an executor: no context there
        try:
            outcome.append(io.new_input(victim["data"]).open().read(4))
        except ValueError as exc:
            outcome.append(exc)

    worker = threading.Thread(target=read)
    worker.start()
    worker.join()
    assert isinstance(outcome[0], ValueError), outcome


def test_dest_precount_does_not_count_another_workspaces_file(victim):
    from services import dest_precount

    with _bound(WS_B):
        assert dest_precount._iceberg_local_path(victim["data"]) is None
    with _bound(WS_A):
        assert dest_precount._iceberg_local_path(victim["data"]) is not None


def test_the_writer_refuses_another_workspaces_warehouse(victim):
    from connectors import iceberg_writer

    with _bound(WS_B), pytest.raises(ValueError, match="(?i)another workspace"):
        iceberg_writer._warehouse_root("", str(victim["wh"]), "")


@pytest.mark.parametrize("field", ["warehouse", "connection_string", "catalog_path"])
def test_save_refuses_another_workspaces_paths(victim, root, field):
    from services.egress_guard import connector_save_refusal

    data = {"name": "b-iceberg", "type": "iceberg", "workspace_id": WS_B, "table": "t", "extra": {}}
    if field == "warehouse":
        data["warehouse"] = str(victim["wh"])
    elif field == "connection_string":
        data.update(connection_string=f"sqlite:///{victim['cat']}", extra={"catalog_type": "sql"})
    else:
        data.update(warehouse="mine", extra={"catalog_type": "sql", "catalog_path": str(victim["cat"])})
    refusal = connector_save_refusal(data)
    assert "another workspace" in refusal.lower()
    assert str(root) not in refusal and WS_A not in refusal
    assert not _area(root, WS_B).exists()  # judging creates nothing


def test_save_accepts_the_own_area(victim, root):
    from services.egress_guard import connector_save_refusal

    for data in (
        {"warehouse": "mine"},
        {"warehouse": str(victim["wh"]).replace(WS_A, WS_B)},
        {"connection_string": "sqlite:///mine/cat.db", "warehouse": "mine", "extra": {"catalog_type": "sql"}},
    ):
        payload = {"name": "b", "type": "iceberg", "workspace_id": WS_B, "table": "t", "extra": {}, **data}
        assert connector_save_refusal(payload) == "", data


# --- the own area, platform callers and single-operator mode ---------------------------------


def test_a_reads_its_own_table_by_absolute_and_relative_names(victim):
    with _bound(WS_A):
        assert _rows(_sql(f"sqlite:///{victim['cat']}", warehouse=str(victim["wh"])))[0] == "A-SECRET"
        assert _rows(_sql("sqlite:///victim/cat.db", warehouse="victim"))[0] == "A-SECRET"


def test_a_platform_caller_keeps_the_whole_root(victim):
    from services.iceberg_workspace_root import platform_iceberg_scope

    # Follow-up review 5 (H1): only an explicit platform caller, never "no workspace bound".
    with platform_iceberg_scope():
        assert _rows(_sql(f"sqlite:///{victim['cat']}", warehouse=str(victim["wh"])))[0] == "A-SECRET"


def test_single_operator_mode_is_unchanged(victim, monkeypatch):
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "0")
    with _bound(WS_B):
        assert _rows(_sql(f"sqlite:///{victim['cat']}", warehouse=str(victim["wh"])))[0] == "A-SECRET"


# --- files in the flat root (from before the partition) --------------------------------------


@pytest.fixture
def legacy(root):
    table = _table(root / "legacy" / "cat.db", root / "legacy", "LEGACY-ROWS")
    return {"cat": root / "legacy" / "cat.db", "wh": root / "legacy", "meta": table.metadata_location}


@pytest.mark.parametrize("spelling", ["relative", "absolute"])
def test_a_flat_root_catalog_is_refused_for_a_workspace_with_the_remedy(legacy, spelling):
    endpoint = (
        _sql("sqlite:///legacy/cat.db", warehouse="legacy") if spelling == "relative"
        else _sql(f"sqlite:///{legacy['cat']}", warehouse=str(legacy["wh"]))
    )
    with _bound(WS_B), pytest.raises(ValueError) as caught:
        _rows(endpoint)
    message = str(caught.value)
    assert "iceberg_assign_flat_root" in message and "operator" in message
    assert str(legacy["wh"]) not in message


def test_a_flat_root_catalog_opens_for_the_platform(legacy):
    from services.iceberg_workspace_root import platform_iceberg_scope

    with platform_iceberg_scope():
        assert _rows(_sql(f"sqlite:///{legacy['cat']}", warehouse=str(legacy["wh"])))[0] == "LEGACY-ROWS"


def test_an_operator_assignment_opens_the_flat_entry_for_that_workspace_only(legacy):
    from services.iceberg_workspace_root import assign_flat_entry

    assign_flat_entry("legacy", WS_A)
    with _bound(WS_A):
        assert _rows(_sql("sqlite:///legacy/cat.db", warehouse="legacy"))[0] == "LEGACY-ROWS"
        assert _rows(_sql(f"sqlite:///{legacy['cat']}", warehouse=str(legacy["wh"])))[0] == "LEGACY-ROWS"
    with _bound(WS_B), pytest.raises(ValueError):
        _rows(_sql(f"sqlite:///{legacy['cat']}", warehouse=str(legacy["wh"])))


def test_the_assignment_command(legacy, root, capsys, monkeypatch, tmp_path):
    from scripts.iceberg_assign_flat_root import main
    from services import connector_store

    monkeypatch.setattr(connector_store, "STORE_PATH", tmp_path / "connectors.json")
    monkeypatch.setattr(connector_store, "_backend_choice", "file")

    assert main(["--list"]) == 0
    assert "legacy" in capsys.readouterr().out
    assert main(["--assign", "legacy", "--workspace", WS_A]) == 0
    assert main(["--assign", "legacy", "--workspace", WS_B]) != 0  # already another workspace's
    assert main(["--assign", "../etc", "--workspace", WS_A]) != 0
    assert main(["--assign", "_workspaces", "--workspace", WS_A]) != 0
    assert main(["--assign", "missing", "--workspace", WS_A]) != 0
    with _bound(WS_A):
        assert _rows(_sql("sqlite:///legacy/cat.db", warehouse="legacy"))[0] == "LEGACY-ROWS"
    assert main(["--revoke", "legacy"]) == 0
    with _bound(WS_A), pytest.raises(ValueError):
        _rows(_sql("sqlite:///legacy/cat.db", warehouse="legacy"))


def test_the_assignment_record_cannot_be_named(root):
    from connectors.iceberg_catalog import _jail_local_path

    record = root / "_workspaces" / ".flat_owners.json"
    for workspace in (WS_A, WS_B):
        with _bound(workspace), pytest.raises(ValueError):
            _jail_local_path(str(record), "catalog_path")
