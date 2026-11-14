"""Follow-up review 3 (M1): a ``file://name`` location is not a path inside the Iceberg folder.

pyiceberg reads ``file://app.env`` as netloc ``app.env`` and hands pyarrow the
path ``app.env``: relative, so pyarrow opened it in the API process's working
directory. The FileIO's check anchored the same relative path at the Iceberg
root and passed it. Now a ``file`` location with a host part, or whose path is
not absolute, is refused before pyarrow sees it.
"""

from __future__ import annotations

import pytest

pytest.importorskip("pyarrow")


@pytest.fixture
def layout(tmp_path, monkeypatch):
    for prefix in ("DATAFLOW", "DATAWRAP"):
        monkeypatch.delenv(f"{prefix}_ICEBERG_ROOT", raising=False)
    root = tmp_path / "iceroot"
    root.mkdir()
    (root / "inside.txt").write_text("INSIDE\n")
    cwd = tmp_path / "apiproc"
    (cwd / "sub").mkdir(parents=True)
    (cwd / "app.env").write_text("HOST-SECRET-IN-CWD\n")
    (cwd / "sub" / "x.txt").write_text("NESTED\n")
    monkeypatch.setenv("DATAFLOW_ICEBERG_ROOT", str(root))
    monkeypatch.chdir(cwd)
    return root, cwd


def _io():
    from connectors.iceberg_pyarrow_io import LocalUriPyArrowFileIO

    return LocalUriPyArrowFileIO(properties={})  # what a SQL / Hadoop catalog hands it


@pytest.mark.parametrize("location", ["file://app.env", "file://sub/x.txt", "file://app.env/",
                                      "file://localhost/app.env"])
def test_a_file_uri_with_a_host_part_is_not_opened(layout, location):
    io = _io()
    with pytest.raises(ValueError) as caught:
        io.new_input(location).open().read()
    assert "app.env" not in str(caught.value) and "HOST-SECRET" not in str(caught.value)


def test_a_file_uri_with_a_host_part_is_not_written_or_deleted(layout):
    _root, cwd = layout
    io = _io()
    with pytest.raises(ValueError):
        io.new_output("file://written.txt").create().write(b"x")
    with pytest.raises(ValueError):
        io.delete("file://app.env")
    assert (cwd / "app.env").exists()
    assert not (cwd / "written.txt").exists()


def test_a_location_inside_the_iceberg_folder_still_opens(layout):
    root, _cwd = layout
    io = _io()
    assert io.new_input(f"file://{root}/inside.txt").open().read() == b"INSIDE\n"
    assert io.new_input(str(root / "inside.txt")).open().read() == b"INSIDE\n"


@pytest.mark.parametrize("location", ["file:///etc/hostname", "/etc/hostname", "app.env"])
def test_locations_outside_the_folder_stay_refused(layout, location):
    with pytest.raises(ValueError):
        _io().new_input(location).open().read()
