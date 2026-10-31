"""Client-named exports are confined to apps/api/exports with an export extension."""

from __future__ import annotations

import os
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest

from services.export_paths import (
    ExportPathError,
    api_root,
    exports_dir,
    generated_export_path,
    resolve_export_path,
)
from services.file_export_append import land_export_bytes


@pytest.fixture
def name() -> str:
    return f"t_{uuid.uuid4().hex[:10]}"


@pytest.mark.parametrize(
    "form",
    ["{n}.csv", "exports/{n}.csv", "sub_{n}/{n}.csv", "exports\\sub_{n}\\{n}.csv", "./{n}.csv"],
)
def test_names_inside_exports_folder_resolve_there(form, name):
    resolved = resolve_export_path(form.format(n=name), default_ext="csv")
    base = exports_dir()
    assert os.path.commonpath([str(base), str(resolved)]) == str(base)
    assert resolved.name == f"{name}.csv"


def test_absolute_path_inside_exports_is_allowed(name):
    target = exports_dir() / "nested" / f"{name}.parquet"
    assert resolve_export_path(str(target), default_ext="csv") == target


def test_missing_extension_gets_the_export_format(name):
    assert resolve_export_path(name, default_ext="jsonl").name == f"{name}.jsonl"


@pytest.mark.parametrize(
    "raw",
    [
        "../x.csv",
        "../../x.csv",
        "exports/../x.csv",
        "../api_evil/x.csv",
        "../exports_evil/x.csv",
        "..\\..\\x.csv",
        "/etc/passwd.csv",
        "/tmp/x.csv",
        "exports",
        "exports/",
        ".",
        "",
    ],
)
def test_escapes_and_empty_names_are_refused(raw):
    with pytest.raises(ExportPathError):
        resolve_export_path(raw, default_ext="csv")


def test_sibling_dir_sharing_the_prefix_is_refused():
    sibling = Path(str(exports_dir()) + "_evil") / "x.csv"
    with pytest.raises(ExportPathError):
        resolve_export_path(str(sibling), default_ext="csv")
    sibling_root = Path(str(api_root()) + "_evil") / "x.csv"
    with pytest.raises(ExportPathError):
        resolve_export_path(str(sibling_root), default_ext="csv")


@pytest.mark.parametrize(
    "raw",
    ["x.py", "src/routers/x.py", "../src/routers/query_router.py", "x.pth", "x.cfg", "x.csv.py", "x.so"],
)
def test_non_export_extensions_are_refused(raw):
    with pytest.raises(ExportPathError):
        resolve_export_path(raw, default_ext="csv")


def test_symlink_out_of_exports_is_refused(tmp_path, name):
    link = exports_dir() / f"link_{name}"
    os.symlink(tmp_path, link)
    try:
        with pytest.raises(ExportPathError):
            resolve_export_path(f"link_{name}/x.csv", default_ext="csv")
    finally:
        link.unlink()


def test_nul_byte_is_refused():
    with pytest.raises(ExportPathError):
        resolve_export_path("x\x00.csv", default_ext="csv")


def test_generated_name_must_be_bare():
    with pytest.raises(ExportPathError):
        generated_export_path("../x.csv")
    assert generated_export_path("query_abc.csv").parent == exports_dir()


def _dest(output_path: str, fmt: str = "csv"):
    return SimpleNamespace(output_path=output_path, format=fmt)


def test_land_export_bytes_refuses_sibling_prefix_escape():
    """Finding: ``../api_evil/x`` passed the separator-less ``startswith``."""
    root = api_root()
    evil = root.parent / f"{root.name}_evil_{uuid.uuid4().hex[:6]}"
    with pytest.raises(ValueError):
        land_export_bytes(
            _dest(f"../{evil.name}/payload.csv"),
            b"id\n1\n",
            job_id="j1",
            sync_mode="full_refresh_overwrite",
            export_name="export.csv",
        )
    with pytest.raises(ValueError):
        land_export_bytes(
            _dest(str(evil / "payload.csv")),
            b"id\n1\n",
            job_id="j1",
            sync_mode="full_refresh_overwrite",
            export_name="export.csv",
        )
    assert not evil.exists()


def test_land_export_bytes_refuses_app_module_overwrite():
    target = api_root() / "services" / "export_paths.py"
    before = target.read_bytes()
    with pytest.raises(ValueError):
        land_export_bytes(
            _dest("../services/export_paths.py"),
            b"import os\n",
            job_id="j1",
            sync_mode="full_refresh_overwrite",
            export_name="export.csv",
        )
    assert target.read_bytes() == before


def test_land_export_bytes_writes_and_appends_inside_exports(name):
    rel = f"sub_{name}/{name}.csv"
    try:
        out = land_export_bytes(
            _dest(rel), b"id\n1\n", job_id="j1", sync_mode="full_refresh_overwrite", export_name="export.csv"
        )
        land_export_bytes(
            _dest(rel), b"id\n2\n", job_id="j2", sync_mode="incremental_append", export_name="export.csv"
        )
        path = Path(out["path"])
        assert path == exports_dir() / f"sub_{name}" / f"{name}.csv"
        assert path.read_bytes() == b"id\n1\n2\n"
        assert out["filename"] == f"{name}.csv"
    finally:
        target = exports_dir() / f"sub_{name}"
        for f in target.glob("*"):
            f.unlink()
        if target.exists():
            target.rmdir()


def test_land_export_bytes_generated_name_lands_in_exports():
    job = uuid.uuid4().hex[:12]
    out = land_export_bytes(
        _dest(""), b"[]", job_id=job, sync_mode="full_refresh_overwrite", export_name="export.json"
    )
    try:
        assert Path(out["path"]) == exports_dir() / f"export_{job}.json"
    finally:
        Path(out["path"]).unlink()


def test_exports_root_is_configurable(monkeypatch, tmp_path):
    custom = tmp_path / "ops-exports"
    monkeypatch.setenv("DATAFLOW_EXPORTS_DIR", str(custom))
    assert exports_dir() == custom.resolve()
    assert resolve_export_path("orders.csv", default_ext="csv") == custom.resolve() / "orders.csv"
    # Confinement follows the configured root, not the default folder.
    with pytest.raises(ExportPathError):
        resolve_export_path("../escape.csv", default_ext="csv")
    with pytest.raises(ExportPathError):
        resolve_export_path(str(tmp_path / "elsewhere" / "x.csv"), default_ext="csv")


def test_exports_root_defaults_to_api_exports(monkeypatch):
    monkeypatch.delenv("DATAFLOW_EXPORTS_DIR", raising=False)
    monkeypatch.delenv("DATAWRAP_EXPORTS_DIR", raising=False)
    assert exports_dir().name == "exports"
    assert exports_dir().parent.name == "api"
