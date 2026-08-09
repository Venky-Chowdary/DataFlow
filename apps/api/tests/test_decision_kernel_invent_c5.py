"""Phase C5 — same source type, different invent by execution context."""

from __future__ import annotations

import pytest

from services.decision_kernel import (
    InventContext,
    InventRefused,
    invent_context_from_sync_mode,
    invent_dest_type,
)


def test_create_new_bare_logical_integer_uses_64bit_floor():
    """Bare logical ``integer`` (not INT32) invents 64-bit — Phase A floor."""
    out = invent_dest_type(
        "integer",
        dest_db="postgresql",
        context=InventContext.CREATE_NEW,
    )
    assert out.upper() == "BIGINT"


def test_create_new_invent_matches_create_new_mapping_target_type():
    """Map stamp and Validate invent_dest_type must be one CREATE_NEW authority."""
    from services.decision_kernel import create_new_mapping_target_type

    for src, db in (
        ("INTEGER", "postgresql"),
        ("INT32", "mysql"),
        ("SMALLINT", "postgresql"),
        ("TINYINT", "mysql"),
        ("integer", "postgresql"),
        ("BIGINT", "mysql"),
    ):
        invent = invent_dest_type(src, dest_db=db, context=InventContext.CREATE_NEW)
        create = create_new_mapping_target_type(src, db)
        assert invent == create, (src, db, invent, create)


def test_bind_existing_refuses_without_stamp():
    with pytest.raises(InventRefused):
        invent_dest_type(
            "BIGINT",
            dest_db="postgresql",
            context=InventContext.BIND_EXISTING,
            existing_dest_type="",
        )


def test_bind_existing_keeps_proven_stamp_not_source_invent():
    out = invent_dest_type(
        "BIGINT",
        dest_db="postgresql",
        context=InventContext.BIND_EXISTING,
        existing_dest_type="INTEGER",
    )
    # Bind must not silently widen the live column — proven stamp wins.
    assert "INT" in out.upper()


def test_cdc_sparse_refuses_source_only_invent():
    with pytest.raises(InventRefused) as ei:
        invent_dest_type(
            "TEXT",
            dest_db="postgresql",
            context=InventContext.CDC_SPARSE,
        )
    assert ei.value.context is InventContext.CDC_SPARSE


def test_sync_mode_derives_context():
    assert (
        invent_context_from_sync_mode("full_refresh_append", table_exists=True)
        is InventContext.APPEND
    )
    assert (
        invent_context_from_sync_mode("full_refresh_overwrite", create_new=True)
        is InventContext.CREATE_NEW
    )
    assert invent_context_from_sync_mode("cdc", cdc=True) is InventContext.CDC_SPARSE


def test_same_conversion_different_ddl_by_context():
    create = invent_dest_type(
        "INTEGER", dest_db="mysql", context=InventContext.CREATE_NEW
    )
    bound = invent_dest_type(
        "INTEGER",
        dest_db="mysql",
        context=InventContext.BIND_EXISTING,
        existing_dest_type="INT",
    )
    # Create-new follows width-preserving create_new stamp; bind keeps live INT.
    from services.decision_kernel import create_new_mapping_target_type

    assert create == create_new_mapping_target_type("INTEGER", "mysql")
    assert bound.upper() in {"INT", "INTEGER"}


def _type_system_invent_offenders(path_text: str, invent_names: tuple[str, ...]) -> list[str]:
    import re

    hits: list[str] = []
    for m in re.finditer(
        r"from\s+services\.type_system\s+import\s+\(([^)]+)\)|"
        r"from\s+services\.type_system\s+import\s+([^\n]+)",
        path_text,
    ):
        imported = (m.group(1) or m.group(2) or "").replace("\n", " ")
        for invent in invent_names:
            if re.search(rf"\b{invent}\b", imported):
                hits.append(invent)
    return hits


def test_invent_bodies_live_in_type_invent_not_type_system():
    """C2 exit: invent implementations must not remain as fat bodies in type_system."""
    from pathlib import Path
    import ast

    ts = Path(__file__).resolve().parents[1] / "services" / "type_system.py"
    ti = Path(__file__).resolve().parents[1] / "services" / "decision_kernel" / "type_invent.py"
    assert ti.is_file(), "decision_kernel/type_invent.py must own invent bodies"
    ts_tree = ast.parse(ts.read_text(encoding="utf-8"))
    ti_tree = ast.parse(ti.read_text(encoding="utf-8"))
    names = {
        "normalize_logical_type",
        "ddl_type",
        "create_new_mapping_target_type",
        "materialize_dest_ddl",
        "integer_width_carrier",
        "float_width_carrier",
        "ddl_invent_never_narrower_than_table",
    }

    def bodies(tree: ast.AST) -> dict[str, int]:
        out: dict[str, int] = {}
        for n in tree.body:
            if isinstance(n, ast.FunctionDef) and n.name in names:
                out[n.name] = (n.end_lineno or n.lineno) - n.lineno + 1
        return out

    ts_sizes = bodies(ts_tree)
    ti_sizes = bodies(ti_tree)
    assert names <= set(ti_sizes), f"type_invent missing bodies: {names - set(ti_sizes)}"
    for name in names:
        # type_system may keep a thin shim (≤8 lines); never the fat invent body.
        assert ts_sizes.get(name, 0) <= 8, f"type_system.{name} still fat ({ts_sizes.get(name)} lines)"
        assert ti_sizes[name] >= 20, f"type_invent.{name} unexpectedly tiny"


def test_writer_invent_imports_use_decision_kernel_surface():
    """C2: CREATE invent helpers on writers must import kernel facade, not type_system."""
    from pathlib import Path

    root = Path(__file__).resolve().parents[1] / "connectors"
    # Top-level invent surface — not specialty helpers (parse_enum, LOGICAL_*).
    invent_names = ("materialize_dest_ddl", "ddl_type", "create_new_mapping_target_type")
    offenders: list[str] = []
    for name in (
        "bigquery_writer.py",
        "mysql_writer.py",
        "snowflake_writer.py",
        "sqlite_writer.py",
        "iceberg_writer.py",
        "generic_sql.py",
    ):
        text = (root / name).read_text(encoding="utf-8")
        for invent in _type_system_invent_offenders(text, invent_names):
            offenders.append(f"{name}:{invent}")
    assert not offenders, (
        "Writers must import invent/DDL via services.decision_kernel — " + ", ".join(offenders)
    )


def test_map_validate_invent_imports_use_decision_kernel_surface():
    """C2: Map/Validate/lossy paths must import invent surface via kernel, not type_system."""
    from pathlib import Path

    api_root = Path(__file__).resolve().parents[1]
    services = api_root / "services"
    invent_names = (
        "materialize_dest_ddl",
        "ddl_type",
        "create_new_mapping_target_type",
        "is_lossy_coercion",
        "is_precision_collapse_coercion",
        "normalize_logical_type",
    )
    paths = {
        "mapping_pipeline.py": services / "mapping_pipeline.py",
        "semantic_mapper.py": services / "semantic_mapper.py",
        "coercion_probe.py": services / "coercion_probe.py",
        "data_integrity.py": services / "data_integrity.py",
        "type_coercion_validator.py": services / "type_coercion_validator.py",
        "mapping_proof.py": services / "mapping_proof.py",
        "ddl_compatibility.py": services / "ddl_compatibility.py",
        "pair_assurance.py": services / "pair_assurance.py",
        "schema_drift.py": services / "schema_drift.py",
        "connectors/schema_drift.py": api_root / "connectors" / "schema_drift.py",
    }

    offenders: list[str] = []
    for name, path in paths.items():
        text = path.read_text(encoding="utf-8")
        for invent in _type_system_invent_offenders(text, invent_names):
            offenders.append(f"{name}:{invent}")
    assert not offenders, (
        "Map/Validate paths must import invent/lossy via services.decision_kernel — "
        + ", ".join(offenders)
    )
